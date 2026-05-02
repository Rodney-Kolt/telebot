import os
import json
import asyncio
import logging
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from pocketoptionapi_async import AsyncPocketOptionClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("No BOT_TOKEN environment variable set")

CHAT_ID = os.environ.get("CHAT_ID")
IS_RENDER = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

# SSID: full 42["auth",{...}] string from browser DevTools
# Can be updated at runtime by SessionManager when a fresh one is provided
SSID = os.environ.get("SSID", "")
if not SSID:
    raise ValueError("SSID environment variable not set")

PO_DEMO = int(os.environ.get("PO_DEMO", "1"))  # 1 = demo, 0 = real

TRADE_ASSET  = "EURUSD_otc"
CANDLE_PERIOD = 60   # seconds
CANDLE_COUNT  = 10   # candles to fetch per poll

# ---------------------------------------------------------------------------
# Global signal state
# ---------------------------------------------------------------------------
latest_signal: dict = {
    "direction": None,
    "price":     None,
    "reason":    None,
    "timestamp": None,
}

# Recent error log (shown by /logs command)
recent_errors: list[str] = []
MAX_ERRORS = 20


def log_error(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    logger.error(msg)
    recent_errors.append(entry)
    if len(recent_errors) > MAX_ERRORS:
        recent_errors.pop(0)


# ---------------------------------------------------------------------------
# SessionManager
# ---------------------------------------------------------------------------
# Error substrings that indicate the SSID is dead and needs manual refresh
_AUTH_ERROR_MARKERS = (
    "authentication timeout",
    "websocket connection closed",
    "unauthorized",
    "token expired",
    "connect() returned false",
    "failed to connect",
)

PREEMPTIVE_REFRESH_HOURS = 23   # reconnect before the 24-h session boundary
PING_INTERVAL_SECONDS    = 20   # keep-alive ping cadence


class SessionManager:
    """
    Owns the AsyncPocketOptionClient lifecycle.

    Responsibilities
    ----------------
    1. Pre-emptive refresh  – reconnects every 23 h via a scheduled task.
    2. Keep-alive pings     – sends a ping every 20 s to prevent idle drops.
    3. On-error reconnect   – detects auth/WS errors and reconnects
                              automatically; notifies Telegram when the SSID
                              itself needs a manual update.
    """

    def __init__(self, ssid: str, is_demo: bool = True) -> None:
        self.ssid:        str  = ssid
        self.is_demo:     bool = is_demo
        self.client:      "AsyncPocketOptionClient | None" = None
        self.connected_at: "datetime | None" = None

        # Background task handles
        self._ping_task:    "asyncio.Task | None" = None
        self._refresh_task: "asyncio.Task | None" = None

        # Reconnect state
        self._reconnecting = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    @property
    def is_connected(self) -> bool:
        return self.client is not None and self.client.is_connected

    async def start(self) -> bool:
        """Create client, connect, and launch background tasks."""
        async with self._lock:
            return await self._connect()

    async def stop(self) -> None:
        """Cancel background tasks and disconnect cleanly."""
        self._cancel_tasks()
        if self.client:
            try:
                await self.client.disconnect()
            except Exception as exc:
                logger.warning(f"SessionManager.stop disconnect error: {exc}")
        self.client = None
        self.connected_at = None
        logger.info("SessionManager stopped.")

    async def get_candles(self, **kwargs):
        """Proxy to client.get_candles; triggers reconnect on failure."""
        if not self.is_connected:
            await self._handle_error("get_candles called while disconnected")
            return []
        try:
            return await self.client.get_candles(**kwargs)
        except Exception as exc:
            await self._handle_error(f"get_candles error: {exc}")
            return []

    async def notify_error(self, raw_error: str) -> None:
        """
        Called by the polling loop when it catches an exception.
        Decides whether to reconnect or escalate to manual-refresh alert.
        """
        lower = raw_error.lower()
        if any(m in lower for m in _AUTH_ERROR_MARKERS):
            await self._handle_error(raw_error)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _connect(self) -> bool:
        """(Re)create client and connect. Must be called under self._lock."""
        # Tear down any existing client first
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

        try:
            self.client = AsyncPocketOptionClient(
                ssid=self.ssid,
                is_demo=self.is_demo,
            )
            logger.info("SessionManager: connecting to Pocket Option...")
            connected = await self.client.connect()
        except Exception as exc:
            log_error(f"SessionManager connect exception: {exc}")
            self.client = None
            return False

        if not connected:
            log_error("SessionManager: connect() returned False — SSID may be expired")
            self.client = None
            return False

        self.connected_at = datetime.now()
        logger.info(f"SessionManager: connected at {self.connected_at.strftime('%H:%M:%S')}")

        # (Re)launch background tasks
        self._cancel_tasks()
        self._ping_task    = asyncio.create_task(self._ping_loop(),    name="po_ping")
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="po_refresh")
        return True

    def _cancel_tasks(self) -> None:
        for task in (self._ping_task, self._refresh_task):
            if task and not task.done():
                task.cancel()
        self._ping_task = self._refresh_task = None

    # ── Background task 1: keep-alive pings ──────────────────────────── #

    async def _ping_loop(self) -> None:
        """Send a lightweight ping every PING_INTERVAL_SECONDS."""
        logger.info(f"Ping loop started (interval={PING_INTERVAL_SECONDS}s)")
        while True:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            if not self.is_connected:
                logger.warning("Ping loop: client disconnected, stopping ping loop")
                return
            try:
                # pocketoptionapi_async exposes get_balance as a lightweight
                # round-trip; fall back to a no-op if unavailable.
                if hasattr(self.client, "get_balance"):
                    await self.client.get_balance()
                logger.debug("Ping sent")
            except Exception as exc:
                await self._handle_error(f"Ping failed: {exc}")
                return  # _handle_error will restart the loop via _connect

    # ── Background task 2: pre-emptive 23-hour refresh ───────────────── #

    async def _refresh_loop(self) -> None:
        """Reconnect every PREEMPTIVE_REFRESH_HOURS to beat session expiry."""
        interval = PREEMPTIVE_REFRESH_HOURS * 3600
        logger.info(f"Refresh loop started (interval={PREEMPTIVE_REFRESH_HOURS}h)")
        await asyncio.sleep(interval)
        logger.info("SessionManager: pre-emptive session refresh triggered")
        async with self._lock:
            await self._connect()

    # ── Error handler ─────────────────────────────────────────────────── #

    async def _handle_error(self, error_msg: str) -> None:
        """
        Attempt reconnect with exponential back-off (up to 3 tries).
        If all retries fail, send a Telegram alert asking for a new SSID.
        """
        if self._reconnecting:
            return  # already in progress
        self._reconnecting = True
        log_error(f"SessionManager error: {error_msg}")

        delays = [5, 30, 120]  # seconds between retries
        for attempt, delay in enumerate(delays, start=1):
            logger.info(f"Reconnect attempt {attempt}/{len(delays)} in {delay}s...")
            await asyncio.sleep(delay)
            async with self._lock:
                success = await self._connect()
            if success:
                logger.info(f"Reconnect succeeded on attempt {attempt}")
                self._reconnecting = False
                await _telegram_notify(
                    "✅ Pocket Option reconnected successfully after a connection error."
                )
                return

        # All retries exhausted
        self._reconnecting = False
        log_error("All reconnect attempts failed — SSID needs manual refresh")
        await _telegram_notify(
            "⚠️ Pocket Option connection lost and could not reconnect.\n\n"
            "Your SSID has likely expired. To fix:\n"
            "1. Open pocketoption.com in your browser\n"
            "2. Press F12 → Network tab → filter WS\n"
            '3. Find the message starting with 42["auth",\n'
            "4. Copy the full message\n"
            "5. Update the SSID env var on Render and redeploy"
        )


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()


async def _telegram_notify(text: str) -> None:
    """Send a plain-text message to CHAT_ID (best-effort)."""
    if not CHAT_ID:
        return
    try:
        await telegram_app.bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as exc:
        logger.error(f"Telegram notify error: {exc}")


# ---------------------------------------------------------------------------
# Signal logic — 3-candle momentum
# ---------------------------------------------------------------------------
def compute_signal(candles: list) -> tuple[str, str]:
    if not candles or len(candles) < 4:
        return "WAIT ⏸", "Not enough candle data yet"
    try:
        closes = [float(c.close) for c in candles[-4:]]
        if closes[-1] > closes[-2] > closes[-3] > closes[-4]:
            return "CALL ✅", "3 consecutive rising closes (bullish momentum)"
        if closes[-1] < closes[-2] < closes[-3] < closes[-4]:
            return "PUT ❌", "3 consecutive falling closes (bearish momentum)"
        return "WAIT ⏸", "No clear momentum"
    except Exception as exc:
        logger.warning(f"compute_signal error: {exc}")
        return "WAIT ⏸", f"Signal error: {exc}"


# ---------------------------------------------------------------------------
# Global SessionManager instance (created in lifespan)
# ---------------------------------------------------------------------------
session_manager: "SessionManager | None" = None


# ---------------------------------------------------------------------------
# Background polling loop
# ---------------------------------------------------------------------------
async def pocket_option_loop() -> None:
    """
    Polls Pocket Option for candles every CANDLE_PERIOD seconds.
    Delegates all connection/reconnection logic to SessionManager.
    """
    global latest_signal

    logger.info("Pocket Option polling loop started.")

    # Wait for initial connection (with timeout)
    for _ in range(30):
        if session_manager and session_manager.is_connected:
            break
        await asyncio.sleep(2)
    else:
        log_error("Polling loop: timed out waiting for initial connection")

    while True:
        try:
            if not (session_manager and session_manager.is_connected):
                logger.warning("Polling loop: not connected, waiting...")
                await asyncio.sleep(10)
                continue

            candles = await session_manager.get_candles(
                asset=TRADE_ASSET,
                timeframe=CANDLE_PERIOD,
                count=CANDLE_COUNT,
            )
            logger.info(f"Received {len(candles) if candles else 0} candles")

            direction, reason = compute_signal(candles)

            price = None
            if candles:
                try:
                    price = float(candles[-1].close)
                except (AttributeError, TypeError, ValueError):
                    pass

            latest_signal = {
                "direction": direction,
                "price":     price,
                "reason":    reason,
                "timestamp": str(datetime.now()),
            }
            logger.info(f"Signal: {direction} | {reason} | price={price}")

            # Broadcast actionable signals
            if CHAT_ID and direction != "WAIT ⏸":
                price_str = f"{price:.5f}" if price else "N/A"
                await _telegram_notify(
                    f"📡 Live Signal\n"
                    f"Asset: EURUSD OTC\n"
                    f"Direction: {direction}\n"
                    f"Price: {price_str}\n"
                    f"Reason: {reason}\n"
                    f"Time: {datetime.now().strftime('%H:%M:%S')}"
                )

        except Exception as exc:
            err = str(exc)
            log_error(f"Polling loop error: {err}")
            if session_manager:
                await session_manager.notify_error(err)

        await asyncio.sleep(CANDLE_PERIOD)


# ---------------------------------------------------------------------------
# Telegram command handlers
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 Real-time Signal Bot is running!\n\n"
        "Commands:\n"
        "/signal  — current signal\n"
        "/status  — connection status\n"
        "/debug   — diagnostics\n"
        "/logs    — recent errors\n"
        "/reconnect — force reconnect"
    )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sig = latest_signal
    po_ok = session_manager is not None and session_manager.is_connected
    if sig["direction"] is None:
        await update.message.reply_text(
            f"No signal yet.\n"
            f"PO connected: {'yes' if po_ok else 'no'}\n"
            f"Waiting for first candle poll (up to 60s after startup).\n"
            f"Use /debug or /logs if this persists."
        )
        return
    price_str = f"{sig['price']:.5f}" if sig["price"] else "N/A"
    await update.message.reply_text(
        f"Current Signal\n"
        f"Asset: EURUSD OTC\n"
        f"Direction: {sig['direction']}\n"
        f"Price: {price_str}\n"
        f"Reason: {sig['reason']}\n"
        f"Updated: {sig['timestamp']}"
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    po_ok = session_manager is not None and session_manager.is_connected
    connected_since = (
        session_manager.connected_at.strftime("%H:%M:%S")
        if session_manager and session_manager.connected_at else "never"
    )
    sig = latest_signal
    await update.message.reply_text(
        f"Bot Status\n"
        f"Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"PO connected: {'yes' if po_ok else 'no'}\n"
        f"Connected since: {connected_since}\n"
        f"Last signal: {sig['direction'] or 'none'} @ {sig['timestamp'] or 'never'}"
    )


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    po_ok = session_manager is not None and session_manager.is_connected
    ssid_preview = f"{SSID[:25]}...{SSID[-8:]}" if len(SSID) > 33 else SSID

    # Next scheduled refresh time
    refresh_in = "unknown"
    if session_manager and session_manager.connected_at:
        next_refresh = session_manager.connected_at + timedelta(hours=PREEMPTIVE_REFRESH_HOURS)
        delta = next_refresh - datetime.now()
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m = rem // 60
        refresh_in = f"{h}h {m}m" if delta.total_seconds() > 0 else "imminent"

    await update.message.reply_text(
        f"Debug Info\n"
        f"IS_RENDER: {IS_RENDER}\n"
        f"RENDER_URL: {RENDER_EXTERNAL_URL or 'not set'}\n"
        f"SSID set: {'yes' if SSID else 'no'}\n"
        f"SSID preview: {ssid_preview}\n"
        f"PO connected: {'yes' if po_ok else 'no'}\n"
        f"Next pre-emptive refresh: {refresh_in}\n"
        f"Ping interval: {PING_INTERVAL_SECONDS}s\n"
        f"Signal: {latest_signal['direction'] or 'none'}\n"
        f"Last update: {latest_signal['timestamp'] or 'never'}"
    )


async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_errors:
        await update.message.reply_text("No recent errors.")
        return
    await update.message.reply_text("Recent Errors:\n\n" + "\n".join(recent_errors[-10:]))


async def reconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Force an immediate reconnect (useful after manually updating SSID on Render)."""
    await update.message.reply_text("Forcing reconnect to Pocket Option...")
    if session_manager:
        async with session_manager._lock:
            success = await session_manager._connect()
        if success:
            await update.message.reply_text("Reconnected successfully.")
        else:
            await update.message.reply_text(
                "Reconnect failed. Check /logs for details.\n"
                "If SSID is expired, update it on Render and redeploy."
            )
    else:
        await update.message.reply_text("Session manager not initialised yet.")


telegram_app.add_handler(CommandHandler("start",      start))
telegram_app.add_handler(CommandHandler("signal",     signal_command))
telegram_app.add_handler(CommandHandler("status",     status_command))
telegram_app.add_handler(CommandHandler("debug",      debug_command))
telegram_app.add_handler(CommandHandler("logs",       logs_command))
telegram_app.add_handler(CommandHandler("reconnect",  reconnect_command))


# ---------------------------------------------------------------------------
# Telegram webhook registration
# ---------------------------------------------------------------------------
async def reset_and_set_webhook() -> None:
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL not set — cannot register webhook.")
        return
    webhook_url = f"{RENDER_EXTERNAL_URL}/telegram"
    base = f"https://api.telegram.org/bot{TOKEN}"
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{base}/deleteWebhook", params={"drop_pending_updates": "true"})
        logger.info(f"deleteWebhook: {r.json()}")
        r = await client.post(f"{base}/setWebhook", params={"url": webhook_url})
        logger.info(f"setWebhook: {r.json()}")


# ---------------------------------------------------------------------------
# Starlette routes
# ---------------------------------------------------------------------------
async def health(request: Request) -> JSONResponse:
    return JSONResponse({
        "status":        "healthy",
        "timestamp":     str(datetime.now()),
        "po_connected":  session_manager is not None and session_manager.is_connected,
        "latest_signal": latest_signal,
    })


async def telegram_webhook(request: Request) -> JSONResponse:
    data = await request.json()
    try:
        await telegram_app.process_update(Update.de_json(data, telegram_app.bot))
    except Exception as exc:
        logger.error(f"Webhook processing error: {exc}", exc_info=True)
    return JSONResponse({"status": "ok"})


routes = [
    Route("/health",   health,           methods=["GET"]),
    Route("/telegram", telegram_webhook, methods=["POST"]),
]


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: Starlette):
    global session_manager

    logger.info("Starting up...")
    await telegram_app.initialize()
    await telegram_app.start()
    await reset_and_set_webhook()

    # Create and start the session manager
    session_manager = SessionManager(ssid=SSID, is_demo=bool(PO_DEMO))
    connected = await session_manager.start()
    if connected:
        logger.info("Initial PO connection established.")
    else:
        log_error("Initial PO connection failed — will retry in polling loop")

    # Start the candle-polling loop
    poll_task = asyncio.create_task(pocket_option_loop(), name="po_poll")

    yield  # ── server is running ──────────────────────────────────────────

    poll_task.cancel()
    try:
        await poll_task
    except (asyncio.CancelledError, Exception):
        pass

    await session_manager.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if IS_RENDER:
    web_app = Starlette(routes=routes, lifespan=lifespan)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"IS_RENDER=true — starting webhook server on port {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
else:
    logger.info("Local mode — starting polling...")
    telegram_app.run_polling()
