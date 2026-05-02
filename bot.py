import os
import json
import asyncio
import logging
import math
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, ContextTypes,
)
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
CANDLE_PERIOD = 60   # seconds for the auto-signal loop
CANDLE_COUNT  = 50   # candles to fetch (enough for RSI-14 + EMA-20)

# ---------------------------------------------------------------------------
# Asset catalogue
# ---------------------------------------------------------------------------
# Maps display label → Pocket Option asset code
ASSETS: dict[str, str] = {
    "EUR/USD (OTC)": "EURUSD_otc",
    "AUD/USD (OTC)": "AUDUSD_otc",
    "USD/JPY (OTC)": "USDJPY_otc",
    "GBP/USD (OTC)": "GBPUSD_otc",
    "USD/CAD (OTC)": "USDCAD_otc",
    "EUR/JPY (OTC)": "EURJPY_otc",
}

# Maps display label → candle timeframe in seconds
TIMEFRAMES: dict[str, int] = {
    "5 seconds":  5,
    "1 minute":   60,
    "2 minutes":  120,
    "5 minutes":  300,
}

# ConversationHandler states
CHOOSE_ASSET, CHOOSE_TIME = range(2)

# ---------------------------------------------------------------------------
# Per-user settings  {user_id: {"asset": label, "timeframe": label, "auto": bool}}
# ---------------------------------------------------------------------------
user_settings: dict[int, dict] = {}

DEFAULT_SETTINGS = {
    "asset":     "EUR/USD (OTC)",
    "timeframe": "1 minute",
    "auto":      True,          # receive auto-broadcast signals
}

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
            "Your SSID has likely expired. To fix without redeploying:\n"
            "1. Open pocketoption.com and log in\n"
            "2. Press F12 → Network tab → filter WS\n"
            '3. Find the message starting with 42["auth",\n'
            "4. Copy the full string\n"
            "5. Send: /setssid <paste the string here>\n\n"
            "The bot will reconnect immediately."
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
# Technical analysis helpers (pure Python, no extra libraries)
# ---------------------------------------------------------------------------

def _closes(candles: list) -> list[float]:
    return [float(c.close) for c in candles]


def _ema(values: list[float], period: int) -> list[float]:
    """Exponential moving average."""
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _rsi(values: list[float], period: int = 14) -> float | None:
    """Wilder RSI. Returns the most recent RSI value or None."""
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    # Wilder smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _market_condition(candles: list) -> str:
    """Classify market as Trending, Ranging, or Volatile."""
    if len(candles) < 10:
        return "Unknown"
    closes = _closes(candles[-20:])
    highs  = [float(c.high) for c in candles[-20:]] if hasattr(candles[-1], "high") else closes
    lows   = [float(c.low)  for c in candles[-20:]] if hasattr(candles[-1], "low")  else closes
    atr    = sum(h - l for h, l in zip(highs, lows)) / len(highs)
    price  = closes[-1]
    atr_pct = (atr / price) * 100 if price else 0

    ema_fast = _ema(closes, 5)
    ema_slow = _ema(closes, 20)
    if ema_fast and ema_slow:
        trend_strength = abs(ema_fast[-1] - ema_slow[-1]) / price * 100 if price else 0
        if trend_strength > 0.05:
            return "Trending"
    if atr_pct > 0.1:
        return "Volatile"
    return "Ranging"


def compute_signal_advanced(
    candles: list,
    timeframe_seconds: int = 60,
) -> dict:
    """
    Returns a dict with keys:
      direction   – "HIGHER" | "LOWER" | "WAIT"
      confidence  – int 0-100
      rsi         – float | None
      reason      – str
      market      – str
    """
    result = {
        "direction":  "WAIT",
        "confidence": 0,
        "rsi":        None,
        "reason":     "Not enough data",
        "market":     "Unknown",
    }

    if not candles or len(candles) < 20:
        return result

    closes = _closes(candles)
    rsi    = _rsi(closes, 14)
    ema9   = _ema(closes, 9)
    ema21  = _ema(closes, 21)
    market = _market_condition(candles)

    result["rsi"]    = rsi
    result["market"] = market

    if rsi is None or not ema9 or not ema21:
        result["reason"] = "Insufficient data for indicators"
        return result

    price     = closes[-1]
    ema9_val  = ema9[-1]
    ema21_val = ema21[-1]

    # ── Direction logic ──────────────────────────────────────────────────
    bullish_signals = 0
    bearish_signals = 0

    # RSI
    if rsi < 35:
        bullish_signals += 2   # oversold → likely bounce up
    elif rsi < 45:
        bullish_signals += 1
    elif rsi > 65:
        bearish_signals += 2   # overbought → likely drop
    elif rsi > 55:
        bearish_signals += 1

    # EMA crossover
    if ema9_val > ema21_val:
        bullish_signals += 2
    elif ema9_val < ema21_val:
        bearish_signals += 2

    # Price vs EMA9
    if price > ema9_val:
        bullish_signals += 1
    else:
        bearish_signals += 1

    # 3-candle momentum (existing logic)
    if len(closes) >= 4:
        c = closes[-4:]
        if c[-1] > c[-2] > c[-3] > c[-4]:
            bullish_signals += 1
        elif c[-1] < c[-2] < c[-3] < c[-4]:
            bearish_signals += 1

    total = bullish_signals + bearish_signals
    if total == 0:
        return result

    if bullish_signals > bearish_signals:
        direction = "HIGHER"
        raw_conf  = bullish_signals / total
    elif bearish_signals > bullish_signals:
        direction = "LOWER"
        raw_conf  = bearish_signals / total
    else:
        result["direction"] = "WAIT"
        result["reason"]    = "Mixed signals — no clear edge"
        result["confidence"] = 50
        return result

    # ── Confidence score ─────────────────────────────────────────────────
    # Base: signal ratio (50–80%)
    base_conf = 50 + raw_conf * 30

    # RSI extremity bonus (up to +15%)
    if direction == "HIGHER" and rsi is not None:
        rsi_bonus = max(0, (40 - rsi) / 40) * 15   # RSI=0 → +15, RSI=40 → 0
    elif direction == "LOWER" and rsi is not None:
        rsi_bonus = max(0, (rsi - 60) / 40) * 15   # RSI=100 → +15, RSI=60 → 0
    else:
        rsi_bonus = 0

    # Timeframe penalty: very short timeframes are noisier
    tf_penalty = 0
    if timeframe_seconds <= 5:
        tf_penalty = 8
    elif timeframe_seconds <= 15:
        tf_penalty = 4

    confidence = min(97, max(51, int(base_conf + rsi_bonus - tf_penalty)))

    # ── Reason string ────────────────────────────────────────────────────
    ema_desc = "EMA9 > EMA21" if ema9_val > ema21_val else "EMA9 < EMA21"
    reason = (
        f"RSI={rsi:.1f}, {ema_desc}, "
        f"{'bullish' if direction == 'HIGHER' else 'bearish'} momentum"
    )

    result.update({
        "direction":  direction,
        "confidence": confidence,
        "reason":     reason,
    })
    return result


# Legacy wrapper used by the auto-broadcast loop
def compute_signal(candles: list) -> tuple[str, str]:
    r = compute_signal_advanced(candles)
    if r["direction"] == "HIGHER":
        return "CALL ✅", r["reason"]
    if r["direction"] == "LOWER":
        return "PUT ❌", r["reason"]
    return "WAIT ⏸", r["reason"]


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
# Per-user helpers
# ---------------------------------------------------------------------------

def _get_settings(user_id: int) -> dict:
    if user_id not in user_settings:
        user_settings[user_id] = dict(DEFAULT_SETTINGS)
    return user_settings[user_id]


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------

def _main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    settings = _get_settings(user_id)
    auto_label = "\U0001f514 Auto-signals: ON" if settings["auto"] else "\U0001f515 Auto-signals: OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Get Signal",   callback_data="menu:signal")],
        [InlineKeyboardButton("\u2139\ufe0f How it works", callback_data="menu:howto")],
        [InlineKeyboardButton("\u2699\ufe0f Settings",   callback_data="menu:settings")],
        [InlineKeyboardButton(auto_label,               callback_data="menu:toggle_auto")],
    ])


def _asset_keyboard() -> InlineKeyboardMarkup:
    rows = []
    items = list(ASSETS.keys())
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(items[i], callback_data=f"asset:{items[i]}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(items[i + 1], callback_data=f"asset:{items[i + 1]}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def _timeframe_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(label, callback_data=f"tf:{label}")]
            for label in TIMEFRAMES]
    rows.append([InlineKeyboardButton("\U0001f519 Back", callback_data="asset:back")])
    return InlineKeyboardMarkup(rows)


def _settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    s = _get_settings(user_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Asset: {s['asset']}",         callback_data="settings:asset")],
        [InlineKeyboardButton(f"Timeframe: {s['timeframe']}", callback_data="settings:tf")],
        [InlineKeyboardButton("\U0001f519 Back",              callback_data="menu:back")],
    ])


# ---------------------------------------------------------------------------
# /start  — main menu
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user  = update.effective_user
    uid   = user.id
    name  = user.first_name or "Trader"
    po_ok = session_manager is not None and session_manager.is_connected
    conn_str = "\U0001f7e2 Live" if po_ok else "\U0001f534 Offline"
    text  = (
        f"\U0001f44b Welcome, {name}!\n\n"
        "\U0001f4e1 OTC Signal Bot\n"
        f"Connection: {conn_str}\n\n"
        "Choose an option below:"
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=_main_menu_keyboard(uid))
    else:
        await update.callback_query.edit_message_text(text, reply_markup=_main_menu_keyboard(uid))


# ---------------------------------------------------------------------------
# Inline button router  (ConversationHandler states: CHOOSE_ASSET, CHOOSE_TIME)
# ---------------------------------------------------------------------------
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query    = update.callback_query
    await query.answer()
    data     = query.data
    user_id  = query.from_user.id
    settings = _get_settings(user_id)

    # ── Main menu ────────────────────────────────────────────────────────
    if data == "menu:back":
        await start(update, context)
        return ConversationHandler.END

    if data == "menu:signal":
        await query.edit_message_text(
            "\U0001f4ca Select an asset:",
            reply_markup=_asset_keyboard(),
        )
        return CHOOSE_ASSET

    if data == "menu:howto":
        await query.edit_message_text(
            "\u2139\ufe0f How the bot works\n\n"
            "The bot connects to Pocket Option via WebSocket and fetches "
            "real-time OHLC candle data for the selected asset.\n\n"
            "Signals are generated using:\n"
            "\u2022 RSI (14) \u2014 identifies overbought/oversold conditions\n"
            "\u2022 EMA (9) & EMA (21) \u2014 trend direction via crossover\n"
            "\u2022 3-candle momentum \u2014 confirms short-term price direction\n\n"
            "Confidence % reflects how strongly the indicators agree.\n"
            "Higher RSI extremity + EMA alignment = higher confidence.\n\n"
            "\u26a0\ufe0f Signals are for informational purposes only.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:settings":
        await query.edit_message_text(
            "\u2699\ufe0f Settings\n\nYour current defaults:",
            reply_markup=_settings_keyboard(user_id),
        )
        return ConversationHandler.END

    if data == "menu:toggle_auto":
        settings["auto"] = not settings["auto"]
        state = "ON \u2705" if settings["auto"] else "OFF \U0001f515"
        await query.edit_message_text(
            f"Auto-signals turned {state}\n\n"
            f"You will {'now' if settings['auto'] else 'no longer'} receive "
            f"automatic signals every {CANDLE_PERIOD // 60} minute(s).",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    # ── Settings sub-menu ────────────────────────────────────────────────
    if data == "settings:asset":
        context.user_data["settings_mode"] = "asset"
        await query.edit_message_text(
            "\u2699\ufe0f Choose your default asset:",
            reply_markup=_asset_keyboard(),
        )
        return CHOOSE_ASSET

    if data == "settings:tf":
        context.user_data["settings_mode"] = "tf"
        await query.edit_message_text(
            "\u2699\ufe0f Choose your default timeframe:",
            reply_markup=_timeframe_keyboard(),
        )
        return CHOOSE_TIME

    # ── Asset selection ──────────────────────────────────────────────────
    if data.startswith("asset:"):
        asset_label = data[len("asset:"):]

        if asset_label == "back":
            await start(update, context)
            return ConversationHandler.END

        context.user_data["chosen_asset"] = asset_label

        if context.user_data.get("settings_mode") == "asset":
            settings["asset"] = asset_label
            context.user_data.pop("settings_mode", None)
            await query.edit_message_text(
                f"\u2705 Default asset set to: {asset_label}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
                ]),
            )
            return ConversationHandler.END

        await query.edit_message_text(
            f"Asset: {asset_label}\n\nNow select a trading time:",
            reply_markup=_timeframe_keyboard(),
        )
        return CHOOSE_TIME

    # ── Timeframe selection → fetch & display signal ─────────────────────
    if data.startswith("tf:"):
        tf_label = data[len("tf:"):]

        if context.user_data.get("settings_mode") == "tf":
            settings["timeframe"] = tf_label
            context.user_data.pop("settings_mode", None)
            await query.edit_message_text(
                f"\u2705 Default timeframe set to: {tf_label}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
                ]),
            )
            return ConversationHandler.END

        asset_label = context.user_data.get("chosen_asset", settings["asset"])
        tf_seconds  = TIMEFRAMES.get(tf_label, 60)
        asset_code  = ASSETS.get(asset_label, "EURUSD_otc")

        await query.edit_message_text(
            f"\u23f3 Fetching signal for {asset_label} / {tf_label}..."
        )

        result = await _fetch_signal(asset_code, tf_seconds)
        text   = _format_signal(result, asset_label, tf_label)

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f504 New Signal", callback_data=f"tf:{tf_label}")],
                [InlineKeyboardButton("\U0001f4ca Change Asset", callback_data="menu:signal")],
                [InlineKeyboardButton("\U0001f3e0 Main Menu",   callback_data="menu:back")],
            ]),
        )
        context.user_data["chosen_asset"] = asset_label
        return ConversationHandler.END

    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Signal fetching & formatting
# ---------------------------------------------------------------------------

async def _fetch_signal(asset_code: str, tf_seconds: int) -> dict:
    if not (session_manager and session_manager.is_connected):
        return {
            "direction": "WAIT", "confidence": 0,
            "rsi": None, "reason": "Not connected to Pocket Option",
            "market": "Unknown",
        }
    try:
        candles = await session_manager.get_candles(
            asset=asset_code,
            timeframe=tf_seconds,
            count=CANDLE_COUNT,
        )
        return compute_signal_advanced(candles, tf_seconds)
    except Exception as exc:
        log_error(f"_fetch_signal error: {exc}")
        return {
            "direction": "WAIT", "confidence": 0,
            "rsi": None, "reason": f"Error: {exc}",
            "market": "Unknown",
        }


def _format_signal(result: dict, asset_label: str, tf_label: str) -> str:
    direction  = result["direction"]
    confidence = result["confidence"]
    rsi        = result["rsi"]
    reason     = result["reason"]
    market     = result["market"]

    if direction == "HIGHER":
        dir_line = "Signal: HIGHER \u2705"
    elif direction == "LOWER":
        dir_line = "Signal: LOWER \U0001f534"
    else:
        dir_line = "Signal: WAIT \u23f8"

    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    filled  = round(confidence / 20)
    bar     = "\u2588" * filled + "\u2591" * (5 - filled)

    return (
        f"{dir_line}\n"
        f"Asset: {asset_label}\n"
        f"Timeframe: {tf_label}\n"
        f"Reliability: {confidence}%  {bar}\n"
        f"RSI: {rsi_str}\n"
        f"Market: {market}\n"
        f"Reason: {reason}\n"
        f"Time: {__import__('datetime').datetime.now().strftime('%H:%M:%S')}"
    )


# ---------------------------------------------------------------------------
# Remaining command handlers
# ---------------------------------------------------------------------------

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
        f"Last auto-signal: {sig['direction'] or 'none'} @ {sig['timestamp'] or 'never'}"
    )


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    po_ok = session_manager is not None and session_manager.is_connected
    ssid_preview = f"{SSID[:25]}...{SSID[-8:]}" if len(SSID) > 33 else SSID
    refresh_in = "unknown"
    if session_manager and session_manager.connected_at:
        next_refresh = session_manager.connected_at + timedelta(hours=PREEMPTIVE_REFRESH_HOURS)
        delta = next_refresh - datetime.now()
        h, rem = divmod(int(max(delta.total_seconds(), 0)), 3600)
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
        f"Last auto-signal: {latest_signal['direction'] or 'none'}\n"
        f"Last update: {latest_signal['timestamp'] or 'never'}"
    )


async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_errors:
        await update.message.reply_text("No recent errors.")
        return
    await update.message.reply_text("Recent Errors:\n\n" + "\n".join(recent_errors[-10:]))


async def reconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Forcing reconnect to Pocket Option...")
    if session_manager:
        async with session_manager._lock:
            success = await session_manager._connect()
        if success:
            await update.message.reply_text("Reconnected successfully.")
        else:
            await update.message.reply_text(
                "Reconnect failed. Check /logs for details.\n"
                "If SSID is expired, use /setssid <new_ssid> to update it without redeploying."
            )
    else:
        await update.message.reply_text("Session manager not initialised yet.")


async def setssid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Update the SSID at runtime without redeploying.

    Usage:  /setssid 42["auth",{"session":"...","isDemo":1,"uid":123,"platform":1}]

    The new SSID is stored in the SessionManager and used immediately for
    reconnection. It is NOT persisted to disk — if the service restarts you
    will need to update the SSID env var on Render as well.
    """
    global SSID

    if not context.args:
        await update.message.reply_text(
            "Usage: /setssid <full auth string>\n\n"
            'Example:\n/setssid 42["auth",{"session":"abc...","isDemo":1,"uid":123,"platform":1}]\n\n'
            "How to get a fresh SSID:\n"
            "1. Open pocketoption.com and log in\n"
            "2. Press F12 → Network tab → filter WS\n"
            "3. Click the WebSocket connection → Messages tab\n"
            '4. Find the message starting with 42["auth",\n'
            "5. Copy the entire string and paste it after /setssid"
        )
        return

    new_ssid = " ".join(context.args).strip()

    # Basic validation
    if not (new_ssid.startswith('42["auth"') or new_ssid.startswith("42['auth'")):
        await update.message.reply_text(
            'Invalid format. The SSID must start with: 42["auth",\n\n'
            "Copy the full WebSocket auth message from your browser DevTools."
        )
        return

    SSID = new_ssid
    if session_manager:
        session_manager.ssid = new_ssid

    await update.message.reply_text("SSID updated. Reconnecting now...")

    if session_manager:
        async with session_manager._lock:
            success = await session_manager._connect()
        if success:
            await update.message.reply_text(
                "Connected successfully with new SSID.\n\n"
                "Remember to also update the SSID env var on Render so it "
                "survives a service restart."
            )
        else:
            await update.message.reply_text(
                "Reconnect failed — the new SSID may also be invalid or expired.\n"
                "Check /logs for details."
            )
    else:
        await update.message.reply_text("Session manager not initialised yet.")


# ---------------------------------------------------------------------------
# Register handlers
# ---------------------------------------------------------------------------
conv_handler = ConversationHandler(
    entry_points=[
        CommandHandler("start", start),
        CallbackQueryHandler(button_handler),
    ],
    states={
        CHOOSE_ASSET: [CallbackQueryHandler(button_handler)],
        CHOOSE_TIME:  [CallbackQueryHandler(button_handler)],
    },
    fallbacks=[CommandHandler("start", start)],
    per_message=False,
)

telegram_app.add_handler(conv_handler)
telegram_app.add_handler(CommandHandler("status",    status_command))
telegram_app.add_handler(CommandHandler("debug",     debug_command))
telegram_app.add_handler(CommandHandler("logs",      logs_command))
telegram_app.add_handler(CommandHandler("reconnect", reconnect_command))
telegram_app.add_handler(CommandHandler("setssid",   setssid_command))


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
