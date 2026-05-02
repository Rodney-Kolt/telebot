import os
import asyncio
import logging
import httpx
from contextlib import asynccontextmanager
from datetime import datetime
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

# Full SSID string from browser DevTools:
# 42["auth",{"session":"...","isDemo":1,"uid":...,"platform":1}]
SSID = os.environ.get("SSID")
if not SSID:
    raise ValueError("SSID environment variable not set")

TRADE_ASSET = "EURUSD_otc"
CANDLE_PERIOD = 60   # seconds
CANDLE_COUNT  = 10   # how many candles to fetch for signal

# ---------------------------------------------------------------------------
# Global signal state — updated by the background loop
# ---------------------------------------------------------------------------
latest_signal: dict = {
    "direction": None,
    "price": None,
    "reason": None,
    "timestamp": None,
}

# Module-level PocketOption client (created once, reused)
po_client: "AsyncPocketOptionClient | None" = None

# Store recent error logs for /logs command
recent_errors: list = []
MAX_ERRORS = 10


def log_error(msg: str):
    """Store error messages for /logs command"""
    recent_errors.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    if len(recent_errors) > MAX_ERRORS:
        recent_errors.pop(0)


# ---------------------------------------------------------------------------
# Signal logic — 3-candle momentum on closing prices
# ---------------------------------------------------------------------------
def compute_signal(candles: list) -> tuple[str, str]:
    """
    Returns (direction, reason).
    Needs at least 4 candles; uses the last 4 closing prices.
    Candles are Candle objects with .close attribute.
    """
    if not candles or len(candles) < 4:
        return "WAIT ⏸", "Not enough candle data yet"
    try:
        closes = [float(c.close) for c in candles[-4:]]
        if closes[-1] > closes[-2] > closes[-3] > closes[-4]:
            return "CALL ✅", "3 consecutive rising closes (bullish momentum)"
        if closes[-1] < closes[-2] < closes[-3] < closes[-4]:
            return "PUT ❌", "3 consecutive falling closes (bearish momentum)"
        return "WAIT ⏸", "No clear momentum"
    except Exception as e:
        logger.warning(f"compute_signal error: {e}")
        return "WAIT ⏸", f"Signal error: {e}"


# ---------------------------------------------------------------------------
# Background task — connects to Pocket Option and polls for candles
# ---------------------------------------------------------------------------
async def pocket_option_loop():
    """
    Connects to Pocket Option, then polls for candle data every 60 seconds.
    Updates latest_signal on each tick and broadcasts CALL/PUT to Telegram.
    Retries connection every 5 minutes if it fails (e.g. expired SSID).
    """
    global latest_signal, po_client

    logger.info("Pocket Option loop starting...")

    CONNECT_RETRY_INTERVAL = 300  # retry connection every 5 minutes

    while True:
        # ── (Re)initialize client if needed ──────────────────────────────
        if po_client is None:
            try:
                po_client = AsyncPocketOptionClient(ssid=SSID, is_demo=True)
                logger.info("PO client (re)initialized.")
            except Exception as e:
                error_msg = f"Client init error: {e}"
                logger.error(error_msg, exc_info=True)
                log_error(error_msg)
                await asyncio.sleep(CONNECT_RETRY_INTERVAL)
                continue

        # ── Connect ───────────────────────────────────────────────────────
        if not po_client.is_connected:
            try:
                logger.info("Calling po_client.connect()...")
                connected = await po_client.connect()
                logger.info(f"connect() returned: {connected}, is_connected: {po_client.is_connected}")

                if not connected:
                    error_msg = (
                        "connect() returned False — SSID is likely expired or invalid. "
                        "Go to pocketoption.com > DevTools > Network > WS, copy the full "
                        '42["auth",{...}] message and update the SSID env var on Render.'
                    )
                    logger.error(error_msg)
                    log_error("SSID expired/invalid — update SSID on Render")
                    # Notify via Telegram if possible
                    if CHAT_ID:
                        try:
                            await telegram_app.bot.send_message(
                                chat_id=CHAT_ID,
                                text=(
                                    "⚠️ Pocket Option connection failed.\n\n"
                                    "Your SSID is likely expired.\n\n"
                                    "To fix:\n"
                                    "1. Open pocketoption.com in browser\n"
                                    "2. Press F12 → Network tab → filter WS\n"
                                    "3. Find message starting with 42[\"auth\",\n"
                                    "4. Copy the full message\n"
                                    "5. Update SSID on Render dashboard\n"
                                    "6. Redeploy the service"
                                )
                            )
                        except Exception:
                            pass
                    await asyncio.sleep(CONNECT_RETRY_INTERVAL)
                    continue

                logger.info("Pocket Option: connected successfully.")

            except Exception as e:
                error_msg = f"Connect exception: {e}"
                logger.error(error_msg, exc_info=True)
                log_error(error_msg)
                await asyncio.sleep(CONNECT_RETRY_INTERVAL)
                continue

        # ── Poll candles ──────────────────────────────────────────────────
        try:
            logger.info(f"Fetching candles for {TRADE_ASSET}...")
            candles = await po_client.get_candles(
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
                except (AttributeError, TypeError, ValueError) as e:
                    logger.warning(f"Could not extract price: {e}")

            latest_signal = {
                "direction": direction,
                "price": price,
                "reason": reason,
                "timestamp": str(datetime.now()),
            }
            logger.info(f"Signal updated: {direction} | {reason} | price={price}")

            # Broadcast actionable signals to Telegram
            if CHAT_ID and direction != "WAIT ⏸":
                price_str = f"{price:.5f}" if price else "N/A"
                message = (
                    f"📡 *Live Signal*\n"
                    f"Asset: `EURUSD OTC`\n"
                    f"Direction: *{direction}*\n"
                    f"Price: `{price_str}`\n"
                    f"Reason: {reason}\n"
                    f"Time: {datetime.now().strftime('%H:%M:%S')}"
                )
                try:
                    await telegram_app.bot.send_message(
                        chat_id=CHAT_ID, text=message, parse_mode='Markdown'
                    )
                    logger.info(f"Broadcast signal: {direction}")
                except Exception as e:
                    logger.error(f"Telegram send error: {e}")

        except Exception as e:
            error_msg = f"Candle fetch error: {e}"
            logger.error(error_msg, exc_info=True)
            log_error(error_msg)

        await asyncio.sleep(CANDLE_PERIOD)


# ---------------------------------------------------------------------------
# Telegram app
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Real-time Signal Bot is running!*\n\n"
        "Commands:\n"
        "• /signal — current real-time signal\n"
        "• /status — connection and bot status\n"
        "• /debug  — detailed diagnostics\n"
        "• /logs   — recent error logs",
        parse_mode='Markdown'
    )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sig = latest_signal
    logger.info(f"Signal command called. po_client={po_client}, is_connected={po_client.is_connected if po_client else 'N/A'}")
    if sig["direction"] is None:
        po_ok = po_client is not None and po_client.is_connected
        await update.message.reply_text(
            f"⏳ No signal yet.\n\n"
            f"PO client connected: {'✅' if po_ok else '❌'}\n"
            f"Waiting for first candle poll (up to 60s after startup).\n"
            f"If this persists, use /debug to check connection."
        )
        return
    price_str = f"{sig['price']:.5f}" if sig["price"] else "N/A"
    await update.message.reply_text(
        f"📊 *Current Signal*\n"
        f"Asset: `EURUSD OTC`\n"
        f"Direction: *{sig['direction']}*\n"
        f"Price: `{price_str}`\n"
        f"Reason: {sig['reason']}\n"
        f"Updated: {sig['timestamp']}",
        parse_mode='Markdown'
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    po_ok = po_client is not None and po_client.is_connected
    sig = latest_signal
    await update.message.reply_text(
        f"✅ *Bot Status*\n"
        f"Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"PO client: {'🟢 Connected' if po_ok else '🔴 Not connected'}\n"
        f"Last signal: {sig['direction'] or 'none'} @ {sig['timestamp'] or 'never'}",
        parse_mode='Markdown'
    )


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Debug command called. po_client={po_client}")
    try:
        po_ok = po_client is not None and po_client.is_connected
        ssid_preview = f"{SSID[:20]}...{SSID[-10:]}" if SSID and len(SSID) > 30 else str(SSID)
        
        # Use plain text (no parse_mode) to avoid Markdown parsing errors
        debug_text = (
            "🔧 Debug Info\n"
            f"IS_RENDER: {IS_RENDER}\n"
            f"RENDER_EXTERNAL_URL: {RENDER_EXTERNAL_URL or 'not set'}\n"
            f"SSID set: {'✅' if SSID else '❌'}\n"
            f"SSID preview: {ssid_preview}\n"
            f"PO client exists: {'✅' if po_client else '❌'}\n"
            f"PO client connected: {'✅' if po_ok else '❌'}\n"
            f"Signal: {latest_signal['direction'] or 'none'}\n"
            f"Last update: {latest_signal['timestamp'] or 'never'}"
        )
        
        await update.message.reply_text(debug_text)
        logger.info("Debug response sent")
    except Exception as e:
        logger.error(f"Debug command error: {e}", exc_info=True)
        await update.message.reply_text(f"Debug error: {e}")


async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show recent error logs"""
    if not recent_errors:
        await update.message.reply_text("📋 No recent errors logged.")
        return
    
    logs_text = "📋 Recent Errors:\n\n" + "\n".join(recent_errors[-10:])
    await update.message.reply_text(logs_text)


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("signal", signal_command))
telegram_app.add_handler(CommandHandler("status", status_command))
telegram_app.add_handler(CommandHandler("debug", debug_command))
telegram_app.add_handler(CommandHandler("logs", logs_command))


# ---------------------------------------------------------------------------
# Telegram webhook registration
# ---------------------------------------------------------------------------
async def reset_and_set_webhook():
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL not set — cannot register Telegram webhook.")
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
async def health(request: Request):
    return JSONResponse({
        "status": "healthy",
        "timestamp": str(datetime.now()),
        "po_connected": po_client is not None and po_client.is_connected,
        "latest_signal": latest_signal,
    })


async def telegram_webhook(request: Request):
    data = await request.json()
    logger.info(f"Received Telegram update: {data}")
    try:
        await telegram_app.process_update(Update.de_json(data, telegram_app.bot))
        logger.info("Update processed successfully")
    except Exception as e:
        logger.error(f"Error processing update: {e}", exc_info=True)
    return JSONResponse({"status": "ok"})


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/telegram", telegram_webhook, methods=["POST"]),
]


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: Starlette):
    global po_client
    
    logger.info("Starting up — webhook mode (Render)...")

    await telegram_app.initialize()
    await telegram_app.start()
    await reset_and_set_webhook()

    # Initialize PO client before starting the background loop
    try:
        po_client = AsyncPocketOptionClient(ssid=SSID, is_demo=True)
        logger.info(f"Pocket Option client initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize PO client: {e}")
        po_client = None

    # Start Pocket Option background loop (handles connect + retry internally)
    po_task = asyncio.create_task(pocket_option_loop())
    logger.info("Pocket Option background task created.")

    yield

    po_task.cancel()
    try:
        await po_task
    except (asyncio.CancelledError, Exception):
        pass

    if po_client:
        try:
            await po_client.disconnect()
        except Exception as e:
            logger.warning(f"Disconnect error: {e}")

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
