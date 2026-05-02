import os
import asyncio
import logging
import json
import re
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
from pocket_option import PocketOptionClient
from pocket_option.constants import Regions
from pocket_option.contrib.candles import MemoryCandleStorage
from pocket_option.models import (
    Asset,
    AuthorizationData,
    ChangeAssetRequest,
    SuccessAuthEvent,
    UpdateCloseValueItem,
)

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
SIGNAL_INTERVAL = int(os.environ.get("SIGNAL_INTERVAL", 300))

IS_RENDER = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

SSID = os.environ.get("SSID")
if not SSID:
    raise ValueError("SSID environment variable not set")

# Optional: numeric uid. Required by the API but not critical in demo mode.
PO_UID = int(os.environ.get("PO_UID", "0"))

TRADE_ASSET = Asset.EURUSD_otc
CANDLE_PERIOD = 60  # 1-minute candles

# ---------------------------------------------------------------------------
# SSID parser
# Supports two formats:
#   1. Full WS auth string: 42["auth",{"session":"...","uid":123,...}]
#   2. Raw session string:  A6ApZ88gUyEt74yQh,,Ai-Q3sLv75FvWTvWK
# ---------------------------------------------------------------------------
def parse_ssid(ssid_string: str) -> tuple[str, int]:
    """
    Parse the SSID env var into (session, uid).
    Handles both the full WebSocket auth message and a bare session string.
    """
    ssid_string = ssid_string.strip()
    # Try full WS auth format first: 42["auth",{...}]
    match = re.search(r'42\["auth",(\{.*?\})\]', ssid_string)
    if match:
        data = json.loads(match.group(1))
        return data["session"], int(data.get("uid", PO_UID))
    # Fall back: treat the whole string as a raw session token
    return ssid_string, PO_UID


PO_SESSION, PO_UID_PARSED = parse_ssid(SSID)
logger.info(f"Parsed PO_SESSION (first 8 chars): {PO_SESSION[:8]}... uid={PO_UID_PARSED}")

# ---------------------------------------------------------------------------
# Global signal state — updated on every price tick
# ---------------------------------------------------------------------------
latest_signal: dict = {
    "direction": None,
    "price": None,
    "reason": None,
    "timestamp": None,
}

# ---------------------------------------------------------------------------
# Pocket Option client + candle storage
# ---------------------------------------------------------------------------
po_client = PocketOptionClient()
candle_storage = MemoryCandleStorage(po_client)


# ---------------------------------------------------------------------------
# Signal logic — 3-candle momentum
# ---------------------------------------------------------------------------
def compute_signal() -> tuple[str, str]:
    """
    Returns (direction, reason).
    direction: "CALL ✅" | "PUT ❌" | "WAIT ⏸"
    """
    try:
        candles = candle_storage.get_candles(TRADE_ASSET, CANDLE_PERIOD, count=4)
        if not candles or len(candles) < 4:
            return "WAIT ⏸", "Not enough candle data yet"
        closes = [c.close for c in candles[-4:]]
        if closes[-1] > closes[-2] > closes[-3] > closes[-4]:
            return "CALL ✅", "3 consecutive rising closes (bullish momentum)"
        if closes[-1] < closes[-2] < closes[-3] < closes[-4]:
            return "PUT ❌", "3 consecutive falling closes (bearish momentum)"
        return "WAIT ⏸", "No clear momentum"
    except Exception as e:
        logger.warning(f"compute_signal error: {e}")
        return "WAIT ⏸", f"Error: {e}"


# ---------------------------------------------------------------------------
# Pocket Option event handlers
# ---------------------------------------------------------------------------
@po_client.on.connect
async def on_po_connect(data: None):
    logger.info("Pocket Option WebSocket connected — authenticating...")
    await po_client.emit.auth(
        AuthorizationData.model_validate({
            "session": PO_SESSION,
            "isDemo": 1,
            "uid": PO_UID_PARSED,
            "platform": 2,
            "isFastHistory": True,
            "isOptimized": True,
        })
    )


@po_client.on.success_auth
async def on_po_auth(data: SuccessAuthEvent):
    logger.info(f"Pocket Option authenticated — uid={data.id}")
    await po_client.emit.subscribe_to_asset(TRADE_ASSET)
    await po_client.emit.change_asset(
        ChangeAssetRequest(asset=TRADE_ASSET, period=CANDLE_PERIOD)
    )
    logger.info(f"Subscribed to {TRADE_ASSET} @ {CANDLE_PERIOD}s candles")


@po_client.on.update_close_value
async def on_price_update(assets: list[UpdateCloseValueItem]):
    """
    Fires on every real-time price tick from Pocket Option.
    Updates latest_signal and broadcasts CALL/PUT signals to the chat.
    """
    global latest_signal

    direction, reason = compute_signal()

    # Get the latest price from the tick data if available
    price = None
    if assets:
        try:
            price = assets[0].price
        except AttributeError:
            pass

    latest_signal = {
        "direction": direction,
        "price": price,
        "reason": reason,
        "timestamp": str(datetime.now()),
    }

    # Broadcast only actionable signals (not WAIT)
    if CHAT_ID and direction != "WAIT ⏸":
        try:
            price_str = f"{price:.5f}" if price else "N/A"
            message = (
                f"📡 *Live Signal*\n"
                f"Asset: `EURUSD OTC`\n"
                f"Direction: *{direction}*\n"
                f"Price: `{price_str}`\n"
                f"Reason: {reason}\n"
                f"Time: {datetime.now().strftime('%H:%M:%S')}"
            )
            await telegram_app.bot.send_message(
                chat_id=CHAT_ID, text=message, parse_mode='Markdown'
            )
            logger.info(f"Live signal broadcast: {direction}")
        except Exception as e:
            logger.error(f"Failed to broadcast signal: {e}")


# ---------------------------------------------------------------------------
# Telegram app
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Real-time Signal Bot is running!*\n\n"
        "Commands:\n"
        "• /signal — get the current real-time signal\n"
        "• /status — check bot and connection status",
        parse_mode='Markdown'
    )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Return the latest signal computed from live Pocket Option data."""
    sig = latest_signal
    if sig["direction"] is None:
        await update.message.reply_text(
            "⏳ No signal yet — waiting for Pocket Option data...\n"
            "Make sure PO_SESSION and PO_UID are set correctly."
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
    po_status = "🟢 Connected" if getattr(po_client, 'connected', False) else "🔴 Disconnected"
    sig = latest_signal
    await update.message.reply_text(
        f"✅ *Bot Status*\n"
        f"Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Pocket Option: {po_status}\n"
        f"Last signal: {sig['direction'] or 'none'} @ {sig['timestamp'] or 'never'}",
        parse_mode='Markdown'
    )


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("signal", signal_command))
telegram_app.add_handler(CommandHandler("status", status_command))


# ---------------------------------------------------------------------------
# Telegram webhook registration (raw HTTP)
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
        "po_connected": getattr(po_client, 'connected', False),
        "latest_signal": latest_signal,
    })


async def telegram_webhook(request: Request):
    data = await request.json()
    logger.info(f"Received Telegram update: {data}")
    await telegram_app.process_update(Update.de_json(data, telegram_app.bot))
    return JSONResponse({"status": "ok"})


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/telegram", telegram_webhook, methods=["POST"]),
]


# ---------------------------------------------------------------------------
# Lifespan (Render / webhook mode only)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: Starlette):
    # --- Startup ---
    logger.info("Starting up — webhook mode (Render)...")

    await telegram_app.initialize()
    await telegram_app.start()
    await reset_and_set_webhook()

    # Connect Pocket Option WebSocket as a background task
    po_task = asyncio.create_task(po_client.connect(Regions.DEMO))
    logger.info("Pocket Option WebSocket task started.")

    yield

    # --- Shutdown ---
    po_task.cancel()
    try:
        await po_task
    except (asyncio.CancelledError, Exception):
        pass
    logger.info("Pocket Option task stopped.")

    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram app shut down.")


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
