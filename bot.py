import os
import asyncio
import logging
import httpx
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime

# Pocket Option SDK
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
# Config
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set!")

CHAT_ID = os.environ.get("CHAT_ID")
SIGNAL_INTERVAL = int(os.environ.get("SIGNAL_INTERVAL", 300))

# Render sets RENDER=true automatically on all deployments.
IS_RENDER = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

# Pocket Option credentials — get these from your browser's DevTools (WS tab).
# Look for a message like: 42["auth",{"session":"...","uid":12345,...}]
PO_SESSION = os.environ.get("PO_SESSION")  # the "session" value
PO_UID = os.environ.get("PO_UID")          # the "uid" value (integer)

TRADE_ASSET = Asset.EURUSD_otc
CANDLE_PERIOD = 60  # 1-minute candles

# ---------------------------------------------------------------------------
# Pocket Option client + candle storage (module-level singletons)
# ---------------------------------------------------------------------------
po_client = PocketOptionClient()
candle_storage = MemoryCandleStorage(po_client)

# Holds the most recent computed signal so /signal can return it instantly
_latest_signal: dict = {"direction": "N/A", "asset": str(TRADE_ASSET), "updated": "never"}


# ---------------------------------------------------------------------------
# Signal logic — called whenever a new candle closes
# ---------------------------------------------------------------------------
def compute_signal(storage: MemoryCandleStorage) -> str:
    """
    Simple 3-candle momentum strategy:
      - If the last 3 closes are all rising  → CALL ✅
      - If the last 3 closes are all falling → PUT ❌
      - Otherwise                            → WAIT ⏸
    """
    try:
        candles = storage.get_candles(TRADE_ASSET, CANDLE_PERIOD, count=4)
        if not candles or len(candles) < 4:
            return "WAIT ⏸"
        closes = [c.close for c in candles[-4:]]
        if closes[-1] > closes[-2] > closes[-3] > closes[-4]:
            return "CALL ✅"
        if closes[-1] < closes[-2] < closes[-3] < closes[-4]:
            return "PUT ❌"
        return "WAIT ⏸"
    except Exception as e:
        logger.warning(f"compute_signal error: {e}")
        return "WAIT ⏸"


# ---------------------------------------------------------------------------
# Pocket Option event handlers
# ---------------------------------------------------------------------------
@po_client.on.connect
async def on_po_connect(data: None):
    logger.info("Pocket Option WebSocket connected — authenticating...")
    if not PO_SESSION or not PO_UID:
        logger.error("PO_SESSION or PO_UID not set — cannot authenticate with Pocket Option.")
        return
    await po_client.emit.auth(
        AuthorizationData.model_validate({
            "session": PO_SESSION,
            "isDemo": 1,
            "uid": int(PO_UID),
            "platform": 2,
            "isFastHistory": True,
            "isOptimized": True,
        })
    )


@po_client.on.success_auth
async def on_po_auth(data: SuccessAuthEvent):
    logger.info(f"Pocket Option authenticated — uid={data.id}")
    await po_client.emit.subscribe_to_asset(TRADE_ASSET)
    await po_client.emit.change_asset(ChangeAssetRequest(asset=TRADE_ASSET, period=CANDLE_PERIOD))
    logger.info(f"Subscribed to {TRADE_ASSET} @ {CANDLE_PERIOD}s candles")


@po_client.on.update_close_value
async def on_price_update(assets: list[UpdateCloseValueItem]):
    """
    Fires on every real-time price tick. We update the latest signal
    and optionally push an auto-signal to the chat when the direction is clear.
    """
    global _latest_signal
    direction = compute_signal(candle_storage)
    _latest_signal = {
        "direction": direction,
        "asset": str(TRADE_ASSET),
        "updated": str(datetime.now()),
    }

    # Only broadcast CALL/PUT signals (skip WAIT)
    if CHAT_ID and direction != "WAIT ⏸":
        try:
            message = (
                f"📡 *Live Signal*\n"
                f"Asset: `{TRADE_ASSET}`\n"
                f"Direction: *{direction}*\n"
                f"Strategy: 3-candle momentum\n"
                f"Time: {datetime.now().strftime('%H:%M:%S')}"
            )
            await telegram_app.bot.send_message(
                chat_id=CHAT_ID, text=message, parse_mode='Markdown'
            )
            logger.info(f"Live signal sent: {direction}")
        except Exception as e:
            logger.error(f"Failed to send live signal: {e}")


# ---------------------------------------------------------------------------
# Telegram app
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Signal Bot is running!*\n\n"
        "Commands:\n"
        "• /signal — get the current real-time signal\n"
        "• /status — check bot and connection status",
        parse_mode='Markdown'
    )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Return the most recent signal computed from live Pocket Option data."""
    sig = _latest_signal
    await update.message.reply_text(
        f"📊 *Current Signal*\n"
        f"Asset: `{sig['asset']}`\n"
        f"Direction: *{sig['direction']}*\n"
        f"Last updated: {sig['updated']}",
        parse_mode='Markdown'
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    po_status = "🟢 Connected" if po_client.connected else "🔴 Disconnected"
    await update.message.reply_text(
        f"✅ *Bot Status*\n"
        f"Server time: {datetime.now()}\n"
        f"Pocket Option: {po_status}\n"
        f"Last signal: {_latest_signal['direction']} @ {_latest_signal['updated']}",
        parse_mode='Markdown'
    )


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("signal", signal_command))
telegram_app.add_handler(CommandHandler("status", status_command))


# ---------------------------------------------------------------------------
# Webhook registration (raw HTTP — no PTB state dependency)
# ---------------------------------------------------------------------------
async def reset_and_set_webhook():
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL is not set — cannot register webhook.")
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
        "po_connected": po_client.connected,
        "last_signal": _latest_signal,
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
# Lifespan — webhook mode (Render only)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: Starlette):
    # --- Startup ---
    logger.info("Starting up on Render — webhook mode...")

    # Boot PTB
    await telegram_app.initialize()
    await telegram_app.start()

    # Register Telegram webhook (clears any old polling/webhook state first)
    await reset_and_set_webhook()

    # Connect Pocket Option WebSocket in the background
    if PO_SESSION and PO_UID:
        po_task = asyncio.create_task(po_client.connect(Regions.DEMO))
        logger.info("Pocket Option WebSocket connection task started.")
    else:
        po_task = None
        logger.warning(
            "PO_SESSION or PO_UID not set — Pocket Option connection skipped. "
            "Add them to your Render environment variables."
        )

    yield

    # --- Shutdown ---
    if po_task:
        po_task.cancel()
        try:
            await po_task
        except (asyncio.CancelledError, Exception):
            pass
        logger.info("Pocket Option task cancelled.")

    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram app shut down.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if IS_RENDER:
    # Render: webhook mode — uvicorn binds the port Render expects
    web_app = Starlette(routes=routes, lifespan=lifespan)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"IS_RENDER=true — starting webhook server on port {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
else:
    # Local development: polling mode (no webhook needed)
    logger.info("IS_RENDER not set — starting in polling mode for local development...")
    telegram_app.run_polling()
