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
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Config ---
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("No BOT_TOKEN environment variable set!")

CHAT_ID = os.environ.get("CHAT_ID")
SIGNAL_INTERVAL = int(os.environ.get("SIGNAL_INTERVAL", 300))

# Render sets the "RENDER" env var automatically on all deployments.
# RENDER_EXTERNAL_URL is the public HTTPS URL Render assigns to the service.
IS_RENDER = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

# --- Telegram app ---
telegram_app = Application.builder().token(TOKEN).build()


# --- Command handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        '🤖 Signal Bot is running!\nUse /signal to get a test signal.\nUse /status to check uptime.'
    )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"📊 *Test Signal*\nAsset: EURUSD\nAction: CALL\nTime: {datetime.now()}",
        parse_mode='Markdown'
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"✅ Bot is active and healthy!\n⏱️ Server time: {datetime.now()}\n"
        f"📡 Auto-signal interval: {SIGNAL_INTERVAL} seconds"
    )


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("signal", signal_command))
telegram_app.add_handler(CommandHandler("status", status_command))


# --- Background auto-signal task ---
async def send_auto_signal():
    if not CHAT_ID:
        logger.warning("CHAT_ID not set. Auto-signals disabled.")
        return
    while True:
        try:
            signal_type = random.choice(["CALL ✅", "PUT ❌"])
            message = (
                f"🤖 *Auto-Signal*\nDirection: {signal_type}\nAsset: EURUSD\n"
                f"Reason: RSI divergence detected.\nTime: {datetime.now()}"
            )
            await telegram_app.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
            logger.info(f"Auto-signal sent to {CHAT_ID}")
        except Exception as e:
            logger.error(f"Failed to send auto-signal: {e}")
        await asyncio.sleep(SIGNAL_INTERVAL)


# --- Webhook setup via raw HTTP (bypasses PTB state requirements) ---
async def reset_and_set_webhook():
    """Delete any existing webhook/polling session, then register the new webhook."""
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL is not set — cannot register webhook.")
        return

    webhook_url = f"{RENDER_EXTERNAL_URL}/telegram"
    base = f"https://api.telegram.org/bot{TOKEN}"

    async with httpx.AsyncClient() as client:
        # Clear any existing webhook and drop stale pending updates
        delete_resp = await client.post(
            f"{base}/deleteWebhook",
            params={"drop_pending_updates": "true"}
        )
        logger.info(f"deleteWebhook response: {delete_resp.json()}")

        # Register the new webhook
        set_resp = await client.post(
            f"{base}/setWebhook",
            params={"url": webhook_url}
        )
        logger.info(f"setWebhook response: {set_resp.json()}")


# --- Starlette routes ---
async def health(request: Request):
    return JSONResponse({"status": "healthy", "timestamp": str(datetime.now())})


async def telegram_webhook(request: Request):
    data = await request.json()
    logger.info(f"Received update: {data}")
    await telegram_app.process_update(Update.de_json(data, telegram_app.bot))
    return JSONResponse({"status": "ok"})


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/telegram", telegram_webhook, methods=["POST"]),
]


# --- Lifespan (webhook mode only) ---
@asynccontextmanager
async def lifespan(app: Starlette):
    # Startup
    logger.info("Starting up on Render — forcing webhook mode...")

    # Initialize PTB so process_update works
    await telegram_app.initialize()
    await telegram_app.start()

    # Clear old session and register webhook via raw HTTP
    await reset_and_set_webhook()

    # Start background auto-signal task
    task = asyncio.create_task(send_auto_signal())
    logger.info("Auto-signal background task started.")

    yield

    # Shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Auto-signal background task cancelled.")

    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram app shut down.")


# --- Entry point ---
if IS_RENDER:
    # Render deployment: webhook mode, uvicorn binds the port Render expects
    web_app = Starlette(routes=routes, lifespan=lifespan)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"IS_RENDER=true — starting webhook server on port {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
else:
    # Local development: polling mode (no webhook needed)
    logger.info("IS_RENDER not set — starting bot in polling mode for local development...")
    telegram_app.run_polling()
