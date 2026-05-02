import os
import asyncio
import logging
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

TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("No BOT_TOKEN environment variable set!")

CHAT_ID = os.environ.get("CHAT_ID")
SIGNAL_INTERVAL = int(os.environ.get("SIGNAL_INTERVAL", 300))
RENDER_URL = os.environ.get("RENDER_URL", "").rstrip("/")

telegram_app = Application.builder().token(TOKEN).build()


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
        f"✅ Bot is active and healthy!\n⏱️ Server time: {datetime.now()}\n📡 Auto-signal interval: {SIGNAL_INTERVAL} seconds"
    )


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("signal", signal_command))
telegram_app.add_handler(CommandHandler("status", status_command))


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


@asynccontextmanager
async def lifespan(app):
    # --- Startup ---
    # Initialize the telegram app (sets up bot, dispatcher, etc.)
    await telegram_app.initialize()
    await telegram_app.start()

    # Always clear any existing webhook/polling state first to avoid Conflict errors.
    # drop_pending_updates=True discards any queued updates from the old session.
    await telegram_app.bot.delete_webhook(drop_pending_updates=True)
    logger.info("Cleared any existing webhook/polling state.")

    # Register the webhook with Telegram so it knows where to send updates
    if RENDER_URL:
        webhook_url = f"{RENDER_URL}/telegram"
        await telegram_app.bot.set_webhook(webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.warning("RENDER_URL not set — webhook not registered. Bot won't receive messages.")

    # Launch the background auto-signal task
    task = asyncio.create_task(send_auto_signal())
    logger.info("Auto-signal background task started.")

    yield

    # --- Shutdown ---
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Auto-signal background task cancelled.")

    # Remove the webhook and stop the telegram app cleanly
    await telegram_app.bot.delete_webhook()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram app shut down.")


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

web_app = Starlette(routes=routes, lifespan=lifespan)

if __name__ == "__main__":
    # Local polling mode for development
    logger.info("Starting bot in polling mode...")
    telegram_app.run_polling()
else:
    # Production: uvicorn serves the Starlette app, webhook handles updates
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting webhook server on port {port}...")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
