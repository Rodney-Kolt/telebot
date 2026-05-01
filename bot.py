"""
OTC Trading Signal Bot
- Sends scheduled trading signals to your Telegram chat
- Webhook mode on Render (24/7)
- Polling mode for local dev
"""

import os
import random
import logging
import asyncio
from datetime import datetime, timezone

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn

from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN")
CHAT_ID     = os.environ.get("CHAT_ID")          # your personal Telegram ID
RENDER_URL  = os.environ.get("RENDER_URL", "")   # set on Render
PORT        = int(os.environ.get("PORT", 8000))
# How often to send a signal automatically (seconds). Default = 5 minutes.
SIGNAL_INTERVAL = int(os.environ.get("SIGNAL_INTERVAL", 300))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set.")

# ── Signal Generator ──────────────────────────────────────────────────────────
ASSETS = [
    "EUR/USD (OTC)", "GBP/USD (OTC)", "USD/JPY (OTC)",
    "AUD/USD (OTC)", "EUR/GBP (OTC)", "USD/CAD (OTC)",
    "NZD/USD (OTC)", "USD/CHF (OTC)", "EUR/JPY (OTC)",
    "GBP/JPY (OTC)",
]

DURATIONS = ["1 min", "2 min", "3 min", "5 min"]

DIRECTIONS = [
    ("CALL ⬆️", "BUY"),
    ("PUT ⬇️",  "SELL"),
]

def generate_signal() -> str:
    asset     = random.choice(ASSETS)
    duration  = random.choice(DURATIONS)
    direction, action = random.choice(DIRECTIONS)
    confidence = random.randint(72, 94)
    now        = datetime.now(timezone.utc).strftime("%H:%M UTC")

    return (
        f"📊 *OTC Trading Signal*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Time      : `{now}`\n"
        f"💱 Asset     : `{asset}`\n"
        f"📈 Action    : *{direction}*\n"
        f"⏱ Duration  : `{duration}`\n"
        f"🎯 Confidence: `{confidence}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_Execute manually on Pocket Option_"
    )


# ── Telegram Command Handlers ─────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 *Welcome to OTC Signal Bot!*\n\n"
        "I will send you trading signals automatically.\n\n"
        "Commands:\n"
        "  /start    – Show this message\n"
        "  /signal   – Get a signal right now\n"
        "  /status   – Check bot status\n\n"
        f"⏱ Auto-signals every *{SIGNAL_INTERVAL // 60} minutes*.",
        parse_mode="Markdown",
    )


async def signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(generate_signal(), parse_mode="Markdown")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"✅ *Bot is running*\n"
        f"� Server time: `{now}`\n"
        f"⏱ Signal interval: every `{SIGNAL_INTERVAL // 60}` minutes",
        parse_mode="Markdown",
    )


# ── Auto-signal scheduler ─────────────────────────────────────────────────────
async def auto_signal_loop(bot: Bot) -> None:
    """Runs forever, sending a signal every SIGNAL_INTERVAL seconds."""
    if not CHAT_ID:
        logger.warning("CHAT_ID not set — auto-signals disabled.")
        return

    logger.info("Auto-signal loop started. Interval: %ds", SIGNAL_INTERVAL)
    while True:
        await asyncio.sleep(SIGNAL_INTERVAL)
        try:
            msg = generate_signal()
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
            logger.info("Auto-signal sent to %s", CHAT_ID)
        except Exception as exc:
            logger.error("Failed to send auto-signal: %s", exc)


# ── Build Application ─────────────────────────────────────────────────────────
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("signal", signal_cmd))
    app.add_handler(CommandHandler("status", status))
    return app


# ── Starlette webhook app (Render) ────────────────────────────────────────────
telegram_app: Application = build_app()
_initialized = False


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


async def telegram_webhook(request: Request) -> JSONResponse:
    global _initialized
    try:
        data   = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        if not _initialized:
            await telegram_app.initialize()
            _initialized = True
        await telegram_app.process_update(update)
        return JSONResponse({"ok": True})
    except Exception as exc:
        logger.exception("Webhook error: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


async def on_startup() -> None:
    """Start the auto-signal loop when the server boots."""
    await telegram_app.initialize()
    asyncio.create_task(auto_signal_loop(telegram_app.bot))
    logger.info("Startup complete.")


web_app = Starlette(
    on_startup=[on_startup],
    routes=[
        Route("/health",   health,           methods=["GET"]),
        Route("/telegram", telegram_webhook, methods=["POST"]),
    ],
)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if RENDER_URL:
        logger.info("Webhook mode — port %d", PORT)
        uvicorn.run(web_app, host="0.0.0.0", port=PORT)
    else:
        logger.info("Polling mode (local dev)")

        async def run_polling():
            app = build_app()
            await app.initialize()
            # Also run auto-signals in polling mode if CHAT_ID is set
            asyncio.create_task(auto_signal_loop(app.bot))
            await app.start()
            await app.updater.start_polling()
            # Keep running
            await asyncio.Event().wait()

        asyncio.run(run_polling())
