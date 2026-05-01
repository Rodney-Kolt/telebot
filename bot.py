"""
Telegram Signal Bot
- Polling mode: run directly with `python bot.py`
- Webhook mode: deployed on Render, listens on PORT env variable
"""

import os
import json
import logging
import asyncio
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set.")

PORT = int(os.environ.get("PORT", 8000))
RENDER_URL = os.environ.get("RENDER_URL", "")  # e.g. https://my-signal-bot.onrender.com

# ── Command Handlers ─────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Welcome message."""
    await update.message.reply_text(
        "👋 Welcome to the Trading Signal Bot!\n\n"
        "Available commands:\n"
        "  /start  – Show this message\n"
        "  /signal – Get the latest trading signal\n\n"
        "Signals will be posted here automatically when received."
    )


async def signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Return a placeholder trading signal."""
    # TODO: Replace this with real signal logic or an external data source
    placeholder_signal = (
        "📊 *Trading Signal*\n"
        "──────────────────\n"
        "Asset   : EUR/USD\n"
        "Action  : BUY ⬆️\n"
        "Duration: 5 minutes\n"
        "Confidence: 78%\n"
        "──────────────────\n"
        "_This is a placeholder signal. Replace with real data._"
    )
    await update.message.reply_text(placeholder_signal, parse_mode="Markdown")


# ── Build the Application ─────────────────────────────────────────────────────
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("signal", signal))
    return app


# ── Webhook / Starlette server (used on Render) ───────────────────────────────
telegram_app: Application = build_app()


async def health(request: Request) -> JSONResponse:
    """Health-check endpoint — Render pings this to keep the service alive."""
    return JSONResponse({"status": "ok"})


async def telegram_webhook(request: Request) -> JSONResponse:
    """Receive Telegram updates via webhook POST."""
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.initialize()
        await telegram_app.process_update(update)
        return JSONResponse({"ok": True})
    except Exception as exc:
        logger.exception("Error processing update: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


# Starlette ASGI app
web_app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/telegram", telegram_webhook, methods=["POST"]),
    ]
)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    if RENDER_URL:
        # Webhook mode (Render deployment)
        logger.info("Starting in WEBHOOK mode on port %d", PORT)
        uvicorn.run(web_app, host="0.0.0.0", port=PORT)
    else:
        # Polling mode (local development)
        logger.info("Starting in POLLING mode (local dev)")
        app = build_app()
        app.run_polling()
