import os
import json
import time
import asyncio
import logging
import math
import httpx
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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

IS_RENDER = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

# Signal photo URLs — override with env vars on Render, or use built-in defaults.
_DEFAULT_IMG_BUY  = "https://i.ibb.co/HDC7G1D0/image.jpg"   # BUY (CALL) image
_DEFAULT_IMG_SELL = "https://i.ibb.co/YTfbPc72/image.jpg"   # SELL (PUT) image

SIGNAL_IMG_BUY  = os.environ.get("SIGNAL_IMG_BUY",  "").strip() or _DEFAULT_IMG_BUY
SIGNAL_IMG_SELL = os.environ.get("SIGNAL_IMG_SELL", "").strip() or _DEFAULT_IMG_SELL

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TRADE_ASSET   = "EURUSD_otc"
CANDLE_PERIOD = 60    # seconds for the auto-signal loop
CANDLE_COUNT  = 50    # candles to fetch (enough for RSI-14 + EMA-20)

PREEMPTIVE_REFRESH_HOURS = 23   # reconnect before the 24-h session boundary
PING_INTERVAL_SECONDS    = 20   # keep-alive ping cadence
SIGNAL_EXPIRY_SEC        = 600  # 10 minutes
STATS_FILE               = "stats.json"

# ConversationHandler states
CHOOSE_ASSET, CHOOSE_TIME = range(2)

# ---------------------------------------------------------------------------
# Asset catalogue
# ---------------------------------------------------------------------------
ASSETS: dict[str, str] = {
    "EUR/USD (OTC)": "EURUSD_otc",
    "AUD/USD (OTC)": "AUDUSD_otc",
    "USD/JPY (OTC)": "USDJPY_otc",
    "GBP/USD (OTC)": "GBPUSD_otc",
    "USD/CAD (OTC)": "USDCAD_otc",
    "EUR/JPY (OTC)": "EURJPY_otc",
}

TIMEFRAMES: dict[str, int] = {
    "5 seconds":  5,
    "1 minute":   60,
    "2 minutes":  120,
    "5 minutes":  300,
}

DEFAULT_SETTINGS = {
    "asset":     "EUR/USD (OTC)",
    "timeframe": "1 minute",
    "auto":      False,
}

# Error substrings that indicate the SSID is dead and needs manual refresh
_AUTH_ERROR_MARKERS = (
    "authentication timeout",
    "websocket connection closed",
    "unauthorized",
    "token expired",
    "connect() returned false",
    "failed to connect",
)

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_user_configs() -> list[dict]:
    """
    Parse USERS_CONFIG env var (JSON array) or fall back to single-user SSID+CHAT_ID.
    Returns list of dicts: [{"name": str, "chat_id": int, "ssid": str, "is_demo": bool}]
    """
    raw = os.environ.get("USERS_CONFIG", "").strip()
    if raw:
        try:
            configs = json.loads(raw)
            result = []
            for c in configs:
                result.append({
                    "name":    c.get("name", f"User_{c['chat_id']}"),
                    "chat_id": int(c["chat_id"]),
                    "ssid":    c["ssid"],
                    "is_demo": bool(int(c.get("is_demo", 1))),
                })
            logger.info(f"Loaded {len(result)} users from USERS_CONFIG")
            return result
        except Exception as exc:
            logger.error(f"Failed to parse USERS_CONFIG: {exc}")

    # Single-user fallback
    ssid    = os.environ.get("SSID", "")
    chat_id = os.environ.get("CHAT_ID", "")
    if ssid and chat_id:
        logger.info("Using single-user fallback (SSID + CHAT_ID)")
        return [{
            "name":    "Default",
            "chat_id": int(chat_id),
            "ssid":    ssid,
            "is_demo": bool(int(os.environ.get("PO_DEMO", "1"))),
        }]

    raise ValueError("No user config found. Set USERS_CONFIG or SSID+CHAT_ID env vars.")

# ---------------------------------------------------------------------------
# UserSession dataclass
# ---------------------------------------------------------------------------

@dataclass
class UserSession:
    name:     str
    chat_id:  int
    ssid:     str
    is_demo:  bool

    # Created after init
    session_manager: "SessionManager | None" = field(default=None, repr=False)
    settings: dict = field(default_factory=lambda: dict(DEFAULT_SETTINGS))
    latest_signal: dict = field(default_factory=lambda: {
        "direction": None, "price": None, "reason": None, "timestamp": None
    })
    poll_task: "asyncio.Task | None" = field(default=None, repr=False)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

# Registry of all active user sessions, keyed by chat_id
user_registry: dict[int, UserSession] = {}

# Win/loss tracking (keyed by chat_id)
user_stats: dict[int, dict] = {}
pending_signals: dict[str, dict] = {}

# Per-user Telegram settings (for users not in user_registry, e.g. menu state)
user_settings: dict[int, dict] = {}

# Recent errors
recent_errors: list[str] = []
MAX_ERRORS = 20

# ---------------------------------------------------------------------------
# Error logging
# ---------------------------------------------------------------------------

def log_error(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    logger.error(msg)
    recent_errors.append(entry)
    if len(recent_errors) > MAX_ERRORS:
        recent_errors.pop(0)

# ---------------------------------------------------------------------------
# Helper to get user session
# ---------------------------------------------------------------------------

def _get_user_session(chat_id: int) -> "UserSession | None":
    return user_registry.get(chat_id)

# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def _load_stats() -> None:
    """Load persisted stats from stats.json on startup."""
    global user_stats
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            user_stats = {int(k): v for k, v in raw.items()}
            logger.info(f"Loaded stats for {len(user_stats)} users from {STATS_FILE}")
    except Exception as exc:
        logger.warning(f"Could not load stats: {exc}")


def _save_stats() -> None:
    """Persist stats to stats.json (best-effort)."""
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(user_stats, f)
    except Exception as exc:
        logger.warning(f"Could not save stats: {exc}")


def _get_stats(user_id: int) -> dict:
    if user_id not in user_stats:
        user_stats[user_id] = {"total": 0, "wins": 0, "losses": 0, "last_5": []}
    return user_stats[user_id]


def _record_vote(user_id: int, won: bool) -> None:
    s = _get_stats(user_id)
    s["total"] += 1
    if won:
        s["wins"] += 1
        s["last_5"].append("\U0001f44d")
    else:
        s["losses"] += 1
        s["last_5"].append("\U0001f44e")
    s["last_5"] = s["last_5"][-5:]
    _save_stats()


def _make_signal_id(user_id: int) -> str:
    """Generate a unique signal ID: timestamp_userid."""
    return f"{int(time.time())}_{user_id}"


def _vote_keyboard(signal_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("\U0001f44d Win",  callback_data=f"vote:win:{signal_id}"),
        InlineKeyboardButton("\U0001f44e Loss", callback_data=f"vote:loss:{signal_id}"),
    ]])


# ---------------------------------------------------------------------------
# Telegram app (created early so SessionManager can reference it)
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()

# ---------------------------------------------------------------------------
# SessionManager
# ---------------------------------------------------------------------------

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

    def __init__(self, ssid: str, is_demo: bool = True,
                 chat_id: int = 0, name: str = "Unknown") -> None:
        self.ssid:        str  = ssid
        self.is_demo:     bool = is_demo
        self.chat_id:     int  = chat_id
        self.name:        str  = name
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
                logger.warning(f"[{self.name}] SessionManager.stop disconnect error: {exc}")
        self.client = None
        self.connected_at = None
        logger.info(f"[{self.name}] SessionManager stopped.")

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
            logger.info(f"[{self.name}] SessionManager: connecting to Pocket Option...")
            connected = await self.client.connect()
        except Exception as exc:
            log_error(f"[{self.name}] SessionManager connect exception: {exc}")
            self.client = None
            return False

        if not connected:
            log_error(f"[{self.name}] SessionManager: connect() returned False — SSID may be expired")
            self.client = None
            return False

        self.connected_at = datetime.now()
        logger.info(f"[{self.name}] SessionManager: connected at {self.connected_at.strftime('%H:%M:%S')}")

        self._cancel_tasks()
        self._ping_task    = asyncio.create_task(self._ping_loop(),    name=f"po_ping_{self.chat_id}")
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name=f"po_refresh_{self.chat_id}")
        return True

    def _cancel_tasks(self) -> None:
        for task in (self._ping_task, self._refresh_task):
            if task and not task.done():
                task.cancel()
        self._ping_task = self._refresh_task = None

    # ── Background task 1: keep-alive pings ──────────────────────────── #

    async def _ping_loop(self) -> None:
        """Send a lightweight ping every PING_INTERVAL_SECONDS."""
        logger.info(f"[{self.name}] Ping loop started (interval={PING_INTERVAL_SECONDS}s)")
        while True:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            if not self.is_connected:
                logger.warning(f"[{self.name}] Ping loop: client disconnected, stopping ping loop")
                return
            try:
                if hasattr(self.client, "get_balance"):
                    await self.client.get_balance()
                logger.debug(f"[{self.name}] Ping sent")
            except Exception as exc:
                await self._handle_error(f"Ping failed: {exc}")
                return

    # ── Background task 2: pre-emptive 23-hour refresh ───────────────── #

    async def _refresh_loop(self) -> None:
        """Reconnect every PREEMPTIVE_REFRESH_HOURS to beat session expiry."""
        interval = PREEMPTIVE_REFRESH_HOURS * 3600
        logger.info(f"[{self.name}] Refresh loop started (interval={PREEMPTIVE_REFRESH_HOURS}h)")
        await asyncio.sleep(interval)
        logger.info(f"[{self.name}] SessionManager: pre-emptive session refresh triggered")
        async with self._lock:
            await self._connect()

    # ── Error handler ─────────────────────────────────────────────────── #

    async def _handle_error(self, error_msg: str) -> None:
        """
        Attempt reconnect with exponential back-off (up to 3 tries).
        If all retries fail, send a Telegram alert asking for a new SSID.
        """
        if self._reconnecting:
            return
        self._reconnecting = True
        log_error(f"[{self.name}] SessionManager error: {error_msg}")

        delays = [5, 30, 120]
        for attempt, delay in enumerate(delays, start=1):
            logger.info(f"[{self.name}] Reconnect attempt {attempt}/{len(delays)} in {delay}s...")
            await asyncio.sleep(delay)
            async with self._lock:
                success = await self._connect()
            if success:
                logger.info(f"[{self.name}] Reconnect succeeded on attempt {attempt}")
                self._reconnecting = False
                try:
                    await telegram_app.bot.send_message(
                        chat_id=self.chat_id,
                        text="\u2705 Pocket Option reconnected successfully after a connection error.",
                    )
                except Exception as exc:
                    logger.error(f"[{self.name}] Telegram notify error: {exc}")
                return

        self._reconnecting = False
        log_error(f"[{self.name}] All reconnect attempts failed — SSID needs manual refresh")
        try:
            await telegram_app.bot.send_message(
                chat_id=self.chat_id,
                text=(
                    "\u26a0\ufe0f Pocket Option connection lost and could not reconnect.\n\n"
                    "Your SSID has likely expired. To fix without redeploying:\n"
                    "1. Open pocketoption.com and log in\n"
                    "2. Press F12 \u2192 Network tab \u2192 filter WS\n"
                    '3. Find the message starting with 42["auth",\n'
                    "4. Copy the full string\n"
                    "5. Send: /setssid <paste the string here>\n\n"
                    "The bot will reconnect immediately."
                ),
            )
        except Exception as exc:
            logger.error(f"[{self.name}] Telegram notify error: {exc}")


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

    bullish_signals = 0
    bearish_signals = 0

    # RSI
    if rsi < 35:
        bullish_signals += 2
    elif rsi < 45:
        bullish_signals += 1
    elif rsi > 65:
        bearish_signals += 2
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

    # 3-candle momentum
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
        result["direction"]  = "WAIT"
        result["reason"]     = "Mixed signals \u2014 no clear edge"
        result["confidence"] = 50
        return result

    # Confidence score
    base_conf = 50 + raw_conf * 30

    if direction == "HIGHER" and rsi is not None:
        rsi_bonus = max(0, (40 - rsi) / 40) * 15
    elif direction == "LOWER" and rsi is not None:
        rsi_bonus = max(0, (rsi - 60) / 40) * 15
    else:
        rsi_bonus = 0

    tf_penalty = 0
    if timeframe_seconds <= 5:
        tf_penalty = 8
    elif timeframe_seconds <= 15:
        tf_penalty = 4

    confidence = min(97, max(51, int(base_conf + rsi_bonus - tf_penalty)))

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


def compute_signal(candles: list) -> tuple[str, str]:
    """Legacy wrapper used by the auto-broadcast loop."""
    r = compute_signal_advanced(candles)
    if r["direction"] == "HIGHER":
        return "CALL \u2705", r["reason"]
    if r["direction"] == "LOWER":
        return "PUT \u274c", r["reason"]
    return "WAIT \u23f8", r["reason"]


# ---------------------------------------------------------------------------
# Per-user polling loop
# ---------------------------------------------------------------------------

async def _user_poll_loop(us: UserSession) -> None:
    """Independent polling loop for one user's Pocket Option connection."""
    logger.info(f"[{us.name}] Polling loop started.")

    # Wait for initial connection
    for _ in range(30):
        if us.session_manager and us.session_manager.is_connected:
            break
        await asyncio.sleep(2)
    else:
        log_error(f"[{us.name}] Timed out waiting for initial connection")

    while True:
        try:
            if not (us.session_manager and us.session_manager.is_connected):
                await asyncio.sleep(10)
                continue

            candles = await us.session_manager.get_candles(
                asset=TRADE_ASSET, timeframe=CANDLE_PERIOD, count=CANDLE_COUNT
            )

            direction, reason = compute_signal(candles)
            price = None
            if candles:
                try:
                    price = float(candles[-1].close)
                except (AttributeError, TypeError, ValueError):
                    pass

            us.latest_signal = {
                "direction": direction,
                "price":     price,
                "reason":    reason,
                "timestamp": str(datetime.now()),
            }

            if direction != "WAIT \u23f8" and us.settings.get("auto", False):
                result    = compute_signal_advanced(candles)
                price_str = f"{price:.5f}" if price else "N/A"
                await _send_signal_message(
                    chat_id=us.chat_id,
                    result=result,
                    asset_label="EUR/USD (OTC)",
                    tf_label="1 minute",
                    price_str=price_str,
                )

        except Exception as exc:
            err = str(exc)
            log_error(f"[{us.name}] Poll error: {err}")
            if us.session_manager:
                await us.session_manager.notify_error(err)

        await asyncio.sleep(CANDLE_PERIOD)


# ---------------------------------------------------------------------------
# Signal fetching & formatting
# ---------------------------------------------------------------------------

async def _fetch_signal(asset_code: str, tf_seconds: int,
                        us: "UserSession | None" = None) -> dict:
    sm = us.session_manager if us else None
    if not (sm and sm.is_connected):
        return {
            "direction": "WAIT", "confidence": 0,
            "rsi": None, "reason": "Not connected to Pocket Option",
            "market": "Unknown",
        }
    try:
        candles = await sm.get_candles(
            asset=asset_code, timeframe=tf_seconds, count=CANDLE_COUNT
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
    """
    Returns a plain-text signal message with large BUY/SELL header.
    Used for inline menu display (edit_message_text).
    """
    direction  = result["direction"]
    confidence = result["confidence"]
    rsi        = result["rsi"]
    reason     = result["reason"]
    market     = result["market"]

    if direction == "HIGHER":
        header = "\U0001f4c8 BUY (CALL)"
    elif direction == "LOWER":
        header = "\U0001f4c9 SELL (PUT)"
    else:
        header = "\u23f8 WAIT \u2014 no clear signal"

    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    filled  = round(confidence / 20)
    bar     = "\u2588" * filled + "\u2591" * (5 - filled)

    return (
        f"{header}\n\n"
        f"Asset: {asset_label}\n"
        f"Timeframe: {tf_label}\n"
        f"Reliability: {confidence}%  {bar}\n"
        f"RSI: {rsi_str}\n"
        f"Market: {market}\n"
        f"Reason: {reason}\n"
        f"Time: {datetime.now().strftime('%H:%M:%S')}"
    )


def _signal_caption(result: dict, asset_label: str, tf_label: str,
                    price_str: str = "") -> str:
    """
    Caption for send_photo — same info as _format_signal but without
    the large header (the image carries the visual direction cue).
    """
    direction  = result["direction"]
    confidence = result["confidence"]
    rsi        = result["rsi"]
    reason     = result["reason"]
    market     = result["market"]

    action  = "BUY" if direction == "HIGHER" else "SELL"
    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    filled  = round(confidence / 20)
    bar     = "\u2588" * filled + "\u2591" * (5 - filled)

    lines = [
        f"{action} Signal",
        f"Asset: {asset_label}",
        f"Timeframe: {tf_label}",
    ]
    if price_str:
        lines.append(f"Price: {price_str}")
    lines += [
        f"Reliability: {confidence}%  {bar}",
        f"RSI: {rsi_str}",
        f"Market: {market}",
        f"Reason: {reason}",
        f"Time: {datetime.now().strftime('%H:%M:%S')}",
    ]
    return "\n".join(lines)


async def _send_signal_message(
    chat_id: int,
    result: dict,
    asset_label: str,
    tf_label: str,
    price_str: str = "",
) -> None:
    """
    Send a signal to a single chat_id with Win/Loss vote buttons.
    - HIGHER → send SIGNAL_IMG_BUY photo (fallback to text)
    - LOWER  → send SIGNAL_IMG_SELL photo (fallback to text)
    - WAIT   → skip (no broadcast)
    """
    direction = result["direction"]
    if direction == "WAIT":
        return

    signal_id = _make_signal_id(chat_id)
    pending_signals[signal_id] = {
        "user_id": chat_id,
        "ts":      time.time(),
        "voted":   False,
    }

    img_url  = SIGNAL_IMG_BUY if direction == "HIGHER" else SIGNAL_IMG_SELL
    caption  = _signal_caption(result, asset_label, tf_label, price_str)
    keyboard = _vote_keyboard(signal_id)

    if img_url:
        try:
            await telegram_app.bot.send_photo(
                chat_id=chat_id,
                photo=img_url,
                caption=caption,
                reply_markup=keyboard,
            )
            return
        except Exception as exc:
            logger.warning(f"send_photo failed for {chat_id}: {exc} — falling back to text")

    text = _format_signal(result, asset_label, tf_label)
    if price_str:
        text += f"\nPrice: {price_str}"
    try:
        await telegram_app.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
        )
    except Exception as exc:
        logger.error(f"send_message fallback also failed for {chat_id}: {exc}")


# ---------------------------------------------------------------------------
# Auth guard decorator
# ---------------------------------------------------------------------------

def _require_auth(func):
    """Decorator that rejects commands from unregistered chat IDs."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id not in user_registry:
            await update.effective_message.reply_text(
                "\u26d4 You are not authorized to use this bot."
            )
            return
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------

def _main_menu_keyboard(user_id: int, us: "UserSession | None" = None) -> InlineKeyboardMarkup:
    auto = us.settings.get("auto", False) if us else False
    auto_label = "\U0001f514 Auto-signals: ON" if auto else "\U0001f515 Auto-signals: OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Get Signal",     callback_data="menu:signal")],
        [InlineKeyboardButton("\u2139\ufe0f How it works", callback_data="menu:howto")],
        [InlineKeyboardButton("\u2699\ufe0f Settings",     callback_data="menu:settings")],
        [InlineKeyboardButton(auto_label,                  callback_data="menu:toggle_auto")],
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


def _settings_keyboard(user_id: int, us: "UserSession | None" = None) -> InlineKeyboardMarkup:
    s = us.settings if us else DEFAULT_SETTINGS
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Asset: {s['asset']}",         callback_data="settings:asset")],
        [InlineKeyboardButton(f"Timeframe: {s['timeframe']}", callback_data="settings:tf")],
        [InlineKeyboardButton("\U0001f519 Back",              callback_data="menu:back")],
    ])


# ---------------------------------------------------------------------------
# /start  — main menu
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    us = _get_user_session(chat_id)
    if not us:
        await update.effective_message.reply_text(
            "\u26d4 You are not authorized to use this bot."
        )
        return

    name  = update.effective_user.first_name or us.name
    po_ok = us.session_manager is not None and us.session_manager.is_connected
    conn_str = "\U0001f7e2 Live" if po_ok else "\U0001f534 Offline"

    text = (
        f"\U0001f44b Welcome, {name}!\n\n"
        "\U0001f4e1 OTC Signal Bot\n"
        f"Connection: {conn_str}\n\n"
        "Tap a button below, or use these commands:\n"
        "/signal \u2014 latest signal\n"
        "/stats \u2014 your win/loss statistics\n"
        "/autoon \u2014 enable auto-signals\n"
        "/autooff \u2014 disable auto-signals\n"
        "/status \u2014 connection status\n"
        "/getssid \u2014 show current SSID info\n"
        "/setssid <string> \u2014 update SSID\n"
        "/reconnect \u2014 force reconnect\n"
        "/logs \u2014 recent errors\n"
        "/debug \u2014 full diagnostics"
    )
    uid = update.effective_user.id
    if update.message:
        await update.message.reply_text(text, reply_markup=_main_menu_keyboard(uid, us))
    else:
        await update.callback_query.edit_message_text(text, reply_markup=_main_menu_keyboard(uid, us))


# ---------------------------------------------------------------------------
# Inline button router  (ConversationHandler states: CHOOSE_ASSET, CHOOSE_TIME)
# ---------------------------------------------------------------------------

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    data    = query.data
    user_id = query.from_user.id
    chat_id = query.message.chat.id

    us = _get_user_session(chat_id)
    if not us:
        await query.edit_message_text("\u26d4 You are not authorized to use this bot.")
        return ConversationHandler.END

    settings = us.settings

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
            reply_markup=_settings_keyboard(user_id, us),
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

        result = await _fetch_signal(asset_code, tf_seconds, us)

        if result["direction"] == "WAIT":
            await query.edit_message_text(
                _format_signal(result, asset_label, tf_label),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f504 Retry",        callback_data=f"tf:{tf_label}")],
                    [InlineKeyboardButton("\U0001f4ca Change Asset", callback_data="menu:signal")],
                    [InlineKeyboardButton("\U0001f3e0 Main Menu",    callback_data="menu:back")],
                ]),
            )
        else:
            await query.edit_message_text(
                _format_signal(result, asset_label, tf_label),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f504 New Signal",   callback_data=f"tf:{tf_label}")],
                    [InlineKeyboardButton("\U0001f4ca Change Asset", callback_data="menu:signal")],
                    [InlineKeyboardButton("\U0001f3e0 Main Menu",    callback_data="menu:back")],
                ]),
            )
            await _send_signal_message(
                chat_id=query.message.chat_id,
                result=result,
                asset_label=asset_label,
                tf_label=tf_label,
            )

        context.user_data["chosen_asset"] = asset_label
        return ConversationHandler.END

    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

@_require_auth
async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually request the latest signal for the user's default asset/timeframe."""
    us = _get_user_session(update.effective_chat.id)
    asset_label = us.settings["asset"]
    tf_label    = us.settings["timeframe"]
    asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
    tf_seconds  = TIMEFRAMES.get(tf_label, 60)

    await update.message.reply_text(
        f"\u23f3 Fetching signal for {asset_label} / {tf_label}..."
    )

    result = await _fetch_signal(asset_code, tf_seconds, us)

    if result["direction"] == "WAIT":
        await update.message.reply_text(_format_signal(result, asset_label, tf_label))
    else:
        await _send_signal_message(
            chat_id=update.effective_chat.id,
            result=result,
            asset_label=asset_label,
            tf_label=tf_label,
        )


@_require_auth
async def autoon_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enable auto-signals for this user."""
    us = _get_user_session(update.effective_chat.id)
    us.settings["auto"] = True
    await update.message.reply_text(
        "\U0001f514 Auto-signals enabled.\n"
        f"You will receive signals every {CANDLE_PERIOD // 60} minute(s).\n\n"
        "Use /autooff to disable, or toggle via the main menu."
    )


@_require_auth
async def autooff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Disable auto-signals for this user."""
    us = _get_user_session(update.effective_chat.id)
    us.settings["auto"] = False
    await update.message.reply_text(
        "\U0001f515 Auto-signals disabled.\n"
        "You will no longer receive scheduled signals.\n\n"
        "Use /autoon to re-enable, or tap the toggle in the main menu.\n"
        "You can still request signals manually via /signal or the menu."
    )


@_require_auth
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    us = _get_user_session(update.effective_chat.id)
    sm = us.session_manager
    po_ok = sm is not None and sm.is_connected
    connected_since = (
        sm.connected_at.strftime("%H:%M:%S")
        if sm and sm.connected_at else "never"
    )
    sig = us.latest_signal
    await update.message.reply_text(
        f"Bot Status\n"
        f"User: {us.name}\n"
        f"Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"PO connected: {'yes' if po_ok else 'no'}\n"
        f"Connected since: {connected_since}\n"
        f"Last auto-signal: {sig['direction'] or 'none'} @ {sig['timestamp'] or 'never'}"
    )


@_require_auth
async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    us = _get_user_session(update.effective_chat.id)
    sm = us.session_manager
    po_ok = sm is not None and sm.is_connected
    ssid = us.ssid
    ssid_preview = f"{ssid[:25]}...{ssid[-8:]}" if len(ssid) > 33 else ssid
    refresh_in = "unknown"
    if sm and sm.connected_at:
        next_refresh = sm.connected_at + timedelta(hours=PREEMPTIVE_REFRESH_HOURS)
        delta = next_refresh - datetime.now()
        h, rem = divmod(int(max(delta.total_seconds(), 0)), 3600)
        m = rem // 60
        refresh_in = f"{h}h {m}m" if delta.total_seconds() > 0 else "imminent"
    await update.message.reply_text(
        f"Debug Info\n"
        f"User: {us.name} (chat_id={us.chat_id})\n"
        f"IS_RENDER: {IS_RENDER}\n"
        f"RENDER_URL: {RENDER_EXTERNAL_URL or 'not set'}\n"
        f"SSID set: {'yes' if ssid else 'no'}\n"
        f"SSID preview: {ssid_preview}\n"
        f"PO connected: {'yes' if po_ok else 'no'}\n"
        f"Next pre-emptive refresh: {refresh_in}\n"
        f"Ping interval: {PING_INTERVAL_SECONDS}s\n"
        f"Last auto-signal: {us.latest_signal['direction'] or 'none'}\n"
        f"Last update: {us.latest_signal['timestamp'] or 'never'}"
    )


@_require_auth
async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_errors:
        await update.message.reply_text("No recent errors.")
        return
    await update.message.reply_text("Recent Errors:\n\n" + "\n".join(recent_errors[-10:]))


@_require_auth
async def reconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    us = _get_user_session(update.effective_chat.id)
    sm = us.session_manager
    await update.message.reply_text("Forcing reconnect to Pocket Option...")
    if sm:
        async with sm._lock:
            success = await sm._connect()
        if success:
            await update.message.reply_text("Reconnected successfully.")
        else:
            await update.message.reply_text(
                "Reconnect failed. Check /logs for details.\n"
                "If SSID is expired, use /setssid <new_ssid> to update it without redeploying."
            )
    else:
        await update.message.reply_text("Session manager not initialised yet.")


def _parse_uid_from_ssid(ssid: str) -> str | None:
    """Extract the uid value from a 42["auth",{...}] string."""
    try:
        payload = json.loads(ssid[2:])
        if isinstance(payload, list) and len(payload) >= 2:
            return str(payload[1].get("uid", ""))
    except Exception:
        pass
    return None


@_require_auth
async def setssid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Update the SSID at runtime without redeploying.

    Usage:
        /setssid 42["auth",{"session":"...","isDemo":1,"uid":123,"platform":1}]
    """
    us = _get_user_session(update.effective_chat.id)

    HELP = (
        "Usage: /setssid <full auth string>\n\n"
        "How to get a fresh SSID:\n"
        "1. Open pocketoption.com and log in (demo or real)\n"
        "2. Press F12 to open DevTools\n"
        "3. Go to Network tab and filter by WS\n"
        "4. Click the WebSocket connection\n"
        "5. Open the Messages tab\n"
        '6. Find the message starting with 42["auth",\n'
        "7. Copy the entire string\n"
        "8. Send: /setssid <paste here>\n\n"
        "Example:\n"
        '/setssid 42["auth",{"session":"abc123","isDemo":1,"uid":441012369,"platform":1}]'
    )

    if not context.args:
        await update.message.reply_text(HELP)
        return

    new_ssid = " ".join(context.args).strip()

    if not (new_ssid.startswith('42["auth"') or new_ssid.startswith("42['auth'")):
        await update.message.reply_text(
            '\u274c Invalid format \u2014 must start with: 42["auth",\n\n' + HELP
        )
        return

    if '"session"' not in new_ssid and "'session'" not in new_ssid:
        await update.message.reply_text(
            '\u274c Invalid SSID \u2014 missing "session" field.\n\n' + HELP
        )
        return

    if '"uid"' not in new_ssid and "'uid'" not in new_ssid:
        await update.message.reply_text(
            '\u274c Invalid SSID \u2014 missing "uid" field.\n\n' + HELP
        )
        return

    uid_str = _parse_uid_from_ssid(new_ssid)
    preview = f"{new_ssid[:30]}...{new_ssid[-10:]}" if len(new_ssid) > 40 else new_ssid

    us.ssid = new_ssid
    if us.session_manager:
        us.session_manager.ssid = new_ssid

    await update.message.reply_text(
        f"SSID received. Reconnecting...\n"
        f"Preview: {preview}\n"
        f"UID: {uid_str or 'could not parse'}"
    )

    if us.session_manager:
        async with us.session_manager._lock:
            success = await us.session_manager._connect()
        if success:
            await update.message.reply_text(
                f"\u2705 Connected successfully!\n"
                f"SSID starts with: {new_ssid[:30]}...\n"
                f"UID: {uid_str or 'N/A'}\n\n"
                "Also update the SSID env var on Render so it survives a restart."
            )
        else:
            await update.message.reply_text(
                "\u274c Reconnect failed \u2014 the SSID may be invalid or already expired.\n"
                "Check /logs for the exact error, then try a fresh SSID."
            )
    else:
        await update.message.reply_text("Session manager not initialised yet.")


@_require_auth
async def getssid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the current SSID preview and uid for debugging."""
    us = _get_user_session(update.effective_chat.id)
    sm = us.session_manager
    current_ssid = sm.ssid if sm else us.ssid
    uid_str      = _parse_uid_from_ssid(current_ssid) if current_ssid else None
    po_ok        = sm is not None and sm.is_connected

    if not current_ssid:
        await update.message.reply_text("No SSID is currently set.")
        return

    preview = (
        f"{current_ssid[:30]}...{current_ssid[-10:]}"
        if len(current_ssid) > 40 else current_ssid
    )

    connected_since = (
        sm.connected_at.strftime("%Y-%m-%d %H:%M:%S")
        if sm and sm.connected_at else "never"
    )

    await update.message.reply_text(
        f"Current SSID Info\n\n"
        f"User: {us.name}\n"
        f"Preview: {preview}\n"
        f"UID: {uid_str or 'could not parse'}\n"
        f"Connected: {'yes' if po_ok else 'no'}\n"
        f"Connected since: {connected_since}\n\n"
        "To update: /setssid <new_ssid>"
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the user's win/loss trading statistics."""
    uid = update.effective_user.id
    s   = _get_stats(uid)

    total   = s["total"]
    wins    = s["wins"]
    losses  = s["losses"]
    last_5  = " ".join(s["last_5"]) if s["last_5"] else "\u2014"

    win_pct  = f"{wins / total * 100:.1f}%" if total else "\u2014"
    loss_pct = f"{losses / total * 100:.1f}%" if total else "\u2014"

    await update.message.reply_text(
        f"\U0001f4ca Your Trading Stats\n\n"
        f"Total signals: {total}\n"
        f"Wins:   {wins} ({win_pct})\n"
        f"Losses: {losses} ({loss_pct})\n"
        f"Last 5: {last_5}"
    )


async def vote_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Win / Loss button presses on signal messages."""
    query = update.callback_query
    data  = query.data  # format: "vote:win:<signal_id>" or "vote:loss:<signal_id>"

    parts = data.split(":", 2)
    if len(parts) != 3:
        await query.answer("Invalid vote data.")
        return

    _, outcome, signal_id = parts
    user_id = query.from_user.id

    signal = pending_signals.get(signal_id)

    if signal is None:
        await query.answer("This signal is too old to vote on.")
        return

    if time.time() - signal["ts"] > SIGNAL_EXPIRY_SEC:
        await query.answer("This signal has expired (10 min limit).")
        pending_signals.pop(signal_id, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if signal["voted"]:
        await query.answer("Already recorded for this signal.")
        return

    won = outcome == "win"
    signal["voted"] = True
    _record_vote(user_id, won)

    label = "Win \U0001f44d" if won else "Loss \U0001f44e"
    await query.answer(f"Recorded as {label}!")

    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"\u2705 Recorded: {label}", callback_data="vote:noop")
            ]])
        )
    except Exception as exc:
        logger.warning(f"Could not edit vote buttons: {exc}")


async def vote_noop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Silently acknowledge taps on the 'Recorded' confirmation button."""
    await update.callback_query.answer("Already recorded.")


# ---------------------------------------------------------------------------
# Register handlers
# ---------------------------------------------------------------------------

conv_handler = ConversationHandler(
    entry_points=[
        CommandHandler("start", start),
        CallbackQueryHandler(button_handler, pattern="^(?!vote:)"),
    ],
    states={
        CHOOSE_ASSET: [CallbackQueryHandler(button_handler, pattern="^(?!vote:)")],
        CHOOSE_TIME:  [CallbackQueryHandler(button_handler, pattern="^(?!vote:)")],
    },
    fallbacks=[CommandHandler("start", start)],
    per_message=False,
)

telegram_app.add_handler(conv_handler)
telegram_app.add_handler(CallbackQueryHandler(vote_noop_handler, pattern="^vote:noop$"))
telegram_app.add_handler(CallbackQueryHandler(vote_handler,      pattern="^vote:(win|loss):"))
telegram_app.add_handler(CommandHandler("signal",    signal_command))
telegram_app.add_handler(CommandHandler("stats",     stats_command))
telegram_app.add_handler(CommandHandler("autoon",    autoon_command))
telegram_app.add_handler(CommandHandler("autooff",   autooff_command))
telegram_app.add_handler(CommandHandler("status",    status_command))
telegram_app.add_handler(CommandHandler("debug",     debug_command))
telegram_app.add_handler(CommandHandler("logs",      logs_command))
telegram_app.add_handler(CommandHandler("reconnect", reconnect_command))
telegram_app.add_handler(CommandHandler("setssid",   setssid_command))
telegram_app.add_handler(CommandHandler("getssid",   getssid_command))


# ---------------------------------------------------------------------------
# Telegram webhook registration
# ---------------------------------------------------------------------------

async def reset_and_set_webhook() -> None:
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL not set \u2014 cannot register webhook.")
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
    users_status = {
        str(cid): {
            "name":      us.name,
            "connected": us.session_manager is not None and us.session_manager.is_connected,
            "last_signal": us.latest_signal,
        }
        for cid, us in user_registry.items()
    }
    return JSONResponse({
        "status":    "healthy",
        "timestamp": str(datetime.now()),
        "users":     users_status,
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
    logger.info("Starting up...")
    await telegram_app.initialize()
    await telegram_app.start()
    await reset_and_set_webhook()
    _load_stats()

    # Build user registry and start per-user sessions
    configs = _load_user_configs()
    for cfg in configs:
        us = UserSession(
            name=cfg["name"],
            chat_id=cfg["chat_id"],
            ssid=cfg["ssid"],
            is_demo=cfg["is_demo"],
        )
        us.session_manager = SessionManager(
            ssid=cfg["ssid"],
            is_demo=cfg["is_demo"],
            chat_id=cfg["chat_id"],
            name=cfg["name"],
        )
        connected = await us.session_manager.start()
        if connected:
            logger.info(f"[{us.name}] Connected to Pocket Option")
        else:
            log_error(f"[{us.name}] Initial connection failed \u2014 will retry in poll loop")

        us.poll_task = asyncio.create_task(
            _user_poll_loop(us), name=f"poll_{us.chat_id}"
        )
        user_registry[us.chat_id] = us
        logger.info(f"Registered user: {us.name} (chat_id={us.chat_id})")

    yield  # ── server is running ──────────────────────────────────────────

    # Shutdown all user sessions
    for us in user_registry.values():
        if us.poll_task and not us.poll_task.done():
            us.poll_task.cancel()
            try:
                await us.poll_task
            except (asyncio.CancelledError, Exception):
                pass
        if us.session_manager:
            await us.session_manager.stop()

    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if IS_RENDER:
    web_app = Starlette(routes=routes, lifespan=lifespan)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"IS_RENDER=true \u2014 starting webhook server on port {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
else:
    logger.info("Local mode \u2014 starting polling...")
    telegram_app.run_polling()
