import os
import csv
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
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

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
# Cloudflare Worker SSID relay (optional — set WORKER_URL + WORKER_API_KEY)
# ---------------------------------------------------------------------------
WORKER_URL     = os.environ.get("WORKER_URL",     "").rstrip("/")
WORKER_API_KEY = os.environ.get("WORKER_API_KEY", "")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TRADE_ASSET   = "EURUSD_otc"
CANDLE_PERIOD = 60    # seconds for the auto-signal loop
CANDLE_COUNT  = 50    # candles to fetch (enough for RSI-14 + EMA-20)

# ---------------------------------------------------------------------------
# Multi-asset scanner configuration
# ---------------------------------------------------------------------------
_DEFAULT_SCAN_ASSETS = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc",
    "USDZARUSD_otc", "EURTRY_otc", "USDINR_otc",
]
_MAX_SCAN_ASSETS = 10   # Render free tier limit

def _load_scan_assets() -> list[str]:
    """Parse SCAN_ASSETS env var (comma-separated) or return defaults."""
    raw = os.environ.get("SCAN_ASSETS", "").strip()
    if raw:
        assets = [a.strip() for a in raw.split(",") if a.strip()]
        return assets[:_MAX_SCAN_ASSETS]
    return _DEFAULT_SCAN_ASSETS

SCAN_ASSETS: list[str] = _load_scan_assets()

# Default cooldown between signals for the same asset (seconds)
DEFAULT_COOLDOWN_SECONDS = 120

# ---------------------------------------------------------------------------
# Cloudflare Worker SSID relay helpers
# ---------------------------------------------------------------------------

async def fetch_ssid_from_worker() -> str | None:
    """
    Fetch the current SSID from the Cloudflare Worker relay.
    Returns the SSID string or None if unavailable/not configured.
    """
    if not WORKER_URL or not WORKER_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"{WORKER_URL}/ssid",
                headers={"X-API-Key": WORKER_API_KEY},
            )
            if r.status_code == 200:
                data = r.json()
                ssid = data.get("ssid", "")
                if ssid:
                    logger.info(f"SSID fetched from Worker (uid={data.get('uid')})")
                    return ssid
            else:
                logger.warning(f"Worker returned {r.status_code}: {r.text[:200]}")
    except Exception as exc:
        logger.warning(f"fetch_ssid_from_worker error: {exc}")
    return None


async def push_ssid_to_worker(ssid: str) -> bool:
    """
    Push a new SSID to the Worker (for record-keeping).
    Returns True on success.
    """
    if not WORKER_URL or not WORKER_API_KEY:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{WORKER_URL}/ssid",
                headers={"X-API-Key": WORKER_API_KEY, "Content-Type": "application/json"},
                json={"ssid": ssid},
            )
            return r.status_code == 200
    except Exception as exc:
        logger.warning(f"push_ssid_to_worker error: {exc}")
        return False


async def _worker_refresh_loop() -> None:
    """
    Background task: every 23 hours, fetch a fresh SSID from the Worker
    and apply it to all active user sessions.
    Falls back to Telegram alert if Worker is unavailable.
    """
    INTERVAL = PREEMPTIVE_REFRESH_HOURS * 3600
    logger.info(f"Worker refresh loop started (interval={PREEMPTIVE_REFRESH_HOURS}h)")

    while True:
        await asyncio.sleep(INTERVAL)

        if not WORKER_URL:
            continue   # Worker not configured — SessionManager handles its own refresh

        logger.info("Worker refresh loop: fetching fresh SSID...")
        new_ssid = await fetch_ssid_from_worker()

        if not new_ssid:
            # Worker failed — notify all users
            msg = (
                "⚠️ SSID Worker unreachable during scheduled refresh.\n\n"
                "Your session may expire soon. To refresh manually:\n"
                "1. Open pocketoption.com → log in\n"
                "2. F12 → Network → WS → copy 42[\"auth\",...]\n"
                "3. Send: /setssid <paste>\n\n"
                "Then update the Worker secret:\n"
                "wrangler secret put SSID"
            )
            for us in set(user_registry.values()):
                try:
                    await telegram_app.bot.send_message(chat_id=us.chat_id, text=msg)
                except Exception:
                    pass
            continue

        # Apply new SSID to all sessions
        updated = 0
        for us in set(user_registry.values()):
            if us.ssid != new_ssid:
                us.ssid = new_ssid
                if us.session_manager:
                    us.session_manager.ssid = new_ssid
                    async with us.session_manager._lock:
                        success = await us.session_manager._connect()
                    if success:
                        updated += 1
                        logger.info(f"[{us.name}] SSID refreshed from Worker")

        if updated:
            logger.info(f"Worker refresh: updated {updated} session(s)")

# ---------------------------------------------------------------------------
# Signal validation & paper trading constants
# ---------------------------------------------------------------------------
VALIDATION_SECONDS   = 60      # seconds after signal to auto-validate
PAPER_STAKE          = 1.0     # default stake per paper trade ($)
PAPER_PAYOUT         = 0.85    # payout ratio on win (85%)
PAPER_START_BALANCE  = 1000.0  # starting virtual balance
MIN_PRICE_MOVE       = 0.0001  # minimum move to count as win (forex OTC)
FOLLOW_INTERVAL_SEC  = 5       # price update interval when following
FOLLOW_DURATION_SEC  = 60      # total follow duration

# In-flight validation tasks: {signal_id: asyncio.Task}
_validation_tasks: dict[str, asyncio.Task] = {}
# In-flight follow tasks: {signal_id: asyncio.Task}  (prevent duplicates)
_follow_tasks: dict[str, asyncio.Task] = {}

# Active tracked trades: {chat_id: {trade_id: trade_dict}}
# trade_dict keys: asset_code, asset_label, direction, entry_price,
#                  message_id, task, tp_pips, sl_pips, status, ts
active_trades: dict[int, dict[str, dict]] = {}

# ---------------------------------------------------------------------------
# TradingSession — session-based martingale trading tracker
# ---------------------------------------------------------------------------

class TradingSession:
    """
    Tracks a single trading session with martingale step management.

    Attributes
    ----------
    chat_id         : Telegram chat ID of the user
    asset_label     : Display name (e.g. "EUR/USD (OTC)")
    asset_code      : API code (e.g. "EURUSD_otc")
    base_stake      : Starting stake amount
    step_multiplier : Multiplier per loss step (default 2.2)
    target_profit   : Session ends when net_profit >= this (default 73% of base)
    """

    PAYOUT_RATIO = 0.85   # 85% payout on win

    def __init__(self, chat_id: int, asset_label: str, asset_code: str,
                 base_stake: float, step_multiplier: float = 2.2,
                 target_pct: float = 0.73) -> None:
        self.chat_id         = chat_id
        self.asset_label     = asset_label
        self.asset_code      = asset_code
        self.base_stake      = base_stake
        self.step_multiplier = step_multiplier
        self.target_profit   = round(base_stake * target_pct, 2)
        self.net_profit      = 0.0
        self.current_step    = 0       # 0 = first trade at base_stake
        self.active          = True
        self.trade_count     = 0

        # Current trade state
        self.trade_direction: str | None = None
        self.entry_price:     float      = 0.0
        self.expiry_seconds:  int        = 60
        self.start_time:      float      = 0.0
        self.live_message_id: int | None = None
        self.update_task:     "asyncio.Task | None" = None

    # ── Stake management ─────────────────────────────────────────────── #

    def next_stake(self) -> float:
        """Return the stake for the current step."""
        return round(self.base_stake * (self.step_multiplier ** self.current_step), 2)

    def is_target_reached(self) -> bool:
        return self.net_profit >= self.target_profit

    # ── Trade recording ──────────────────────────────────────────────── #

    def record_win(self, profit: float | None = None) -> float:
        """Record a win. Returns profit amount."""
        stake  = self.next_stake()
        profit = profit if profit is not None else round(stake * self.PAYOUT_RATIO, 2)
        self.net_profit   = round(self.net_profit + profit, 2)
        self.current_step = 0   # reset to base stake after win
        self.trade_count += 1
        return profit

    def record_loss(self, loss: float | None = None) -> float:
        """Record a loss. Returns loss amount (negative)."""
        stake = self.next_stake()
        loss  = loss if loss is not None else -stake
        self.net_profit   = round(self.net_profit + loss, 2)
        self.current_step += 1   # advance martingale step
        self.trade_count  += 1
        return loss

    # ── Live tracking ────────────────────────────────────────────────── #

    def start_trade(self, direction: str, entry_price: float,
                    expiry_seconds: int = 60) -> None:
        self.trade_direction = direction
        self.entry_price     = entry_price
        self.expiry_seconds  = expiry_seconds
        self.start_time      = time.time()

    def stop_live_tracking(self) -> None:
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()
        self.update_task = None

    def summary(self) -> str:
        stake = self.next_stake()
        bar   = "█" * min(10, int(self.net_profit / max(self.target_profit, 0.01) * 10))
        bar  += "░" * (10 - len(bar))
        return (
            f"🚀 Session: {self.asset_label}\n"
            f"Base stake: ${self.base_stake:.2f}  "
            f"Target: +${self.target_profit:.2f} (73%)\n"
            f"Net P&L: {'+' if self.net_profit >= 0 else ''}{self.net_profit:.2f}$  "
            f"Step: {self.current_step + 1}\n"
            f"Progress: {bar}\n"
            f"Next stake: ${stake:.2f}  Trades: {self.trade_count}"
        )


# Global registry: {chat_id: TradingSession}
trading_sessions: dict[int, TradingSession] = {}

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
    # ── Major OTC pairs ──────────────────────────────────────────────────
    "EUR/USD (OTC)": "EURUSD_otc",
    "AUD/USD (OTC)": "AUDUSD_otc",
    "USD/JPY (OTC)": "USDJPY_otc",
    "GBP/USD (OTC)": "GBPUSD_otc",
    "USD/CAD (OTC)": "USDCAD_otc",
    "EUR/JPY (OTC)": "EURJPY_otc",
    # ── Emerging market exotic pairs ─────────────────────────────────────
    "USD/ZAR (OTC)": "USDZARUSD_otc",
    "USD/TRY (OTC)": "USDTRY_otc",
    "USD/MXN (OTC)": "USDMXN_otc",
    "USD/INR (OTC)": "USDINR_otc",
    "USD/BRL (OTC)": "USDBRL_otc",
    "USD/IDR (OTC)": "USDIDR_otc",
    # ── Minor exotic crosses ─────────────────────────────────────────────
    "EUR/TRY (OTC)": "EURTRY_otc",
    "GBP/ZAR (OTC)": "GBPZAR_otc",
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
    "strategy":  "trend",    # "trend" = MACD+RSI  |  "reversal" = Bollinger Bands
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
    Returns list of dicts: [{"name", "chat_id", "ssid", "is_demo", "broadcast_ids"}]

    Each entry may include an optional "broadcast_ids" list of extra chat IDs that
    receive auto-signal broadcasts from this account (in addition to chat_id itself).

    Alternatively, set BROADCAST_CHAT_IDS="id1,id2" as a global env var to add
    extra recipients to every user session.
    """
    # Global extra broadcast IDs (comma-separated env var)
    global_extra: list[int] = []
    raw_extra = os.environ.get("BROADCAST_CHAT_IDS", "").strip()
    if raw_extra:
        for part in raw_extra.split(","):
            part = part.strip()
            if part:
                try:
                    global_extra.append(int(part))
                except ValueError:
                    logger.warning(f"Invalid chat ID in BROADCAST_CHAT_IDS: {part!r}")
        if global_extra:
            logger.info(f"Global broadcast IDs: {global_extra}")

    raw = os.environ.get("USERS_CONFIG", "").strip()
    if raw:
        try:
            configs = json.loads(raw)
            result = []
            for c in configs:
                # Per-user extra broadcast IDs from config
                per_user_extra: list[int] = []
                for cid in c.get("broadcast_ids", []):
                    try:
                        per_user_extra.append(int(cid))
                    except (ValueError, TypeError):
                        pass
                # Merge global + per-user extras (deduplicated)
                all_extra = list(dict.fromkeys(per_user_extra + global_extra))
                result.append({
                    "name":          c.get("name", f"User_{c['chat_id']}"),
                    "chat_id":       int(c["chat_id"]),
                    "ssid":          c["ssid"],
                    "is_demo":       bool(int(c.get("is_demo", 1))),
                    "broadcast_ids": all_extra,
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
            "name":          "Default",
            "chat_id":       int(chat_id),
            "ssid":          ssid,
            "is_demo":       bool(int(os.environ.get("PO_DEMO", "1"))),
            "broadcast_ids": global_extra,
        }]

    raise ValueError("No user config found. Set USERS_CONFIG or SSID+CHAT_ID env vars.")

# ---------------------------------------------------------------------------
# UserSession dataclass
# ---------------------------------------------------------------------------

@dataclass
class UserSession:
    name:     str
    chat_id:  int          # primary chat — owns the SSID, receives commands
    ssid:     str
    is_demo:  bool

    # Additional chat IDs that receive auto-signal broadcasts from this account.
    # The primary chat_id is always included automatically.
    broadcast_ids: list[int] = field(default_factory=list)

    # Created after init
    session_manager: "SessionManager | None" = field(default=None, repr=False)
    settings: dict = field(default_factory=lambda: dict(DEFAULT_SETTINGS))
    latest_signal: dict = field(default_factory=lambda: {
        "direction": None, "price": None, "reason": None, "timestamp": None
    })
    poll_task: "asyncio.Task | None" = field(default=None, repr=False)

    def all_broadcast_ids(self) -> list[int]:
        """Return the full broadcast list: primary chat_id + any extras."""
        ids = [self.chat_id]
        for cid in self.broadcast_ids:
            if cid not in ids:
                ids.append(cid)
        return ids

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
# Trade logging (CSV per user)
# ---------------------------------------------------------------------------

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_CSV_HEADERS = [
    "timestamp", "asset", "direction", "result",
    "confidence", "rsi", "price", "timeframe",
    "market_condition", "reason",
]

USER_SETTINGS_FILE = os.path.join(_BASE_DIR, "user_settings.json")
# Per-user persistent settings: {str(chat_id): {"restrict_to_best_window": bool,
#                                                "best_window_start": int,
#                                                "best_window_end": int}}
user_persistent_settings: dict[str, dict] = {}


def _load_user_persistent_settings() -> None:
    global user_persistent_settings
    try:
        if os.path.exists(USER_SETTINGS_FILE):
            with open(USER_SETTINGS_FILE, "r", encoding="utf-8") as f:
                user_persistent_settings = json.load(f)
            logger.info(f"Loaded persistent settings for {len(user_persistent_settings)} users")
    except Exception as exc:
        logger.warning(f"Could not load user_settings.json: {exc}")


def _save_user_persistent_settings() -> None:
    try:
        with open(USER_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(user_persistent_settings, f, indent=2)
    except Exception as exc:
        logger.warning(f"Could not save user_settings.json: {exc}")


def _get_user_persistent(chat_id: int) -> dict:
    key = str(chat_id)
    if key not in user_persistent_settings:
        user_persistent_settings[key] = {
            "restrict_to_best_window": False,
            "best_window_start":       None,
            "best_window_end":         None,
            "news_filter_enabled":     True,
            "restrict_to_best_time":   False,
            "best_time_start":         None,
            "best_time_end":           None,
            "best_time_updated":       None,
            # Per-user signal settings (persisted so they survive restarts)
            "strategy":  "trend",   # "trend" | "reversal"
            "auto":      False,
            "asset":     "EUR/USD (OTC)",
            "timeframe": "1 minute",
        }
    # Back-fill keys added in later versions
    ps = user_persistent_settings[key]
    ps.setdefault("strategy",      "trend")
    ps.setdefault("auto",          False)
    ps.setdefault("asset",         "EUR/USD (OTC)")
    ps.setdefault("timeframe",     "1 minute")
    ps.setdefault("auto_strategy", True)
    ps.setdefault("scanner",       False)
    ps.setdefault("cooldown",         DEFAULT_COOLDOWN_SECONDS)
    ps.setdefault("auto_validate",    True)
    ps.setdefault("paper_balance",    PAPER_START_BALANCE)
    ps.setdefault("paper_stake",      PAPER_STAKE)
    ps.setdefault("energy",           1000)    # gamification energy
    ps.setdefault("is_demo",          True)    # demo/real mode
    ps.setdefault("virtual_balance",  0.0)     # session cumulative P&L
    return ps


# ---------------------------------------------------------------------------
# Economic Calendar Filter
# ---------------------------------------------------------------------------
# Source: ForexFactory weekly JSON (no auth required, updated weekly)
# URL: https://nfs.faireconomy.media/ff_calendar_thisweek.json
# Date format in response: ISO 8601 with UTC offset e.g. "2026-05-05T00:30:00-04:00"

# Maps asset labels used in this bot → relevant currency codes for news filtering
_ASSET_CURRENCIES: dict[str, list[str]] = {
    "EUR/USD (OTC)": ["EUR", "USD"],
    "AUD/USD (OTC)": ["AUD", "USD"],
    "USD/JPY (OTC)": ["USD", "JPY"],
    "GBP/USD (OTC)": ["GBP", "USD"],
    "USD/CAD (OTC)": ["USD", "CAD"],
    "EUR/JPY (OTC)": ["EUR", "JPY"],
    # Commodities — USD news affects Gold/Oil
    "XAUUSD_otc":    ["USD"],
    "USCrude_otc":   ["USD"],
}

# In-memory cache: list of {"title", "country", "dt_utc": datetime, "impact"}
_calendar_cache: list[dict] = []
_calendar_last_fetch: float = 0.0
_CALENDAR_FETCH_INTERVAL = 3600   # refresh every hour
_NEWS_BLOCK_MINUTES       = 10    # block signals N minutes before event


async def _refresh_calendar() -> None:
    """Fetch today's + this week's high-impact events from ForexFactory."""
    global _calendar_cache, _calendar_last_fetch
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            events = r.json()

        parsed = []
        for ev in events:
            if ev.get("impact") not in ("High",):
                continue
            try:
                # Parse ISO 8601 with offset → UTC datetime
                from datetime import timezone
                dt_str = ev["date"]
                # Python 3.11+ fromisoformat handles offset; earlier needs manual strip
                try:
                    dt_local = datetime.fromisoformat(dt_str)
                except ValueError:
                    # Fallback: strip offset and treat as UTC
                    dt_local = datetime.fromisoformat(dt_str[:19])
                    dt_local = dt_local.replace(tzinfo=timezone.utc)
                dt_utc = dt_local.astimezone(timezone.utc).replace(tzinfo=None)
                parsed.append({
                    "title":   ev.get("title", ""),
                    "country": ev.get("country", ""),
                    "dt_utc":  dt_utc,
                    "impact":  ev.get("impact", ""),
                })
            except Exception as exc:
                logger.debug(f"Calendar parse error for {ev}: {exc}")

        _calendar_cache = parsed
        _calendar_last_fetch = time.time()
        logger.info(f"Economic calendar refreshed: {len(parsed)} high-impact events this week")

    except Exception as exc:
        logger.warning(f"Could not refresh economic calendar: {exc}")


async def _ensure_calendar_fresh() -> None:
    """Refresh the calendar cache if it's older than _CALENDAR_FETCH_INTERVAL."""
    if time.time() - _calendar_last_fetch > _CALENDAR_FETCH_INTERVAL:
        await _refresh_calendar()


def is_high_impact_news_approaching(asset_label: str,
                                    window_minutes: int = _NEWS_BLOCK_MINUTES
                                    ) -> tuple[bool, str]:
    """
    Returns (True, event_description) if a high-impact news event is scheduled
    within the next `window_minutes` minutes for the currencies of `asset_label`.
    Returns (False, "") otherwise.
    """
    currencies = _ASSET_CURRENCIES.get(asset_label, [])
    if not currencies:
        # Unknown asset — check USD as a safe default
        currencies = ["USD"]

    now_utc = datetime.utcnow()
    cutoff  = now_utc + timedelta(minutes=window_minutes)

    for ev in _calendar_cache:
        if ev["country"] not in currencies:
            continue
        dt = ev["dt_utc"]
        if now_utc <= dt <= cutoff:
            mins_away = int((dt - now_utc).total_seconds() / 60)
            desc = f"{ev['title']} ({ev['country']}) in ~{mins_away}m"
            return True, desc

    return False, ""


def _news_filter_active(chat_id: int) -> bool:
    """Return True if the news filter is enabled for this user (default: True)."""
    return _get_user_persistent(chat_id).get("news_filter_enabled", True)


def _get_user_settings(chat_id: int) -> dict:
    """
    Returns the per-user signal settings dict (persisted in user_settings.json).
    This is keyed by chat_id so broadcast recipients each have independent settings.
    """
    return _get_user_persistent(chat_id)


def _csv_path(chat_id: int) -> str:
    return os.path.join(_BASE_DIR, f"trades_{chat_id}.csv")


def _log_trade(chat_id: int, signal_meta: dict, result: str) -> None:
    """Append one trade row to trades_<chat_id>.csv."""
    path = _csv_path(chat_id)
    write_header = not os.path.exists(path)
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_HEADERS)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "timestamp":        datetime.utcnow().isoformat(timespec="seconds"),
                "asset":            signal_meta.get("asset_label", "EUR/USD (OTC)"),
                "direction":        signal_meta.get("direction", ""),
                "result":           result,
                "confidence":       signal_meta.get("confidence", ""),
                "rsi":              signal_meta.get("rsi", ""),
                "price":            signal_meta.get("price", ""),
                "timeframe":        signal_meta.get("tf_label", "1 minute"),
                "market_condition": signal_meta.get("market", ""),
                "reason":           signal_meta.get("reason", ""),
            })
    except Exception as exc:
        logger.warning(f"Could not write trade log for {chat_id}: {exc}")


def _read_trades(chat_id: int) -> list[dict]:
    """Return all rows from trades_<chat_id>.csv as list of dicts."""
    path = _csv_path(chat_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        logger.warning(f"Could not read trade log for {chat_id}: {exc}")
        return []


# ---------------------------------------------------------------------------
# Paper trading & signal validation helpers
# ---------------------------------------------------------------------------

_PAPER_HEADERS     = ["timestamp", "asset", "direction", "stake",
                       "entry_price", "exit_price", "result", "pnl", "balance_after"]
_VALIDATION_HEADERS = ["timestamp", "asset", "direction", "entry_price",
                        "exit_price", "result", "validated_at"]


def _paper_path(chat_id: int) -> str:
    return os.path.join(_BASE_DIR, f"paper_{chat_id}.csv")


def _validation_path(chat_id: int) -> str:
    return os.path.join(_BASE_DIR, f"validation_{chat_id}.csv")


def _log_paper_trade(chat_id: int, asset: str, direction: str, stake: float,
                     entry_price: float, exit_price: float,
                     result: str, balance_after: float) -> None:
    path = _paper_path(chat_id)
    write_header = not os.path.exists(path)
    pnl = stake * PAPER_PAYOUT if result == "win" else -stake
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_PAPER_HEADERS)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "timestamp":    datetime.utcnow().isoformat(timespec="seconds"),
                "asset":        asset,
                "direction":    direction,
                "stake":        f"{stake:.2f}",
                "entry_price":  f"{entry_price:.5f}",
                "exit_price":   f"{exit_price:.5f}",
                "result":       result,
                "pnl":          f"{pnl:+.2f}",
                "balance_after": f"{balance_after:.2f}",
            })
    except Exception as exc:
        logger.warning(f"Could not write paper trade for {chat_id}: {exc}")


def _log_validation(chat_id: int, asset: str, direction: str,
                    entry_price: float, exit_price: float, result: str) -> None:
    path = _validation_path(chat_id)
    write_header = not os.path.exists(path)
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_VALIDATION_HEADERS)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "timestamp":    datetime.utcnow().isoformat(timespec="seconds"),
                "asset":        asset,
                "direction":    direction,
                "entry_price":  f"{entry_price:.5f}",
                "exit_price":   f"{exit_price:.5f}",
                "result":       result,
                "validated_at": datetime.utcnow().strftime("%H:%M:%S"),
            })
    except Exception as exc:
        logger.warning(f"Could not write validation for {chat_id}: {exc}")


def _read_paper_trades(chat_id: int) -> list[dict]:
    path = _paper_path(chat_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _read_validations(chat_id: int) -> list[dict]:
    path = _validation_path(chat_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


async def _get_current_price(us: "UserSession", asset_code: str) -> float | None:
    """Fetch the most recent close price for an asset."""
    sm = us.session_manager if us else None
    if not (sm and sm.is_connected):
        return None
    try:
        candles = await sm.get_candles(asset=asset_code, timeframe=60, count=2)
        if candles:
            c = candles[-1]
            return float(c["close"] if isinstance(c, dict) else c.close)
    except Exception:
        pass
    return None


def _ascii_candle(price_changes: list[float]) -> str:
    """Return a simple ASCII candle bar based on recent price changes."""
    if not price_changes:
        return "▬▬▬"
    net = sum(price_changes)
    if net > 0:
        strength = min(3, int(abs(net) / 0.00005) + 1)
        return "\U0001f7e9" + "█" * strength + "▬" * (3 - strength)
    elif net < 0:
        strength = min(3, int(abs(net) / 0.00005) + 1)
        return "\U0001f7e5" + "█" * strength + "▬" * (3 - strength)
    return "⬜▬▬▬"


async def _validate_signal(
    signal_id: str,
    chat_id: int,
    us: "UserSession",
    asset_code: str,
    asset_label: str,
    direction: str,
    entry_price: float,
    delay_secs: int = VALIDATION_SECONDS,
) -> None:
    """
    Wait delay_secs, then fetch price and auto-validate the signal.
    Updates paper balance and logs to validation CSV.
    """
    try:
        await asyncio.sleep(delay_secs)

        exit_price = await _get_current_price(us, asset_code)
        if exit_price is None:
            logger.warning(f"Validation: could not fetch price for {asset_code}")
            return

        move = exit_price - entry_price
        if direction == "CALL":
            result = "win" if move > MIN_PRICE_MOVE else "loss"
        else:
            result = "win" if move < -MIN_PRICE_MOVE else "loss"

        ps = _get_user_settings(chat_id)
        if not ps.get("auto_validate", True):
            return

        # Log validation
        _log_validation(chat_id, asset_label, direction, entry_price, exit_price, result)

        # Paper trading update
        stake   = float(ps.get("paper_stake", PAPER_STAKE))
        balance = float(ps.get("paper_balance", PAPER_START_BALANCE))
        pnl     = stake * PAPER_PAYOUT if result == "win" else -stake
        balance += pnl
        ps["paper_balance"] = round(balance, 2)
        _save_user_persistent_settings()
        _log_paper_trade(chat_id, asset_label, direction, stake,
                         entry_price, exit_price, result, balance)

        # Notify user
        icon = "✅ WIN" if result == "win" else "❌ LOSS"
        move_str = f"{move:+.5f}"
        try:
            await telegram_app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔍 Signal Validated ({delay_secs}s)\n\n"
                    f"Asset: {asset_label}\n"
                    f"Direction: {direction}\n"
                    f"Entry: {entry_price:.5f}\n"
                    f"Exit:  {exit_price:.5f}  ({move_str})\n"
                    f"Result: {icon}\n\n"
                    f"📊 Paper: {'+' if pnl >= 0 else ''}{pnl:.2f}$  "
                    f"Balance: {balance:.2f}$"
                ),
            )
        except Exception as exc:
            logger.warning(f"Validation notify error: {exc}")

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log_error(f"_validate_signal error: {exc}")
    finally:
        _validation_tasks.pop(signal_id, None)


async def _follow_price_task(
    signal_id: str,
    chat_id: int,
    message_id: int,
    us: "UserSession",
    asset_code: str,
    asset_label: str,
    direction: str,
    entry_price: float,
    duration: int = FOLLOW_DURATION_SEC,
    interval: int = FOLLOW_INTERVAL_SEC,
) -> None:
    """
    Edit the signal message every `interval` seconds with live price updates.
    Shows ASCII candle, price change, and countdown.
    """
    try:
        price_history: list[float] = [entry_price]
        elapsed = 0

        while elapsed < duration:
            await asyncio.sleep(interval)
            elapsed += interval

            current = await _get_current_price(us, asset_code)
            if current is None:
                continue

            price_history.append(current)
            if len(price_history) > 6:
                price_history.pop(0)

            changes = [price_history[i] - price_history[i-1]
                       for i in range(1, len(price_history))]
            candle  = _ascii_candle(changes)
            move    = current - entry_price
            move_pct = move / entry_price * 100 if entry_price else 0
            arrow   = "📈" if move >= 0 else "📉"
            remaining = duration - elapsed

            dir_icon = "✅" if (
                (direction == "CALL" and move > 0) or
                (direction == "PUT"  and move < 0)
            ) else "⚠️"

            text = (
                f"{arrow} {asset_label} ({direction}) | Entry: {entry_price:.5f}\n"
                f"Now: {current:.5f}  ({move:+.5f} / {move_pct:+.4f}%)  {dir_icon}\n"
                f"Candle: {candle}\n"
                f"Time remaining: {max(0, remaining)}s"
            )

            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                )
            except Exception:
                pass   # message may have been deleted

        # Final message
        final_price = price_history[-1] if price_history else entry_price
        final_move  = final_price - entry_price
        final_result = (
            "WIN ✅" if (
                (direction == "CALL" and final_move > MIN_PRICE_MOVE) or
                (direction == "PUT"  and final_move < -MIN_PRICE_MOVE)
            ) else "LOSS ❌"
        )
        try:
            await telegram_app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=(
                    f"🏁 {asset_label} ({direction}) — FINAL\n"
                    f"Entry: {entry_price:.5f}\n"
                    f"Exit:  {final_price:.5f}  ({final_move:+.5f})\n"
                    f"Result: {final_result}"
                ),
            )
        except Exception:
            pass

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log_error(f"_follow_price_task error: {exc}")
    finally:
        _follow_tasks.pop(signal_id, None)

def _win_rate(rows: list[dict]) -> float:
    if not rows:
        return 0.0
    wins = sum(1 for r in rows if r.get("result") == "win")
    return wins / len(rows) * 100


def _group_win_rate(rows: list[dict], key_fn) -> dict[str, tuple[int, int]]:
    """Returns {label: (wins, total)} grouped by key_fn(row)."""
    groups: dict[str, list] = {}
    for r in rows:
        label = key_fn(r)
        groups.setdefault(label, []).append(r)
    return {
        label: (sum(1 for r in g if r.get("result") == "win"), len(g))
        for label, g in sorted(groups.items())
    }


def _hour_window(row: dict) -> str:
    try:
        hour = int(row["timestamp"][11:13])
        start = (hour // 4) * 4
        return f"{start:02d}-{start+4:02d} UTC"
    except Exception:
        return "Unknown"


def _day_of_week(row: dict) -> str:
    try:
        from datetime import datetime as _dt
        dt = _dt.fromisoformat(row["timestamp"])
        return dt.strftime("%A")
    except Exception:
        return "Unknown"


def _confidence_bin(row: dict) -> str:
    try:
        c = int(row["confidence"])
        if c < 60:   return "50-59"
        if c < 70:   return "60-69"
        if c < 80:   return "70-79"
        if c < 90:   return "80-89"
        return "90-100"
    except Exception:
        return "Unknown"


def _rsi_bin(row: dict) -> str:
    try:
        r = float(row["rsi"])
        if r < 30:   return "<30"
        if r < 40:   return "30-40"
        if r < 60:   return "40-60"
        if r < 70:   return "60-70"
        return ">70"
    except Exception:
        return "Unknown"


def _best_window(rows: list[dict]) -> tuple[str | None, float]:
    """Return (window_label, win_rate%) for the best 4-hour UTC window."""
    groups = _group_win_rate(rows, _hour_window)
    best_label, best_wr = None, -1.0
    for label, (wins, total) in groups.items():
        if total >= 3:   # need at least 3 trades to be meaningful
            wr = wins / total * 100
            if wr > best_wr:
                best_wr, best_label = wr, label
    return best_label, best_wr


def _build_analysis(chat_id: int) -> str:
    rows = _read_trades(chat_id)
    if not rows:
        return "No trades recorded yet. Start trading and mark results with 👍/👎."

    total  = len(rows)
    wins   = sum(1 for r in rows if r.get("result") == "win")
    losses = total - wins
    wr     = wins / total * 100
    profit = wins - losses

    lines = [
        "📊 Trade Analysis",
        "",
        f"Total trades: {total}",
        f"Wins: {wins}  Losses: {losses}",
        f"Win rate: {wr:.1f}%",
        f"Simulated P&L: {'+'if profit>=0 else ''}{profit}$",
        "",
    ]

    def _section(title: str, groups: dict) -> list[str]:
        out = [title]
        for label, (w, t) in groups.items():
            bar = "█" * round(w/t*5) + "░" * (5 - round(w/t*5)) if t else "░░░░░"
            out.append(f"  {label}: {w}/{t} ({w/t*100:.0f}%) {bar}")
        return out + [""]

    lines += _section("📈 By Asset:", _group_win_rate(rows, lambda r: r.get("asset", "?")))
    lines += _section("🕐 By Time (UTC):", _group_win_rate(rows, _hour_window))
    lines += _section("📅 By Day:", _group_win_rate(rows, _day_of_week))
    lines += _section("🎯 By Confidence:", _group_win_rate(rows, _confidence_bin))
    lines += _section("📉 By RSI:", _group_win_rate(rows, _rsi_bin))

    best_win, worst_win = None, None
    best_wr_val, worst_wr_val = -1.0, 101.0
    for label, (w, t) in _group_win_rate(rows, _hour_window).items():
        if t >= 3:
            wr_val = w / t * 100
            if wr_val > best_wr_val:
                best_wr_val, best_win = wr_val, label
            if wr_val < worst_wr_val:
                worst_wr_val, worst_win = wr_val, label

    if best_win:
        lines.append(f"✅ Best window:  {best_win} ({best_wr_val:.0f}%)")
    if worst_win:
        lines.append(f"❌ Worst window: {worst_win} ({worst_wr_val:.0f}%)")

    return "\n".join(lines)


async def _maybe_send_10_trade_summary(chat_id: int) -> None:
    """Send a summary every 10 trades and suggest restricting to best window."""
    rows = _read_trades(chat_id)
    if not rows or len(rows) % 10 != 0:
        return

    last_10 = rows[-10:]
    wr_10   = _win_rate(last_10)
    best_label, best_wr = _best_window(rows)

    msg = (
        f"🔔 10-Trade Milestone ({len(rows)} total)\n\n"
        f"Last 10 win rate: {wr_10:.1f}%\n"
    )
    if best_label:
        msg += (
            f"Best time window: {best_label} ({best_wr:.0f}%)\n\n"
            f"💡 Your win rate is highest between {best_label}.\n"
            "Enable time-window restriction? Send /restrict_window to activate."
        )
    try:
        await telegram_app.bot.send_message(chat_id=chat_id, text=msg)
    except Exception as exc:
        logger.warning(f"Could not send 10-trade summary to {chat_id}: {exc}")


# ---------------------------------------------------------------------------
# Telegram app (created early so SessionManager can reference it)
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()

async def retry_async(func, max_retries: int = 5, base_delay: float = 2.0):
    """
    Exponential back-off retry wrapper for async callables.

    Usage:
        candles = await retry_async(lambda: sm.client.get_candles(...))

    Parameters
    ----------
    func        : zero-argument async callable to retry
    max_retries : maximum number of attempts (default 5)
    base_delay  : initial wait in seconds; doubles each attempt (default 2)
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as exc:
            last_exc = exc
            if attempt == max_retries - 1:
                raise
            wait = base_delay * (2 ** attempt)   # 2, 4, 8, 16, 32 …
            logger.warning(
                f"retry_async attempt {attempt + 1}/{max_retries} "
                f"failed: {exc}. Retrying in {wait:.0f}s…"
            )
            await asyncio.sleep(wait)
    raise last_exc  # unreachable but satisfies type checkers


# ---------------------------------------------------------------------------
# SessionManager
# ---------------------------------------------------------------------------

class SessionManager:
    """
    Wraps PocketOptionAsync (BinaryOptionsToolsV2) per user.

    BinaryOptionsToolsV2 differences from pocketoptionapi_async:
    - Client is created synchronously; Rust core connects in the background.
    - No is_connected property — use wait_for_assets() to confirm readiness.
    - Built-in auto-reconnect in the Rust core (no manual ping loop needed).
    - get_candles(asset, period_secs, offset_secs) returns List[Dict] with
      plain string keys: "open", "high", "low", "close", "time".
    - Pre-emptive 23h refresh still handled here to rotate the SSID.
    """

    def __init__(self, ssid: str, is_demo: bool = True,
                 chat_id: int = 0, name: str = "Unknown") -> None:
        self.ssid:         str  = ssid
        self.is_demo:      bool = is_demo
        self.chat_id:      int  = chat_id
        self.name:         str  = name
        self.client:       "PocketOptionAsync | None" = None
        self.connected_at: "datetime | None" = None
        self._ready:       bool = False

        # Pre-emptive refresh task
        self._refresh_task: "asyncio.Task | None" = None

        # Reconnect guard
        self._reconnecting = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    @property
    def is_connected(self) -> bool:
        """True once the client has been created and assets loaded."""
        return self._ready and self.client is not None

    async def start(self) -> bool:
        """Create client and wait for the connection to be ready."""
        async with self._lock:
            return await self._connect()

    async def stop(self) -> None:
        """Cancel background tasks and disconnect cleanly."""
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        self._refresh_task = None
        if self.client:
            try:
                await self.client.disconnect()
            except Exception as exc:
                logger.warning(f"[{self.name}] stop disconnect error: {exc}")
        self.client = None
        self._ready = False
        self.connected_at = None
        logger.info(f"[{self.name}] SessionManager stopped.")

    async def get_candles(self, asset: str, timeframe: int, count: int) -> list:
        """
        Fetch candles using BinaryOptionsToolsV2 API.

        BinaryOptionsToolsV2.get_candles(asset, period_secs, offset_secs)
        where offset_secs = count * period_secs gives enough history.
        Returns List[Dict] with keys: time, open, high, low, close.
        """
        if not self.is_connected or self.client is None:
            await self._handle_error("get_candles called while disconnected")
            return []
        offset = count * timeframe   # total seconds of history to fetch
        try:
            return await retry_async(
                lambda: self.client.get_candles(asset, timeframe, offset),
                max_retries=5,
                base_delay=2.0,
            )
        except Exception as exc:
            await self._handle_error(f"get_candles error after retries: {exc}")
            return []

    async def notify_error(self, raw_error: str) -> None:
        lower = raw_error.lower()
        if any(m in lower for m in _AUTH_ERROR_MARKERS):
            await self._handle_error(raw_error)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _connect(self) -> bool:
        """Create a new PocketOptionAsync client and wait for readiness."""
        # Tear down existing client
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None
            self._ready = False

        try:
            logger.info(f"[{self.name}] Creating PocketOptionAsync client…")
            # PocketOptionAsync.__init__ is synchronous; Rust connects in background
            self.client = PocketOptionAsync(ssid=self.ssid)

            # Wait up to 60 s for the connection and asset list to be ready
            logger.info(f"[{self.name}] Waiting for assets (up to 60s)…")
            await self.client.wait_for_assets(timeout=60.0)

        except Exception as exc:
            log_error(f"[{self.name}] Connect exception: {exc}")
            self.client = None
            self._ready = False
            return False

        self._ready = True
        self.connected_at = datetime.now()
        logger.info(
            f"[{self.name}] Connected at {self.connected_at.strftime('%H:%M:%S')}"
        )

        # Start pre-emptive 23h refresh
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        self._refresh_task = asyncio.create_task(
            self._refresh_loop(), name=f"po_refresh_{self.chat_id}"
        )
        return True

    # ── Pre-emptive 23-hour refresh ───────────────────────────────────── #

    async def _refresh_loop(self) -> None:
        """Reconnect every PREEMPTIVE_REFRESH_HOURS to beat session expiry."""
        interval = PREEMPTIVE_REFRESH_HOURS * 3600
        logger.info(f"[{self.name}] Refresh loop started ({PREEMPTIVE_REFRESH_HOURS}h)")
        await asyncio.sleep(interval)
        logger.info(f"[{self.name}] Pre-emptive session refresh triggered")
        async with self._lock:
            await self._connect()

    # ── Error handler ─────────────────────────────────────────────────── #

    async def _handle_error(self, error_msg: str) -> None:
        """Reconnect with exponential back-off; alert user if all fail."""
        if self._reconnecting:
            return
        self._reconnecting = True
        log_error(f"[{self.name}] Error: {error_msg}")

        delays = [5, 30, 120]
        for attempt, delay in enumerate(delays, start=1):
            logger.info(f"[{self.name}] Reconnect {attempt}/{len(delays)} in {delay}s…")
            await asyncio.sleep(delay)
            async with self._lock:
                success = await self._connect()
            if success:
                logger.info(f"[{self.name}] Reconnect succeeded on attempt {attempt}")
                self._reconnecting = False
                try:
                    await telegram_app.bot.send_message(
                        chat_id=self.chat_id,
                        text="\u2705 Pocket Option reconnected successfully.",
                    )
                except Exception:
                    pass
                return

        self._reconnecting = False
        log_error(f"[{self.name}] All reconnect attempts failed — SSID needs refresh")
        try:
            await telegram_app.bot.send_message(
                chat_id=self.chat_id,
                text=(
                    "\u26a0\ufe0f Pocket Option connection lost.\n\n"
                    "Your SSID has likely expired. To fix:\n"
                    "1. Open pocketoption.com and log in\n"
                    "2. F12 \u2192 Network \u2192 WS filter\n"
                    '3. Find 42["auth",... message\n'
                    "4. Send: /setssid <paste here>"
                ),
            )
        except Exception as exc:
            logger.error(f"[{self.name}] Telegram notify error: {exc}")


# ---------------------------------------------------------------------------
# Technical analysis helpers (pure Python, no extra libraries)
# ---------------------------------------------------------------------------

def _closes(candles: list) -> list[float]:
    """Extract closing prices. Handles both dict candles (BinaryOptionsToolsV2)
    and object candles (legacy pocketoptionapi_async)."""
    result = []
    for c in candles:
        if isinstance(c, dict):
            result.append(float(c["close"]))
        else:
            result.append(float(c.close))
    return result


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


def calculate_macd(
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[float | None, float | None, float | None]:
    """
    Pure-Python MACD calculation.

    Returns (macd_line, signal_line, histogram) using the most recent values,
    or (None, None, None) if there is not enough data.

    Parameters
    ----------
    closes  : list of closing prices (oldest → newest)
    fast    : fast EMA period  (default 12)
    slow    : slow EMA period  (default 26)
    signal  : signal EMA period (default 9)
    """
    # Need at least slow + signal - 1 candles to produce one signal value
    min_len = slow + signal - 1
    if len(closes) < min_len:
        return None, None, None

    fast_ema  = _ema(closes, fast)
    slow_ema  = _ema(closes, slow)

    if not fast_ema or not slow_ema:
        return None, None, None

    # Align: fast_ema is longer than slow_ema by (slow - fast) elements
    offset     = len(fast_ema) - len(slow_ema)
    macd_line  = [f - s for f, s in zip(fast_ema[offset:], slow_ema)]

    signal_ema = _ema(macd_line, signal)
    if not signal_ema:
        return None, None, None

    # Return the most recent values
    macd_val   = macd_line[-1]
    signal_val = signal_ema[-1]
    hist_val   = macd_val - signal_val

    return macd_val, signal_val, hist_val


def calculate_bollinger_bands(
    closes: list[float],
    period: int = 20,
    std_dev: float = 2.0,
) -> tuple[float | None, float | None, float | None]:
    """
    Pure-Python Bollinger Bands calculation.

    Returns (middle, upper, lower) using the most recent `period` closes,
    or (None, None, None) if there is not enough data.

    Parameters
    ----------
    closes  : list of closing prices (oldest → newest)
    period  : SMA period (default 20)
    std_dev : number of standard deviations for the bands (default 2)
    """
    if len(closes) < period:
        return None, None, None

    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    sd = variance ** 0.5

    upper = middle + std_dev * sd
    lower = middle - std_dev * sd
    return middle, upper, lower


def calculate_adx(
    high: list[float],
    low:  list[float],
    close: list[float],
    period: int = 14,
) -> float | None:
    """
    Pure-Python Average Directional Index (ADX) using Wilder smoothing.

    Returns a float 0-100, or None if there is not enough data.
    ADX > 25 → strong trend   (prefer MACD+RSI)
    ADX < 20 → ranging market (prefer Bollinger reversal)

    Parameters
    ----------
    high, low, close : price lists (oldest → newest), equal length
    period           : smoothing period (default 14)
    """
    n = len(close)
    if n < period + 1 or len(high) < n or len(low) < n:
        return None

    # True Range and Directional Movement
    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        h, l, pc = high[i], low[i], close[i - 1]
        tr  = max(h - l, abs(h - pc), abs(l - pc))
        pdm = max(high[i] - high[i - 1], 0) if (high[i] - high[i - 1]) > (low[i - 1] - low[i]) else 0
        ndm = max(low[i - 1] - low[i], 0)   if (low[i - 1] - low[i]) > (high[i] - high[i - 1]) else 0
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    if len(tr_list) < period:
        return None

    # Wilder smoothing (initial sum then rolling)
    def _wilder(values: list[float], p: int) -> list[float]:
        smoothed = [sum(values[:p])]
        for v in values[p:]:
            smoothed.append(smoothed[-1] - smoothed[-1] / p + v)
        return smoothed

    atr  = _wilder(tr_list,  period)
    pdi  = _wilder(pdm_list, period)
    ndi  = _wilder(ndm_list, period)

    # DX and ADX
    dx_list = []
    for a, p, nd in zip(atr, pdi, ndi):
        if a == 0:
            continue
        pdi_val = 100 * p / a
        ndi_val = 100 * nd / a
        denom   = pdi_val + ndi_val
        dx_list.append(100 * abs(pdi_val - ndi_val) / denom if denom else 0)

    if len(dx_list) < period:
        return None

    # ADX = Wilder smoothed DX
    adx_series = _wilder(dx_list, period)
    return round(adx_series[-1], 2)


def _adx_strategy_recommendation(adx: float | None) -> str:
    """Return the recommended strategy name based on ADX value."""
    if adx is None:
        return "trend"   # default when not enough data
    if adx > 25:
        return "trend"
    if adx < 20:
        return "reversal"
    return "trend"       # weak zone — default to trend


def calculate_atr(high: list[float], low: list[float],
                  close: list[float], period: int = 14) -> float | None:
    """
    Average True Range — measures market volatility.
    Returns ATR value or None if insufficient data.
    Used for TP/SL suggestion and position sizing.
    """
    n = len(close)
    if n < period + 1 or len(high) < n or len(low) < n:
        return None
    tr_list = []
    for i in range(1, n):
        tr = max(high[i] - low[i],
                 abs(high[i] - close[i-1]),
                 abs(low[i]  - close[i-1]))
        tr_list.append(tr)
    if len(tr_list) < period:
        return None
    # Wilder smoothing
    atr = sum(tr_list[:period]) / period
    for tr in tr_list[period:]:
        atr = (atr * (period - 1) + tr) / period
    return round(atr, 6)


def compute_signal_tick(ticks: list[float], window: int = 5) -> dict:
    """
    Tick-based micro-window strategy.
    Uses the last `window` ticks to detect momentum.

    CALL if: last `window` ticks are all rising AND net move > MIN_PRICE_MOVE
    PUT  if: last `window` ticks are all falling AND net move < -MIN_PRICE_MOVE

    Returns same dict shape as compute_signal_advanced.
    """
    result = {
        "direction":          "WAIT",
        "confidence":         0,
        "rsi":                None,
        "macd":               None,
        "macd_signal":        None,
        "histogram":          None,
        "reason":             "Not enough tick data",
        "market":             "Tick",
        "strategy":           "Tick",
        "effective_strategy": "Tick",
        "adx":                None,
    }

    if len(ticks) < window + 1:
        return result

    recent = ticks[-(window + 1):]
    moves  = [recent[i] - recent[i-1] for i in range(1, len(recent))]
    net    = sum(moves)

    all_up   = all(m > 0 for m in moves)
    all_down = all(m < 0 for m in moves)

    if all_up and net > MIN_PRICE_MOVE:
        confidence = min(90, 60 + int(net / MIN_PRICE_MOVE * 5))
        result.update({
            "direction":  "HIGHER",
            "confidence": confidence,
            "reason":     f"Tick: {window} consecutive up moves, net={net:+.5f}",
        })
    elif all_down and net < -MIN_PRICE_MOVE:
        confidence = min(90, 60 + int(abs(net) / MIN_PRICE_MOVE * 5))
        result.update({
            "direction":  "LOWER",
            "confidence": confidence,
            "reason":     f"Tick: {window} consecutive down moves, net={net:+.5f}",
        })
    else:
        result["reason"] = (
            f"Tick: mixed moves over {window} ticks, net={net:+.5f}"
        )
    return result


def compute_signal_bollinger(
    candles: list,
    timeframe_seconds: int = 60,
) -> dict:
    """
    Bollinger Bands reversal strategy.

    CALL when:
      - Previous close < lower band AND current close >= lower band (price bouncing up)
      - AND RSI < 45 (confirms oversold)

    PUT when:
      - Previous close > upper band AND current close <= upper band (price bouncing down)
      - AND RSI > 55 (confirms overbought)

    Confidence is based on how far the price penetrated outside the band.

    Returns the same dict shape as compute_signal_advanced.
    """
    result = {
        "direction":   "WAIT",
        "confidence":  0,
        "rsi":         None,
        "macd":        None,
        "macd_signal": None,
        "histogram":   None,
        "reason":      "Not enough data",
        "market":      "Unknown",
        "strategy":    "Bollinger Reversal",
    }

    if not candles or len(candles) < 22:   # need 20 for BB + 2 for prev/curr
        return result

    closes = _closes(candles)
    rsi    = _rsi(closes, 14)
    market = _market_condition(candles)

    result["rsi"]    = rsi
    result["market"] = market

    if rsi is None:
        result["reason"] = "Insufficient RSI data"
        return result

    middle, upper, lower = calculate_bollinger_bands(closes, period=20, std_dev=2.0)
    if middle is None:
        result["reason"] = "Insufficient Bollinger Bands data"
        return result

    curr_close = closes[-1]
    prev_close = closes[-2]
    band_width = upper - lower if upper != lower else 1e-10

    logger.debug(
        f"BB Reversal | RSI={rsi:.1f} | Price={curr_close:.5f} "
        f"| Upper={upper:.5f} | Lower={lower:.5f} | Middle={middle:.5f}"
    )

    direction  = "WAIT"
    confidence = 0
    reason     = ""

    # ── CALL: price bouncing off lower band ──────────────────────────────
    if prev_close < lower and curr_close >= lower and rsi < 45:
        direction = "HIGHER"
        # How far below the band the previous candle was (as % of band width)
        penetration = (lower - prev_close) / band_width
        conf_pts    = min(25, penetration * 500)   # scale to 0-25 pts
        rsi_pts     = max(0, (45 - rsi) / 45) * 10
        confidence  = min(95, max(60, int(65 + conf_pts + rsi_pts)))
        reason = (
            f"Price bounced off lower BB ({lower:.5f}), "
            f"RSI={rsi:.1f} (oversold), penetration={penetration:.3f}"
        )

    # ── PUT: price bouncing off upper band ───────────────────────────────
    elif prev_close > upper and curr_close <= upper and rsi > 55:
        direction = "LOWER"
        penetration = (prev_close - upper) / band_width
        conf_pts    = min(25, penetration * 500)
        rsi_pts     = max(0, (rsi - 55) / 45) * 10
        confidence  = min(95, max(60, int(65 + conf_pts + rsi_pts)))
        reason = (
            f"Price bounced off upper BB ({upper:.5f}), "
            f"RSI={rsi:.1f} (overbought), penetration={penetration:.3f}"
        )

    else:
        # Explain why no signal
        if rsi < 45 and curr_close > lower:
            reason = f"RSI={rsi:.1f} oversold but price not touching lower BB ({lower:.5f})"
        elif rsi > 55 and curr_close < upper:
            reason = f"RSI={rsi:.1f} overbought but price not touching upper BB ({upper:.5f})"
        else:
            reason = (
                f"Price within bands (L={lower:.5f} M={middle:.5f} U={upper:.5f}), "
                f"RSI={rsi:.1f} — no reversal signal"
            )
        result["reason"] = reason
        return result

    # Timeframe noise penalty
    if timeframe_seconds <= 5:
        confidence = max(55, confidence - 8)
    elif timeframe_seconds <= 15:
        confidence = max(55, confidence - 4)

    result.update({
        "direction":  direction,
        "confidence": confidence,
        "reason":     reason,
    })
    return result


def _market_condition(candles: list) -> str:
    """Classify market as Trending, Ranging, or Volatile."""
    if len(candles) < 10:
        return "Unknown"
    closes = _closes(candles[-20:])
    # Support both dict candles (BinaryOptionsToolsV2) and object candles
    sample = candles[-1]
    if isinstance(sample, dict):
        highs = [float(c["high"]) for c in candles[-20:]]
        lows  = [float(c["low"])  for c in candles[-20:]]
    elif hasattr(sample, "high"):
        highs = [float(c.high) for c in candles[-20:]]
        lows  = [float(c.low)  for c in candles[-20:]]
    else:
        highs = closes
        lows  = closes
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


def _bollinger_ranging_signal(
    closes: list[float],
    rsi: float,
    timeframe_seconds: int = 60,
) -> dict:
    """
    Bollinger Bands proximity signal for ranging markets.
    Used as a fallback inside compute_signal_advanced when MACD+RSI gives no signal.

    CALL if: price within 0.05% of lower band AND RSI < 45
    PUT  if: price within 0.05% of upper band AND RSI > 55
    Confidence = 70 + (0.05 - dist) * 100, capped at 85.
    """
    base = {"direction": "WAIT", "confidence": 0, "strategy": "BB-Ranging"}

    middle, upper, lower = calculate_bollinger_bands(closes, period=20, std_dev=2.0)
    if middle is None:
        base["reason"] = "No signal — ranging without band touch (insufficient BB data)"
        return base

    last_close = closes[-1]
    if last_close <= 0:
        base["reason"] = "No signal — invalid price"
        return base

    dist_to_lower = (last_close - lower) / last_close * 100
    dist_to_upper = (upper - last_close) / last_close * 100

    logger.debug(
        f"BB-Ranging | RSI={rsi:.1f} | Price={last_close:.5f} "
        f"| Upper={upper:.5f} | Lower={lower:.5f} "
        f"| dist_lower={dist_to_lower:.4f}% | dist_upper={dist_to_upper:.4f}%"
    )

    THRESHOLD = 0.05   # 0.05% proximity threshold

    if dist_to_lower <= THRESHOLD and rsi < 45:
        raw_conf   = 70 + (THRESHOLD - dist_to_lower) * 100
        confidence = min(85, max(60, int(raw_conf)))
        if timeframe_seconds <= 5:
            confidence = max(55, confidence - 8)
        elif timeframe_seconds <= 15:
            confidence = max(55, confidence - 4)
        return {
            "direction":  "HIGHER",
            "confidence": confidence,
            "strategy":   "BB-Ranging",
            "reason": (
                f"Ranging: price near lower BB ({lower:.5f}), "
                f"dist={dist_to_lower:.4f}%, RSI={rsi:.1f} (oversold)"
            ),
        }

    if dist_to_upper <= THRESHOLD and rsi > 55:
        raw_conf   = 70 + (THRESHOLD - dist_to_upper) * 100
        confidence = min(85, max(60, int(raw_conf)))
        if timeframe_seconds <= 5:
            confidence = max(55, confidence - 8)
        elif timeframe_seconds <= 15:
            confidence = max(55, confidence - 4)
        return {
            "direction":  "LOWER",
            "confidence": confidence,
            "strategy":   "BB-Ranging",
            "reason": (
                f"Ranging: price near upper BB ({upper:.5f}), "
                f"dist={dist_to_upper:.4f}%, RSI={rsi:.1f} (overbought)"
            ),
        }

    base["reason"] = (
        f"No signal — ranging without band touch "
        f"(dist_lower={dist_to_lower:.4f}%, dist_upper={dist_to_upper:.4f}%, RSI={rsi:.1f})"
    )
    return base


def compute_signal_advanced(
    candles: list,
    timeframe_seconds: int = 60,
) -> dict:
    """
    Hybrid MACD + RSI signal engine.

    Primary strategy (requires both confirmations):
      CALL  →  RSI < 35  AND  MACD line > Signal line  (oversold + bullish momentum)
      PUT   →  RSI > 65  AND  MACD line < Signal line  (overbought + bearish momentum)

    Fallback (when MACD data is insufficient):
      Uses the original RSI + EMA crossover logic with a reduced confidence cap.

    Returns a dict with keys:
      direction   – "HIGHER" | "LOWER" | "WAIT"
      confidence  – int 0-100
      rsi         – float | None
      macd        – float | None   (MACD line value)
      macd_signal – float | None   (Signal line value)
      histogram   – float | None
      reason      – str
      market      – str
      strategy    – "MACD+RSI" | "RSI+EMA (fallback)"
    """
    result = {
        "direction":   "WAIT",
        "confidence":  0,
        "rsi":         None,
        "macd":        None,
        "macd_signal": None,
        "histogram":   None,
        "reason":      "Not enough data",
        "market":      "Unknown",
        "strategy":    "MACD+RSI",
    }

    if not candles or len(candles) < 20:
        return result

    closes = _closes(candles)
    rsi    = _rsi(closes, 14)
    market = _market_condition(candles)

    result["rsi"]    = rsi
    result["market"] = market

    if rsi is None:
        result["reason"] = "Insufficient RSI data"
        return result

    # ── Try primary MACD+RSI strategy ────────────────────────────────────
    macd_val, signal_val, hist_val = calculate_macd(closes)

    if macd_val is not None:
        result["macd"]        = round(macd_val,   6)
        result["macd_signal"] = round(signal_val, 6)
        result["histogram"]   = round(hist_val,   6)
        result["strategy"]    = "MACD+RSI"

        logger.debug(
            f"MACD+RSI | RSI={rsi:.1f} | MACD={macd_val:.6f} "
            f"| Signal={signal_val:.6f} | Hist={hist_val:.6f}"
        )

        direction = "WAIT"
        confidence = 0

        if rsi < 35 and macd_val > signal_val:
            direction = "HIGHER"
            # Confidence: RSI extremity (0-20 pts) + histogram strength (0-10 pts)
            rsi_pts  = max(0, (35 - rsi) / 35) * 20
            hist_pts = min(10, abs(hist_val) * 1e5)   # scale tiny forex values
            confidence = min(97, max(60, int(70 + rsi_pts + hist_pts)))
            reason = (
                f"RSI={rsi:.1f} (oversold), MACD crossover bullish "
                f"(hist={hist_val:+.6f})"
            )

        elif rsi > 65 and macd_val < signal_val:
            direction = "LOWER"
            rsi_pts  = max(0, (rsi - 65) / 35) * 20
            hist_pts = min(10, abs(hist_val) * 1e5)
            confidence = min(97, max(60, int(70 + rsi_pts + hist_pts)))
            reason = (
                f"RSI={rsi:.1f} (overbought), MACD crossover bearish "
                f"(hist={hist_val:+.6f})"
            )

        else:
            # MACD+RSI conditions not met — try Bollinger fallback for ranging markets
            if market == "Ranging" and len(closes) >= 22:
                bb_result = _bollinger_ranging_signal(closes, rsi, timeframe_seconds)
                if bb_result["direction"] != "WAIT":
                    # Merge BB result into our result dict
                    result.update(bb_result)
                    result["macd"]        = round(macd_val,   6)
                    result["macd_signal"] = round(signal_val, 6)
                    result["histogram"]   = round(hist_val,   6)
                    return result
                # BB also found nothing — use its reason
                result["reason"] = bb_result["reason"]
            else:
                if rsi < 35:
                    reason = f"RSI={rsi:.1f} oversold but MACD not yet bullish (hist={hist_val:+.6f})"
                elif rsi > 65:
                    reason = f"RSI={rsi:.1f} overbought but MACD not yet bearish (hist={hist_val:+.6f})"
                else:
                    reason = f"RSI={rsi:.1f} neutral — waiting for RSI extreme + MACD confirmation"
                result["reason"] = reason
            return result

        # Timeframe noise penalty
        if timeframe_seconds <= 5:
            confidence = max(55, confidence - 8)
        elif timeframe_seconds <= 15:
            confidence = max(55, confidence - 4)

        result.update({
            "direction":  direction,
            "confidence": confidence,
            "reason":     reason,
        })
        return result

    # ── Fallback: RSI + EMA crossover (not enough data for MACD) ─────────
    logger.warning(
        f"MACD calculation failed (need {26 + 9 - 1} candles, got {len(closes)}). "
        "Falling back to RSI+EMA strategy."
    )
    result["strategy"] = "RSI+EMA (fallback)"

    ema9  = _ema(closes, 9)
    ema21 = _ema(closes, 21)

    if not ema9 or not ema21:
        result["reason"] = "Insufficient EMA data"
        return result

    price     = closes[-1]
    ema9_val  = ema9[-1]
    ema21_val = ema21[-1]

    bullish = 0
    bearish = 0

    if rsi < 35:   bullish += 2
    elif rsi < 45: bullish += 1
    elif rsi > 65: bearish += 2
    elif rsi > 55: bearish += 1

    if ema9_val > ema21_val: bullish += 2
    else:                    bearish += 2

    if price > ema9_val: bullish += 1
    else:                bearish += 1

    if len(closes) >= 4:
        c = closes[-4:]
        if c[-1] > c[-2] > c[-3] > c[-4]:   bullish += 1
        elif c[-1] < c[-2] < c[-3] < c[-4]: bearish += 1

    total = bullish + bearish
    if total == 0:
        return result

    if bullish > bearish:
        direction = "HIGHER"
        raw_conf  = bullish / total
    elif bearish > bullish:
        direction = "LOWER"
        raw_conf  = bearish / total
    else:
        result["reason"] = "Mixed signals — no clear edge"
        result["confidence"] = 50
        return result

    base_conf = 50 + raw_conf * 25   # capped lower than primary strategy
    if direction == "HIGHER":
        rsi_bonus = max(0, (40 - rsi) / 40) * 10
    else:
        rsi_bonus = max(0, (rsi - 60) / 40) * 10

    tf_penalty = 8 if timeframe_seconds <= 5 else (4 if timeframe_seconds <= 15 else 0)
    confidence = min(85, max(51, int(base_conf + rsi_bonus - tf_penalty)))

    ema_desc = "EMA9>EMA21" if ema9_val > ema21_val else "EMA9<EMA21"
    reason = (
        f"[Fallback] RSI={rsi:.1f}, {ema_desc}, "
        f"{'bullish' if direction == 'HIGHER' else 'bearish'} momentum"
    )

    result.update({
        "direction":  direction,
        "confidence": confidence,
        "reason":     reason,
    })
    return result


def compute_signal(candles: list, strategy: str = "trend") -> tuple[str, str]:
    """Legacy wrapper used by the auto-broadcast loop."""
    if strategy == "reversal":
        r = compute_signal_bollinger(candles)
    else:
        r = compute_signal_advanced(candles)
    if r["direction"] == "HIGHER":
        return "CALL \u2705", r["reason"]
    if r["direction"] == "LOWER":
        return "PUT \u274c", r["reason"]
    return "WAIT \u23f8", r["reason"]


# ---------------------------------------------------------------------------
# Per-user polling loop
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Multi-asset scanner
# ---------------------------------------------------------------------------
# Per-UserSession cooldown tracker: {asset_code: last_signal_timestamp}
# Stored on the UserSession via us._scanner_cooldowns (set in lifespan)

def _get_display_label(asset_code: str) -> str:
    """Reverse-lookup display label from asset code, or return code itself."""
    for label, code in ASSETS.items():
        if code == asset_code:
            return label
    return asset_code


async def _scan_asset_loop(
    us: "UserSession",
    asset_code: str,
) -> None:
    """
    Subscribe to one asset and generate signals whenever a new candle closes.
    Runs as an independent asyncio.Task per asset per UserSession.
    """
    from datetime import timedelta as _td

    logger.info(f"[{us.name}] Scanner starting for {asset_code}")

    # Initialise cooldown tracker on the UserSession if not present
    if not hasattr(us, "_scanner_cooldowns"):
        us._scanner_cooldowns = {}

    sm = us.session_manager
    if not sm or not sm.is_connected or sm.client is None:
        logger.warning(f"[{us.name}] Scanner: not connected, skipping {asset_code}")
        return

    asset_label = _get_display_label(asset_code)

    try:
        # Subscribe to timed updates every CANDLE_PERIOD seconds
        subscription = await sm.client.subscribe_symbol_timed(
            asset_code, _td(seconds=CANDLE_PERIOD)
        )

        async for _tick in subscription:
            # Check if scanner is still enabled for any recipient
            any_scanner = any(
                _get_user_settings(cid).get("scanner", False)
                for cid in us.all_broadcast_ids()
            )
            if not any_scanner:
                logger.info(f"[{us.name}] Scanner disabled, stopping {asset_code}")
                return

            # Fetch candle history for signal computation
            try:
                candles = await sm.get_candles(
                    asset=asset_code,
                    timeframe=CANDLE_PERIOD,
                    count=CANDLE_COUNT,
                )
            except Exception as exc:
                log_error(f"[{us.name}] Scanner candle fetch error ({asset_code}): {exc}")
                continue

            if not candles or len(candles) < 20:
                continue

            # Compute signal for each recipient using their own strategy
            now = time.time()
            for cid in us.all_broadcast_ids():
                cid_ps = _get_user_settings(cid)

                # Skip if scanner is off for this recipient
                if not cid_ps.get("scanner", False):
                    continue

                # Skip if auto is off (scanner respects auto toggle)
                if not cid_ps.get("auto", False):
                    continue

                # Cooldown check per (user, asset)
                cooldown_key = f"{cid}:{asset_code}"
                last_ts = us._scanner_cooldowns.get(cooldown_key, 0)
                cooldown_secs = cid_ps.get("cooldown", DEFAULT_COOLDOWN_SECONDS)
                if now - last_ts < cooldown_secs:
                    continue

                # Time-window restriction
                current_hour = datetime.utcnow().hour
                ps = _get_user_persistent(cid)
                if ps.get("restrict_to_best_time"):
                    ts = ps.get("best_time_start")
                    te = ps.get("best_time_end")
                    if ts is not None and te is not None and not (ts <= current_hour < te):
                        continue

                # News filter
                if _news_filter_active(cid):
                    await _ensure_calendar_fresh()
                    blocked, event_desc = is_high_impact_news_approaching(asset_label)
                    if blocked:
                        continue

                # ADX auto-strategy
                strategy = cid_ps.get("strategy", "trend")
                try:
                    highs  = [float(c["high"]  if isinstance(c, dict) else c.high)  for c in candles]
                    lows   = [float(c["low"]   if isinstance(c, dict) else c.low)   for c in candles]
                    closes = _closes(candles)
                    adx_val = calculate_adx(highs, lows, closes)
                    if cid_ps.get("auto_strategy", True) and adx_val is not None:
                        strategy = _adx_strategy_recommendation(adx_val)
                except Exception:
                    adx_val = None

                # Compute signal
                if strategy == "reversal":
                    result = compute_signal_bollinger(candles, CANDLE_PERIOD)
                else:
                    result = compute_signal_advanced(candles, CANDLE_PERIOD)

                if result["direction"] == "WAIT":
                    continue

                # Signal fired — update cooldown and send
                us._scanner_cooldowns[cooldown_key] = now

                price = None
                try:
                    c = candles[-1]
                    price = float(c["close"] if isinstance(c, dict) else c.close)
                except Exception:
                    pass

                price_str = f"{price:.5f}" if price else "N/A"
                result["adx"] = adx_val
                result["effective_strategy"] = strategy

                logger.info(
                    f"[{us.name}] Scanner signal: {asset_code} "
                    f"{result['direction']} ({strategy}) conf={result['confidence']}%"
                )

                await _send_signal_message(
                    chat_id=cid,
                    result=result,
                    asset_label=asset_label,
                    tf_label="1 minute",
                    price_str=price_str,
                )

    except asyncio.CancelledError:
        logger.info(f"[{us.name}] Scanner task cancelled for {asset_code}")
    except Exception as exc:
        log_error(f"[{us.name}] Scanner loop error ({asset_code}): {exc}")


async def _start_scanner(us: "UserSession") -> list[asyncio.Task]:
    """Start one scan task per asset. Returns list of tasks."""
    tasks = []
    for asset_code in SCAN_ASSETS:
        task = asyncio.create_task(
            _scan_asset_loop(us, asset_code),
            name=f"scan_{us.chat_id}_{asset_code}",
        )
        tasks.append(task)
    logger.info(f"[{us.name}] Scanner started for {len(tasks)} assets: {SCAN_ASSETS}")
    return tasks


async def _user_poll_loop(us: "UserSession") -> None:
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

            # Use primary user's strategy for candle computation
            primary_ps    = _get_user_settings(us.chat_id)
            user_strategy = primary_ps.get("strategy", "trend")
            direction, reason = compute_signal(candles, strategy=user_strategy)
            price = None
            if candles:
                try:
                    c = candles[-1]
                    price = float(c["close"] if isinstance(c, dict) else c.close)
                except (AttributeError, TypeError, ValueError, KeyError):
                    pass

            us.latest_signal = {
                "direction": direction,
                "price":     price,
                "reason":    reason,
                "timestamp": str(datetime.now()),
            }

            # Check if ANY recipient has auto ON before computing signal result
            any_auto = any(
                _get_user_settings(cid).get("auto", False)
                for cid in us.all_broadcast_ids()
            )

            if direction != "WAIT \u23f8" and any_auto:
                if user_strategy == "reversal":
                    result = compute_signal_bollinger(candles, CANDLE_PERIOD)
                else:
                    result = compute_signal_advanced(candles, CANDLE_PERIOD)
                price_str = f"{price:.5f}" if price else "N/A"

                # Refresh calendar cache if stale (non-blocking)
                await _ensure_calendar_fresh()

                # Check per-recipient settings before sending
                current_hour = datetime.utcnow().hour
                for cid in us.all_broadcast_ids():
                    cid_ps = _get_user_settings(cid)

                    # Skip if this recipient has auto OFF
                    if not cid_ps.get("auto", False):
                        continue

                    ps = _get_user_persistent(cid)

                    # ── restrict_to_best_time (/autotime on) ─────────────
                    if ps.get("restrict_to_best_time"):
                        ts = ps.get("best_time_start")
                        te = ps.get("best_time_end")
                        if ts is not None and te is not None:
                            if not (ts <= current_hour < te):
                                logger.debug(
                                    f"[{us.name}] Silent skip for {cid} "
                                    f"— outside best time {ts:02d}-{te:02d} UTC"
                                )
                                continue   # silent skip, no message

                    # ── Legacy restrict_to_best_window ───────────────────
                    if ps.get("restrict_to_best_window"):
                        ws = ps.get("best_window_start")
                        we = ps.get("best_window_end")
                        if ws is not None and we is not None:
                            if not (ws <= current_hour < we):
                                logger.debug(f"[{us.name}] Skipping signal for {cid} — outside window {ws}-{we} UTC")
                                continue

                    # ── News filter ───────────────────────────────────────
                    if _news_filter_active(cid):
                        asset_label = "EUR/USD (OTC)"
                        blocked, event_desc = is_high_impact_news_approaching(asset_label)
                        if blocked:
                            logger.info(f"[{us.name}] Signal paused for {cid} — {event_desc}")
                            try:
                                await telegram_app.bot.send_message(
                                    chat_id=cid,
                                    text=(
                                        f"\u26a0\ufe0f Signal paused for {asset_label}\n"
                                        f"High-impact news approaching: {event_desc}\n"
                                        "Use /disable_news_filter to trade through news."
                                    ),
                                )
                            except Exception:
                                pass
                            continue

                    await _send_signal_message(
                        chat_id=cid,
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
                        us: "UserSession | None" = None,
                        strategy: str = "trend",
                        chat_id: int = 0) -> dict:
    """
    Fetch candles and compute a signal using the specified strategy.

    If the user has auto_strategy=True, ADX overrides the manual strategy choice.

    strategy: "trend"    → MACD+RSI (default)
              "reversal" → Bollinger Bands reversal
    """
    sm = us.session_manager if us else None
    if not (sm and sm.is_connected):
        return {
            "direction": "WAIT", "confidence": 0,
            "rsi": None, "reason": "Not connected to Pocket Option",
            "market": "Unknown", "adx": None, "effective_strategy": strategy,
        }
    try:
        candles = await sm.get_candles(
            asset=asset_code, timeframe=tf_seconds, count=CANDLE_COUNT
        )

        # ── ADX auto-strategy override ────────────────────────────────────
        adx_val = None
        effective_strategy = strategy
        if candles and len(candles) >= 16:
            highs  = [float(c["high"]  if isinstance(c, dict) else c.high)  for c in candles]
            lows   = [float(c["low"]   if isinstance(c, dict) else c.low)   for c in candles]
            closes = _closes(candles)
            adx_val = calculate_adx(highs, lows, closes)

            # Apply ADX override if user has auto_strategy enabled
            if chat_id:
                ps = _get_user_settings(chat_id)
                if ps.get("auto_strategy", True) and adx_val is not None:
                    effective_strategy = _adx_strategy_recommendation(adx_val)
                    if effective_strategy != strategy:
                        logger.info(
                            f"ADX={adx_val:.1f} → auto-switching strategy "
                            f"{strategy} → {effective_strategy} for {asset_code}"
                        )

        if effective_strategy == "reversal":
            result = compute_signal_bollinger(candles, tf_seconds)
        else:
            result = compute_signal_advanced(candles, tf_seconds)

        result["adx"]                = adx_val
        result["effective_strategy"] = effective_strategy
        return result

    except Exception as exc:
        log_error(f"_fetch_signal error: {exc}")
        return {
            "direction": "WAIT", "confidence": 0,
            "rsi": None, "reason": f"Error: {exc}",
            "market": "Unknown", "adx": None, "effective_strategy": strategy,
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
    adx_val = result.get("adx")
    adx_str = f"{adx_val:.1f}" if adx_val is not None else "N/A"
    eff_strat = result.get("effective_strategy", result.get("strategy", ""))
    strat_tag = f" [{eff_strat}]" if eff_strat else ""

    return (
        f"{header}\n\n"
        f"Asset: {asset_label}\n"
        f"Timeframe: {tf_label}\n"
        f"Reliability: {confidence}%  {bar}\n"
        f"RSI: {rsi_str}  |  ADX: {adx_str}{strat_tag}\n"
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
    adx_val = result.get("adx")
    adx_str = f"{adx_val:.1f}" if adx_val is not None else "N/A"
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
        f"RSI: {rsi_str}  |  ADX: {adx_str}",
        f"Market: {market}",
        f"Reason: {reason}",
        f"Time: {datetime.now().strftime('%H:%M:%S')}",
    ]
    return "\n".join(lines)


def _schedule_validation(signal_id: str, chat_id: int, asset_label: str,
                         entry_price: float, direction: str) -> None:
    """Schedule auto-validation after VALIDATION_SECONDS if user has auto_validate on."""
    ps = _get_user_settings(chat_id)
    if not ps.get("auto_validate", True) or entry_price <= 0:
        return
    us = _get_user_session(chat_id)
    if not us:
        return
    asset_code = ASSETS.get(asset_label, asset_label)
    task = asyncio.create_task(
        _validate_signal(signal_id, chat_id, us, asset_code, asset_label,
                         direction, entry_price),
        name=f"validate_{signal_id}",
    )
    _validation_tasks[signal_id] = task


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
    entry_price_float = 0.0
    try:
        entry_price_float = float(price_str) if price_str and price_str != "N/A" else 0.0
    except (ValueError, TypeError):
        pass

    pending_signals[signal_id] = {
        "user_id":     chat_id,
        "ts":          time.time(),
        "voted":       False,
        "followed":    False,   # prevent duplicate follow tasks
        # Metadata for CSV logging
        "asset_label": asset_label,
        "tf_label":    tf_label,
        "direction":   "CALL" if result["direction"] == "HIGHER" else "PUT",
        "confidence":  result.get("confidence", ""),
        "rsi":         result.get("rsi", ""),
        "price":       price_str,
        "entry_price": entry_price_float,
        "market":      result.get("market", ""),
        "reason":      result.get("reason", ""),
    }

    img_url  = SIGNAL_IMG_BUY if direction == "HIGHER" else SIGNAL_IMG_SELL
    caption  = _signal_caption(result, asset_label, tf_label, price_str)

    # Build keyboard: Win/Loss + Follow/Log buttons
    trade_direction = "CALL" if direction == "HIGHER" else "PUT"
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f44d Win",  callback_data=f"vote:win:{signal_id}"),
            InlineKeyboardButton("\U0001f44e Loss", callback_data=f"vote:loss:{signal_id}"),
        ],
        [
            InlineKeyboardButton("\U0001f50d Follow price", callback_data=f"follow:{signal_id}"),
        ],
    ])

    if img_url:
        try:
            sent = await telegram_app.bot.send_photo(
                chat_id=chat_id,
                photo=img_url,
                caption=caption,
                reply_markup=keyboard,
            )
            _schedule_validation(signal_id, chat_id, asset_label, entry_price_float, trade_direction)
            return
        except Exception as exc:
            logger.warning(f"send_photo failed for {chat_id}: {exc} — falling back to text")

    text = _format_signal(result, asset_label, tf_label)
    if price_str:
        text += f"\nPrice: {price_str}"
    try:
        sent = await telegram_app.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
        )
        pending_signals[signal_id]["message_id"] = sent.message_id
        _schedule_validation(signal_id, chat_id, asset_label, entry_price_float, trade_direction)
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
    ps      = _get_user_settings(user_id)
    auto    = ps.get("auto", False)
    scanner = ps.get("scanner", False)
    auto_label    = "\U0001f514 Auto-signals: ON"  if auto    else "\U0001f515 Auto-signals: OFF"
    scanner_label = "\U0001f4e1 Scanner: ON"        if scanner else "\U0001f4e1 Scanner: OFF"
    return InlineKeyboardMarkup([
        # ── Signals ──────────────────────────────────────────────────────
        [InlineKeyboardButton("\U0001f4ca Get Signal",       callback_data="menu:signal")],
        [InlineKeyboardButton("\U0001f3ae New Session",      callback_data="sess:new"),
         InlineKeyboardButton("\U0001f4ca ADX",              callback_data="menu:adx")],
        [InlineKeyboardButton(auto_label,                    callback_data="menu:toggle_auto"),
         InlineKeyboardButton(scanner_label,                 callback_data="menu:toggle_scanner")],
        # ── Strategy & analysis ───────────────────────────────────────────
        [InlineKeyboardButton("\U0001f9e0 Strategy",         callback_data="menu:strategy"),
         InlineKeyboardButton("\U0001f4ca ADX",              callback_data="menu:adx")],
        [InlineKeyboardButton("\U0001f4cb Analyze",          callback_data="menu:analyze"),
         InlineKeyboardButton("\U0001f3c6 Recommend",        callback_data="menu:recommend")],
        # ── Paper trading & validation ────────────────────────────────────
        [InlineKeyboardButton("\U0001f4b0 Paper Balance",    callback_data="menu:paper"),
         InlineKeyboardButton("\U0001f50d Validation",       callback_data="menu:validation")],
        [InlineKeyboardButton("\U0001f4c4 Manual Trades",    callback_data="menu:manual_trades"),
         InlineKeyboardButton("\U0001f4e4 Export CSV",       callback_data="menu:export")],
        # ── Settings & info ───────────────────────────────────────────────
        [InlineKeyboardButton("\u2699\ufe0f Settings",       callback_data="menu:settings"),
         InlineKeyboardButton("\u2139\ufe0f How it works",   callback_data="menu:howto")],
        [InlineKeyboardButton("\U0001f511 Refresh SSID",     callback_data="menu:ssid"),
         InlineKeyboardButton("\U0001f4b0 Account",          callback_data="menu:account")],
        [InlineKeyboardButton("\U0001f3ae Mode: Demo" if ps.get("is_demo", True) else "\U0001f4b5 Mode: Real",
                              callback_data="menu:toggle_mode"),
         InlineKeyboardButton("\u26a1 Energy",               callback_data="menu:energy")],
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
    s = _get_user_settings(user_id)
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
        f"Connection: {conn_str}\n"
        f"Account: {us.name}\n"
    )
    if len(us.all_broadcast_ids()) > 1:
        text += f"Team broadcast: {len(us.all_broadcast_ids())} recipients\n"
    text += "\nTap a button below to get started:"
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

    settings = _get_user_settings(chat_id)   # persistent per-user settings

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
        _save_user_persistent_settings()
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

    if data == "menu:toggle_scanner":
        settings["scanner"] = not settings.get("scanner", False)
        _save_user_persistent_settings()
        state = "ON \u2705" if settings["scanner"] else "OFF \U0001f515"
        # Start/stop scanner tasks
        if settings["scanner"] and us and not getattr(us, "_scanner_tasks", None):
            us._scanner_tasks = await _start_scanner(us)
        elif not settings["scanner"] and us:
            for t in getattr(us, "_scanner_tasks", []):
                if not t.done():
                    t.cancel()
            us._scanner_tasks = []
        await query.edit_message_text(
            f"\U0001f4e1 Scanner turned {state}\n\n"
            f"{'Monitoring' if settings['scanner'] else 'Stopped monitoring'} "
            f"{len(SCAN_ASSETS)} assets.\n"
            f"{'Make sure /autoon is enabled.' if settings['scanner'] else ''}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:strategy":
        current = settings.get("strategy", "trend")
        auto_s  = settings.get("auto_strategy", True)
        await query.edit_message_text(
            f"\U0001f9e0 Strategy\n\n"
            f"Current: {current.upper()}\n"
            f"ADX auto-switch: {'ON' if auto_s else 'OFF'}\n\n"
            "Choose a strategy:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 TREND (MACD+RSI)",      callback_data="menu:set_strategy:trend")],
                [InlineKeyboardButton("📉 REVERSAL (Bollinger)",  callback_data="menu:set_strategy:reversal")],
                [InlineKeyboardButton("🤖 ADX Auto: ON" if auto_s else "🤖 ADX Auto: OFF",
                                      callback_data="menu:toggle_autostrategy")],
                [InlineKeyboardButton("\U0001f519 Back",           callback_data="menu:back")],
            ]),
        )
        return ConversationHandler.END

    if data.startswith("menu:set_strategy:"):
        chosen = data.split(":")[-1]
        if chosen in ("trend", "reversal"):
            settings["strategy"] = chosen
            _save_user_persistent_settings()
        await query.edit_message_text(
            f"\u2705 Selected strategy is now: {chosen.upper()}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:toggle_autostrategy":
        settings["auto_strategy"] = not settings.get("auto_strategy", True)
        _save_user_persistent_settings()
        state = "ON \u2705" if settings["auto_strategy"] else "OFF \U0001f515"
        await query.edit_message_text(
            f"\U0001f916 ADX Auto-Strategy: {state}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:strategy")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:adx":
        await query.edit_message_text("\u23f3 Fetching ADX...")
        # Reuse adx_command logic inline
        ps          = _get_user_settings(user_id)
        asset_label = ps.get("asset", DEFAULT_SETTINGS["asset"])
        asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
        tf_seconds  = TIMEFRAMES.get(ps.get("timeframe", "1 minute"), 60)
        sm = us.session_manager if us else None
        adx_text = "Could not compute ADX."
        if sm and sm.is_connected:
            try:
                candles = await sm.get_candles(asset=asset_code, timeframe=tf_seconds, count=CANDLE_COUNT)
                if candles and len(candles) >= 16:
                    highs  = [float(c["high"]  if isinstance(c, dict) else c.high)  for c in candles]
                    lows   = [float(c["low"]   if isinstance(c, dict) else c.low)   for c in candles]
                    closes = _closes(candles)
                    adx    = calculate_adx(highs, lows, closes)
                    if adx is not None:
                        rec = _adx_strategy_recommendation(adx)
                        strength = ("Strong trend \U0001f4c8" if adx > 25
                                    else "Ranging \u27a1\ufe0f" if adx < 20
                                    else "Weak trend \u26a0\ufe0f")
                        adx_text = (f"\U0001f4ca ADX: {adx:.1f}  ({strength})\n"
                                    f"Recommended: {rec.upper()}\n"
                                    f"Asset: {asset_label}")
            except Exception as exc:
                adx_text = f"ADX error: {exc}"
        await query.edit_message_text(
            adx_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:analyze":
        await query.edit_message_text("\u23f3 Analysing trades...")
        text = _build_analysis(user_id)
        if len(text) > 4000:
            text = text[:3990] + "\n...(truncated)"
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:recommend":
        await query.edit_message_text("\u23f3 Building recommendations...")
        rows = _read_trades(user_id)
        if len(rows) < 5:
            text = f"Not enough data ({len(rows)} trades). Need at least 5."
        else:
            lines = ["\U0001f4cb Recommendations\n"]
            sorted_assets = sorted(
                [(lbl, w, t) for lbl, (w, t) in _group_win_rate(rows, lambda r: r.get("asset","?")).items() if t >= 3],
                key=lambda x: x[1]/x[2], reverse=True,
            )
            if sorted_assets:
                lines.append("Best asset: " + sorted_assets[0][0])
            sorted_times = sorted(
                [(lbl, w, t) for lbl, (w, t) in _group_win_rate(rows, _hour_window).items() if t >= 3],
                key=lambda x: x[1]/x[2], reverse=True,
            )
            if sorted_times:
                lines.append("Best time:  " + sorted_times[0][0])
            text = "\n".join(lines)
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:paper":
        ps      = _get_user_settings(user_id)
        balance = float(ps.get("paper_balance", PAPER_START_BALANCE))
        rows    = _read_paper_trades(user_id)
        total   = len(rows)
        wins    = sum(1 for r in rows if r.get("result") == "win")
        pnl     = sum(float(r.get("pnl", 0)) for r in rows)
        wr      = f"{wins/total*100:.1f}%" if total else "—"
        await query.edit_message_text(
            f"\U0001f4b0 Paper Trading\n\n"
            f"Balance: ${balance:.2f}\n"
            f"P&L: {'+' if pnl >= 0 else ''}{pnl:.2f}$\n"
            f"Trades: {total}  Win rate: {wr}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f504 Reset Balance", callback_data="menu:paper_reset")],
                [InlineKeyboardButton("\U0001f519 Back",          callback_data="menu:back")],
            ]),
        )
        return ConversationHandler.END

    if data == "menu:paper_reset":
        ps = _get_user_settings(user_id)
        ps["paper_balance"] = PAPER_START_BALANCE
        _save_user_persistent_settings()
        await query.edit_message_text(
            f"\u2705 Paper balance reset to ${PAPER_START_BALANCE:.2f}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:validation":
        rows  = _read_validations(user_id)
        total = len(rows)
        wins  = sum(1 for r in rows if r.get("result") == "win")
        wr    = f"{wins/total*100:.1f}%" if total else "—"
        last5 = "  ".join(("✅" if r.get("result")=="win" else "❌") for r in rows[-5:]) or "—"
        ps    = _get_user_settings(user_id)
        av    = ps.get("auto_validate", True)
        await query.edit_message_text(
            f"\U0001f50d Validation\n\n"
            f"Auto-validate: {'ON \u2705' if av else 'OFF \U0001f515'}\n"
            f"Total: {total}  Win rate: {wr}\n"
            f"Last 5: {last5}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Toggle Auto-Validate", callback_data="menu:toggle_autovalidate")],
                [InlineKeyboardButton("\U0001f519 Back",      callback_data="menu:back")],
            ]),
        )
        return ConversationHandler.END

    if data == "menu:toggle_autovalidate":
        ps = _get_user_settings(user_id)
        ps["auto_validate"] = not ps.get("auto_validate", True)
        _save_user_persistent_settings()
        state = "ON \u2705" if ps["auto_validate"] else "OFF \U0001f515"
        await query.edit_message_text(
            f"\U0001f50d Auto-Validate: {state}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:manual_trades":
        rows = _read_trades(user_id)
        if not rows:
            text = "No trades logged yet. Mark signals with \U0001f44d/\U0001f44e."
        else:
            last_5 = rows[-5:]
            lines  = ["\U0001f4cb Last 5 Trades\n"]
            for r in reversed(last_5):
                icon = "\u2705" if r.get("result") == "win" else "\u274c"
                lines.append(f"{icon} {r.get('timestamp','')[:16]}  {r.get('asset','')}  {r.get('direction','')}")
            lines.append(f"\nTotal: {len(rows)}")
            text = "\n".join(lines)
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:export":
        path = _csv_path(user_id)
        if not os.path.exists(path):
            await query.edit_message_text(
                "No trades recorded yet.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
                ]),
            )
        else:
            await query.edit_message_text("Sending your trade log...")
            try:
                with open(path, "rb") as f:
                    await telegram_app.bot.send_document(
                        chat_id=query.message.chat_id,
                        document=f,
                        filename=f"trades_{user_id}.csv",
                        caption=f"\U0001f4c1 Trade log — {len(_read_trades(user_id))} trades",
                    )
            except Exception as exc:
                await telegram_app.bot.send_message(chat_id=query.message.chat_id, text=f"Export error: {exc}")
            await query.edit_message_text(
                "File sent above.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:back")]
                ]),
            )
        return ConversationHandler.END

    if data == "menu:ssid":
        current_ssid = us.ssid if us else ""
        uid_str = _parse_uid_from_ssid(current_ssid) if current_ssid else "unknown"
        preview = f"{current_ssid[:25]}...{current_ssid[-8:]}" if len(current_ssid) > 33 else current_ssid
        po_ok   = us.session_manager is not None and us.session_manager.is_connected if us else False
        await query.edit_message_text(
            f"\U0001f511 SSID Manager\n\n"
            f"Status: {'🟢 Connected' if po_ok else '🔴 Disconnected'}\n"
            f"UID: {uid_str}\n"
            f"Preview: {preview}\n\n"
            "To refresh your SSID:\n"
            "1. Open pocketoption.com → log in\n"
            "2. F12 → Network → WS filter\n"
            '3. Find 42["auth",... message → copy it\n'
            "4. Send: /setssid <paste>\n\n"
            "Or tap the button below for the full guide.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Full guide",    callback_data="menu:ssid_guide")],
                [InlineKeyboardButton("🔄 Reconnect now", callback_data="menu:reconnect")],
                [InlineKeyboardButton("🔙 Back",          callback_data="menu:back")],
            ]),
        )
        return ConversationHandler.END

    if data == "menu:ssid_guide":
        await query.edit_message_text(
            "🔑 SSID Refresh Guide\n\n"
            "1️⃣ Open https://pocketoption.com\n"
            "2️⃣ Log in to your account\n"
            "3️⃣ Press F12 → Network tab → WS filter\n"
            "4️⃣ Refresh page if no WS connections\n"
            "5️⃣ Click the WebSocket connection\n"
            "6️⃣ Click Messages tab\n"
            '7️⃣ Find 42["auth",... → copy entire string\n'
            "8️⃣ Send: /setssid <paste>\n\n"
            "Bot reconnects immediately. No redeploy needed.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu:ssid")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:reconnect":
        if us and us.session_manager:
            async with us.session_manager._lock:
                success = await us.session_manager._connect()
            state = "🟢 Reconnected!" if success else "🔴 Failed — try /setssid"
        else:
            state = "🔴 No session manager"
        await query.edit_message_text(
            state,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:account":
        sm = us.session_manager if us else None
        if not (sm and sm.is_connected and sm.client):
            await query.edit_message_text(
                "🔴 Not connected to Pocket Option.\nUse 🔑 Refresh SSID to reconnect.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Refresh SSID", callback_data="menu:ssid")],
                    [InlineKeyboardButton("🔙 Back",         callback_data="menu:back")],
                ]),
            )
            return ConversationHandler.END
        await query.edit_message_text("⏳ Fetching balance...")
        try:
            balance = await sm.client.balance()
            try:
                server_ts = await sm.client.get_server_time()
                server_dt = datetime.utcfromtimestamp(server_ts).strftime("%H:%M:%S UTC")
            except Exception:
                server_dt = "N/A"
            ps_u = _get_user_settings(user_id)
            paper_bal = float(ps_u.get("paper_balance", PAPER_START_BALANCE))
            await query.edit_message_text(
                f"💰 Account\n\n"
                f"Type: {'Demo 🎮' if sm.is_demo else 'Real 💵'}\n"
                f"Balance: ${balance:.2f}\n"
                f"Server time: {server_dt}\n\n"
                f"📊 Paper balance: ${paper_bal:.2f}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="menu:back")]
                ]),
            )
        except Exception as exc:
            await query.edit_message_text(
                f"Could not fetch balance: {exc}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="menu:back")]
                ]),
            )
        return ConversationHandler.END

    if data == "menu:toggle_mode":
        ps = _get_user_settings(user_id)
        ps["is_demo"] = not ps.get("is_demo", True)
        _save_user_persistent_settings()
        if us and us.session_manager:
            us.session_manager.is_demo = ps["is_demo"]
        mode_str = "Demo 🎮" if ps["is_demo"] else "Real 💵"
        await query.edit_message_text(
            f"Switched to {mode_str} mode.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu:back")]
            ]),
        )
        return ConversationHandler.END

    if data == "menu:energy":
        ps     = _get_user_settings(user_id)
        energy = ps.get("energy", 1000)
        bar    = "⚡" * (energy // 100) + "░" * (10 - energy // 100)
        await query.edit_message_text(
            f"⚡ Energy: {energy}/1000\n{bar}\n\nDecreases by 10 per trade.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔋 Recharge", callback_data="menu:energy_recharge")],
                [InlineKeyboardButton("🔙 Back",     callback_data="menu:back")],
            ]),
        )
        return ConversationHandler.END

    if data == "menu:energy_recharge":
        ps = _get_user_settings(user_id)
        ps["energy"] = 1000
        _save_user_persistent_settings()
        await query.edit_message_text(
            "⚡ Energy recharged to 1000!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu:back")]
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
            _save_user_persistent_settings()
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
            _save_user_persistent_settings()
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

        result = await _fetch_signal(asset_code, tf_seconds, us,
                                     strategy=settings.get("strategy", "trend"),
                                     chat_id=chat_id)

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
    ps          = _get_user_settings(update.effective_chat.id)
    asset_label = ps.get("asset",     DEFAULT_SETTINGS["asset"])
    tf_label    = ps.get("timeframe", DEFAULT_SETTINGS["timeframe"])
    asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
    tf_seconds  = TIMEFRAMES.get(tf_label, 60)

    await update.message.reply_text(
        f"\u23f3 Fetching signal for {asset_label} / {tf_label}..."
    )

    user_strategy = ps.get("strategy", "trend")
    result = await _fetch_signal(asset_code, tf_seconds, us,
                                 strategy=user_strategy,
                                 chat_id=update.effective_chat.id)

    # News filter check for manual signals too
    if result["direction"] != "WAIT" and _news_filter_active(update.effective_chat.id):
        await _ensure_calendar_fresh()
        blocked, event_desc = is_high_impact_news_approaching(asset_label)
        if blocked:
            await update.message.reply_text(
                f"\u26a0\ufe0f Signal paused for {asset_label}\n"
                f"High-impact news approaching: {event_desc}\n\n"
                "Use /disable_news_filter to trade through news events."
            )
            return

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
    _get_user_settings(update.effective_chat.id)["auto"] = True
    _save_user_persistent_settings()
    await update.message.reply_text(
        "\U0001f514 Auto-signals enabled.\n"
        f"You will receive signals every {CANDLE_PERIOD // 60} minute(s).\n\n"
        "Use /autooff to disable, or toggle via the main menu."
    )


@_require_auth
async def autooff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Disable auto-signals for this user."""
    us = _get_user_session(update.effective_chat.id)
    _get_user_settings(update.effective_chat.id)["auto"] = False
    _save_user_persistent_settings()
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
async def login_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step-by-step guide to refresh your SSID in 30 seconds."""
    await update.message.reply_text(
        "🔑 How to refresh your SSID (30 seconds)\n\n"
        "Your SSID is the WebSocket auth token Pocket Option uses.\n"
        "It lasts weeks — you only need this when the bot loses connection.\n\n"
        "Steps:\n"
        "1️⃣ Open https://pocketoption.com in Chrome/Firefox\n"
        "2️⃣ Log in to your account\n"
        "3️⃣ Press F12 to open DevTools\n"
        "4️⃣ Click the Network tab\n"
        "5️⃣ Click the WS filter button\n"
        "6️⃣ Refresh the page (F5) if no WS connections appear\n"
        "7️⃣ Click on the WebSocket connection (URL starts with wss://)\n"
        "8️⃣ Click the Messages tab\n"
        '9️⃣ Find the message starting with 42["auth",\n'
        "🔟 Click it, select all, copy\n\n"
        "Then send it here:\n"
        "/setssid <paste your copied string>\n\n"
        "Example:\n"
        '/setssid 42["auth",{"session":"abc123...","isDemo":1,"uid":27658142,"platform":2}]\n\n'
        "The bot reconnects immediately — no redeploy needed.\n\n"
        "💡 Tip: Also update the SSID env var on Render so it survives restarts."
    )


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
            # Push to Worker relay if configured
            if WORKER_URL and WORKER_API_KEY:
                pushed = await push_ssid_to_worker(new_ssid)
                if pushed:
                    await update.message.reply_text(
                        "✅ SSID also pushed to Cloudflare Worker relay.\n"
                        "Remember to run: wrangler secret put SSID"
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

    # Log to CSV
    _log_trade(user_id, signal, "win" if won else "loss")

    # Trigger 10-trade milestone summary (async, best-effort)
    asyncio.create_task(_maybe_send_10_trade_summary(user_id))

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


async def follow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle 'Follow price' button press — start live price tracking."""
    query   = update.callback_query
    await query.answer("Starting live price tracking...")
    data    = query.data   # "follow:{signal_id}"
    logger.info(f"follow_handler triggered: {data}")
    parts   = data.split(":", 1)
    if len(parts) != 2:
        return
    signal_id = parts[1]

    signal = pending_signals.get(signal_id)
    if signal is None:
        await query.answer("Signal expired — cannot follow.")
        return

    if signal.get("followed"):
        await query.answer("Already following this signal.")
        return

    if time.time() - signal["ts"] > SIGNAL_EXPIRY_SEC:
        await query.answer("Signal too old to follow.")
        return

    signal["followed"] = True
    chat_id   = query.message.chat.id
    us        = _get_user_session(chat_id)
    if not us:
        await query.answer("Not authorised.")
        return

    asset_label = signal.get("asset_label", "EUR/USD (OTC)")
    asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
    direction   = signal.get("direction", "CALL")
    entry_price = float(signal.get("entry_price", 0) or 0)

    if entry_price <= 0:
        # Try to fetch current price as entry
        entry_price = await _get_current_price(us, asset_code) or 0.0

    # Send a new message to track (don't edit the photo message)
    try:
        sent = await telegram_app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔍 Following {asset_label} ({direction})\n"
                f"Entry: {entry_price:.5f}\n"
                f"Updating every {FOLLOW_INTERVAL_SEC}s for {FOLLOW_DURATION_SEC}s..."
            ),
        )
        task = asyncio.create_task(
            _follow_price_task(
                signal_id=signal_id,
                chat_id=chat_id,
                message_id=sent.message_id,
                us=us,
                asset_code=asset_code,
                asset_label=asset_label,
                direction=direction,
                entry_price=entry_price,
            ),
            name=f"follow_{signal_id}",
        )
        _follow_tasks[signal_id] = task
    except Exception as exc:
        logger.error(f"follow_handler error: {exc}")


# ---------------------------------------------------------------------------
# Analytics & trade log commands
# ---------------------------------------------------------------------------

@_require_auth
async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show detailed trade analytics from the user's CSV log.
    Use /analyze --refresh to also recompute and save the best time window.
    """
    chat_id = update.effective_chat.id
    refresh = context.args and "--refresh" in context.args

    await update.message.reply_text("⏳ Analysing your trades...")
    text = _build_analysis(chat_id)

    # Auto-save best time window on --refresh or if not yet set
    ps = _get_user_persistent(chat_id)
    if refresh or ps.get("best_time_start") is None:
        rows = _read_trades(chat_id)
        if rows:
            best_label, best_wr = _best_window(rows)
            if best_label:
                try:
                    parts = best_label.replace(" UTC", "").split("-")
                    ts, te = int(parts[0]), int(parts[1])
                    ps["best_time_start"]   = ts
                    ps["best_time_end"]     = te
                    ps["best_time_updated"] = datetime.utcnow().date().isoformat()
                    _save_user_persistent_settings()
                    if refresh:
                        text += f"\n\n✅ Best time window refreshed: {best_label} ({best_wr:.0f}%)"
                except Exception:
                    pass

    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000])
    else:
        await update.message.reply_text(text)


@_require_auth
async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the user's trades CSV as a file attachment."""
    chat_id = update.effective_chat.id
    path    = _csv_path(chat_id)
    if not os.path.exists(path):
        await update.message.reply_text("No trades recorded yet. Mark signals with 👍/👎 to start logging.")
        return
    try:
        with open(path, "rb") as f:
            await telegram_app.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=f"trades_{chat_id}.csv",
                caption=f"📁 Your trade log — {len(_read_trades(chat_id))} trades",
            )
    except Exception as exc:
        logger.error(f"export_command error: {exc}")
        await update.message.reply_text(f"Could not send file: {exc}")


@_require_auth
async def restrict_window_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enable time-window restriction based on the user's best trading window."""
    chat_id = update.effective_chat.id
    rows    = _read_trades(chat_id)
    if len(rows) < 10:
        await update.message.reply_text(
            f"Not enough data yet ({len(rows)} trades). Need at least 10 trades to compute a best window."
        )
        return

    best_label, best_wr = _best_window(rows)
    if not best_label:
        await update.message.reply_text("Could not determine a best window — need at least 3 trades per time slot.")
        return

    # Parse "HH-HH UTC" → start/end hours
    try:
        parts = best_label.replace(" UTC", "").split("-")
        ws, we = int(parts[0]), int(parts[1])
    except Exception:
        await update.message.reply_text(f"Could not parse window: {best_label}")
        return

    ps = _get_user_persistent(chat_id)
    ps["restrict_to_best_window"] = True
    ps["best_window_start"]       = ws
    ps["best_window_end"]         = we
    _save_user_persistent_settings()

    await update.message.reply_text(
        f"✅ Time-window restriction enabled!\n\n"
        f"Best window: {best_label} ({best_wr:.0f}% win rate)\n"
        f"Auto-signals will only be sent between {ws:02d}:00 and {we:02d}:00 UTC.\n\n"
        "Use /clear_restriction to remove this filter."
    )


@_require_auth
async def clear_restriction_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove the time-window restriction for this user."""
    chat_id = update.effective_chat.id
    ps = _get_user_persistent(chat_id)
    ps["restrict_to_best_window"] = False
    ps["best_window_start"]       = None
    ps["best_window_end"]         = None
    _save_user_persistent_settings()
    await update.message.reply_text(
        "🔓 Time-window restriction removed.\n"
        "You will now receive auto-signals at all hours."
    )


@_require_auth
async def enable_news_filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enable the economic calendar news filter for this user."""
    chat_id = update.effective_chat.id
    ps = _get_user_persistent(chat_id)
    ps["news_filter_enabled"] = True
    _save_user_persistent_settings()
    await update.message.reply_text(
        "\U0001f4f0 Economic Calendar Filter ENABLED\n\n"
        "Signals will be paused for 10 minutes before any high-impact news event "
        "(NFP, CPI, interest rate decisions, etc.).\n\n"
        "Use /disable_news_filter to trade through news events."
    )


@_require_auth
async def disable_news_filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Disable the economic calendar news filter for this user."""
    chat_id = update.effective_chat.id
    ps = _get_user_persistent(chat_id)
    ps["news_filter_enabled"] = False
    _save_user_persistent_settings()
    await update.message.reply_text(
        "\U0001f4f0 Economic Calendar Filter DISABLED\n\n"
        "You will now receive signals even during high-impact news events.\n\n"
        "\u26a0\ufe0f Trading during news carries higher risk.\n"
        "Use /enable_news_filter to re-enable protection."
    )


@_require_auth
async def news_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show upcoming high-impact news events and current filter status."""
    chat_id = update.effective_chat.id
    await _ensure_calendar_fresh()

    enabled = _news_filter_active(chat_id)
    status  = "ON \u2705" if enabled else "OFF \U0001f515"

    now_utc = datetime.utcnow()
    # Show next 5 high-impact events
    upcoming = sorted(
        [ev for ev in _calendar_cache if ev["dt_utc"] >= now_utc],
        key=lambda e: e["dt_utc"]
    )[:5]

    lines = [
        f"\U0001f4f0 Economic Calendar Filter: {status}",
        "",
        "Upcoming high-impact events (UTC):",
    ]
    if upcoming:
        for ev in upcoming:
            mins = int((ev["dt_utc"] - now_utc).total_seconds() / 60)
            time_str = ev["dt_utc"].strftime("%a %H:%M")
            lines.append(f"  \u2022 {time_str} — {ev['title']} ({ev['country']}) in {mins}m")
    else:
        lines.append("  No high-impact events found for this week.")

    lines += [
        "",
        "/enable_news_filter  — pause signals before news",
        "/disable_news_filter — trade through news",
    ]
    await update.message.reply_text("\n".join(lines))


@_require_auth
async def strategy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Switch signal strategy.

    Usage:
      /strategy          — show current strategy
      /strategy trend    — MACD+RSI (default)
      /strategy reversal — Bollinger Bands reversal
    """
    us = _get_user_session(update.effective_chat.id)

    # Parse args from context.args OR directly from message text (fallback)
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        current = _get_user_settings(update.effective_chat.id).get("strategy", "trend")
        await update.message.reply_text(
            f"📊 Current strategy: {current}\n\n"
            "To switch, send:\n"
            "  /strategy trend\n"
            "  /strategy reversal"
        )
        return

    chosen = args[0].lower()
    if chosen not in ("trend", "reversal", "tick"):
        await update.message.reply_text(
            "❌ Unknown strategy. Choose:\n"
            "  /strategy trend\n"
            "  /strategy reversal\n"
            "  /strategy tick"
        )
        return

    _get_user_settings(update.effective_chat.id)["strategy"] = chosen
    _save_user_persistent_settings()
    descriptions = {
        "trend":    "MACD+RSI — signals when RSI is extreme AND MACD confirms direction",
        "reversal": "Bollinger Bands — signals when price bounces off upper/lower band",
    }
    await update.message.reply_text(
        f"✅ Selected strategy is now: {chosen.upper()}\n"
        f"{descriptions[chosen]}"
    )


@_require_auth
async def autotime_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Toggle time-restricted auto-signals based on your best historical window.

    Usage:
      /autotime on   — only send auto-signals during your best UTC window
      /autotime off  — send auto-signals at all hours
      /autotime      — show current status
    """
    chat_id = update.effective_chat.id
    ps      = _get_user_persistent(chat_id)

    if not context.args:
        enabled = ps.get("restrict_to_best_time", False)
        ts      = ps.get("best_time_start")
        te      = ps.get("best_time_end")
        updated = ps.get("best_time_updated", "never")
        window  = f"{ts:02d}:00–{te:02d}:00 UTC" if ts is not None else "not computed yet"
        await update.message.reply_text(
            f"⏰ Auto-time restriction: {'ON ✅' if enabled else 'OFF 🔕'}\n"
            f"Best window: {window}\n"
            f"Last refreshed: {updated}\n\n"
            "Commands:\n"
            "  /autotime on  — enable restriction\n"
            "  /autotime off — disable restriction\n"
            "  /analyze --refresh — recompute best window from your trade history"
        )
        return

    arg = context.args[0].lower()

    if arg == "on":
        # Compute best window if not yet available
        ts = ps.get("best_time_start")
        te = ps.get("best_time_end")
        if ts is None:
            rows = _read_trades(chat_id)
            if len(rows) < 10:
                await update.message.reply_text(
                    f"Not enough trade history ({len(rows)} trades). "
                    "Need at least 10 trades with 👍/👎 results.\n"
                    "Run /analyze --refresh after you have more data."
                )
                return
            best_label, best_wr = _best_window(rows)
            if not best_label:
                await update.message.reply_text(
                    "Could not determine a best window — need at least 3 trades per time slot."
                )
                return
            try:
                parts = best_label.replace(" UTC", "").split("-")
                ts, te = int(parts[0]), int(parts[1])
                ps["best_time_start"]   = ts
                ps["best_time_end"]     = te
                ps["best_time_updated"] = datetime.utcnow().date().isoformat()
            except Exception as exc:
                await update.message.reply_text(f"Could not parse window: {exc}")
                return

        ps["restrict_to_best_time"] = True
        _save_user_persistent_settings()
        await update.message.reply_text(
            f"⏰ Auto-time restriction ENABLED ✅\n\n"
            f"Auto-signals will only be sent between "
            f"{ts:02d}:00 and {te:02d}:00 UTC.\n"
            f"Outside this window, signals are silently skipped.\n\n"
            "Use /autotime off to disable."
        )

    elif arg == "off":
        ps["restrict_to_best_time"] = False
        _save_user_persistent_settings()
        await update.message.reply_text(
            "⏰ Auto-time restriction DISABLED 🔕\n"
            "You will now receive auto-signals at all hours."
        )

    else:
        await update.message.reply_text("Usage: /autotime on  or  /autotime off")

@_require_auth
async def adx_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current ADX value and recommended strategy for the user's asset."""
    us      = _get_user_session(update.effective_chat.id)
    ps      = _get_user_settings(update.effective_chat.id)
    asset_label = ps.get("asset", DEFAULT_SETTINGS["asset"])
    tf_label    = ps.get("timeframe", DEFAULT_SETTINGS["timeframe"])
    asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
    tf_seconds  = TIMEFRAMES.get(tf_label, 60)

    sm = us.session_manager if us else None
    if not (sm and sm.is_connected):
        await update.message.reply_text("Not connected to Pocket Option.")
        return

    await update.message.reply_text(f"\u23f3 Calculating ADX for {asset_label}...")
    try:
        candles = await sm.get_candles(asset=asset_code, timeframe=tf_seconds, count=CANDLE_COUNT)
        if not candles or len(candles) < 16:
            await update.message.reply_text("Not enough candle data for ADX.")
            return

        highs  = [float(c["high"]  if isinstance(c, dict) else c.high)  for c in candles]
        lows   = [float(c["low"]   if isinstance(c, dict) else c.low)   for c in candles]
        closes = _closes(candles)
        adx    = calculate_adx(highs, lows, closes)

        if adx is None:
            await update.message.reply_text("Could not compute ADX — not enough data.")
            return

        rec    = _adx_strategy_recommendation(adx)
        auto_on = ps.get("auto_strategy", True)

        strength = ("Strong trend \U0001f4c8" if adx > 25
                    else "Ranging market \u27a1\ufe0f" if adx < 20
                    else "Weak trend \u26a0\ufe0f")

        await update.message.reply_text(
            f"\U0001f4ca ADX Analysis — {asset_label}\n\n"
            f"ADX: {adx:.1f}  ({strength})\n"
            f"Recommended strategy: {rec.upper()}\n"
            f"Auto-strategy: {'ON \u2705' if auto_on else 'OFF \U0001f515'}\n\n"
            "ADX > 25 \u2192 TREND (MACD+RSI)\n"
            "ADX < 20 \u2192 REVERSAL (Bollinger)\n"
            "20-25    \u2192 Weak zone (defaults to TREND)\n\n"
            "Use /autostrategy on|off to toggle ADX auto-switching."
        )
    except Exception as exc:
        await update.message.reply_text(f"ADX error: {exc}")


@_require_auth
async def autostrategy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggle ADX-based automatic strategy selection."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)

    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        enabled = ps.get("auto_strategy", True)
        manual  = ps.get("strategy", "trend")
        await update.message.reply_text(
            f"ADX Auto-Strategy: {'ON \u2705' if enabled else 'OFF \U0001f515'}\n"
            f"Manual strategy: {manual}\n\n"
            "When ON, ADX overrides your manual strategy:\n"
            "  ADX > 25 \u2192 TREND (MACD+RSI)\n"
            "  ADX < 20 \u2192 REVERSAL (Bollinger)\n\n"
            "Commands:\n"
            "  /autostrategy on\n"
            "  /autostrategy off\n"
            "  /adx \u2014 see current ADX value"
        )
        return

    arg = args[0].lower()
    if arg == "on":
        ps["auto_strategy"] = True
        _save_user_persistent_settings()
        await update.message.reply_text(
            "\u2705 ADX Auto-Strategy ENABLED\n"
            "The bot will automatically use TREND or REVERSAL based on ADX strength."
        )
    elif arg == "off":
        ps["auto_strategy"] = False
        _save_user_persistent_settings()
        await update.message.reply_text(
            f"\U0001f515 ADX Auto-Strategy DISABLED\n"
            f"Using manual strategy: {ps.get('strategy','trend').upper()}\n"
            "Change with /strategy trend or /strategy reversal."
        )
    else:
        await update.message.reply_text("Usage: /autostrategy on  or  /autostrategy off")


@_require_auth
async def recommend_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Analyse trade history and recommend best assets, windows, and strategy."""
    chat_id = update.effective_chat.id
    await update.message.reply_text("\u23f3 Analysing your trade history...")

    rows = _read_trades(chat_id)
    if len(rows) < 5:
        await update.message.reply_text(
            f"Not enough data ({len(rows)} trades). "
            "Need at least 5 trades with \U0001f44d/\U0001f44e results."
        )
        return

    lines = ["\U0001f4cb Trade Recommendations\n"]

    # ── Best assets ───────────────────────────────────────────────────────
    asset_groups = _group_win_rate(rows, lambda r: r.get("asset", "?"))
    lines.append("\U0001f4c8 Best assets:")
    sorted_assets = sorted(
        [(lbl, w, t) for lbl, (w, t) in asset_groups.items() if t >= 3],
        key=lambda x: x[1] / x[2], reverse=True,
    )
    if sorted_assets:
        for lbl, w, t in sorted_assets[:5]:
            wr  = w / t * 100
            bar = "\u2588" * round(wr / 20) + "\u2591" * (5 - round(wr / 20))
            lines.append(f"  {lbl}: {w}/{t} ({wr:.0f}%) {bar}")
    else:
        lines.append("  Not enough data per asset yet.")

    # ── Best time windows ─────────────────────────────────────────────────
    lines.append("\n\u23f0 Best time windows (UTC):")
    sorted_times = sorted(
        [(lbl, w, t) for lbl, (w, t) in _group_win_rate(rows, _hour_window).items() if t >= 3],
        key=lambda x: x[1] / x[2], reverse=True,
    )
    if sorted_times:
        for lbl, w, t in sorted_times[:3]:
            lines.append(f"  {lbl}: {w}/{t} ({w/t*100:.0f}%)")
    else:
        lines.append("  Not enough data per window yet.")

    # ── Strategy performance ──────────────────────────────────────────────
    lines.append("\n\U0001f9e0 Strategy performance:")
    macd_rows = [r for r in rows if "MACD" in r.get("reason","") or "EMA" in r.get("reason","")]
    bb_rows   = [r for r in rows if "BB" in r.get("reason","") or "band" in r.get("reason","").lower()]
    if macd_rows:
        lines.append(f"  TREND (MACD+RSI): {len(macd_rows)} trades, {_win_rate(macd_rows):.0f}% win rate")
    if bb_rows:
        lines.append(f"  REVERSAL (BB):    {len(bb_rows)} trades, {_win_rate(bb_rows):.0f}% win rate")
    if not macd_rows and not bb_rows:
        lines.append("  Not enough strategy-tagged data yet.")

    # ── Top recommendation ────────────────────────────────────────────────
    lines.append("\n\u2b50 Top recommendation:")
    if sorted_assets:
        lines.append(f"  Best asset: {sorted_assets[0][0]}")
    if sorted_times:
        lines.append(f"  Best time:  {sorted_times[0][0]}")
        lines.append(f"  Tip: /autotime on to restrict to {sorted_times[0][0]}")

    await update.message.reply_text("\n".join(lines))


@_require_auth
async def scanner_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggle multi-asset scanner mode.

    Usage:
      /scanner on   — scan all SCAN_ASSETS for signals
      /scanner off  — only signal for your selected asset (default)
      /scanner      — show current status
    """
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    us      = _get_user_session(chat_id)

    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        enabled = ps.get("scanner", False)
        await update.message.reply_text(
            f"📡 Multi-Asset Scanner: {'ON ✅' if enabled else 'OFF 🔕'}\n\n"
            f"Scanning {len(SCAN_ASSETS)} assets:\n"
            + "\n".join(f"  • {a}" for a in SCAN_ASSETS) +
            "\n\nCommands:\n"
            "  /scanner on  — enable\n"
            "  /scanner off — disable\n"
            f"  /set_cooldown <seconds> — signal cooldown (current: {ps.get('cooldown', DEFAULT_COOLDOWN_SECONDS)}s)"
        )
        return

    arg = args[0].lower()
    if arg == "on":
        ps["scanner"] = True
        _save_user_persistent_settings()
        # Start scanner tasks if not already running
        if us and not getattr(us, "_scanner_tasks", None):
            us._scanner_tasks = await _start_scanner(us)
        await update.message.reply_text(
            f"📡 Scanner ENABLED ✅\n"
            f"Monitoring {len(SCAN_ASSETS)} assets every {CANDLE_PERIOD}s.\n"
            f"Cooldown: {ps.get('cooldown', DEFAULT_COOLDOWN_SECONDS)}s per asset.\n\n"
            "⚠️ Make sure /autoon is also enabled to receive signals."
        )
    elif arg == "off":
        ps["scanner"] = False
        _save_user_persistent_settings()
        # Cancel scanner tasks
        if us and getattr(us, "_scanner_tasks", None):
            for t in us._scanner_tasks:
                if not t.done():
                    t.cancel()
            us._scanner_tasks = []
        await update.message.reply_text(
            "📡 Scanner DISABLED 🔕\n"
            "Signals will only be sent for your selected asset (/signal)."
        )
    else:
        await update.message.reply_text("Usage: /scanner on  or  /scanner off")


@_require_auth
async def set_cooldown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set the per-asset signal cooldown duration.

    Usage: /set_cooldown <seconds>
    Default: 120 seconds. Minimum: 30 seconds.
    """
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)

    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        current = ps.get("cooldown", DEFAULT_COOLDOWN_SECONDS)
        await update.message.reply_text(
            f"Current cooldown: {current}s\n\n"
            "Usage: /set_cooldown <seconds>\n"
            "Example: /set_cooldown 60\n"
            "Minimum: 30 seconds"
        )
        return

    try:
        secs = int(args[0])
        if secs < 30:
            await update.message.reply_text("Minimum cooldown is 30 seconds.")
            return
        ps["cooldown"] = secs
        _save_user_persistent_settings()
        await update.message.reply_text(
            f"✅ Cooldown set to {secs}s\n"
            "The scanner will wait at least this long before sending another signal for the same asset."
        )
    except ValueError:
        await update.message.reply_text("Invalid value. Usage: /set_cooldown 120")


# ---------------------------------------------------------------------------
# Trade tracking engine
# ---------------------------------------------------------------------------

def _pips(price_diff: float, asset_code: str) -> float:
    """Convert price difference to pips (JPY pairs use 2dp, others 4dp)."""
    if "JPY" in asset_code.upper():
        return round(price_diff * 100, 1)
    return round(price_diff * 10000, 1)


def _trade_keyboard(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("❌ Close trade",  callback_data=f"trade:close:{trade_id}"),
            InlineKeyboardButton("🎯 Set TP/SL",    callback_data=f"trade:tpsl:{trade_id}"),
        ],
    ])


async def _trade_monitor_task(
    chat_id: int,
    trade_id: str,
    us: "UserSession",
) -> None:
    """Background task: edit trade message every 5s with live P&L."""
    trade = active_trades.get(chat_id, {}).get(trade_id)
    if not trade:
        return

    asset_code  = trade["asset_code"]
    asset_label = trade["asset_label"]
    direction   = trade["direction"]
    entry       = trade["entry_price"]
    message_id  = trade["message_id"]
    price_hist: list[float] = [entry]

    try:
        while True:
            await asyncio.sleep(5)

            # Check if trade was closed
            trade = active_trades.get(chat_id, {}).get(trade_id)
            if not trade or trade.get("status") != "active":
                break

            current = await _get_current_price(us, asset_code)
            if current is None:
                continue

            price_hist.append(current)
            if len(price_hist) > 20:
                price_hist.pop(0)

            diff  = current - entry
            pips  = _pips(diff, asset_code)
            pct   = diff / entry * 100 if entry else 0
            arrow = "📈" if diff >= 0 else "📉"

            # P&L direction relative to trade direction
            if direction == "CALL":
                winning = diff > 0
            else:
                winning = diff < 0
                pips    = -pips
                pct     = -pct

            pnl_icon = "✅" if winning else "❌"

            # Mini chart (last 10 prices)
            chart_pts = price_hist[-10:]
            if len(chart_pts) >= 2:
                mn, mx = min(chart_pts), max(chart_pts)
                rng = mx - mn or 1e-10
                bars = ""
                for p in chart_pts:
                    lvl = int((p - mn) / rng * 4)
                    bars += ["▁","▃","▅","▇","█"][lvl]
            else:
                bars = "▬▬▬"

            text = (
                f"{arrow} {asset_label} | {direction}\n"
                f"Entry:   {entry:.5f}\n"
                f"Current: {current:.5f}\n"
                f"P&L: {pips:+.1f} pips  ({pct:+.3f}%)  {pnl_icon}\n"
                f"Chart: {bars}\n"
            )

            # TP/SL check
            tp = trade.get("tp_pips")
            sl = trade.get("sl_pips")
            if tp and abs(pips) >= tp and winning:
                text += f"\n🎯 TAKE-PROFIT HIT! +{abs(pips):.1f} pips"
                trade["status"] = "tp_hit"
                try:
                    await telegram_app.bot.edit_message_text(
                        chat_id=chat_id, message_id=message_id, text=text
                    )
                    await telegram_app.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ **TAKE-PROFIT HIT** on {asset_label}!\nClosed with {pips:+.1f} pips.",
                    )
                except Exception:
                    pass
                break

            if sl and abs(pips) >= sl and not winning:
                text += f"\n🛑 STOP-LOSS HIT! {pips:+.1f} pips"
                trade["status"] = "sl_hit"
                try:
                    await telegram_app.bot.edit_message_text(
                        chat_id=chat_id, message_id=message_id, text=text
                    )
                    await telegram_app.bot.send_message(
                        chat_id=chat_id,
                        text=f"🛑 **STOP-LOSS HIT** on {asset_label}!\nClosed at {pips:+.1f} pips.",
                    )
                except Exception:
                    pass
                break

            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=_trade_keyboard(trade_id),
                )
            except Exception:
                pass

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log_error(f"_trade_monitor_task error: {exc}")
    finally:
        # Mark closed if still active
        trade = active_trades.get(chat_id, {}).get(trade_id)
        if trade and trade.get("status") == "active":
            trade["status"] = "closed"


@_require_auth
async def track_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start monitoring a trade with live P&L updates.

    Usage: /track <asset_code> <entry_price> <CALL|PUT>
    Example: /track EURUSD_otc 1.12345 CALL
    """
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)

    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /track <asset> <entry_price> <CALL|PUT>\n"
            "Example: /track EURUSD_otc 1.12345 CALL\n\n"
            "Or tap 🔍 Follow price on any signal message."
        )
        return

    asset_code = args[0].strip()
    try:
        entry_price = float(args[1])
    except ValueError:
        await update.message.reply_text("Invalid entry price.")
        return

    direction = args[2].upper()
    if direction not in ("CALL", "PUT"):
        await update.message.reply_text("Direction must be CALL or PUT.")
        return

    asset_label = _get_display_label(asset_code)
    trade_id    = f"{chat_id}_{int(time.time())}"

    sent = await update.message.reply_text(
        f"📡 Tracking {asset_label} | {direction}\n"
        f"Entry: {entry_price:.5f}\n"
        "Fetching live price...",
        reply_markup=_trade_keyboard(trade_id),
    )

    if chat_id not in active_trades:
        active_trades[chat_id] = {}

    active_trades[chat_id][trade_id] = {
        "asset_code":  asset_code,
        "asset_label": asset_label,
        "direction":   direction,
        "entry_price": entry_price,
        "message_id":  sent.message_id,
        "status":      "active",
        "tp_pips":     None,
        "sl_pips":     None,
        "ts":          time.time(),
    }

    task = asyncio.create_task(
        _trade_monitor_task(chat_id, trade_id, us),
        name=f"track_{trade_id}",
    )
    active_trades[chat_id][trade_id]["task"] = task


@_require_auth
async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stop tracking the most recent active trade."""
    chat_id = update.effective_chat.id
    trades  = active_trades.get(chat_id, {})
    active  = {tid: t for tid, t in trades.items() if t.get("status") == "active"}

    if not active:
        await update.message.reply_text("No active tracked trades.")
        return

    # Close the most recent
    trade_id = max(active, key=lambda tid: active[tid]["ts"])
    trade    = active[trade_id]
    trade["status"] = "closed"

    task = trade.get("task")
    if task and not task.done():
        task.cancel()

    current = await _get_current_price(
        _get_user_session(chat_id), trade["asset_code"]
    )
    if current:
        diff  = current - trade["entry_price"]
        pips  = _pips(diff, trade["asset_code"])
        if trade["direction"] == "PUT":
            pips = -pips
        result = "win" if pips > 0 else "loss"
        _log_trade(chat_id, {
            "asset_label": trade["asset_label"],
            "tf_label":    "manual",
            "direction":   trade["direction"],
            "confidence":  "",
            "rsi":         "",
            "price":       str(trade["entry_price"]),
            "market":      "",
            "reason":      f"Manual track closed at {current:.5f}",
        }, result)
        await update.message.reply_text(
            f"✅ Trade closed: {trade['asset_label']} {trade['direction']}\n"
            f"Entry: {trade['entry_price']:.5f}\n"
            f"Exit:  {current:.5f}\n"
            f"P&L: {pips:+.1f} pips  ({'WIN' if pips > 0 else 'LOSS'})"
        )
    else:
        await update.message.reply_text(f"Trade {trade['asset_label']} closed.")


@_require_auth
async def set_tp_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set take-profit in pips for the active trade. Usage: /set_tp 5"""
    chat_id = update.effective_chat.id
    trades  = active_trades.get(chat_id, {})
    active  = {tid: t for tid, t in trades.items() if t.get("status") == "active"}

    if not active:
        await update.message.reply_text("No active tracked trades.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /set_tp <pips>  e.g. /set_tp 5")
        return

    try:
        tp = float(args[0])
    except ValueError:
        await update.message.reply_text("Invalid pips value.")
        return

    trade_id = max(active, key=lambda tid: active[tid]["ts"])
    active[trade_id]["tp_pips"] = tp
    await update.message.reply_text(
        f"🎯 Take-profit set at {tp} pips for {active[trade_id]['asset_label']}."
    )


@_require_auth
async def set_sl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set stop-loss in pips for the active trade. Usage: /set_sl 3"""
    chat_id = update.effective_chat.id
    trades  = active_trades.get(chat_id, {})
    active  = {tid: t for tid, t in trades.items() if t.get("status") == "active"}

    if not active:
        await update.message.reply_text("No active tracked trades.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /set_sl <pips>  e.g. /set_sl 3")
        return

    try:
        sl = float(args[0])
    except ValueError:
        await update.message.reply_text("Invalid pips value.")
        return

    trade_id = max(active, key=lambda tid: active[tid]["ts"])
    active[trade_id]["sl_pips"] = sl
    await update.message.reply_text(
        f"🛑 Stop-loss set at {sl} pips for {active[trade_id]['asset_label']}."
    )


@_require_auth
async def account_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show live Pocket Option account balance and session info."""
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)
    sm      = us.session_manager if us else None

    if not (sm and sm.is_connected and sm.client):
        await update.message.reply_text(
            "Not connected to Pocket Option.\n"
            "Use /reconnect or /setssid to connect."
        )
        return

    await update.message.reply_text("⏳ Fetching account info...")
    try:
        balance = await sm.client.balance()
        is_demo = sm.is_demo
        uid_str = _parse_uid_from_ssid(us.ssid) or "unknown"

        # Try server time for sync info
        try:
            server_ts  = await sm.client.get_server_time()
            # get_server_time returns milliseconds on some versions — normalise
            if server_ts and server_ts > 1_000_000_000_000:
                server_ts = server_ts // 1000
            if server_ts and server_ts > 1_000_000_000:
                server_dt = datetime.utcfromtimestamp(server_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                server_dt = "unavailable"
        except Exception:
            server_dt = "unavailable"

        connected_since = (
            sm.connected_at.strftime("%Y-%m-%d %H:%M:%S")
            if sm.connected_at else "unknown"
        )

        await update.message.reply_text(
            f"💰 Pocket Option Account\n\n"
            f"Account type: {'Demo 🎮' if is_demo else 'Real 💵'}\n"
            f"Balance: ${balance:.2f}\n"
            f"UID: {uid_str}\n\n"
            f"Connection: 🟢 Live\n"
            f"Connected since: {connected_since}\n"
            f"Server time: {server_dt}\n\n"
            f"📊 Paper trading balance: ${float(_get_user_settings(chat_id).get('paper_balance', PAPER_START_BALANCE)):.2f}\n\n"
            "Note: Today's P&L is not available via the API.\n"
            "Use /paper for paper trading stats, /analyze for signal history."
        )
    except Exception as exc:
        await update.message.reply_text(f"Could not fetch account info: {exc}")


@_require_auth
async def whoami_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Debug: show UID and demo/real flag parsed from the current SSID."""
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)
    ssid    = us.ssid if us else ""

    if not ssid:
        await update.message.reply_text("No SSID set for this session.")
        return

    # Parse all fields from the SSID JSON
    uid        = "could not parse"
    is_demo_ssid = None
    platform   = None
    session_preview = ""
    parse_error = None

    try:
        payload = json.loads(ssid[2:])   # strip leading "42"
        auth_data = payload[1] if isinstance(payload, list) and len(payload) >= 2 else {}
        uid           = str(auth_data.get("uid", "not found"))
        is_demo_ssid  = auth_data.get("isDemo")
        platform      = auth_data.get("platform")
        session_val   = auth_data.get("session", "")
        session_preview = f"{session_val[:12]}...{session_val[-6:]}" if len(session_val) > 18 else session_val
    except Exception as exc:
        parse_error = str(exc)

    # is_demo from SessionManager (set at connection time)
    sm_is_demo = us.session_manager.is_demo if us and us.session_manager else None

    # Determine effective account type
    if is_demo_ssid is not None:
        account_type = "Demo 🎮" if is_demo_ssid else "Real 💵"
        source = "SSID isDemo flag"
    elif sm_is_demo is not None:
        account_type = "Demo 🎮" if sm_is_demo else "Real 💵"
        source = "SessionManager is_demo setting"
    else:
        account_type = "Unknown"
        source = "no data"

    lines = [
        "🔍 Who Am I?\n",
        f"UID: {uid}",
        f"Account type: {account_type}",
        f"Source: {source}",
        f"Platform: {platform if platform is not None else 'N/A'}",
        f"Session preview: {session_preview}",
        f"SSID length: {len(ssid)} chars",
    ]
    if parse_error:
        lines.append(f"\n⚠️ Parse error: {parse_error}")
    if us and us.session_manager:
        sm = us.session_manager
        lines.append(f"\nSessionManager.is_demo: {sm.is_demo}")
        lines.append(f"Connected: {'yes' if sm.is_connected else 'no'}")
        if sm.connected_at:
            lines.append(f"Connected since: {sm.connected_at.strftime('%H:%M:%S UTC')}")

    await update.message.reply_text("\n".join(lines))


@_require_auth
async def balance_raw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Debug: fetch balance using raw client method and print full response."""
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)
    sm      = us.session_manager if us else None

    if not (sm and sm.is_connected and sm.client):
        await update.message.reply_text(
            "Not connected to Pocket Option.\n"
            "Use /reconnect or /setssid to connect."
        )
        return

    await update.message.reply_text("⏳ Fetching raw balance data...")

    lines = ["💰 Raw Balance Response\n"]

    # 1. Standard balance() call
    try:
        balance = await sm.client.balance()
        lines.append(f"client.balance() → {balance}")
        lines.append(f"Interpreted: ${float(balance):.2f}")
    except Exception as exc:
        lines.append(f"client.balance() → ERROR: {exc}")

    # 2. Try payout() which may return richer data
    try:
        payout_raw = await sm.client.payout()
        lines.append(f"\nclient.payout() raw:\n{str(payout_raw)[:500]}")
    except Exception as exc:
        lines.append(f"\nclient.payout() → ERROR: {exc}")

    # 3. Try opened_deals() for active trade context
    try:
        deals_raw = await sm.client.opened_deals()
        deals_str = str(deals_raw)[:300]
        lines.append(f"\nclient.opened_deals() raw:\n{deals_str}")
    except Exception as exc:
        lines.append(f"\nclient.opened_deals() → ERROR: {exc}")

    # 4. Server time for reference
    try:
        server_ts = await sm.client.get_server_time()
        server_dt = datetime.utcfromtimestamp(server_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append(f"\nServer time: {server_dt}")
    except Exception as exc:
        lines.append(f"\nServer time → ERROR: {exc}")

    # 5. is_demo flag
    lines.append(f"\nis_demo: {sm.is_demo}")
    lines.append(f"UID: {_parse_uid_from_ssid(us.ssid) or 'unknown'}")

    text = "\n".join(lines)
    # Split if too long
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000])
    else:
        await update.message.reply_text(text)


@_require_auth
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show all active tracked trades and paper balance."""
    chat_id = update.effective_chat.id
    trades  = active_trades.get(chat_id, {})
    active  = {tid: t for tid, t in trades.items() if t.get("status") == "active"}
    ps      = _get_user_settings(chat_id)
    balance = float(ps.get("paper_balance", PAPER_START_BALANCE))
    us      = _get_user_session(chat_id)

    lines = ["📊 Dashboard\n"]

    if active:
        lines.append(f"Active trades: {len(active)}")
        for tid, t in active.items():
            current = await _get_current_price(us, t["asset_code"])
            if current:
                diff = current - t["entry_price"]
                pips = _pips(diff, t["asset_code"])
                if t["direction"] == "PUT":
                    pips = -pips
                icon = "✅" if pips > 0 else "❌"
                lines.append(
                    f"  {icon} {t['asset_label']} {t['direction']} "
                    f"| {pips:+.1f} pips"
                )
                tp = t.get("tp_pips")
                sl = t.get("sl_pips")
                if tp or sl:
                    lines.append(f"     TP: {tp or '—'} pips  SL: {sl or '—'} pips")
    else:
        lines.append("No active trades.")

    lines.append(f"\n💰 Paper balance: ${balance:.2f}")

    paper_rows = _read_paper_trades(chat_id)
    if paper_rows:
        total = len(paper_rows)
        wins  = sum(1 for r in paper_rows if r.get("result") == "win")
        pnl   = sum(float(r.get("pnl", 0)) for r in paper_rows)
        lines.append(f"Paper trades: {total}  Win rate: {wins/total*100:.0f}%  P&L: {pnl:+.2f}$")

    await update.message.reply_text("\n".join(lines))


@_require_auth
async def suggestion_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Suggest entry, TP, and SL levels based on ATR for the current asset."""
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)
    ps      = _get_user_settings(chat_id)
    asset_label = ps.get("asset", DEFAULT_SETTINGS["asset"])
    asset_code  = ASSETS.get(asset_label, "EURUSD_otc")
    tf_seconds  = TIMEFRAMES.get(ps.get("timeframe", "1 minute"), 60)

    sm = us.session_manager if us else None
    if not (sm and sm.is_connected):
        await update.message.reply_text("Not connected to Pocket Option.")
        return

    await update.message.reply_text(f"⏳ Calculating ATR suggestion for {asset_label}...")
    try:
        candles = await sm.get_candles(asset=asset_code, timeframe=tf_seconds, count=CANDLE_COUNT)
        if not candles or len(candles) < 16:
            await update.message.reply_text("Not enough data.")
            return

        highs  = [float(c["high"]  if isinstance(c, dict) else c.high)  for c in candles]
        lows   = [float(c["low"]   if isinstance(c, dict) else c.low)   for c in candles]
        closes = _closes(candles)
        current = closes[-1]

        atr = calculate_atr(highs, lows, closes)
        if atr is None:
            await update.message.reply_text("Could not compute ATR.")
            return

        atr_pips = _pips(atr, asset_code)
        tp_pips  = round(atr_pips * 1.5, 1)   # 1.5× ATR for TP
        sl_pips  = round(atr_pips * 1.0, 1)   # 1× ATR for SL

        # Get signal direction for context
        result = compute_signal_advanced(candles, tf_seconds)
        direction = result.get("direction", "WAIT")
        dir_str   = "CALL 📈" if direction == "HIGHER" else "PUT 📉" if direction == "LOWER" else "WAIT ⏸"

        await update.message.reply_text(
            f"💡 ATR Suggestion — {asset_label}\n\n"
            f"Current price: {current:.5f}\n"
            f"ATR (14): {atr:.5f}  ({atr_pips:.1f} pips)\n\n"
            f"Signal: {dir_str}\n\n"
            f"Suggested levels:\n"
            f"  Entry: {current:.5f}\n"
            f"  Take-profit: {tp_pips:.1f} pips  ({current + atr*1.5:.5f})\n"
            f"  Stop-loss:   {sl_pips:.1f} pips  ({current - atr:.5f})\n\n"
            f"Use /track {asset_code} {current:.5f} CALL to start monitoring."
        )
    except Exception as exc:
        await update.message.reply_text(f"Suggestion error: {exc}")


async def trade_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline buttons on tracked trade messages."""
    query = update.callback_query
    await query.answer()
    data    = query.data   # "trade:close:<trade_id>" or "trade:tpsl:<trade_id>"
    parts   = data.split(":", 2)
    if len(parts) != 3:
        return

    _, action, trade_id = parts
    chat_id = query.message.chat.id
    trades  = active_trades.get(chat_id, {})
    trade   = trades.get(trade_id)

    if not trade:
        await query.answer("Trade not found or already closed.")
        return

    if action == "close":
        trade["status"] = "closed"
        task = trade.get("task")
        if task and not task.done():
            task.cancel()
        await query.edit_message_text(
            query.message.text + "\n\n✅ Trade closed manually.",
        )

    elif action == "tpsl":
        await query.answer()
        await telegram_app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Set TP/SL for {trade['asset_label']}:\n"
                f"  /set_tp <pips>  — e.g. /set_tp 5\n"
                f"  /set_sl <pips>  — e.g. /set_sl 3"
            ),
        )


@_require_auth
async def paper_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show paper trading virtual balance and stats."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    balance = float(ps.get("paper_balance", PAPER_START_BALANCE))
    stake   = float(ps.get("paper_stake",   PAPER_STAKE))
    rows    = _read_paper_trades(chat_id)

    total = len(rows)
    wins  = sum(1 for r in rows if r.get("result") == "win")
    pnl   = sum(float(r.get("pnl", 0)) for r in rows)
    wr    = f"{wins/total*100:.1f}%" if total else "—"

    last_5 = rows[-5:] if rows else []
    last_5_str = "  ".join(
        ("✅" if r.get("result") == "win" else "❌") for r in last_5
    ) or "—"

    await update.message.reply_text(
        f"📊 Paper Trading\n\n"
        f"Virtual balance: ${balance:.2f}\n"
        f"Starting balance: ${PAPER_START_BALANCE:.2f}\n"
        f"Total P&L: {'+' if pnl >= 0 else ''}{pnl:.2f}$\n\n"
        f"Total trades: {total}\n"
        f"Wins: {wins}  Win rate: {wr}\n"
        f"Stake per trade: ${stake:.2f}\n"
        f"Last 5: {last_5_str}\n\n"
        "Use /paper_reset to reset balance.\n"
        "Use /export_paper to download paper trades CSV."
    )


@_require_auth
async def paper_reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reset paper trading balance and history."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    ps["paper_balance"] = PAPER_START_BALANCE
    _save_user_persistent_settings()
    # Archive old paper trades
    path = _paper_path(chat_id)
    if os.path.exists(path):
        archive = path.replace(".csv", f"_archive_{int(time.time())}.csv")
        try:
            os.rename(path, archive)
        except Exception:
            pass
    await update.message.reply_text(
        f"✅ Paper trading reset.\n"
        f"Balance restored to ${PAPER_START_BALANCE:.2f}.\n"
        "Previous trades archived."
    )


@_require_auth
async def export_paper_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export paper trades CSV."""
    chat_id = update.effective_chat.id
    path    = _paper_path(chat_id)
    if not os.path.exists(path):
        await update.message.reply_text("No paper trades yet.")
        return
    try:
        with open(path, "rb") as f:
            await telegram_app.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=f"paper_{chat_id}.csv",
                caption=f"📁 Paper trades — {len(_read_paper_trades(chat_id))} trades",
            )
    except Exception as exc:
        await update.message.reply_text(f"Export error: {exc}")


@_require_auth
async def validation_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show auto-validation stats."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    rows    = _read_validations(chat_id)
    enabled = ps.get("auto_validate", True)

    total = len(rows)
    wins  = sum(1 for r in rows if r.get("result") == "win")
    wr    = f"{wins/total*100:.1f}%" if total else "—"

    last_10 = rows[-10:]
    last_10_str = "  ".join(
        ("✅" if r.get("result") == "win" else "❌") for r in last_10
    ) or "—"

    await update.message.reply_text(
        f"🔍 Signal Validation\n\n"
        f"Auto-validate: {'ON ✅' if enabled else 'OFF 🔕'}\n"
        f"Validation delay: {VALIDATION_SECONDS}s\n\n"
        f"Total validated: {total}\n"
        f"Wins: {wins}  Win rate: {wr}\n"
        f"Last 10: {last_10_str}\n\n"
        "Toggle: /autovalidate on|off"
    )


@_require_auth
async def autovalidate_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggle automatic signal validation and paper trading."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)

    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        enabled = ps.get("auto_validate", True)
        await update.message.reply_text(
            f"Auto-validate: {'ON ✅' if enabled else 'OFF 🔕'}\n\n"
            "When ON, the bot automatically checks the price after 60s\n"
            "and updates your paper trading balance.\n\n"
            "Commands:\n"
            "  /autovalidate on\n"
            "  /autovalidate off"
        )
        return

    arg = args[0].lower()
    if arg == "on":
        ps["auto_validate"] = True
        _save_user_persistent_settings()
        await update.message.reply_text(
            "✅ Auto-validation ON\n"
            f"Signals will be validated {VALIDATION_SECONDS}s after generation.\n"
            "Paper balance updated automatically."
        )
    elif arg == "off":
        ps["auto_validate"] = False
        _save_user_persistent_settings()
        await update.message.reply_text(
            "🔕 Auto-validation OFF\n"
            "No automatic validation or paper trading updates."
        )
    else:
        await update.message.reply_text("Usage: /autovalidate on  or  /autovalidate off")


@_require_auth
async def manual_trades_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List recent manually logged trades (from 👍/👎 votes)."""
    chat_id = update.effective_chat.id
    rows    = _read_trades(chat_id)
    if not rows:
        await update.message.reply_text("No trades logged yet. Mark signals with 👍/👎.")
        return

    last_10 = rows[-10:]
    lines   = ["📋 Recent Manual Trades\n"]
    for r in reversed(last_10):
        icon = "✅" if r.get("result") == "win" else "❌"
        lines.append(
            f"{icon} {r.get('timestamp','')[:16]}  "
            f"{r.get('asset','')}  {r.get('direction','')}  "
            f"conf={r.get('confidence','')}%"
        )
    lines.append(f"\nTotal: {len(rows)} trades. Use /export for full CSV.")
    await update.message.reply_text("\n".join(lines))


    lines.append(f"\nTotal: {len(rows)} trades. Use /export for full CSV.")
    await update.message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# TradingSession commands & live tracking
# ---------------------------------------------------------------------------

def _session_stake_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("$1",    callback_data="sess:stake:1"),
         InlineKeyboardButton("$1.5",  callback_data="sess:stake:1.5"),
         InlineKeyboardButton("$2",    callback_data="sess:stake:2")],
        [InlineKeyboardButton("$2.5",  callback_data="sess:stake:2.5"),
         InlineKeyboardButton("$5",    callback_data="sess:stake:5"),
         InlineKeyboardButton("$10",   callback_data="sess:stake:10")],
        [InlineKeyboardButton("✏️ Custom", callback_data="sess:stake:custom")],
        [InlineKeyboardButton("🔙 Back",   callback_data="menu:back")],
    ])


def _session_expiry_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("60s",  callback_data="sess:expiry:60"),
         InlineKeyboardButton("120s", callback_data="sess:expiry:120"),
         InlineKeyboardButton("300s", callback_data="sess:expiry:300")],
        [InlineKeyboardButton("✏️ Custom", callback_data="sess:expiry:custom")],
    ])


def _session_asset_keyboard() -> InlineKeyboardMarkup:
    """Keyboard showing all available OTC assets for session selection."""
    rows = []
    items = list(ASSETS.keys())
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(items[i], callback_data=f"sess:asset:{items[i]}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(items[i+1], callback_data=f"sess:asset:{items[i+1]}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


async def _live_trade_loop(
    session: TradingSession,
    chat_id: int,
    message_id: int,
    us: "UserSession",
) -> None:
    """Background task: edit trade message every 5s with live P&L, then auto-evaluate."""
    try:
        expiry   = session.expiry_seconds
        elapsed  = 0
        interval = 5

        while elapsed < expiry:
            await asyncio.sleep(interval)
            elapsed += interval

            current = await _get_current_price(us, session.asset_code)
            if current is None:
                continue

            diff      = current - session.entry_price
            direction = session.trade_direction or "CALL"
            pct       = diff / session.entry_price * 100 if session.entry_price else 0
            remaining = max(0, expiry - elapsed)

            if direction == "CALL":
                winning = diff > 0
                pnl_str = f"{diff:+.5f} ({pct:+.3f}%)"
            else:
                winning = diff < 0
                pnl_str = f"{-diff:+.5f} ({-pct:+.3f}%)"

            icon = "✅" if winning else "❌"
            arrow = "📈" if diff >= 0 else "📉"

            text = (
                f"📊 Live Trade | {session.asset_label}\n"
                f"Entry: {session.entry_price:.5f} ({direction})\n"
                f"Remaining: {remaining}s\n"
                f"Current: {current:.5f}\n"
                f"P&L: {pnl_str}  {icon}\n"
                f"{arrow} Stake: ${session.next_stake():.2f}"
            )
            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id, text=text
                )
            except Exception:
                pass

        # ── Auto-evaluate at expiry ───────────────────────────────────────
        final_price = await _get_current_price(us, session.asset_code) or session.entry_price
        diff        = final_price - session.entry_price
        direction   = session.trade_direction or "CALL"
        won         = (diff > MIN_PRICE_MOVE) if direction == "CALL" else (diff < -MIN_PRICE_MOVE)

        if won:
            profit = session.record_win()
            result_icon = "✅ WIN"
        else:
            profit = session.record_loss()
            result_icon = "❌ LOSS"

        # Deduct energy
        ps = _get_user_settings(chat_id)
        ps["energy"] = max(0, ps.get("energy", 1000) - 10)
        _save_user_persistent_settings()

        final_text = (
            f"🏁 Trade Result | {session.asset_label}\n\n"
            f"{result_icon}  {direction}\n"
            f"Entry:  {session.entry_price:.5f}\n"
            f"Exit:   {final_price:.5f}  ({diff:+.5f})\n"
            f"Profit: {'+' if profit >= 0 else ''}{profit:.2f}$\n\n"
            f"{session.summary()}"
        )

        if session.is_target_reached():
            final_text += f"\n\n🎉 TARGET REACHED! Session complete."
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 New Session", callback_data="sess:new"),
                 InlineKeyboardButton("📋 Manual Mode", callback_data="menu:back")],
            ])
            session.active = False
            trading_sessions.pop(chat_id, None)
        else:
            stake = session.next_stake()
            final_text += f"\n\n👉 Next stake: ${stake:.2f} (Step {session.current_step + 1})"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 CALL", callback_data=f"sess:trade:CALL"),
                 InlineKeyboardButton("📉 PUT",  callback_data=f"sess:trade:PUT")],
                [InlineKeyboardButton("⏹ End Session", callback_data="sess:end")],
            ])

        try:
            await telegram_app.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=final_text, reply_markup=keyboard,
            )
        except Exception:
            await telegram_app.bot.send_message(
                chat_id=chat_id, text=final_text, reply_markup=keyboard
            )

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log_error(f"_live_trade_loop error: {exc}")
    finally:
        session.update_task = None


async def _start_live_trade(chat_id: int, direction: str,
                             expiry: int, us: "UserSession") -> None:
    """Start a live trade for the user's active session."""
    session = trading_sessions.get(chat_id)
    if not session:
        await telegram_app.bot.send_message(
            chat_id=chat_id,
            text="No active session. Use 🎮 New Session from the menu."
        )
        return

    # Get entry price
    entry = await _get_current_price(us, session.asset_code) or 0.0
    session.start_trade(direction, entry, expiry)

    stake = session.next_stake()
    sent  = await telegram_app.bot.send_message(
        chat_id=chat_id,
        text=(
            f"📊 Live Trade | {session.asset_label}\n"
            f"Entry: {entry:.5f} ({direction})\n"
            f"Remaining: {expiry}s\n"
            f"Stake: ${stake:.2f}\n"
            "Tracking..."
        ),
    )
    session.live_message_id = sent.message_id
    session.stop_live_tracking()
    session.update_task = asyncio.create_task(
        _live_trade_loop(session, chat_id, sent.message_id, us),
        name=f"live_{chat_id}",
    )


@_require_auth
async def session_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start a new trading session — shows asset picker."""
    chat_id = update.effective_chat.id
    # Cancel any existing session
    old = trading_sessions.pop(chat_id, None)
    if old:
        old.stop_live_tracking()
    await update.message.reply_text(
        "🎮 New Trading Session\n\nSelect an asset:",
        reply_markup=_session_asset_keyboard(),
    )


@_require_auth
async def trade_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually trigger a trade in the active session. Usage: /trade CALL [expiry]"""
    chat_id = update.effective_chat.id
    us      = _get_user_session(chat_id)
    session = trading_sessions.get(chat_id)

    if not session:
        await update.message.reply_text(
            "No active session. Start one with /session or 🎮 New Session."
        )
        return

    args = context.args or []
    direction = (args[0].upper() if args else "CALL")
    if direction not in ("CALL", "PUT"):
        direction = "CALL"
    expiry = int(args[1]) if len(args) > 1 and args[1].isdigit() else 60

    await _start_live_trade(chat_id, direction, expiry, us)


@_require_auth
async def end_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """End the current trading session."""
    chat_id = update.effective_chat.id
    session = trading_sessions.pop(chat_id, None)
    if not session:
        await update.message.reply_text("No active session.")
        return
    session.stop_live_tracking()
    await update.message.reply_text(
        f"⏹ Session ended.\n\n{session.summary()}"
    )


@_require_auth
async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch between demo and real mode. Usage: /mode demo | /mode real"""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    us      = _get_user_session(chat_id)

    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1:
            args = parts[1:]

    if not args:
        current = "Demo 🎮" if ps.get("is_demo", True) else "Real 💵"
        await update.message.reply_text(
            f"Current mode: {current}\n\n"
            "Switch with:\n  /mode demo\n  /mode real"
        )
        return

    arg = args[0].lower()
    if arg == "demo":
        ps["is_demo"] = True
        _save_user_persistent_settings()
        if us and us.session_manager:
            us.session_manager.is_demo = True
        await update.message.reply_text(
            "🎮 Switched to DEMO mode.\n"
            "Signals and paper trading use demo balance."
        )
    elif arg == "real":
        ps["is_demo"] = False
        _save_user_persistent_settings()
        if us and us.session_manager:
            us.session_manager.is_demo = False
        await update.message.reply_text(
            "💵 Switched to REAL mode.\n"
            "⚠️ Signals now reference real account data."
        )
    else:
        await update.message.reply_text("Usage: /mode demo  or  /mode real")


@_require_auth
async def stake_table_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show recommended balance for each stake size."""
    lines = [
        "📊 Stake Recommendation Table\n",
        "STAKE    MIN BALANCE",
        "─────────────────────",
    ]
    for stake in [1.0, 1.5, 2.0, 2.5, 5.0, 10.0, 20.0, 50.0, 100.0]:
        rec = stake * 20
        lines.append(f"${stake:<8.2f} ${rec:.2f}")
    lines.append("\nFormula: min_balance = stake × 20")
    await update.message.reply_text("\n".join(lines))


@_require_auth
async def energy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current energy level."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    energy  = ps.get("energy", 1000)
    bar     = "⚡" * (energy // 100) + "░" * (10 - energy // 100)
    await update.message.reply_text(
        f"⚡ Energy: {energy}/1000\n{bar}\n\n"
        "Energy decreases by 10 per trade.\n"
        "Use /energy_recharge to restore (once per day)."
    )


@_require_auth
async def energy_recharge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Recharge energy to 1000."""
    chat_id = update.effective_chat.id
    ps      = _get_user_settings(chat_id)
    ps["energy"] = 1000
    _save_user_persistent_settings()
    await update.message.reply_text("⚡ Energy recharged to 1000!")


async def session_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all sess:* callback buttons."""
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat.id
    us      = _get_user_session(chat_id)

    if not us:
        await query.edit_message_text("⛔ Not authorised.")
        return

    # ── Asset selection ───────────────────────────────────────────────────
    if data.startswith("sess:asset:"):
        asset_label = data[len("sess:asset:"):]
        context.user_data["sess_asset"] = asset_label
        await query.edit_message_text(
            f"Asset: {asset_label}\n\nSelect stake amount:",
            reply_markup=_session_stake_keyboard(),
        )
        return

    # ── Stake selection ───────────────────────────────────────────────────
    if data.startswith("sess:stake:"):
        val = data[len("sess:stake:"):]
        if val == "custom":
            await query.edit_message_text(
                "Send your custom stake amount (e.g. 3.5):"
            )
            context.user_data["awaiting_custom_stake"] = True
            return

        try:
            stake = float(val)
        except ValueError:
            stake = 1.0

        asset_label = context.user_data.get("sess_asset", "EUR/USD (OTC)")
        asset_code  = ASSETS.get(asset_label, "EURUSD_otc")

        # Create session
        session = TradingSession(
            chat_id=chat_id,
            asset_label=asset_label,
            asset_code=asset_code,
            base_stake=stake,
        )
        trading_sessions[chat_id] = session

        await query.edit_message_text(
            f"🚀 Trading session initialized!\n\n"
            f"Asset: {asset_label}\n"
            f"Base stake: ${stake:.2f}\n"
            f"Target profit: +${session.target_profit:.2f} (73%)\n"
            f"Step: 1\n\n"
            "👉 When ready, tap CALL or PUT to start a trade:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 CALL", callback_data="sess:trade:CALL"),
                 InlineKeyboardButton("📉 PUT",  callback_data="sess:trade:PUT")],
                [InlineKeyboardButton("⏹ End Session", callback_data="sess:end")],
            ]),
        )
        return

    # ── Trade direction ───────────────────────────────────────────────────
    if data.startswith("sess:trade:"):
        direction = data[len("sess:trade:"):]
        session   = trading_sessions.get(chat_id)
        if not session:
            await query.edit_message_text("No active session. Start one with /session.")
            return
        # Ask for expiry
        context.user_data["sess_direction"] = direction
        await query.edit_message_text(
            f"Direction: {direction}\nSelect expiry time:",
            reply_markup=_session_expiry_keyboard(),
        )
        return

    # ── Expiry selection → start live trade ──────────────────────────────
    if data.startswith("sess:expiry:"):
        val = data[len("sess:expiry:"):]
        if val == "custom":
            await query.edit_message_text("Send expiry in seconds (e.g. 90):")
            context.user_data["awaiting_custom_expiry"] = True
            return
        expiry    = int(val)
        direction = context.user_data.get("sess_direction", "CALL")
        await query.edit_message_text(f"⏳ Starting {direction} trade ({expiry}s)...")
        await _start_live_trade(chat_id, direction, expiry, us)
        return

    # ── New session ───────────────────────────────────────────────────────
    if data == "sess:new":
        old = trading_sessions.pop(chat_id, None)
        if old:
            old.stop_live_tracking()
        await query.edit_message_text(
            "🎮 New Trading Session\n\nSelect an asset:",
            reply_markup=_session_asset_keyboard(),
        )
        return

    # ── End session ───────────────────────────────────────────────────────
    if data == "sess:end":
        session = trading_sessions.pop(chat_id, None)
        if session:
            session.stop_live_tracking()
            await query.edit_message_text(
                f"⏹ Session ended.\n\n{session.summary()}"
            )
        else:
            await query.edit_message_text("No active session.")
        return


conv_handler = ConversationHandler(
    entry_points=[
        CommandHandler("start", start),
        CallbackQueryHandler(button_handler, pattern="^(?!vote:|follow:|trade:|sess:)"),
    ],
    states={
        CHOOSE_ASSET: [CallbackQueryHandler(button_handler, pattern="^(?!vote:|follow:|trade:|sess:)")],
        CHOOSE_TIME:  [CallbackQueryHandler(button_handler, pattern="^(?!vote:|follow:|trade:|sess:)")],
    },
    fallbacks=[CommandHandler("start", start)],
    per_message=False,
)

# Vote and follow handlers registered BEFORE conv_handler so they take priority
telegram_app.add_handler(CallbackQueryHandler(vote_noop_handler,       pattern="^vote:noop$"))
telegram_app.add_handler(CallbackQueryHandler(vote_handler,            pattern="^vote:(win|loss):"))
telegram_app.add_handler(CallbackQueryHandler(follow_handler,          pattern="^follow:"))
telegram_app.add_handler(CallbackQueryHandler(trade_callback_handler,  pattern="^trade:"))
telegram_app.add_handler(CallbackQueryHandler(session_callback_handler, pattern="^sess:"))
telegram_app.add_handler(conv_handler)
telegram_app.add_handler(CommandHandler("signal",             signal_command))
telegram_app.add_handler(CommandHandler("stats",              stats_command))
telegram_app.add_handler(CommandHandler("analyze",            analyze_command))
telegram_app.add_handler(CommandHandler("export",             export_command))
telegram_app.add_handler(CommandHandler("session",            session_command))
telegram_app.add_handler(CommandHandler("trade",              trade_session_command))
telegram_app.add_handler(CommandHandler("end_session",        end_session_command))
telegram_app.add_handler(CommandHandler("mode",               mode_command))
telegram_app.add_handler(CommandHandler("stake_table",        stake_table_command))
telegram_app.add_handler(CommandHandler("energy",             energy_command))
telegram_app.add_handler(CommandHandler("energy_recharge",    energy_recharge_command))
telegram_app.add_handler(CommandHandler("paper",              paper_command))
telegram_app.add_handler(CommandHandler("paper_reset",        paper_reset_command))
telegram_app.add_handler(CommandHandler("export_paper",       export_paper_command))
telegram_app.add_handler(CommandHandler("validation",         validation_command))
telegram_app.add_handler(CommandHandler("autovalidate",       autovalidate_command))
telegram_app.add_handler(CommandHandler("manual_trades",      manual_trades_command))
telegram_app.add_handler(CommandHandler("strategy",           strategy_command))
telegram_app.add_handler(CommandHandler("autotime",           autotime_command))
telegram_app.add_handler(CommandHandler("adx",                adx_command))
telegram_app.add_handler(CommandHandler("autostrategy",       autostrategy_command))
telegram_app.add_handler(CommandHandler("recommend",          recommend_command))
telegram_app.add_handler(CommandHandler("scanner",            scanner_command))
telegram_app.add_handler(CommandHandler("set_cooldown",       set_cooldown_command))
telegram_app.add_handler(CommandHandler("track",              track_command))
telegram_app.add_handler(CommandHandler("close",              close_command))
telegram_app.add_handler(CommandHandler("set_tp",             set_tp_command))
telegram_app.add_handler(CommandHandler("set_sl",             set_sl_command))
telegram_app.add_handler(CommandHandler("dashboard",          dashboard_command))
telegram_app.add_handler(CommandHandler("suggestion",         suggestion_command))
telegram_app.add_handler(CommandHandler("account",            account_command))
telegram_app.add_handler(CommandHandler("whoami",             whoami_command))
telegram_app.add_handler(CommandHandler("balance_raw",        balance_raw_command))
telegram_app.add_handler(CommandHandler("restrict_window",    restrict_window_command))
telegram_app.add_handler(CommandHandler("clear_restriction",  clear_restriction_command))
telegram_app.add_handler(CommandHandler("enable_news_filter",  enable_news_filter_command))
telegram_app.add_handler(CommandHandler("disable_news_filter", disable_news_filter_command))
telegram_app.add_handler(CommandHandler("news",                news_status_command))
telegram_app.add_handler(CommandHandler("autoon",             autoon_command))
telegram_app.add_handler(CommandHandler("autooff",            autooff_command))
telegram_app.add_handler(CommandHandler("status",             status_command))
telegram_app.add_handler(CommandHandler("debug",              debug_command))
telegram_app.add_handler(CommandHandler("logs",               logs_command))
telegram_app.add_handler(CommandHandler("reconnect",          reconnect_command))
telegram_app.add_handler(CommandHandler("setssid",            setssid_command))
telegram_app.add_handler(CommandHandler("getssid",            getssid_command))
telegram_app.add_handler(CommandHandler("login_help",         login_help_command))


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
    # Only show primary users (not broadcast aliases) to avoid duplicates
    primary_users = {
        cid: us for cid, us in user_registry.items() if us.chat_id == cid
    }
    users_status = {
        str(cid): {
            "name":          us.name,
            "connected":     us.session_manager is not None and us.session_manager.is_connected,
            "broadcast_ids": us.all_broadcast_ids(),
            "last_signal":   us.latest_signal,
        }
        for cid, us in primary_users.items()
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
    _load_user_persistent_settings()

    # Fetch SSID from Cloudflare Worker if configured (overrides env var)
    worker_ssid = await fetch_ssid_from_worker()
    if worker_ssid:
        logger.info("Using SSID from Cloudflare Worker relay")
        # Patch the module-level SSID used by _load_user_configs fallback
        import sys
        sys.modules[__name__].__dict__["SSID"] = worker_ssid

    # Pre-fetch economic calendar
    await _refresh_calendar()

    # Build user registry and start per-user sessions
    configs = _load_user_configs()
    for cfg in configs:
        us = UserSession(
            name=cfg["name"],
            chat_id=cfg["chat_id"],
            ssid=cfg["ssid"],
            is_demo=cfg["is_demo"],
            broadcast_ids=cfg.get("broadcast_ids", []),
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

        # Start scanner tasks if any registered user has scanner enabled
        us._scanner_tasks = []
        us._scanner_cooldowns = {}
        all_cids = [us.chat_id] + us.broadcast_ids
        if any(_get_user_settings(cid).get("scanner", False) for cid in all_cids):
            us._scanner_tasks = await _start_scanner(us)

        # Register primary chat_id
        user_registry[us.chat_id] = us
        logger.info(f"Registered user: {us.name} (chat_id={us.chat_id})")

        # Also register broadcast IDs so they can use commands (/start, /signal, etc.)
        # They share the same UserSession (same SSID, same connection)
        for extra_id in us.broadcast_ids:
            if extra_id != us.chat_id:
                user_registry[extra_id] = us
                logger.info(f"  Registered broadcast recipient {extra_id} → [{us.name}]")

    # Start Cloudflare Worker SSID refresh loop (only if Worker is configured)
    worker_task = None
    if WORKER_URL and WORKER_API_KEY:
        worker_task = asyncio.create_task(_worker_refresh_loop(), name="worker_refresh")
        logger.info(f"Worker refresh loop started (URL: {WORKER_URL})")

    yield  # ── server is running ──────────────────────────────────────────

    # Shutdown all user sessions
    seen = set()
    for us in user_registry.values():
        if id(us) in seen:
            continue
        seen.add(id(us))
        if us.poll_task and not us.poll_task.done():
            us.poll_task.cancel()
            try:
                await us.poll_task
            except (asyncio.CancelledError, Exception):
                pass
        for t in getattr(us, "_scanner_tasks", []):
            if not t.done():
                t.cancel()
        if us.session_manager:
            await us.session_manager.stop()

    if worker_task and not worker_task.done():
        worker_task.cancel()

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
