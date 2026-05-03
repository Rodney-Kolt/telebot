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
    ps.setdefault("strategy",  "trend")
    ps.setdefault("auto",      False)
    ps.setdefault("asset",     "EUR/USD (OTC)")
    ps.setdefault("timeframe", "1 minute")
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
# Analytics helpers
# ---------------------------------------------------------------------------

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
            # Conditions not both met — explain why
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
                        strategy: str = "trend") -> dict:
    """
    Fetch candles and compute a signal using the specified strategy.

    strategy: "trend"    → MACD+RSI (default)
              "reversal" → Bollinger Bands reversal
    """
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
        if strategy == "reversal":
            return compute_signal_bollinger(candles, tf_seconds)
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
        "user_id":    chat_id,
        "ts":         time.time(),
        "voted":      False,
        # Metadata for CSV logging
        "asset_label": asset_label,
        "tf_label":    tf_label,
        "direction":   "CALL" if result["direction"] == "HIGHER" else "PUT",
        "confidence":  result.get("confidence", ""),
        "rsi":         result.get("rsi", ""),
        "price":       price_str,
        "market":      result.get("market", ""),
        "reason":      result.get("reason", ""),
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
    ps   = _get_user_settings(user_id)
    auto = ps.get("auto", False)
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
    text += (
        "\nTap a button below, or use these commands:\n"
        "/signal \u2014 latest signal\n"
        "/strategy \u2014 switch between trend / reversal\n"
        "/stats \u2014 your win/loss statistics\n"
        "/analyze \u2014 detailed trade analytics\n"
        "/analyze --refresh \u2014 recompute best time window\n"
        "/autotime on|off \u2014 restrict signals to your best hours\n"
        "/export \u2014 download your trade log (CSV)\n"
        "/restrict_window \u2014 only receive signals in your best hours\n"
        "/clear_restriction \u2014 remove time-window filter\n"
        "/news \u2014 upcoming high-impact events + filter status\n"
        "/enable_news_filter \u2014 pause signals before news\n"
        "/disable_news_filter \u2014 trade through news\n"
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
                                     strategy=settings.get("strategy", "trend"))

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
    result = await _fetch_signal(asset_code, tf_seconds, us, strategy=user_strategy)

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

    if not context.args:
        current = _get_user_settings(update.effective_chat.id).get("strategy", "trend")
        await update.message.reply_text(
            f"📊 Current strategy: {current}\n\n"
            "Available strategies:\n"
            "  /strategy trend    — MACD+RSI (requires RSI extreme + MACD confirmation)\n"
            "  /strategy reversal — Bollinger Bands reversal (price bouncing off bands)"
        )
        return

    chosen = context.args[0].lower()
    if chosen not in ("trend", "reversal"):
        await update.message.reply_text(
            "❌ Unknown strategy. Choose:\n"
            "  /strategy trend\n"
            "  /strategy reversal"
        )
        return

    _get_user_settings(update.effective_chat.id)["strategy"] = chosen
    _save_user_persistent_settings()
    descriptions = {
        "trend":    "MACD+RSI — signals when RSI is extreme AND MACD confirms direction",
        "reversal": "Bollinger Bands — signals when price bounces off upper/lower band",
    }
    await update.message.reply_text(
        f"✅ Strategy set to: {chosen}\n\n"
        f"{descriptions[chosen]}\n\n"
        "Use /signal to test it now."
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
telegram_app.add_handler(CommandHandler("signal",             signal_command))
telegram_app.add_handler(CommandHandler("stats",              stats_command))
telegram_app.add_handler(CommandHandler("analyze",            analyze_command))
telegram_app.add_handler(CommandHandler("export",             export_command))
telegram_app.add_handler(CommandHandler("strategy",           strategy_command))
telegram_app.add_handler(CommandHandler("autotime",           autotime_command))
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

        # Register primary chat_id
        user_registry[us.chat_id] = us
        logger.info(f"Registered user: {us.name} (chat_id={us.chat_id})")

        # Also register broadcast IDs so they can use commands (/start, /signal, etc.)
        # They share the same UserSession (same SSID, same connection)
        for extra_id in us.broadcast_ids:
            if extra_id != us.chat_id:
                user_registry[extra_id] = us
                logger.info(f"  Registered broadcast recipient {extra_id} → [{us.name}]")

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
