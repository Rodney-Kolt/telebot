# OTC Signal Bot — Clean version
# Core: signals, scanner, win/loss tracking, analytics, SSID management
import os, csv, json, time, asyncio, logging, httpx
from contextlib import asynccontextmanager
from datetime import datetime
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN not set")

IS_RENDER          = os.environ.get("RENDER") == "true"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
WORKER_URL         = os.environ.get("WORKER_URL", "").rstrip("/")
WORKER_API_KEY     = os.environ.get("WORKER_API_KEY", "")
SSID               = os.environ.get("SSID", "")
CHAT_ID            = os.environ.get("CHAT_ID", "")
CANDLE_PERIOD      = 60
CANDLE_COUNT       = 50
SIGNAL_EXPIRY_SEC  = 600
MIN_PRICE_MOVE     = 0.0001
DEFAULT_COOLDOWN   = 120
MAX_SCAN_ASSETS    = 10

_DEFAULT_IMG_BUY  = "https://i.ibb.co/HDC7G1D0/image.jpg"
_DEFAULT_IMG_SELL = "https://i.ibb.co/YTfbPc72/image.jpg"
SIGNAL_IMG_BUY    = os.environ.get("SIGNAL_IMG_BUY",  "").strip() or _DEFAULT_IMG_BUY
SIGNAL_IMG_SELL   = os.environ.get("SIGNAL_IMG_SELL", "").strip() or _DEFAULT_IMG_SELL

def _load_scan_assets():
    raw = os.environ.get("SCAN_ASSETS", "").strip()
    if raw:
        return [a.strip() for a in raw.split(",") if a.strip()][:MAX_SCAN_ASSETS]
    return ["EURUSD_otc","GBPUSD_otc","USDJPY_otc","AUDUSD_otc","EURUSD_otc","EURTRY_otc"]

SCAN_ASSETS = _load_scan_assets()

ASSETS = {
    "EUR/USD (OTC)": "EURUSD_otc",
    "AUD/USD (OTC)": "AUDUSD_otc",
    "USD/JPY (OTC)": "USDJPY_otc",
    "GBP/USD (OTC)": "GBPUSD_otc",
    "USD/CAD (OTC)": "USDCAD_otc",
    "EUR/JPY (OTC)": "EURJPY_otc",
    "USD/ZAR (OTC)": "USDZARUSD_otc",
    "USD/TRY (OTC)": "USDTRY_otc",
    "USD/MXN (OTC)": "USDMXN_otc",
    "USD/INR (OTC)": "USDINR_otc",
    "EUR/TRY (OTC)": "EURTRY_otc",
    "GBP/ZAR (OTC)": "GBPZAR_otc",
}

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
# Per-user settings: {chat_id: {"auto": bool, "scanner": bool, "asset": str, "timeframe": str}}
user_settings: dict[int, dict] = {}
# Known users for broadcast
known_users: set[int] = set()
# Pending signals for win/loss voting
pending_signals: dict[str, dict] = {}
# Scanner cooldowns: {f"{chat_id}:{asset}": timestamp}
scanner_cooldowns: dict[str, float] = {}
# Scanner tasks per UserSession
scanner_tasks: list = []
# Recent errors
recent_errors: list[str] = []
MAX_ERRORS = 20

def log_error(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    recent_errors.append(f"[{ts}] {msg}")
    if len(recent_errors) > MAX_ERRORS:
        recent_errors.pop(0)
    logger.error(msg)

def _get_settings(chat_id: int) -> dict:
    if chat_id not in user_settings:
        user_settings[chat_id] = {"auto": False, "scanner": False,
                                   "asset": "EUR/USD (OTC)", "timeframe": "1 minute"}
    return user_settings[chat_id]

# ---------------------------------------------------------------------------
# CSV trade logging
# ---------------------------------------------------------------------------
_BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
_CSV_HEADERS = ["timestamp","asset","direction","result","confidence",
                "rsi","price","timeframe","market_condition","reason"]

def _csv_path(chat_id: int) -> str:
    return os.path.join(_BASE_DIR, f"trades_{chat_id}.csv")

def _log_trade(chat_id: int, meta: dict, result: str) -> None:
    path = _csv_path(chat_id)
    write_header = not os.path.exists(path)
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_CSV_HEADERS)
            if write_header:
                w.writeheader()
            w.writerow({
                "timestamp":        datetime.utcnow().isoformat(timespec="seconds"),
                "asset":            meta.get("asset_label",""),
                "direction":        meta.get("direction",""),
                "result":           result,
                "confidence":       meta.get("confidence",""),
                "rsi":              meta.get("rsi",""),
                "price":            meta.get("price",""),
                "timeframe":        meta.get("tf_label","1 minute"),
                "market_condition": meta.get("market",""),
                "reason":           meta.get("reason",""),
            })
    except Exception as exc:
        log_error(f"CSV write error: {exc}")

def _read_trades(chat_id: int) -> list[dict]:
    path = _csv_path(chat_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

# ---------------------------------------------------------------------------
# Technical analysis (pure Python)
# ---------------------------------------------------------------------------
def _closes(candles):
    return [float(c["close"] if isinstance(c, dict) else c.close) for c in candles]

def _ema(values, period):
    if len(values) < period: return []
    k = 2 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def _rsi(values, period=14):
    if len(values) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))

def _macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal - 1: return None, None, None
    fe = _ema(closes, fast); se = _ema(closes, slow)
    if not fe or not se: return None, None, None
    off = len(fe) - len(se)
    ml  = [f - s for f, s in zip(fe[off:], se)]
    sig = _ema(ml, signal)
    if not sig: return None, None, None
    return ml[-1], sig[-1], ml[-1] - sig[-1]

def _bollinger(closes, period=20, std_dev=2.0):
    if len(closes) < period: return None, None, None
    w = closes[-period:]
    m = sum(w) / period
    sd = (sum((x-m)**2 for x in w) / period) ** 0.5
    return m, m + std_dev*sd, m - std_dev*sd

def _market_condition(candles):
    if len(candles) < 10: return "Unknown"
    closes = _closes(candles[-20:])
    sample = candles[-1]
    if isinstance(sample, dict):
        highs = [float(c["high"]) for c in candles[-20:]]
        lows  = [float(c["low"])  for c in candles[-20:]]
    else:
        highs = closes; lows = closes
    atr = sum(h-l for h,l in zip(highs,lows)) / len(highs)
    price = closes[-1]
    atr_pct = (atr/price)*100 if price else 0
    ef = _ema(closes, 5); es = _ema(closes, 20)
    if ef and es:
        ts = abs(ef[-1]-es[-1])/price*100 if price else 0
        if ts > 0.05: return "Trending"
    if atr_pct > 0.1: return "Volatile"
    return "Ranging"

def compute_signal(candles, timeframe_seconds=60):
    result = {"direction":"WAIT","confidence":0,"rsi":None,"reason":"Not enough data","market":"Unknown","strategy":"MACD+RSI"}
    if not candles or len(candles) < 22: return result
    closes = _closes(candles)
    rsi    = _rsi(closes, 14)
    market = _market_condition(candles)
    result["rsi"] = rsi; result["market"] = market
    if rsi is None: return result

    macd_val, sig_val, hist = _macd(closes)
    if macd_val is not None:
        result["strategy"] = "MACD+RSI"
        if rsi < 35 and macd_val > sig_val:
            rsi_pts  = max(0, (35-rsi)/35)*20
            hist_pts = min(10, abs(hist)*1e5)
            conf = min(97, max(60, int(70+rsi_pts+hist_pts)))
            if timeframe_seconds <= 5: conf = max(55, conf-8)
            result.update({"direction":"HIGHER","confidence":conf,
                "reason":f"RSI={rsi:.1f} oversold, MACD bullish (hist={hist:+.6f})"})
            return result
        elif rsi > 65 and macd_val < sig_val:
            rsi_pts  = max(0, (rsi-65)/35)*20
            hist_pts = min(10, abs(hist)*1e5)
            conf = min(97, max(60, int(70+rsi_pts+hist_pts)))
            if timeframe_seconds <= 5: conf = max(55, conf-8)
            result.update({"direction":"LOWER","confidence":conf,
                "reason":f"RSI={rsi:.1f} overbought, MACD bearish (hist={hist:+.6f})"})
            return result
        # Ranging fallback
        if market == "Ranging" and len(closes) >= 22:
            mid, upper, lower = _bollinger(closes)
            if mid is not None:
                last = closes[-1]; prev = closes[-2]
                bw   = upper - lower or 1e-10
                dl   = (last - lower) / last * 100 if last else 0
                du   = (upper - last) / last * 100 if last else 0
                if dl <= 0.05 and rsi < 45:
                    conf = min(85, max(60, int(70 + (0.05-dl)*100)))
                    result.update({"direction":"HIGHER","confidence":conf,"strategy":"BB-Ranging",
                        "reason":f"Ranging: near lower BB ({lower:.5f}), RSI={rsi:.1f}"})
                    return result
                if du <= 0.05 and rsi > 55:
                    conf = min(85, max(60, int(70 + (0.05-du)*100)))
                    result.update({"direction":"LOWER","confidence":conf,"strategy":"BB-Ranging",
                        "reason":f"Ranging: near upper BB ({upper:.5f}), RSI={rsi:.1f}"})
                    return result
        result["reason"] = f"RSI={rsi:.1f} neutral — no signal"
        return result

    # Fallback RSI+EMA
    result["strategy"] = "RSI+EMA"
    e9 = _ema(closes, 9); e21 = _ema(closes, 21)
    if not e9 or not e21: return result
    price = closes[-1]; b = 0; be = 0
    if rsi < 35: b += 2
    elif rsi < 45: b += 1
    elif rsi > 65: be += 2
    elif rsi > 55: be += 1
    if e9[-1] > e21[-1]: b += 2
    else: be += 2
    if price > e9[-1]: b += 1
    else: be += 1
    total = b + be
    if total == 0: return result
    if b > be:
        conf = min(85, max(51, int(50 + b/total*25)))
        result.update({"direction":"HIGHER","confidence":conf,
            "reason":f"[Fallback] RSI={rsi:.1f}, EMA bullish"})
    elif be > b:
        conf = min(85, max(51, int(50 + be/total*25)))
        result.update({"direction":"LOWER","confidence":conf,
            "reason":f"[Fallback] RSI={rsi:.1f}, EMA bearish"})
    return result


# ---------------------------------------------------------------------------
# ADX indicator (pure Python, Wilder smoothing)
# ---------------------------------------------------------------------------
def _adx(candles, period=14):
    """Return ADX value (0-100) or None if insufficient data."""
    n = len(candles)
    if n < period + 1: return None
    sample = candles[-1]
    if isinstance(sample, dict):
        highs  = [float(c["high"])  for c in candles]
        lows   = [float(c["low"])   for c in candles]
        closes = [float(c["close"]) for c in candles]
    else:
        closes = _closes(candles)
        highs = closes; lows = closes

    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr  = max(h-l, abs(h-pc), abs(l-pc))
        pdm = max(highs[i]-highs[i-1], 0) if (highs[i]-highs[i-1]) > (lows[i-1]-lows[i]) else 0
        ndm = max(lows[i-1]-lows[i], 0)   if (lows[i-1]-lows[i]) > (highs[i]-highs[i-1]) else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)

    if len(tr_list) < period: return None

    def _wilder(vals, p):
        s = [sum(vals[:p])]
        for v in vals[p:]: s.append(s[-1] - s[-1]/p + v)
        return s

    atr = _wilder(tr_list, period)
    pdi = _wilder(pdm_list, period)
    ndi = _wilder(ndm_list, period)

    dx_list = []
    for a, p, nd in zip(atr, pdi, ndi):
        if a == 0: continue
        pv = 100*p/a; nv = 100*nd/a
        denom = pv + nv
        dx_list.append(100*abs(pv-nv)/denom if denom else 0)

    if len(dx_list) < period: return None
    adx_series = _wilder(dx_list, period)
    return round(adx_series[-1], 2)


# ---------------------------------------------------------------------------
# Advanced Analysis Mode
# ---------------------------------------------------------------------------
# Default env-var driven settings
_DEFAULT_SHORT_TF      = int(os.environ.get("SHORT_TF",       "60"))
_DEFAULT_LONG_TF       = int(os.environ.get("LONG_TF",        "300"))
_DEFAULT_ANALYSIS_SECS = int(os.environ.get("ANALYSIS_SECONDS","60"))
_DEFAULT_MIN_CONF      = int(os.environ.get("MIN_CONFIDENCE",  "70"))
_STABILITY_DELAY       = int(os.environ.get("STABILITY_DELAY", "15"))
_SAMPLE_INTERVAL       = 10   # seconds between samples during analysis window

# Per-user advanced settings: {chat_id: {"advanced": bool, "analysis_secs": int,
#                                         "short_tf": int, "long_tf": int}}
advanced_settings: dict[int, dict] = {}
# Active analysis tasks: {chat_id: asyncio.Task}
_analysis_tasks: dict[int, asyncio.Task] = {}


def _get_adv(chat_id: int) -> dict:
    if chat_id not in advanced_settings:
        advanced_settings[chat_id] = {
            "advanced":      False,
            "analysis_secs": _DEFAULT_ANALYSIS_SECS,
            "short_tf":      _DEFAULT_SHORT_TF,
            "long_tf":       _DEFAULT_LONG_TF,
            "min_conf":      _DEFAULT_MIN_CONF,
        }
    return advanced_settings[chat_id]


async def _run_advanced_analysis(
    chat_id: int,
    asset_code: str,
    asset_label: str,
    tf_label: str,
    notify_msg_id: int | None = None,
) -> None:
    """
    Full advanced analysis loop:
    1. Observe market for analysis_secs, sampling every _SAMPLE_INTERVAL seconds.
    2. Require multi-TF agreement on every sample.
    3. Apply ADX regime filter.
    4. Wait stability_delay after first signal, re-check.
    5. Send signal only if confidence >= min_conf.
    """
    adv = _get_adv(chat_id)
    analysis_secs = adv["analysis_secs"]
    short_tf      = adv["short_tf"]
    long_tf       = adv["long_tf"]
    min_conf      = adv["min_conf"]

    samples_total   = max(1, analysis_secs // _SAMPLE_INTERVAL)
    bullish_samples = 0
    bearish_samples = 0
    price_high      = None
    price_low       = None
    last_conf       = 0
    last_direction  = "WAIT"

    logger.info(f"[Advanced] Starting {analysis_secs}s analysis for {asset_code} (chat={chat_id})")

    for sample_i in range(samples_total):
        await asyncio.sleep(_SAMPLE_INTERVAL)

        if not (session_manager and session_manager.is_connected):
            break

        # Fetch both timeframes
        short_candles = await session_manager.get_candles(asset_code, short_tf, CANDLE_COUNT)
        long_candles  = await session_manager.get_candles(asset_code, long_tf,  CANDLE_COUNT)

        if not short_candles or len(short_candles) < 22:
            continue

        # Track price range for volatility
        try:
            c = short_candles[-1]
            price = float(c["close"] if isinstance(c, dict) else c.close)
            if price_high is None: price_high = price_low = price
            price_high = max(price_high, price)
            price_low  = min(price_low,  price)
        except Exception:
            price = None

        # Compute signals on both TFs
        short_result = compute_signal(short_candles, short_tf)
        long_result  = compute_signal(long_candles,  long_tf) if long_candles and len(long_candles) >= 22 else {"direction": "WAIT"}

        short_dir = short_result["direction"]
        long_dir  = long_result["direction"]

        # ADX regime filter on short TF
        adx_val = _adx(short_candles)
        if adx_val is not None:
            if 20 <= adx_val <= 25:
                # Weak regime — skip this sample
                logger.debug(f"[Advanced] ADX={adx_val:.1f} weak zone, skipping sample {sample_i+1}")
                continue
            if adx_val > 25 and short_result.get("strategy","") == "BB-Ranging":
                continue   # trend regime but got reversal signal — skip
            if adx_val < 20 and short_result.get("strategy","") == "MACD+RSI":
                continue   # ranging regime but got trend signal — skip

        # Multi-TF agreement check
        if short_dir == "HIGHER" and long_dir in ("HIGHER", "WAIT"):
            bullish_samples += 1
            last_direction = "HIGHER"
            last_conf = short_result.get("confidence", 70)
        elif short_dir == "LOWER" and long_dir in ("LOWER", "WAIT"):
            bearish_samples += 1
            last_direction = "LOWER"
            last_conf = short_result.get("confidence", 70)

        logger.debug(f"[Advanced] Sample {sample_i+1}/{samples_total}: "
                     f"short={short_dir} long={long_dir} "
                     f"bull={bullish_samples} bear={bearish_samples}")

    # ── Decision after observation window ────────────────────────────────
    consistency_threshold = 0.7   # 70% of samples must agree
    bull_ratio = bullish_samples / samples_total
    bear_ratio = bearish_samples / samples_total

    if bull_ratio >= consistency_threshold:
        candidate_dir = "HIGHER"
        consistency   = bull_ratio
    elif bear_ratio >= consistency_threshold:
        candidate_dir = "LOWER"
        consistency   = bear_ratio
    else:
        logger.info(f"[Advanced] No consistent signal for {asset_code} "
                    f"(bull={bull_ratio:.0%} bear={bear_ratio:.0%})")
        if notify_msg_id:
            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id, message_id=notify_msg_id,
                    text=f"\u274c No signal found for {asset_label} after {analysis_secs}s analysis.\n"
                         f"(bull={bull_ratio:.0%} bear={bear_ratio:.0%} — need \u226570%)"
                )
            except Exception: pass
        _analysis_tasks.pop(chat_id, None)
        return

    # ── Stability re-check after STABILITY_DELAY ─────────────────────────
    logger.info(f"[Advanced] Candidate {candidate_dir} ({consistency:.0%}). "
                f"Waiting {_STABILITY_DELAY}s for stability check...")
    if notify_msg_id:
        try:
            await telegram_app.bot.edit_message_text(
                chat_id=chat_id, message_id=notify_msg_id,
                text=f"\u23f3 Candidate signal: {candidate_dir}\n"
                     f"Stability check in {_STABILITY_DELAY}s..."
            )
        except Exception: pass

    await asyncio.sleep(_STABILITY_DELAY)

    if not (session_manager and session_manager.is_connected):
        _analysis_tasks.pop(chat_id, None)
        return

    recheck_candles = await session_manager.get_candles(asset_code, short_tf, CANDLE_COUNT)
    recheck_result  = compute_signal(recheck_candles, short_tf) if recheck_candles else {"direction": "WAIT"}
    stability_bonus = 10 if recheck_result["direction"] == candidate_dir else 0

    if recheck_result["direction"] != candidate_dir:
        logger.info(f"[Advanced] Stability check FAILED — signal reversed to {recheck_result['direction']}")
        if notify_msg_id:
            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id, message_id=notify_msg_id,
                    text=f"\u274c Signal rejected: condition changed during stability check.\n"
                         f"({candidate_dir} \u2192 {recheck_result['direction']})"
                )
            except Exception: pass
        _analysis_tasks.pop(chat_id, None)
        return

    # ── Composite confidence score ────────────────────────────────────────
    base_conf    = recheck_result.get("confidence", 70)
    tf_bonus     = 15 if consistency >= 0.85 else 8
    final_conf   = min(97, int(base_conf + tf_bonus + stability_bonus))

    # Volatility penalty
    vol_warning = ""
    if price_high and price_low and price_low > 0:
        price_range_pct = (price_high - price_low) / price_low * 100
        if price_range_pct > 0.2:
            final_conf  = max(55, final_conf - 10)
            vol_warning = f"\n\u26a0\ufe0f High volatility during analysis ({price_range_pct:.3f}%)"

    if final_conf < min_conf:
        logger.info(f"[Advanced] Confidence {final_conf}% < min {min_conf}% — signal suppressed")
        if notify_msg_id:
            try:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id, message_id=notify_msg_id,
                    text=f"\u274c Signal suppressed: confidence {final_conf}% < minimum {min_conf}%"
                )
            except Exception: pass
        _analysis_tasks.pop(chat_id, None)
        return

    # ── Send confirmed signal ─────────────────────────────────────────────
    recheck_result["confidence"] = final_conf
    price_str = ""
    if recheck_candles:
        try:
            c = recheck_candles[-1]
            p = float(c["close"] if isinstance(c, dict) else c.close)
            price_str = f"{p:.5f}"
        except Exception: pass

    confirmation_note = (
        f"\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"\u2705 Signal confirmed after full analysis\n"
        f"Checked 2 timeframes ({short_tf}s + {long_tf}s)\n"
        f"Observed {analysis_secs}s ({consistency:.0%} consistent)\n"
        f"Stability check: passed (+{stability_bonus}%)\n"
        f"Final confidence: {final_conf}%{vol_warning}"
    )

    # Edit the waiting message first
    if notify_msg_id:
        try:
            await telegram_app.bot.edit_message_text(
                chat_id=chat_id, message_id=notify_msg_id,
                text=f"\u2705 Analysis complete — sending signal..."
            )
        except Exception: pass

    # Send the actual signal
    signal_id = _make_signal_id(chat_id)
    pending_signals[signal_id] = {
        "user_id": chat_id, "ts": time.time(), "voted": False,
        "asset_label": asset_label, "tf_label": tf_label,
        "direction": "CALL" if candidate_dir == "HIGHER" else "PUT",
        "confidence": final_conf, "rsi": recheck_result.get("rsi",""),
        "price": price_str, "market": recheck_result.get("market",""),
        "reason": recheck_result.get("reason","") + confirmation_note,
    }

    img = SIGNAL_IMG_BUY if candidate_dir == "HIGHER" else SIGNAL_IMG_SELL
    caption = _signal_caption(recheck_result, asset_label, tf_label, price_str) + confirmation_note
    keyboard = _vote_keyboard(signal_id)

    if img:
        try:
            await telegram_app.bot.send_photo(chat_id=chat_id, photo=img,
                caption=caption, reply_markup=keyboard)
            _analysis_tasks.pop(chat_id, None)
            return
        except Exception: pass

    text = _format_signal(recheck_result, asset_label, tf_label)
    text += f"\nPrice: {price_str}" if price_str else ""
    text += confirmation_note
    await telegram_app.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    _analysis_tasks.pop(chat_id, None)


async def trigger_advanced_analysis(
    chat_id: int,
    asset_code: str,
    asset_label: str,
    tf_label: str = "1 minute",
) -> None:
    """
    Start an advanced analysis task for a user.
    Cancels any existing analysis for that user first.
    """
    # Cancel existing
    existing = _analysis_tasks.get(chat_id)
    if existing and not existing.done():
        existing.cancel()

    adv = _get_adv(chat_id)
    analysis_secs = adv["analysis_secs"]

    # Send the "analysing..." message
    sent = await telegram_app.bot.send_message(
        chat_id=chat_id,
        text=(
            f"\u23f3 Advanced Analysis Mode\n"
            f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
            f"Asset: {asset_label}\n"
            f"Observing market for {analysis_secs}s...\n"
            f"Timeframes: {adv['short_tf']}s + {adv['long_tf']}s\n"
            f"Min confidence: {adv['min_conf']}%\n\n"
            "Please wait. No signal will be sent until analysis is complete."
        )
    )

    task = asyncio.create_task(
        _run_advanced_analysis(chat_id, asset_code, asset_label, tf_label, sent.message_id),
        name=f"adv_{chat_id}",
    )
    _analysis_tasks[chat_id] = task


# ---------------------------------------------------------------------------
# Cloudflare Worker SSID relay
# ---------------------------------------------------------------------------
async def fetch_ssid_from_worker():
    if not WORKER_URL or not WORKER_API_KEY: return None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{WORKER_URL}/ssid", headers={"X-API-Key": WORKER_API_KEY})
            if r.status_code == 200:
                data = r.json()
                ssid = data.get("ssid","")
                if ssid:
                    logger.info(f"SSID fetched from Worker (uid={data.get('uid')})")
                    return ssid
    except Exception as exc:
        logger.warning(f"fetch_ssid_from_worker: {exc}")
    return None

# ---------------------------------------------------------------------------
# SessionManager
# ---------------------------------------------------------------------------
_AUTH_ERROR_MARKERS = ("authentication timeout","websocket connection closed",
    "unauthorized","token expired","connect() returned false","failed to connect")
PREEMPTIVE_REFRESH_HOURS = 23
PING_INTERVAL_SECONDS    = 20

class SessionManager:
    def __init__(self, ssid, is_demo=True, chat_id=0, name="Bot"):
        self.ssid = ssid; self.is_demo = is_demo
        self.chat_id = chat_id; self.name = name
        self.client = None; self.connected_at = None
        self._ready = False; self._reconnecting = False
        self._refresh_task = None; self._lock = asyncio.Lock()

    @property
    def is_connected(self): return self._ready and self.client is not None

    async def start(self):
        async with self._lock: return await self._connect()

    async def stop(self):
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        if self.client:
            try: await self.client.disconnect()
            except Exception: pass
        self.client = None; self._ready = False

    async def get_candles(self, asset, timeframe, count):
        if not self.is_connected:
            await self._handle_error("get_candles: not connected"); return []
        try:
            return await self.client.get_candles(asset, timeframe, count * timeframe)
        except Exception as exc:
            await self._handle_error(f"get_candles error: {exc}"); return []

    async def notify_error(self, msg):
        if any(m in msg.lower() for m in _AUTH_ERROR_MARKERS):
            await self._handle_error(msg)

    async def _connect(self):
        if self.client:
            try: await self.client.disconnect()
            except Exception: pass
            self.client = None; self._ready = False
        try:
            self.client = PocketOptionAsync(ssid=self.ssid)
            await self.client.wait_for_assets(timeout=60.0)
        except Exception as exc:
            log_error(f"[{self.name}] Connect error: {exc}"); self.client = None; return False
        self._ready = True; self.connected_at = datetime.now()
        logger.info(f"[{self.name}] Connected at {self.connected_at.strftime('%H:%M:%S')}")
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="po_refresh")
        return True

    async def _refresh_loop(self):
        await asyncio.sleep(PREEMPTIVE_REFRESH_HOURS * 3600)
        logger.info(f"[{self.name}] Pre-emptive refresh triggered")
        async with self._lock: await self._connect()

    async def _handle_error(self, msg):
        if self._reconnecting: return
        self._reconnecting = True
        log_error(f"[{self.name}] Error: {msg}")
        for delay in [5, 30, 120]:
            await asyncio.sleep(delay)
            async with self._lock:
                if await self._connect():
                    self._reconnecting = False
                    try:
                        await telegram_app.bot.send_message(chat_id=self.chat_id,
                            text="Pocket Option reconnected.")
                    except Exception: pass
                    return
        self._reconnecting = False
        try:
            await telegram_app.bot.send_message(chat_id=self.chat_id,
                text="SSID expired. Send /setssid <new_ssid> to reconnect.")
        except Exception: pass

# Global session manager
session_manager: SessionManager | None = None

# ---------------------------------------------------------------------------
# Telegram app
# ---------------------------------------------------------------------------
telegram_app = Application.builder().token(TOKEN).build()

async def _get_current_price(asset_code):
    if not session_manager or not session_manager.is_connected: return None
    try:
        candles = await session_manager.get_candles(asset_code, 60, 2)
        if candles:
            c = candles[-1]
            return float(c["close"] if isinstance(c, dict) else c.close)
    except Exception: pass
    return None


# ---------------------------------------------------------------------------
# Signal formatting & sending
# ---------------------------------------------------------------------------
def _make_signal_id(chat_id):
    return f"{int(time.time())}_{chat_id}"

def _vote_keyboard(signal_id):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("\U0001f44d Win",  callback_data=f"vote:win:{signal_id}"),
        InlineKeyboardButton("\U0001f44e Loss", callback_data=f"vote:loss:{signal_id}"),
    ]])

def _format_signal(result, asset_label, tf_label):
    d = result["direction"]
    if d == "HIGHER": header = "\U0001f4c8 BUY (CALL)"
    elif d == "LOWER": header = "\U0001f4c9 SELL (PUT)"
    else: header = "\u23f8 WAIT \u2014 no clear signal"
    rsi = result.get("rsi"); rsi_str = f"{rsi:.1f}" if rsi else "N/A"
    conf = result.get("confidence",0)
    bar  = chr(9608)*round(conf/20) + chr(9617)*(5-round(conf/20))
    return (f"{header}\n\nAsset: {asset_label}\nTimeframe: {tf_label}\n"
            f"Reliability: {conf}%  {bar}\nRSI: {rsi_str}\n"
            f"Market: {result.get('market','')}\nReason: {result.get('reason','')}\n"
            f"Time: {datetime.now().strftime('%H:%M:%S')}")

def _signal_caption(result, asset_label, tf_label, price_str=""):
    d = result["direction"]
    action = "BUY" if d == "HIGHER" else "SELL"
    rsi = result.get("rsi"); rsi_str = f"{rsi:.1f}" if rsi else "N/A"
    conf = result.get("confidence",0)
    bar  = chr(9608)*round(conf/20) + chr(9617)*(5-round(conf/20))
    lines = [f"{action} Signal", f"Asset: {asset_label}", f"Timeframe: {tf_label}"]
    if price_str: lines.append(f"Price: {price_str}")
    lines += [f"Reliability: {conf}%  {bar}", f"RSI: {rsi_str}",
              f"Market: {result.get('market','')}", f"Reason: {result.get('reason','')}",
              f"Time: {datetime.now().strftime('%H:%M:%S')}"]
    return "\n".join(lines)

async def _send_signal_message(chat_id, result, asset_label, tf_label, price_str=""):
    d = result["direction"]
    if d == "WAIT": return
    signal_id = _make_signal_id(chat_id)
    pending_signals[signal_id] = {
        "user_id": chat_id, "ts": time.time(), "voted": False,
        "asset_label": asset_label, "tf_label": tf_label,
        "direction": "CALL" if d == "HIGHER" else "PUT",
        "confidence": result.get("confidence",""), "rsi": result.get("rsi",""),
        "price": price_str, "market": result.get("market",""), "reason": result.get("reason",""),
    }
    img = SIGNAL_IMG_BUY if d == "HIGHER" else SIGNAL_IMG_SELL
    caption  = _signal_caption(result, asset_label, tf_label, price_str)
    keyboard = _vote_keyboard(signal_id)
    if img:
        try:
            await telegram_app.bot.send_photo(chat_id=chat_id, photo=img,
                caption=caption, reply_markup=keyboard)
            return
        except Exception as exc:
            logger.warning(f"send_photo failed: {exc}")
    text = _format_signal(result, asset_label, tf_label)
    if price_str: text += f"\nPrice: {price_str}"
    try:
        await telegram_app.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    except Exception as exc:
        log_error(f"send_message failed: {exc}")

# ---------------------------------------------------------------------------
# Auto-signal polling loop
# ---------------------------------------------------------------------------
async def _poll_loop():
    global session_manager
    logger.info("Poll loop started.")
    for _ in range(30):
        if session_manager and session_manager.is_connected: break
        await asyncio.sleep(2)
    else:
        log_error("Poll loop: timed out waiting for connection")

    while True:
        try:
            if not (session_manager and session_manager.is_connected):
                await asyncio.sleep(10); continue

            candles = await session_manager.get_candles("EURUSD_otc", CANDLE_PERIOD, CANDLE_COUNT)
            if candles:
                result = compute_signal(candles, CANDLE_PERIOD)
                price  = None
                try:
                    c = candles[-1]
                    price = float(c["close"] if isinstance(c, dict) else c.close)
                except Exception: pass

                if result["direction"] != "WAIT":
                    price_str = f"{price:.5f}" if price else "N/A"
                    for cid in list(known_users):
                        ps = _get_settings(cid)
                        if ps.get("auto", False):
                            adv = _get_adv(cid)
                            if adv.get("advanced"):
                                # Advanced mode: only trigger if no analysis running
                                if cid not in _analysis_tasks or _analysis_tasks[cid].done():
                                    asyncio.create_task(
                                        trigger_advanced_analysis(cid, "EURUSD_otc", "EUR/USD (OTC)", "1 minute"),
                                        name=f"adv_auto_{cid}"
                                    )
                            else:
                                await _send_signal_message(cid, result, "EUR/USD (OTC)",
                                                            "1 minute", price_str)

        except Exception as exc:
            log_error(f"Poll loop error: {exc}")
            if session_manager: await session_manager.notify_error(str(exc))

        await asyncio.sleep(CANDLE_PERIOD)

# ---------------------------------------------------------------------------
# Scanner loop
# ---------------------------------------------------------------------------
async def _scan_asset(asset_code):
    global session_manager
    asset_label = next((k for k,v in ASSETS.items() if v == asset_code), asset_code)
    logger.info(f"Scanner started for {asset_code}")
    try:
        from datetime import timedelta
        sub = await session_manager.client.subscribe_symbol_timed(
            asset_code, timedelta(seconds=CANDLE_PERIOD))
        async for _tick in sub:
            any_scanner = any(_get_settings(cid).get("scanner", False) for cid in known_users)
            if not any_scanner: return
            candles = await session_manager.get_candles(asset_code, CANDLE_PERIOD, CANDLE_COUNT)
            if not candles or len(candles) < 22: continue
            result = compute_signal(candles, CANDLE_PERIOD)
            if result["direction"] == "WAIT": continue
            now = time.time()
            for cid in list(known_users):
                if not _get_settings(cid).get("scanner", False): continue
                if not _get_settings(cid).get("auto", False): continue
                key = f"{cid}:{asset_code}"
                if now - scanner_cooldowns.get(key, 0) < DEFAULT_COOLDOWN: continue
                scanner_cooldowns[key] = now
                price = None
                try:
                    c = candles[-1]
                    price = float(c["close"] if isinstance(c, dict) else c.close)
                except Exception: pass
                await _send_signal_message(cid, result, asset_label, "1 minute",
                                            f"{price:.5f}" if price else "N/A")
    except asyncio.CancelledError: pass
    except Exception as exc: log_error(f"Scanner error ({asset_code}): {exc}")


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
def _win_rate(rows):
    if not rows: return 0.0
    return sum(1 for r in rows if r.get("result")=="win") / len(rows) * 100

def _hour_window(row):
    try:
        h = int(row["timestamp"][11:13])
        s = (h//4)*4
        return f"{s:02d}-{s+4:02d} UTC"
    except Exception: return "Unknown"

def _day_of_week(row):
    try:
        from datetime import datetime as _dt
        return _dt.fromisoformat(row["timestamp"]).strftime("%A")
    except Exception: return "Unknown"

def _rsi_bin(row):
    try:
        r = float(row["rsi"])
        if r < 30: return "<30"
        if r < 40: return "30-40"
        if r < 60: return "40-60"
        if r < 70: return "60-70"
        return ">70"
    except Exception: return "Unknown"

def _group_wr(rows, key_fn):
    groups = {}
    for r in rows:
        k = key_fn(r)
        groups.setdefault(k, []).append(r)
    return {k: (sum(1 for r in g if r.get("result")=="win"), len(g))
            for k, g in sorted(groups.items())}

def _build_analysis(chat_id):
    rows = _read_trades(chat_id)
    if not rows: return "No trades recorded yet. Mark signals with Win/Loss buttons."
    total = len(rows); wins = sum(1 for r in rows if r.get("result")=="win")
    losses = total - wins; wr = wins/total*100; pnl = wins - losses
    lines = ["Trade Analysis\n",
             f"Total: {total}  Wins: {wins}  Losses: {losses}",
             f"Win rate: {wr:.1f}%  P&L: {'+' if pnl>=0 else ''}{pnl}$\n"]
    def section(title, groups):
        out = [title]
        for lbl, (w, t) in groups.items():
            bar = chr(9608)*round(w/t*5) + chr(9617)*(5-round(w/t*5)) if t else "-----"
            out.append(f"  {lbl}: {w}/{t} ({w/t*100:.0f}%) {bar}")
        return out + [""]
    lines += section("By Time (UTC):", _group_wr(rows, _hour_window))
    lines += section("By Day:", _group_wr(rows, _day_of_week))
    lines += section("By RSI:", _group_wr(rows, _rsi_bin))
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Trading Smart Bot UI helpers
# ---------------------------------------------------------------------------

TIMEFRAMES_UI = {
    "5 seconds":  5,
    "1 minute":   60,
    "2 minutes":  120,
    "3 minutes":  180,
    "5 minutes":  300,
}

# Assets shown in the UI picker (paginated, 8 per page)
ASSETS_UI_PAGES = [
    [
        ("USD/CAD (OTC)", "USDCAD_otc"),
        ("GBP/JPY (OTC)", "GBPJPY_otc"),
        ("CAD/JPY (OTC)", "CADJPY_otc"),
        ("AUD/USD (OTC)", "AUDUSD_otc"),
        ("EUR/USD (OTC)", "EURUSD_otc"),
        ("USD/JPY (OTC)", "USDJPY_otc"),
        ("GBP/AUD (OTC)", "GBPAUD_otc"),
        ("EUR/CAD (OTC)", "EURCAD_otc"),
    ],
    [
        ("GBP/USD (OTC)", "GBPUSD_otc"),
        ("EUR/JPY (OTC)", "EURJPY_otc"),
        ("USD/TRY (OTC)", "USDTRY_otc"),
        ("USD/MXN (OTC)", "USDMXN_otc"),
        ("USD/INR (OTC)", "USDINR_otc"),
        ("EUR/TRY (OTC)", "EURTRY_otc"),
        ("USD/ZAR (OTC)", "USDZARUSD_otc"),
        ("GBP/ZAR (OTC)", "GBPZAR_otc"),
    ],
]


def _ascii_chart(candles) -> str:
    """Generate a simple ASCII price chart from recent candles."""
    if not candles or len(candles) < 3:
        return "▬▬▬▬▬"
    closes = _closes(candles[-8:])
    mn, mx = min(closes), max(closes)
    rng = mx - mn or 1e-10
    bars = ""
    for p in closes:
        lvl = int((p - mn) / rng * 4)
        bars += ["▁", "▃", "▅", "▇", "█"][min(lvl, 4)]
    # Add timestamps for last 3 points
    now = datetime.utcnow()
    t1  = now.strftime("%H:%M")
    return f"{bars}  {t1}"


def _atr_info(candles) -> tuple[str, str]:
    """Return (market_setting, volatility) based on ATR."""
    if not candles or len(candles) < 10:
        return "Stable", "Medium"
    sample = candles[-1]
    if isinstance(sample, dict):
        highs = [float(c["high"]) for c in candles[-10:]]
        lows  = [float(c["low"])  for c in candles[-10:]]
    else:
        closes = _closes(candles[-10:])
        highs = closes; lows = closes
    atr = sum(h - l for h, l in zip(highs, lows)) / len(highs)
    price = float(candles[-1]["close"] if isinstance(candles[-1], dict) else candles[-1].close)
    atr_pct = (atr / price * 100) if price else 0
    if atr_pct < 0.03:
        return "Stable", "Low"
    elif atr_pct < 0.08:
        return "Stable", "Medium"
    elif atr_pct < 0.15:
        return "Active", "High"
    else:
        return "Volatile", "High"


def _best_asset_recommendation(chat_id: int) -> str:
    """Return the asset with the highest win rate from the user's CSV."""
    rows = _read_trades(chat_id)
    if len(rows) < 5:
        return "EUR/USD (OTC)"
    groups: dict[str, list] = {}
    for r in rows:
        a = r.get("asset", "")
        if a:
            groups.setdefault(a, []).append(r)
    best, best_wr = "EUR/USD (OTC)", 0.0
    for asset, g in groups.items():
        if len(g) >= 3:
            wr = sum(1 for r in g if r.get("result") == "win") / len(g)
            if wr > best_wr:
                best_wr, best = wr, asset
    return best


def _trade_stats(chat_id: int) -> tuple[int, int, int, int]:
    """Return (total, wins, losses, streak) from CSV."""
    rows = _read_trades(chat_id)
    total  = len(rows)
    wins   = sum(1 for r in rows if r.get("result") == "win")
    losses = total - wins
    streak = 0
    for r in reversed(rows):
        if r.get("result") == "win":
            streak += 1
        else:
            break
    return total, wins, losses, streak


# ---------------------------------------------------------------------------
# New UI screens
# ---------------------------------------------------------------------------

async def _show_main_menu(chat_id: int, update_or_query, context) -> None:
    """Show the Trading Smart Bot main screen."""
    known_users.add(chat_id)
    ps = _get_settings(chat_id)

    # Account balance
    balance_str = "—"
    if session_manager and session_manager.is_connected and session_manager.client:
        try:
            bal = await session_manager.client.balance()
            balance_str = f"${bal:,.2f}"
        except Exception:
            pass

    total, wins, losses, streak = _trade_stats(chat_id)
    po_ok = session_manager is not None and session_manager.is_connected
    conn  = "\U0001f7e2 Live" if po_ok else "\U0001f534 Offline"
    auto_icon = "\U0001f514" if ps.get("auto") else "\U0001f515"

    text = (
        f"\U0001f916 Trading Smart Bot\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"\U0001f4b0 Balance: {balance_str}  {conn}\n"
        f"\U0001f4ca Trades: {total}  \u2705 {wins}  \u274c {losses}  \U0001f525 Streak: {streak}\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"Real-time signals using RSI + MACD + Bollinger Bands\n"
        f"Auto-signals: {auto_icon}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Get signal", callback_data="nav:assets:0")],
        [InlineKeyboardButton("\u2139\ufe0f How the bot works", callback_data="nav:howto"),
         InlineKeyboardButton("\U0001f4ac Ask a question",     callback_data="nav:ask")],
        [InlineKeyboardButton(f"{auto_icon} Auto-signals",     callback_data="nav:toggle_auto"),
         InlineKeyboardButton("\U0001f4e1 Scanner",            callback_data="nav:toggle_scanner")],
        [InlineKeyboardButton("\U0001f4cb Analyze",            callback_data="nav:analyze"),
         InlineKeyboardButton("\U0001f511 Refresh SSID",       callback_data="nav:refresh_ssid")],
    ])

    if hasattr(update_or_query, "message") and update_or_query.message:
        await update_or_query.message.reply_text(text, reply_markup=keyboard)
    elif hasattr(update_or_query, "edit_message_text"):
        try:
            await update_or_query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            await telegram_app.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    else:
        await telegram_app.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)


def _assets_keyboard(page: int) -> InlineKeyboardMarkup:
    """Build the asset selection keyboard for a given page."""
    page = max(0, min(page, len(ASSETS_UI_PAGES) - 1))
    items = ASSETS_UI_PAGES[page]
    rows  = []
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(items[i][0], callback_data=f"nav:tf:{items[i][1]}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(items[i+1][0], callback_data=f"nav:tf:{items[i+1][1]}"))
        rows.append(row)
    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("\u2190 Back", callback_data=f"nav:assets:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1} of {len(ASSETS_UI_PAGES)}", callback_data="nav:noop"))
    if page < len(ASSETS_UI_PAGES) - 1:
        nav.append(InlineKeyboardButton("Next \u2192", callback_data=f"nav:assets:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")])
    return InlineKeyboardMarkup(rows)


def _timeframe_keyboard(asset_code: str) -> InlineKeyboardMarkup:
    rows = []
    tfs  = list(TIMEFRAMES_UI.keys())
    for i in range(0, len(tfs), 2):
        row = [InlineKeyboardButton(tfs[i], callback_data=f"nav:signal:{asset_code}:{TIMEFRAMES_UI[tfs[i]]}")]
        if i + 1 < len(tfs):
            row.append(InlineKeyboardButton(tfs[i+1], callback_data=f"nav:signal:{asset_code}:{TIMEFRAMES_UI[tfs[i+1]]}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("\U0001f519 Back", callback_data="nav:assets:0")])
    return InlineKeyboardMarkup(rows)


async def _show_signal_screen(query, chat_id: int, asset_code: str, tf_seconds: int) -> None:
    """Fetch real signal and display in Trading Smart Bot format."""
    asset_label = next((k for k, v in {**ASSETS, **{a[0]: a[1] for p in ASSETS_UI_PAGES for a in p}}
                        .items() if v == asset_code), asset_code)
    tf_label    = next((k for k, v in TIMEFRAMES_UI.items() if v == tf_seconds), f"{tf_seconds}s")

    await query.edit_message_text(
        f"\u23f3 Analysing {asset_label}...",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001f519 Back", callback_data="nav:assets:0")
        ]])
    )

    if not (session_manager and session_manager.is_connected):
        await query.edit_message_text(
            "\U0001f534 Not connected to Pocket Option.\nUse /setssid to connect.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")
            ]])
        )
        return

    candles = await session_manager.get_candles(asset_code, max(tf_seconds, 60), CANDLE_COUNT)
    result  = compute_signal(candles, tf_seconds)

    # If advanced mode is on, trigger full analysis instead
    adv = _get_adv(chat_id)
    if adv.get("advanced"):
        await query.edit_message_text(
            f"\u23f3 Advanced mode active — starting {adv['analysis_secs']}s analysis for {asset_label}...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("\U0001f519 Back", callback_data="nav:assets:0")
            ]])
        )
        asyncio.create_task(
            trigger_advanced_analysis(chat_id, asset_code, asset_label, tf_label),
            name=f"adv_nav_{chat_id}"
        )
        return

    price = None
    if candles:
        try:
            c = candles[-1]
            price = float(c["close"] if isinstance(c, dict) else c.close)
        except Exception:
            pass

    market_setting, volatility = _atr_info(candles) if candles else ("Stable", "Medium")
    conf = result.get("confidence", 75)
    rsi  = result.get("rsi")
    d    = result["direction"]

    # Confidence bar
    filled = round(conf / 10)
    bar    = "\u2588" * filled + "\u2591" * (10 - filled)

    # ASCII chart
    chart = _ascii_chart(candles) if candles else "▬▬▬▬▬"

    if d == "HIGHER":
        signal_line = "\U0001f7e2 HIGHER (CALL)"
    elif d == "LOWER":
        signal_line = "\U0001f534 LOWER (PUT)"
    else:
        signal_line = "\u26aa WAIT — no clear signal"

    price_str = f"{price:.5f}" if price else "N/A"
    rsi_str   = f"{rsi:.1f}" if rsi else "N/A"

    text = (
        f"\U0001f4ca Signal: {asset_label} \u2014 {tf_label}\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"\U0001f4c8 Market: {market_setting}\n"
        f"\u26a1 Volatility: {volatility}\n"
        f"\U0001f9e0 RSI: {rsi_str}\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"\U0001f3af Signal reliability: {conf}%\n"
        f"{bar}\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"\U0001f916 Bot signal: {signal_line}\n"
        f"\U0001f4b2 Price: {price_str}\n"
        f"\U0001f4c9 Chart: {chart}\n"
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500"
    )

    if d == "WAIT":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f504 Try again",  callback_data=f"nav:signal:{asset_code}:{tf_seconds}")],
            [InlineKeyboardButton("\U0001f519 Back",       callback_data="nav:assets:0")],
            [InlineKeyboardButton("\U0001f3e0 Main menu",  callback_data="nav:home")],
        ])
        await query.edit_message_text(text, reply_markup=keyboard)
        return

    # Register pending signal for win/loss voting
    signal_id = _make_signal_id(chat_id)
    pending_signals[signal_id] = {
        "user_id": chat_id, "ts": time.time(), "voted": False,
        "asset_label": asset_label, "tf_label": tf_label,
        "direction": "CALL" if d == "HIGHER" else "PUT",
        "confidence": conf, "rsi": rsi, "price": price_str,
        "market": market_setting, "reason": result.get("reason", ""),
    }

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f44d Win",  callback_data=f"vote:win:{signal_id}"),
         InlineKeyboardButton("\U0001f44e Loss", callback_data=f"vote:loss:{signal_id}")],
        [InlineKeyboardButton("\U0001f504 New signal", callback_data=f"nav:signal:{asset_code}:{tf_seconds}"),
         InlineKeyboardButton("\U0001f519 Back",       callback_data="nav:assets:0")],
        [InlineKeyboardButton("\U0001f3e0 Main menu",  callback_data="nav:home")],
    ])

    # Send as photo if available, else text
    img = SIGNAL_IMG_BUY if d == "HIGHER" else SIGNAL_IMG_SELL
    if img:
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
            await telegram_app.bot.send_photo(
                chat_id=chat_id, photo=img,
                caption=f"{signal_line}\n{asset_label} | {tf_label} | {conf}% reliability",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("\U0001f44d Win",  callback_data=f"vote:win:{signal_id}"),
                    InlineKeyboardButton("\U0001f44e Loss", callback_data=f"vote:loss:{signal_id}"),
                ]])
            )
            return
        except Exception:
            pass
    await query.edit_message_text(text, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# Inline menu
# ---------------------------------------------------------------------------
def _main_menu(chat_id):
    """Legacy compact menu — kept for /start fallback."""
    ps = _get_settings(chat_id)
    auto_lbl    = "\U0001f514 Auto-signals: ON"  if ps.get("auto")    else "\U0001f515 Auto-signals: OFF"
    scanner_lbl = "\U0001f4e1 Scanner: ON"        if ps.get("scanner") else "\U0001f4e1 Scanner: OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Get Signal",    callback_data="m:signal")],
        [InlineKeyboardButton(auto_lbl,                   callback_data="m:toggle_auto"),
         InlineKeyboardButton(scanner_lbl,                callback_data="m:toggle_scanner")],
        [InlineKeyboardButton("\U0001f4cb Analyze",       callback_data="m:analyze"),
         InlineKeyboardButton("\U0001f4b0 Account",       callback_data="m:account")],
        [InlineKeyboardButton("\U0001f511 Refresh SSID",  callback_data="m:refresh_ssid"),
         InlineKeyboardButton("\U0001f4e4 Export CSV",    callback_data="m:export")],
    ])

# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    known_users.add(chat_id)
    await _show_main_menu(chat_id, update, context)

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    known_users.add(chat_id)
    if not (session_manager and session_manager.is_connected):
        await update.message.reply_text("Not connected. Use /setssid to connect.")
        return

    adv = _get_adv(chat_id)
    if adv.get("advanced"):
        # Advanced mode: trigger delayed multi-TF analysis
        await trigger_advanced_analysis(chat_id, "EURUSD_otc", "EUR/USD (OTC)", "1 minute")
        return

    # Simple mode: instant signal
    await update.message.reply_text("Fetching signal for EUR/USD (OTC)...")
    candles = await session_manager.get_candles("EURUSD_otc", 60, CANDLE_COUNT)
    result  = compute_signal(candles, 60)
    if result["direction"] == "WAIT":
        await update.message.reply_text(_format_signal(result, "EUR/USD (OTC)", "1 minute"))
    else:
        price = None
        try:
            c = candles[-1]; price = float(c["close"] if isinstance(c, dict) else c.close)
        except Exception: pass
        await _send_signal_message(chat_id, result, "EUR/USD (OTC)", "1 minute",
                                    f"{price:.5f}" if price else "N/A")

async def autoon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    known_users.add(chat_id)
    _get_settings(chat_id)["auto"] = True
    await update.message.reply_text("Auto-signals enabled.")

async def autooff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    known_users.add(chat_id)
    _get_settings(chat_id)["auto"] = False
    await update.message.reply_text("Auto-signals disabled.")

async def scanner_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    known_users.add(chat_id)
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1: args = parts[1:]
    if not args:
        on = _get_settings(chat_id).get("scanner", False)
        await update.message.reply_text(f"Scanner: {'ON' if on else 'OFF'}\nUse /scanner on or /scanner off")
        return
    if args[0].lower() == "on":
        _get_settings(chat_id)["scanner"] = True
        await update.message.reply_text(f"Scanner enabled. Monitoring {len(SCAN_ASSETS)} assets.")
    else:
        _get_settings(chat_id)["scanner"] = False
        await update.message.reply_text("Scanner disabled.")

async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text("Analysing trades...")
    text = _build_analysis(chat_id)
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000])
    else:
        await update.message.reply_text(text)

async def account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (session_manager and session_manager.is_connected and session_manager.client):
        await update.message.reply_text("Not connected. Use /setssid to connect.")
        return
    await update.message.reply_text("Fetching balance...")
    try:
        balance = await session_manager.client.balance()
        try:
            ts = await session_manager.client.get_server_time()
            if ts and ts > 1_000_000_000_000: ts //= 1000
            server_dt = datetime.utcfromtimestamp(ts).strftime("%H:%M:%S UTC") if ts and ts > 1e9 else "N/A"
        except Exception: server_dt = "N/A"
        await update.message.reply_text(
            f"Account\n\nType: {'Demo' if session_manager.is_demo else 'Real'}\n"
            f"Balance: ${balance:.2f}\nServer time: {server_dt}")
    except Exception as exc:
        await update.message.reply_text(f"Could not fetch balance: {exc}")

def _parse_uid(ssid):
    try:
        p = json.loads(ssid[2:])
        if isinstance(p, list) and len(p) >= 2:
            return str(p[1].get("uid",""))
    except Exception: pass
    return None

async def setssid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global session_manager
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split(None, 1)
        if len(parts) > 1: args = [parts[1]]
    if not args:
        await update.message.reply_text(
            "Usage: /setssid 42[\"auth\",{...}]\n\nGet from: pocketoption.com > F12 > Network > WS > Messages")
        return
    new_ssid = args[0].strip()
    if not new_ssid.startswith('42["auth"'):
        await update.message.reply_text('Invalid format. Must start with 42["auth",')
        return
    uid = _parse_uid(new_ssid)
    if session_manager:
        session_manager.ssid = new_ssid
    await update.message.reply_text(f"SSID received (uid={uid}). Reconnecting...")
    if session_manager:
        async with session_manager._lock:
            ok = await session_manager._connect()
        if ok:
            await update.message.reply_text(f"Connected! UID: {uid}")
        else:
            await update.message.reply_text("Reconnect failed. Check /logs.")

async def refresh_ssid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global session_manager
    await update.message.reply_text("Fetching SSID from Cloudflare Worker...")
    new_ssid = await fetch_ssid_from_worker()
    if not new_ssid:
        await update.message.reply_text("Worker unavailable or SSID not set.\nUse /setssid manually.")
        return
    uid = _parse_uid(new_ssid)
    if session_manager:
        session_manager.ssid = new_ssid
        async with session_manager._lock:
            ok = await session_manager._connect()
        if ok:
            await update.message.reply_text(f"SSID refreshed from Worker. UID: {uid}")
        else:
            await update.message.reply_text("SSID fetched but reconnect failed.")
    else:
        await update.message.reply_text(f"SSID fetched (uid={uid}). Restart to apply.")

async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    path    = _csv_path(chat_id)
    if not os.path.exists(path):
        await update.message.reply_text("No trades recorded yet.")
        return
    try:
        with open(path, "rb") as f:
            await telegram_app.bot.send_document(chat_id=chat_id, document=f,
                filename=f"trades_{chat_id}.csv",
                caption=f"Trade log - {len(_read_trades(chat_id))} trades")
    except Exception as exc:
        await update.message.reply_text(f"Export error: {exc}")

async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not recent_errors:
        await update.message.reply_text("No recent errors.")
        return
    await update.message.reply_text("Recent errors:\n\n" + "\n".join(recent_errors[-10:]))

# ---------------------------------------------------------------------------
# Vote handler
# ---------------------------------------------------------------------------
async def vote_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":", 2)
    if len(parts) != 3: return
    _, outcome, signal_id = parts
    user_id = query.from_user.id
    signal  = pending_signals.get(signal_id)
    if signal is None:
        await query.answer("Signal expired.")
        return
    if time.time() - signal["ts"] > SIGNAL_EXPIRY_SEC:
        await query.answer("Expired (10 min limit).")
        pending_signals.pop(signal_id, None)
        try: await query.edit_message_reply_markup(reply_markup=None)
        except Exception: pass
        return
    if signal.get("voted"):
        await query.answer("Already recorded.")
        return
    signal["voted"] = True
    won = outcome == "win"
    _log_trade(user_id, signal, "win" if won else "loss")
    label = "Win \U0001f44d" if won else "Loss \U0001f44e"
    await query.answer(f"Recorded as {label}!")
    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"\u2705 Recorded: {label}", callback_data="vote:noop")
            ]]))
    except Exception: pass

async def vote_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Already recorded.")

# ---------------------------------------------------------------------------
# Inline menu callback handler
# ---------------------------------------------------------------------------
async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat.id
    known_users.add(chat_id)

    if data == "m:signal":
        await query.edit_message_text("Fetching signal...")
        if not (session_manager and session_manager.is_connected):
            await query.edit_message_text("Not connected.", reply_markup=_main_menu(chat_id))
            return
        candles = await session_manager.get_candles("EURUSD_otc", 60, CANDLE_COUNT)
        result  = compute_signal(candles, 60)
        if result["direction"] == "WAIT":
            await query.edit_message_text(_format_signal(result, "EUR/USD (OTC)", "1 minute"),
                                           reply_markup=_main_menu(chat_id))
        else:
            price = None
            try:
                c = candles[-1]; price = float(c["close"] if isinstance(c, dict) else c.close)
            except Exception: pass
            await query.edit_message_text("Signal sent below.", reply_markup=_main_menu(chat_id))
            await _send_signal_message(chat_id, result, "EUR/USD (OTC)", "1 minute",
                                        f"{price:.5f}" if price else "N/A")

    elif data == "m:toggle_auto":
        ps = _get_settings(chat_id)
        ps["auto"] = not ps.get("auto", False)
        state = "ON" if ps["auto"] else "OFF"
        await query.edit_message_text(f"Auto-signals: {state}", reply_markup=_main_menu(chat_id))

    elif data == "m:toggle_scanner":
        ps = _get_settings(chat_id)
        ps["scanner"] = not ps.get("scanner", False)
        state = "ON" if ps["scanner"] else "OFF"
        await query.edit_message_text(f"Scanner: {state}", reply_markup=_main_menu(chat_id))

    elif data == "m:analyze":
        await query.edit_message_text("Analysing...")
        text = _build_analysis(chat_id)
        if len(text) > 4000: text = text[:3990] + "\n...(truncated)"
        await query.edit_message_text(text, reply_markup=_main_menu(chat_id))

    elif data == "m:account":
        if not (session_manager and session_manager.is_connected and session_manager.client):
            await query.edit_message_text("Not connected.", reply_markup=_main_menu(chat_id))
            return
        await query.edit_message_text("Fetching balance...")
        try:
            balance = await session_manager.client.balance()
            await query.edit_message_text(
                f"Account\nType: {'Demo' if session_manager.is_demo else 'Real'}\nBalance: ${balance:.2f}",
                reply_markup=_main_menu(chat_id))
        except Exception as exc:
            await query.edit_message_text(f"Error: {exc}", reply_markup=_main_menu(chat_id))

    elif data == "m:refresh_ssid":
        await query.edit_message_text("Fetching SSID from Worker...")
        new_ssid = await fetch_ssid_from_worker()
        if new_ssid and session_manager:
            session_manager.ssid = new_ssid
            async with session_manager._lock:
                ok = await session_manager._connect()
            msg = f"SSID refreshed. UID: {_parse_uid(new_ssid)}" if ok else "Fetch OK but reconnect failed."
        else:
            msg = "Worker unavailable. Use /setssid manually."
        await query.edit_message_text(msg, reply_markup=_main_menu(chat_id))

    elif data == "m:export":
        path = _csv_path(chat_id)
        if not os.path.exists(path):
            await query.edit_message_text("No trades yet.", reply_markup=_main_menu(chat_id))
            return
        await query.edit_message_text("Sending file...")
        try:
            with open(path, "rb") as f:
                await telegram_app.bot.send_document(chat_id=chat_id, document=f,
                    filename=f"trades_{chat_id}.csv")
        except Exception as exc:
            await telegram_app.bot.send_message(chat_id=chat_id, text=f"Export error: {exc}")
        await query.edit_message_text("File sent.", reply_markup=_main_menu(chat_id))


async def nav_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all nav:* callbacks for the Trading Smart Bot UI."""
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat.id
    known_users.add(chat_id)

    # ── Home ─────────────────────────────────────────────────────────────
    if data == "nav:home":
        await _show_main_menu(chat_id, query, context)
        return

    # ── Asset list ────────────────────────────────────────────────────────
    if data.startswith("nav:assets:"):
        page = int(data.split(":")[-1])
        rec  = _best_asset_recommendation(chat_id)
        text = (
            f"\U0001f4ca ASSETS\n"
            f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
            f"\U0001f916 Bot recommendation: {rec}\n"
            f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
            "Select an asset:"
        )
        await query.edit_message_text(text, reply_markup=_assets_keyboard(page))
        return

    # ── Timeframe selection ───────────────────────────────────────────────
    if data.startswith("nav:tf:"):
        asset_code = data[len("nav:tf:"):]
        asset_label = next((k for k, v in {**ASSETS, **{a[0]: a[1] for p in ASSETS_UI_PAGES for a in p}}
                            .items() if v == asset_code), asset_code)
        text = (
            f"\u23f1 CHOOSE TRADING TIME\n"
            f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
            f"Asset: {asset_label}\n\n"
            "Select the timeframe:"
        )
        await query.edit_message_text(text, reply_markup=_timeframe_keyboard(asset_code))
        return

    # ── Signal generation ─────────────────────────────────────────────────
    if data.startswith("nav:signal:"):
        parts      = data.split(":")
        asset_code = parts[2]
        tf_seconds = int(parts[3])
        await _show_signal_screen(query, chat_id, asset_code, tf_seconds)
        return

    # ── How it works ──────────────────────────────────────────────────────
    if data == "nav:howto":
        text = (
            "\U0001f916 How Trading Smart Bot works\n"
            "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\n"
            "\U0001f4ca Real market data from Pocket Option\n"
            "The bot connects to Pocket Option via WebSocket and fetches live OHLC candle data.\n\n"
            "\U0001f9e0 Hybrid strategy:\n"
            "\u2022 RSI (14) \u2014 identifies overbought/oversold\n"
            "\u2022 MACD (12/26/9) \u2014 confirms trend direction\n"
            "\u2022 Bollinger Bands \u2014 detects ranging reversals\n\n"
            "\U0001f3af Signal reliability:\n"
            "Based on how strongly all indicators agree. Higher = more confident.\n\n"
            "\U0001f4b0 HIGHER = CALL (price expected to rise)\n"
            "\U0001f4c9 LOWER = PUT (price expected to fall)\n\n"
            "\u26a0\ufe0f Signals are for informational purposes only."
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")
        ]]))
        return

    # ── Ask a question ────────────────────────────────────────────────────
    if data == "nav:ask":
        await query.edit_message_text(
            "\U0001f4ac Ask a question\n"
            "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\n"
            "For support, use /logs to see recent errors.\n"
            "To update your SSID: /setssid\n"
            "To refresh from Cloudflare: /refresh_ssid\n\n"
            "All commands:\n"
            "/signal \u2014 manual signal\n"
            "/autoon / /autooff \u2014 toggle auto-signals\n"
            "/scanner on/off \u2014 multi-asset scanner\n"
            "/analyze \u2014 trade analytics\n"
            "/account \u2014 live balance\n"
            "/export \u2014 download trade CSV",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")
            ]])
        )
        return

    # ── Toggle auto ───────────────────────────────────────────────────────
    if data == "nav:toggle_auto":
        ps = _get_settings(chat_id)
        ps["auto"] = not ps.get("auto", False)
        state = "ON \u2705" if ps["auto"] else "OFF \U0001f515"
        await query.answer(f"Auto-signals: {state}", show_alert=False)
        await _show_main_menu(chat_id, query, context)
        return

    # ── Toggle scanner ────────────────────────────────────────────────────
    if data == "nav:toggle_scanner":
        ps = _get_settings(chat_id)
        ps["scanner"] = not ps.get("scanner", False)
        state = "ON \u2705" if ps["scanner"] else "OFF \U0001f515"
        await query.answer(f"Scanner: {state}", show_alert=False)
        await _show_main_menu(chat_id, query, context)
        return

    # ── Analyze ───────────────────────────────────────────────────────────
    if data == "nav:analyze":
        text = _build_analysis(chat_id)
        if len(text) > 4000:
            text = text[:3990] + "\n...(truncated)"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")
        ]]))
        return

    # ── Refresh SSID ──────────────────────────────────────────────────────
    if data == "nav:refresh_ssid":
        await query.edit_message_text("\u23f3 Fetching SSID from Worker...")
        new_ssid = await fetch_ssid_from_worker()
        if new_ssid and session_manager:
            session_manager.ssid = new_ssid
            async with session_manager._lock:
                ok = await session_manager._connect()
            msg = f"\u2705 SSID refreshed. UID: {_parse_uid(new_ssid)}" if ok else "\u274c Fetch OK but reconnect failed."
        else:
            msg = "\u274c Worker unavailable. Use /setssid manually."
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001f519 Back", callback_data="nav:home")
        ]]))
        return

    # ── Noop ──────────────────────────────────────────────────────────────
    if data == "nav:noop":
        return


async def advanced_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle advanced analysis mode. Usage: /advanced on | /advanced off"""
    chat_id = update.effective_chat.id
    adv = _get_adv(chat_id)
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1: args = parts[1:]

    if not args:
        on = adv.get("advanced", False)
        await update.message.reply_text(
            f"\U0001f9e0 Advanced Analysis Mode: {'ON \u2705' if on else 'OFF \U0001f515'}\n\n"
            f"Analysis window: {adv['analysis_secs']}s\n"
            f"Timeframes: {adv['short_tf']}s (short) + {adv['long_tf']}s (long)\n"
            f"Min confidence: {adv['min_conf']}%\n\n"
            "Commands:\n"
            "  /advanced on|off\n"
            "  /set_analysis <seconds>\n"
            "  /set_timeframes <short> <long>"
        )
        return

    if args[0].lower() == "on":
        adv["advanced"] = True
        await update.message.reply_text(
            "\U0001f9e0 Advanced Analysis Mode ENABLED \u2705\n\n"
            f"The bot will now observe the market for {adv['analysis_secs']}s,\n"
            f"check {adv['short_tf']}s and {adv['long_tf']}s timeframes,\n"
            f"apply ADX regime filter, and only send signals with \u2265{adv['min_conf']}% confidence.\n\n"
            "Use /simple to revert to instant signals."
        )
    elif args[0].lower() == "off":
        adv["advanced"] = False
        await update.message.reply_text(
            "\U0001f4ca Simple Mode ENABLED\n"
            "Signals are generated instantly without multi-TF analysis.\n"
            "Use /advanced on to re-enable."
        )
    else:
        await update.message.reply_text("Usage: /advanced on  or  /advanced off")


async def simple_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shortcut to disable advanced mode."""
    _get_adv(update.effective_chat.id)["advanced"] = False
    await update.message.reply_text(
        "\U0001f4ca Simple Mode enabled.\nSignals are generated instantly."
    )


async def set_analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the analysis observation window. Usage: /set_analysis <seconds>"""
    chat_id = update.effective_chat.id
    adv = _get_adv(chat_id)
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1: args = parts[1:]
    if not args:
        await update.message.reply_text(
            f"Current analysis window: {adv['analysis_secs']}s\n"
            "Usage: /set_analysis <seconds>  (e.g. /set_analysis 90)"
        )
        return
    try:
        secs = int(args[0])
        if secs < 20 or secs > 300:
            await update.message.reply_text("Value must be between 20 and 300 seconds.")
            return
        adv["analysis_secs"] = secs
        await update.message.reply_text(f"\u2705 Analysis window set to {secs}s.")
    except ValueError:
        await update.message.reply_text("Invalid value. Usage: /set_analysis 60")


async def set_timeframes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set short and long timeframes. Usage: /set_timeframes <short_secs> <long_secs>"""
    chat_id = update.effective_chat.id
    adv = _get_adv(chat_id)
    args = context.args or []
    if not args and update.message and update.message.text:
        parts = update.message.text.strip().split()
        if len(parts) > 1: args = parts[1:]
    if len(args) < 2:
        await update.message.reply_text(
            f"Current: short={adv['short_tf']}s  long={adv['long_tf']}s\n"
            "Usage: /set_timeframes <short> <long>\n"
            "Example: /set_timeframes 60 300"
        )
        return
    try:
        short = int(args[0]); long_ = int(args[1])
        if short >= long_:
            await update.message.reply_text("Short TF must be less than long TF.")
            return
        adv["short_tf"] = short; adv["long_tf"] = long_
        await update.message.reply_text(f"\u2705 Timeframes set: {short}s (short) + {long_}s (long).")
    except ValueError:
        await update.message.reply_text("Invalid values. Usage: /set_timeframes 60 300")


# ---------------------------------------------------------------------------
# Register handlers
# ---------------------------------------------------------------------------
telegram_app.add_handler(CallbackQueryHandler(vote_noop,    pattern="^vote:noop$"))
telegram_app.add_handler(CallbackQueryHandler(vote_handler, pattern="^vote:(win|loss):"))
telegram_app.add_handler(CallbackQueryHandler(nav_handler,  pattern="^nav:"))
telegram_app.add_handler(CallbackQueryHandler(menu_handler, pattern="^m:"))
telegram_app.add_handler(CommandHandler("start",        start))
telegram_app.add_handler(CommandHandler("signal",          signal_command))
telegram_app.add_handler(CommandHandler("advanced",        advanced_command))
telegram_app.add_handler(CommandHandler("simple",          simple_command))
telegram_app.add_handler(CommandHandler("set_analysis",    set_analysis_command))
telegram_app.add_handler(CommandHandler("set_timeframes",  set_timeframes_command))
telegram_app.add_handler(CommandHandler("autoon",       autoon_command))
telegram_app.add_handler(CommandHandler("autooff",      autooff_command))
telegram_app.add_handler(CommandHandler("scanner",      scanner_command))
telegram_app.add_handler(CommandHandler("analyze",      analyze_command))
telegram_app.add_handler(CommandHandler("account",      account_command))
telegram_app.add_handler(CommandHandler("setssid",      setssid_command))
telegram_app.add_handler(CommandHandler("refresh_ssid", refresh_ssid_command))
telegram_app.add_handler(CommandHandler("export",       export_command))
telegram_app.add_handler(CommandHandler("logs",         logs_command))

# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------
async def reset_webhook():
    if not RENDER_EXTERNAL_URL:
        logger.error("RENDER_EXTERNAL_URL not set")
        return
    base = f"https://api.telegram.org/bot{TOKEN}"
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{base}/deleteWebhook", params={"drop_pending_updates":"true"})
        logger.info(f"deleteWebhook: {r.json()}")
        r = await c.post(f"{base}/setWebhook", params={"url":f"{RENDER_EXTERNAL_URL}/telegram"})
        logger.info(f"setWebhook: {r.json()}")

async def health(request: Request):
    return JSONResponse({"status":"healthy","timestamp":str(datetime.now()),
        "po_connected": session_manager is not None and session_manager.is_connected})

async def telegram_webhook(request: Request):
    data = await request.json()
    try:
        await telegram_app.process_update(Update.de_json(data, telegram_app.bot))
    except Exception as exc:
        logger.error(f"Webhook error: {exc}", exc_info=True)
    return JSONResponse({"status":"ok"})

routes = [Route("/health", health, methods=["GET"]),
          Route("/telegram", telegram_webhook, methods=["POST"])]

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: Starlette):
    global session_manager, SSID

    logger.info("Starting up...")
    await telegram_app.initialize()
    await telegram_app.start()
    await reset_webhook()

    # Fetch SSID from Worker if configured
    worker_ssid = await fetch_ssid_from_worker()
    if worker_ssid:
        SSID = worker_ssid
        logger.info("Using SSID from Cloudflare Worker")

    if not SSID:
        logger.error("No SSID available. Set SSID env var or configure Cloudflare Worker.")
    else:
        # Parse is_demo from SSID
        is_demo = True
        try:
            p = json.loads(SSID[2:])
            if isinstance(p, list) and len(p) >= 2:
                is_demo = bool(p[1].get("isDemo", 1))
        except Exception: pass

        chat_id = int(CHAT_ID) if CHAT_ID else 0
        session_manager = SessionManager(ssid=SSID, is_demo=is_demo,
                                          chat_id=chat_id, name="Bot")
        ok = await session_manager.start()
        if ok:
            logger.info("Connected to Pocket Option")
            if chat_id: known_users.add(chat_id)
        else:
            log_error("Initial connection failed - will retry")

    poll_task    = asyncio.create_task(_poll_loop(), name="poll")
    scan_tasks   = [asyncio.create_task(_scan_asset(a), name=f"scan_{a}") for a in SCAN_ASSETS]

    yield

    poll_task.cancel()
    for t in scan_tasks: t.cancel()
    if session_manager: await session_manager.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Shutdown complete.")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if IS_RENDER:
    web_app = Starlette(routes=routes, lifespan=lifespan)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"IS_RENDER=true - starting webhook server on port {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port)
else:
    logger.info("Local mode - starting polling...")
    telegram_app.run_polling()
