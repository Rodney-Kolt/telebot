"""
login_pocket.py — Pocket Option SSID Extractor
================================================
Runs Chrome on your PC, logs into Pocket Option, intercepts the
WebSocket auth message to extract the correct SSID, and sends it
to your Telegram bot via /setssid.

The SSID for BinaryOptionsToolsV2 is the short session token inside
the WebSocket 42["auth",...] message — NOT the ci_session HTTP cookie.

Usage:
    python login_pocket.py
    python login_pocket.py --email you@example.com --password yourpass

Environment variables:
    PO_EMAIL, PO_PASSWORD, BOT_TOKEN, CHAT_ID
    WORKER_URL, WORKER_API_KEY  (optional Cloudflare Worker)

Dependencies:
    pip install undetected-chromedriver selenium requests setuptools
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
except ImportError as e:
    print(f"Import error: {e}")
    print("Run:")
    print(r"  C:\Users\rodoa\AppData\Local\Python\pythoncore-3.14-64\python.exe -m pip install undetected-chromedriver selenium requests setuptools")
    sys.exit(1)

LOGIN_URL   = "https://pocketoption.com/en/login/"
TRADE_URL   = "https://pocketoption.com/en/cabinet/demo-quick-high-low/"
WAIT_SECS   = 60   # seconds to wait for page load / CAPTCHA


def get_config() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--email",     default="")
    parser.add_argument("--password",  default="")
    parser.add_argument("--bot-token", default="")
    parser.add_argument("--chat-id",   default="")
    parser.add_argument("--is-demo",   default="1")
    parser.add_argument("--no-send",   action="store_true")
    args = parser.parse_args()

    email     = args.email     or os.environ.get("PO_EMAIL",    "")
    password  = args.password  or os.environ.get("PO_PASSWORD", "")
    bot_token = args.bot_token or os.environ.get("BOT_TOKEN",   "")
    chat_id   = args.chat_id   or os.environ.get("CHAT_ID",     "")

    if not email:
        email = input("Pocket Option email: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("Pocket Option password: ")
    if not args.no_send:
        if not bot_token:
            bot_token = input("Telegram BOT_TOKEN: ").strip()
        if not chat_id:
            chat_id = input("Telegram CHAT_ID: ").strip()

    return {
        "email": email, "password": password,
        "bot_token": bot_token, "chat_id": chat_id,
        "is_demo": int(args.is_demo), "no_send": args.no_send,
    }


def launch_browser() -> uc.Chrome:
    print("Launching Chrome...")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,800")
    return uc.Chrome(options=options, use_subprocess=True, version_main=147)


def login(driver: uc.Chrome, email: str, password: str) -> None:
    """Navigate to login page, fill credentials, wait for cabinet."""
    print(f"Navigating to {LOGIN_URL} ...")
    driver.get(LOGIN_URL)
    print(f"Waiting {WAIT_SECS}s for page to load (solve CAPTCHA if it appears)...")
    time.sleep(WAIT_SECS)

    # Already logged in?
    if "/cabinet" in driver.current_url or "/trade" in driver.current_url:
        print(f"✓ Already logged in: {driver.current_url}")
        return

    # Try to fill form
    wait = WebDriverWait(driver, 20)
    for sel in ['input[name="email"]', 'input[type="email"]', '#email']:
        try:
            f = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            f.clear(); f.send_keys(email)
            print("✓ Email filled")
            break
        except TimeoutException:
            continue

    for sel in ['input[name="password"]', 'input[type="password"]', '#password']:
        try:
            f = driver.find_element(By.CSS_SELECTOR, sel)
            f.clear(); f.send_keys(password)
            print("✓ Password filled")
            break
        except NoSuchElementException:
            continue

    for sel in ['button[type="submit"]', 'input[type="submit"]', 'form button']:
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            print("✓ Login button clicked")
            break
        except NoSuchElementException:
            continue

    # Wait for redirect to cabinet
    print("Waiting for login redirect...")
    deadline = time.time() + 90
    while time.time() < deadline:
        if "/cabinet" in driver.current_url or "/trade" in driver.current_url:
            print(f"✓ Logged in: {driver.current_url}")
            return
        time.sleep(2)

    raise RuntimeError(f"Login did not complete. Current URL: {driver.current_url}")


def extract_ssid_from_websocket(driver: uc.Chrome) -> tuple[str | None, int | None]:
    """
    Navigate to the trading page and intercept the WebSocket auth message.
    This captures the correct short session token, not the HTTP cookie.
    """
    print("Injecting WebSocket interceptor...")

    # Inject before navigating to trading page
    driver.execute_script("""
        window._po_ssid = null;
        window._po_uid  = null;
        const _OrigWS = window.WebSocket;
        function PatchedWS(url, protocols) {
            const ws = protocols ? new _OrigWS(url, protocols) : new _OrigWS(url);
            const _origSend = ws.send.bind(ws);
            ws.send = function(data) {
                if (typeof data === 'string' && data.indexOf('"auth"') !== -1) {
                    try {
                        var payload = JSON.parse(data.slice(2));
                        if (Array.isArray(payload) && payload[0] === 'auth' && payload[1]) {
                            window._po_ssid = payload[1].session || payload[1].sessionToken || null;
                            window._po_uid  = payload[1].uid    || null;
                        }
                    } catch(e) {}
                }
                return _origSend(data);
            };
            return ws;
        }
        PatchedWS.prototype = _OrigWS.prototype;
        window.WebSocket = PatchedWS;
    """)

    print(f"Navigating to trading page to trigger WebSocket auth...")
    driver.get(TRADE_URL)

    print("Waiting up to 30s for WebSocket auth message...")
    for i in range(30):
        ssid = driver.execute_script("return window._po_ssid;")
        uid  = driver.execute_script("return window._po_uid;")
        if ssid:
            print(f"✓ SSID captured from WebSocket: {str(ssid)[:20]}...")
            print(f"✓ UID: {uid}")
            return str(ssid), int(uid) if uid else None
        time.sleep(1)
        if i % 5 == 0:
            print(f"  Still waiting... ({i}s)")

    return None, None


def build_auth_string(ssid: str, uid: int | None, is_demo: int = 1) -> str:
    payload = {
        "session":       ssid,
        "isDemo":        is_demo,
        "uid":           uid or 0,
        "platform":      1,
        "isFastHistory": True,
        "isOptimized":   True,
    }
    return f'42["auth",{json.dumps(payload, separators=(",", ":"))}]'


def send_to_telegram(bot_token: str, chat_id: str, auth_string: str) -> bool:
    message = f"/setssid {auth_string}"
    url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message}).encode()
    try:
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read())
            if resp.get("ok"):
                print("✓ SSID sent to Telegram bot via /setssid")
                return True
            print(f"✗ Telegram error: {resp}")
            return False
    except Exception as exc:
        print(f"✗ Telegram send failed: {exc}")
        return False


def update_cloudflare_worker(worker_url: str, api_key: str, auth_string: str) -> None:
    if not worker_url or not api_key:
        return
    try:
        payload = json.dumps({"ssid": auth_string}).encode()
        req = urllib.request.Request(
            f"{worker_url.rstrip('/')}/ssid", data=payload,
            headers={"Content-Type": "application/json", "X-API-Key": api_key},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"✓ Cloudflare Worker updated: {json.loads(r.read()).get('status','ok')}")
    except Exception as exc:
        print(f"⚠ Cloudflare Worker update failed: {exc}")


if __name__ == "__main__":
    print("=" * 60)
    print("  Pocket Option SSID Extractor")
    print("=" * 60)

    config = get_config()
    driver = None

    try:
        driver = launch_browser()
        login(driver, config["email"], config["password"])

        # Try WebSocket interception first
        ssid_value, uid = extract_ssid_from_websocket(driver)

        if not ssid_value:
            print("\nWebSocket capture failed. Manual input required.")
            print("In the Chrome window:")
            print("  F12 → Network → WS → click connection → Messages")
            print('  Find 42["auth",... → copy the session value only')
            ssid_value = input("Paste session value: ").strip()
            uid_str    = input("Paste UID (numbers): ").strip()
            uid = int(uid_str) if uid_str.isdigit() else None

        if not ssid_value:
            raise RuntimeError("No SSID obtained.")

        auth_string = build_auth_string(ssid_value, uid, config["is_demo"])

        print("\n" + "=" * 60)
        print("SUCCESS!")
        print(f"Session: {ssid_value[:30]}...")
        print(f"UID:     {uid}")
        print(f"Auth:    {auth_string[:70]}...")
        print("=" * 60)
        print("\nFull auth string:")
        print(auth_string)
        print()

        if not config["no_send"] and config["bot_token"] and config["chat_id"]:
            send_to_telegram(config["bot_token"], config["chat_id"], auth_string)

        update_cloudflare_worker(
            os.environ.get("WORKER_URL", ""),
            os.environ.get("WORKER_API_KEY", ""),
            auth_string,
        )

    except RuntimeError as exc:
        print(f"\n✗ {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nCancelled.")
    finally:
        if driver:
            try:
                input("\nPress Enter to close browser...")
            except EOFError:
                pass
            try:
                driver.quit()
            except Exception:
                pass
            print("Browser closed.")
