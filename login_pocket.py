"""
login_pocket.py — Pocket Option SSID Extractor
================================================
Runs Chrome on your PC, logs into Pocket Option, extracts the SSID,
and sends it to your Telegram bot via /setssid.

You solve the CAPTCHA manually in the browser window that opens.
After that, everything is automatic.

Usage:
    python login_pocket.py
    python login_pocket.py --email you@example.com --password yourpass

Environment variables (alternative to command-line args):
    PO_EMAIL      your Pocket Option email
    PO_PASSWORD   your Pocket Option password
    BOT_TOKEN     your Telegram bot token
    CHAT_ID       your Telegram chat ID

Dependencies:
    pip install undetected-chromedriver selenium requests
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
except ImportError:
    print("Missing dependencies. Run:")
    print("    pip install undetected-chromedriver selenium requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOGIN_URL    = "https://pocketoption.com/en/login/"
CABINET_URL  = "https://pocketoption.com/en/cabinet/"
WAIT_TIMEOUT = 60   # seconds to wait for login to complete
CAPTCHA_WAIT = 30   # extra seconds for you to solve CAPTCHA manually


def get_config() -> dict:
    """
    Read credentials from command-line args, then env vars, then prompt.
    Returns dict with keys: email, password, bot_token, chat_id.
    """
    parser = argparse.ArgumentParser(
        description="Extract Pocket Option SSID and send to Telegram bot"
    )
    parser.add_argument("--email",     default="", help="Pocket Option email")
    parser.add_argument("--password",  default="", help="Pocket Option password")
    parser.add_argument("--bot-token", default="", help="Telegram bot token")
    parser.add_argument("--chat-id",   default="", help="Telegram chat ID")
    parser.add_argument("--is-demo",   default="1", help="1=demo, 0=real (default: 1)")
    parser.add_argument("--no-send",   action="store_true",
                        help="Print SSID only, don't send to Telegram")
    args = parser.parse_args()

    email     = args.email     or os.environ.get("PO_EMAIL",    "")
    password  = args.password  or os.environ.get("PO_PASSWORD", "")
    bot_token = args.bot_token or os.environ.get("BOT_TOKEN",   "")
    chat_id   = args.chat_id   or os.environ.get("CHAT_ID",     "")
    is_demo   = int(args.is_demo)

    # Prompt for anything still missing
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
        "email":     email,
        "password":  password,
        "bot_token": bot_token,
        "chat_id":   chat_id,
        "is_demo":   is_demo,
        "no_send":   args.no_send,
    }


# ---------------------------------------------------------------------------
# Browser automation
# ---------------------------------------------------------------------------

def launch_browser() -> uc.Chrome:
    """
    Launch undetected Chrome.
    undetected_chromedriver patches Chrome to avoid bot detection.
    The browser window is visible so you can solve CAPTCHA manually.
    """
    print("Launching Chrome...")
    options = uc.ChromeOptions()
    # Keep browser visible (do NOT add --headless)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,800")

    driver = uc.Chrome(options=options, use_subprocess=True)
    return driver


def fill_login_form(driver: uc.Chrome, email: str, password: str) -> None:
    """Navigate to login page and fill in credentials."""
    print(f"Navigating to {LOGIN_URL} ...")
    driver.get(LOGIN_URL)
    time.sleep(3)   # let page load fully

    wait = WebDriverWait(driver, 20)

    # Fill email
    try:
        email_field = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="email"]'))
        )
        email_field.clear()
        email_field.send_keys(email)
        print("✓ Email filled")
    except TimeoutException:
        raise RuntimeError("Could not find email field — page structure may have changed")

    # Fill password
    try:
        pw_field = driver.find_element(By.CSS_SELECTOR, 'input[name="password"]')
        pw_field.clear()
        pw_field.send_keys(password)
        print("✓ Password filled")
    except NoSuchElementException:
        raise RuntimeError("Could not find password field")

    # Click login button
    try:
        # Try multiple selectors for the submit button
        for selector in [
            'button[type="submit"]',
            'input[type="submit"]',
            '.btn-login',
            'button.btn',
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, selector)
                btn.click()
                print("✓ Login button clicked")
                break
            except NoSuchElementException:
                continue
        else:
            # Fallback: submit the form via JS
            driver.execute_script(
                "document.querySelector('form').submit();"
            )
            print("✓ Form submitted via JS")
    except Exception as exc:
        raise RuntimeError(f"Could not click login button: {exc}")


def wait_for_login(driver: uc.Chrome, captcha_wait: int = CAPTCHA_WAIT) -> None:
    """
    Wait for login to complete.
    The browser window stays open so you can solve CAPTCHA manually.
    """
    print(f"\n{'='*50}")
    print("BROWSER IS OPEN — solve the CAPTCHA if one appears.")
    print(f"Waiting up to {captcha_wait}s for you to complete login...")
    print(f"{'='*50}\n")

    deadline = time.time() + captcha_wait + WAIT_TIMEOUT
    while time.time() < deadline:
        current_url = driver.current_url
        # Success: redirected to cabinet
        if "/cabinet" in current_url or "/trade" in current_url:
            print(f"✓ Login successful! URL: {current_url}")
            return
        # Still on login page — waiting
        time.sleep(2)

    raise RuntimeError(
        f"Login did not complete within {captcha_wait + WAIT_TIMEOUT}s.\n"
        "Make sure you solved the CAPTCHA and the credentials are correct."
    )


def extract_ssid(driver: uc.Chrome) -> tuple[str, int | None]:
    """
    Extract the SSID cookie and UID from the browser.

    Returns (ssid_value, uid) where:
      ssid_value  — the raw session cookie value
      uid         — integer UID or None if not found
    """
    print("Extracting SSID cookie...")

    # Give the page a moment to set all cookies
    time.sleep(3)

    cookies = driver.get_cookies()
    ssid_value = None
    for cookie in cookies:
        name = cookie.get("name", "").lower()
        if name in ("ssid", "session", "po_session"):
            ssid_value = cookie.get("value", "")
            print(f"✓ Found cookie '{cookie['name']}': {ssid_value[:20]}...")
            break

    if not ssid_value:
        # Fallback: try reading from localStorage or sessionStorage
        try:
            ssid_value = driver.execute_script(
                "return localStorage.getItem('ssid') || sessionStorage.getItem('ssid');"
            )
            if ssid_value:
                print(f"✓ Found SSID in storage: {ssid_value[:20]}...")
        except Exception:
            pass

    if not ssid_value:
        # Last resort: dump all cookies for inspection
        print("\nAll cookies found:")
        for c in cookies:
            print(f"  {c['name']} = {c.get('value','')[:40]}")
        raise RuntimeError(
            "Could not find SSID cookie.\n"
            "The cookie name may be different — check the output above."
        )

    # Try to extract UID from the page
    uid = None
    try:
        # Many trading platforms expose user data in a global JS variable
        uid = driver.execute_script(
            "return window.__USER_ID__ || window.userId || "
            "(window.user && window.user.id) || null;"
        )
        if uid:
            uid = int(uid)
            print(f"✓ UID from JS: {uid}")
    except Exception:
        pass

    if not uid:
        # Try extracting from page source
        try:
            source = driver.page_source
            import re
            match = re.search(r'"uid"\s*:\s*(\d+)', source)
            if match:
                uid = int(match.group(1))
                print(f"✓ UID from page source: {uid}")
        except Exception:
            pass

    return ssid_value, uid


def build_auth_string(ssid: str, uid: int | None, is_demo: int = 1) -> str:
    """
    Build the full WebSocket authentication string expected by BinaryOptionsToolsV2.
    Format: 42["auth",{"session":"...","isDemo":1,"uid":123,"platform":1}]
    """
    auth_payload = {
        "session":       ssid,
        "isDemo":        is_demo,
        "uid":           uid if uid else 0,
        "platform":      1,
        "isFastHistory": True,
        "isOptimized":   True,
    }
    return f'42["auth",{json.dumps(auth_payload, separators=(",", ":"))}]'


# ---------------------------------------------------------------------------
# Telegram delivery
# ---------------------------------------------------------------------------

def send_to_telegram(bot_token: str, chat_id: str, auth_string: str) -> bool:
    """
    Send the SSID to the bot via /setssid command using the Bot API.
    Returns True on success.
    """
    message = f"/setssid {auth_string}"
    url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text":    message,
    }).encode()

    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read())
            if resp.get("ok"):
                print("✓ SSID sent to Telegram bot via /setssid")
                return True
            else:
                print(f"✗ Telegram API error: {resp}")
                return False
    except Exception as exc:
        print(f"✗ Could not send to Telegram: {exc}")
        return False


def update_cloudflare_worker(worker_url: str, api_key: str, auth_string: str) -> bool:
    """
    Optionally push the new SSID to the Cloudflare Worker relay.
    Returns True on success.
    """
    if not worker_url or not api_key:
        return False
    try:
        payload = json.dumps({"ssid": auth_string}).encode()
        req = urllib.request.Request(
            f"{worker_url.rstrip('/')}/ssid",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-API-Key":    api_key,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            print(f"✓ Cloudflare Worker updated: {resp.get('status','ok')}")
            return True
    except Exception as exc:
        print(f"⚠ Could not update Cloudflare Worker: {exc}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Pocket Option SSID Extractor")
    print("=" * 60)

    config = get_config()

    driver = None
    try:
        # 1. Launch browser
        driver = launch_browser()

        # 2. Fill login form
        fill_login_form(driver, config["email"], config["password"])

        # 3. Wait for login (you solve CAPTCHA here)
        wait_for_login(driver)

        # 4. Extract SSID
        ssid_value, uid = extract_ssid(driver)

        # 5. Build auth string
        auth_string = build_auth_string(ssid_value, uid, config["is_demo"])

        # 6. Print results
        print("\n" + "=" * 60)
        print("SUCCESS!")
        print(f"SSID value:  {ssid_value[:30]}...")
        print(f"UID:         {uid}")
        print(f"Auth string: {auth_string[:60]}...")
        print("=" * 60)
        print("\nFull auth string (copy this for /setssid):")
        print(auth_string)
        print()

        # 7. Send to Telegram bot
        if not config["no_send"] and config["bot_token"] and config["chat_id"]:
            print("Sending to Telegram bot...")
            sent = send_to_telegram(
                config["bot_token"], config["chat_id"], auth_string
            )
            if sent:
                print("✓ Bot will reconnect automatically with the new SSID.")
            else:
                print("✗ Telegram send failed. Copy the auth string above and")
                print("  send it manually: /setssid <auth_string>")
        else:
            print("(--no-send flag set or missing bot credentials)")
            print("Copy the auth string above and send it to your bot:")
            print(f"  /setssid {auth_string}")

        # 8. Optionally update Cloudflare Worker
        worker_url = os.environ.get("WORKER_URL", "")
        worker_key = os.environ.get("WORKER_API_KEY", "")
        if worker_url and worker_key:
            print("\nUpdating Cloudflare Worker...")
            update_cloudflare_worker(worker_url, worker_key, auth_string)

    except RuntimeError as exc:
        print(f"\n✗ Error: {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        sys.exit(0)
    finally:
        if driver:
            try:
                input("\nPress Enter to close the browser...")
            except EOFError:
                pass
            driver.quit()
            print("Browser closed.")
