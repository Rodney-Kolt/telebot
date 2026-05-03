# OTC Signal Bot

Telegram signal bot for Pocket Option using BinaryOptionsToolsV2.

---

## Quick Start

Set these environment variables on Render:

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Your Telegram bot token |
| `RENDER_EXTERNAL_URL` | Your Render service URL |
| `USERS_CONFIG` | JSON array of user configs (see below) |
| `WORKER_URL` | Cloudflare Worker URL (optional) |
| `WORKER_API_KEY` | Cloudflare Worker API key (optional) |

---

## SSID Refresh — `login_pocket.py`

When your Pocket Option session expires, run this script on your PC.
It opens Chrome, you log in and solve the CAPTCHA once, then it
automatically extracts the SSID and sends it to your bot.

### Install dependencies

```bash
pip install undetected-chromedriver selenium
```

### Set credentials (choose one method)

**Option A — Environment variables:**
```bash
# Windows
set PO_EMAIL=your@email.com
set PO_PASSWORD=yourpassword
set BOT_TOKEN=8134690308:AAEe...
set CHAT_ID=6920729115

# Optional: Cloudflare Worker (auto-updates the relay)
set WORKER_URL=https://pocket-option-ssid-relay.rodoainem.workers.dev
set WORKER_API_KEY=332ad4c9d571505091a5a7b093e79358013ebe147a3dca5acd0c9a0318e842f8
```

**Option B — Command-line arguments:**
```bash
python login_pocket.py --email your@email.com --password yourpass --bot-token 8134... --chat-id 6920729115
```

**Option C — Interactive prompts:**
```bash
python login_pocket.py
# Script will ask for each value
```

### Run the script

```bash
cd my_signal_bot
python login_pocket.py
```

### What happens

1. Chrome opens and navigates to pocketoption.com/en/login
2. Email and password are filled automatically
3. **You solve the CAPTCHA** in the browser window (takes ~10 seconds)
4. Script waits for login to complete
5. SSID is extracted from the browser cookies
6. Full auth string is built: `42["auth",{"session":"...","uid":...}]`
7. Sent to your bot via `/setssid` — bot reconnects immediately
8. If Cloudflare Worker is configured, it's updated too

### Flags

| Flag | Description |
|---|---|
| `--no-send` | Print SSID only, don't send to Telegram |
| `--is-demo 0` | Use real account (default: 1 = demo) |

### After running

Your bot will reply:
```
✅ Connected successfully!
SSID starts with: 42["auth",{"session":"...
UID: 27658142
```

---

## USERS_CONFIG format

```json
[
  {
    "name": "Rodney",
    "chat_id": "6920729115",
    "ssid": "42[\"auth\",{\"session\":\"...\",\"isDemo\":1,\"uid\":27658142,\"platform\":2}]",
    "is_demo": "1",
    "broadcast_ids": ["6577478732"]
  }
]
```

---

## Cloudflare Worker

The Worker stores your SSID securely. Bot fetches it on startup.

**Update SSID in Worker:**
```bash
cd cloudflare-worker
wrangler secret put SSID
# paste your new 42["auth",...] string
```

**Worker endpoints:**
- `GET /health` — public health check
- `GET /ssid` — get stored SSID (requires X-API-Key header)
- `POST /ssid` — update SSID (requires X-API-Key header)
