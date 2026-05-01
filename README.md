# My Signal Bot

A Telegram bot that receives and forwards trading signals for manual execution on Pocket Option.

## Local Development (Polling Mode)

```bash
# 1. Activate the virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set your bot token
export BOT_TOKEN=your_token_here   # macOS/Linux
set BOT_TOKEN=your_token_here      # Windows CMD
$env:BOT_TOKEN="your_token_here"   # Windows PowerShell

# 4. Run in polling mode
python bot.py
```

## Render Deployment (Webhook Mode)

Set these environment variables in Render:
- `BOT_TOKEN` – your Telegram bot token
- `RENDER_URL` – your Render service URL (e.g. `https://my-signal-bot.onrender.com`)

Build command: `pip install -r requirements.txt`  
Start command: `python bot.py`

## Endpoints

| Method | Path        | Description                        |
|--------|-------------|------------------------------------|
| GET    | `/health`   | Health check (returns `{"status":"ok"}`) |
| POST   | `/telegram` | Telegram webhook receiver          |

## Commands

| Command   | Description                  |
|-----------|------------------------------|
| `/start`  | Welcome message              |
| `/signal` | Get a placeholder signal     |

## Sending a Signal via HTTP

```bash
curl -X POST https://my-signal-bot.onrender.com/telegram \
  -H "Content-Type: application/json" \
  -d '{"update_id":1,"message":{"message_id":1,"chat":{"id":<CHAT_ID>,"type":"private"},"date":0,"text":"/signal"}}'
```
