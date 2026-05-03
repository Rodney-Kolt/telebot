/**
 * Pocket Option SSID Relay Worker
 *
 * This Worker acts as a secure vault for your Pocket Option SSID.
 * Your Render bot calls this Worker to get the current SSID on startup
 * and on demand. When the SSID expires, you update the Worker secret
 * once — no Render redeploy needed.
 *
 * Endpoints:
 *   GET  /ssid          → returns current SSID (requires X-API-Key header)
 *   POST /ssid          → update SSID (requires X-API-Key + body: {ssid: "..."})
 *   GET  /health        → public health check
 *
 * Secrets to set (wrangler secret put <NAME>):
 *   SSID       – the full 42["auth",{...}] string
 *   API_KEY    – random secret string (generate with: openssl rand -hex 32)
 *   BOT_TOKEN  – optional: your Telegram bot token for expiry alerts
 *   CHAT_ID    – optional: your Telegram chat ID for expiry alerts
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // ── Public health check ──────────────────────────────────────────────
    if (url.pathname === "/health") {
      return Response.json({
        status: "ok",
        ssid_set: Boolean(env.SSID),
        timestamp: new Date().toISOString(),
      });
    }

    // ── Auth check for all other routes ─────────────────────────────────
    const apiKey = request.headers.get("X-API-Key");
    if (!apiKey || apiKey !== env.API_KEY) {
      return Response.json({ error: "Unauthorized" }, { status: 401 });
    }

    // ── GET /ssid — return current SSID ─────────────────────────────────
    if (url.pathname === "/ssid" && request.method === "GET") {
      if (!env.SSID) {
        return Response.json(
          { error: "SSID not set. Run: wrangler secret put SSID" },
          { status: 404 }
        );
      }

      // Parse UID from SSID for verification
      let uid = null;
      try {
        const payload = JSON.parse(env.SSID.slice(2)); // strip "42"
        uid = payload[1]?.uid ?? null;
      } catch (_) {}

      return Response.json({
        ssid: env.SSID,
        uid: uid,
        retrieved_at: new Date().toISOString(),
      });
    }

    // ── POST /ssid — update SSID (called from bot after manual refresh) ──
    if (url.pathname === "/ssid" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return Response.json({ error: "Invalid JSON body" }, { status: 400 });
      }

      const newSsid = body?.ssid?.trim();
      if (!newSsid || !newSsid.startsWith('42["auth"')) {
        return Response.json(
          { error: 'Invalid SSID format. Must start with 42["auth"' },
          { status: 400 }
        );
      }

      // Workers cannot update their own secrets at runtime.
      // Instead, we store the SSID in a KV namespace if available,
      // or return instructions to update via wrangler.
      //
      // For simplicity: return the SSID back with instructions.
      // The bot will use /setssid to update itself in memory,
      // and you update the Worker secret separately when convenient.
      return Response.json({
        status: "received",
        message: "SSID received by Worker. To persist: wrangler secret put SSID",
        ssid_preview: newSsid.slice(0, 30) + "...",
      });
    }

    return Response.json({ error: "Not found" }, { status: 404 });
  },
};
