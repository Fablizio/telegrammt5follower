import os, re, json, time, asyncio
from collections import deque
from typing import Dict, Any

from dotenv import load_dotenv
from telethon import TelegramClient, events
from aiohttp import web

load_dotenv()

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = (os.getenv("TG_API_HASH") or "").strip()

ALLOWED_CHAT_IDS = set()
for x in (os.getenv("ALLOWED_CHAT_IDS", "") or "").split(","):
    x = x.strip()
    if not x:
        continue
    ALLOWED_CHAT_IDS.add(int(x))

STATE_FILE = "unred_state.json"

LATEST: Dict[str, Any] = {
    "ok": False,
    "ts": 0,
    "chat_id": None,
    "message_id": 0,
    "key": "",
    "text": "",
}

PENDING_QUEUE = deque(maxlen=200)


def empty_payload() -> Dict[str, Any]:
    return {
        "ok": False,
        "ts": 0,
        "chat_id": None,
        "message_id": 0,
        "key": "",
        "text": "",
    }


def payload_from_queue() -> Dict[str, Any]:
    if not PENDING_QUEUE:
        return empty_payload()
    return {"ok": True, **PENDING_QUEUE[0]}


def enqueue_signal(payload: Dict[str, Any]) -> None:
    key = payload.get("key")
    if not key:
        return

    if any(x.get("key") == key for x in PENDING_QUEUE):
        return

    if len(PENDING_QUEUE) == PENDING_QUEUE.maxlen:
        dropped = PENDING_QUEUE.popleft()
        log(f"Queue piena: rimosso il più vecchio {dropped.get('key')}")

    PENDING_QUEUE.append(payload)


def remove_acked_signal(chat_id: Any, message_id: Any, key: Any) -> bool:
    if not PENDING_QUEUE:
        return False

    for i, item in enumerate(PENDING_QUEUE):
        if key and key == item.get("key"):
            del PENDING_QUEUE[i]
            return True
        if chat_id == item.get("chat_id") and message_id == item.get("message_id"):
            del PENDING_QUEUE[i]
            return True

    return False

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def load_state() -> Dict[str, int]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return {str(k): int(v) for k, v in d.items()}
    except Exception:
        return {}

def save_state(state: Dict[str, int]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def clean_text(text: str) -> str:
    t = (text or "").replace("\u00A0", " ")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def normalize_telegram_signal(text: str) -> str:
    lines = [ln.rstrip() for ln in clean_text(text).split("\n")]
    while lines and lines[-1].strip() == "":
        lines.pop()

    while lines:
        last = lines[-1].strip()
        if re.fullmatch(r"\d+", last) or re.fullmatch(r"\d{1,2}:\d{2}", last) or re.fullmatch(r"👁?\s*\d+", last):
            lines.pop()
            while lines and lines[-1].strip() == "":
                lines.pop()
            continue
        break

    return "\n".join(lines).strip()

def looks_like_signal(text: str) -> bool:
    t = (text or "").lower()
    return ("entry" in t) and (("stop" in t) or ("sl" in t)) and ("tp" in t) and (("buy" in t) or ("sell" in t))

# ---------- CORS / Helpers ----------
def cors(resp: web.Response) -> web.Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

async def options_handler(request: web.Request):
    return cors(web.Response(status=204))

# ---------- HTTP server (localhost) ----------
async def latest_handler(request: web.Request):
    return cors(web.json_response(payload_from_queue()))

async def health_handler(request: web.Request):
    return cors(web.json_response({"ok": True, "allowed": sorted(ALLOWED_CHAT_IDS)}))

async def ack_handler(request: web.Request):
    """
    Il browser chiama /ack dopo aver cliccato "Converti & Invia".
    Se (chat_id,message_id) matchano LATEST, svuotiamo LATEST per evitare resend.
    """
    try:
        data = await request.json()
    except Exception:
        return cors(web.json_response({"ok": False, "err": "bad_json"}, status=400))

    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    key = data.get("key")

    cleared = remove_acked_signal(chat_id=chat_id, message_id=message_id, key=key)
    return cors(web.json_response({"ok": True, "cleared": cleared, "pending": len(PENDING_QUEUE)}))

async def run_http():
    app = web.Application()
    app.router.add_route("OPTIONS", "/latest", options_handler)
    app.router.add_route("OPTIONS", "/health", options_handler)
    app.router.add_route("OPTIONS", "/ack", options_handler)

    app.router.add_get("/latest", latest_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_post("/ack", ack_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 8788)
    await site.start()
    log("HTTP bridge up: http://127.0.0.1:8788/latest")

# ---------- Telethon ----------
async def run_telethon():
    if API_ID == 0 or not API_HASH:
        raise SystemExit("Missing TG_API_ID / TG_API_HASH in .env")
    if not ALLOWED_CHAT_IDS:
        raise SystemExit("Missing ALLOWED_CHAT_IDS in .env")

    state = load_state()
    log(f"Telethon starting. Whitelist chatIds: {sorted(ALLOWED_CHAT_IDS)}")
    log("🔐 Al primo avvio: telefono + codice Telegram (login).")

    client = TelegramClient("unred_session", API_ID, API_HASH)

    @client.on(events.NewMessage)
    async def on_new_message(event):
        chat_id = event.chat_id
        if chat_id not in ALLOWED_CHAT_IDS:
            return

        msg_id = event.message.id
        key_state = str(chat_id)
        last_id = int(state.get(key_state, 0))
        if msg_id <= last_id:
            return

        raw = event.raw_text or ""
        normalized = normalize_telegram_signal(raw)

        # aggiorno lo state anche su messaggi non validi, così non rileggo
        state[key_state] = msg_id
        save_state(state)

        if not normalized or not looks_like_signal(normalized):
            return

        payload = {
            "ts": int(time.time() * 1000),
            "chat_id": chat_id,
            "message_id": msg_id,
            "key": f"{chat_id}:{msg_id}",
            "text": normalized,
        }
        enqueue_signal(payload)

        LATEST["ok"] = True
        LATEST["ts"] = payload["ts"]
        LATEST["chat_id"] = payload["chat_id"]
        LATEST["message_id"] = payload["message_id"]
        LATEST["key"] = payload["key"]
        LATEST["text"] = payload["text"]

        log(f"NEW SIGNAL chat={chat_id} msg={msg_id} pending={len(PENDING_QUEUE)}\n{normalized}\n---")

    async with client:
        await client.run_until_disconnected()

async def main():
    await run_http()
    await run_telethon()

if __name__ == "__main__":
    asyncio.run(main())
