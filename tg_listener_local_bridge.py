import os
import re
import json
import time
import asyncio
import atexit
import tempfile
from typing import Dict, Any

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors.common import TypeNotFoundError
import aiohttp

try:
    import msvcrt
except Exception:  # pragma: no cover - non-Windows
    msvcrt = None

load_dotenv()

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = (os.getenv("TG_API_HASH") or "").strip()
ALLOWED_CHAT_IDS = {
    int(x.strip())
    for x in (os.getenv("ALLOWED_CHAT_IDS", "") or "").split(",")
    if x.strip()
}
CHAT_MASTER_MAP = {
    -1003349817033: "master_2",
    -1001467736193: "master_3",
}
CHAT_ROOM_MAP = {
    -1003349817033: "room2",
    -1001467736193: "room3",
}

APP_PIN = (os.getenv("SIGNALCONVERTER_PIN") or os.getenv("APP_PIN") or "").strip() or "5487"
SIGNALCONVERTER_LOGIN_URL = os.getenv(
    "SIGNALCONVERTER_LOGIN_URL",
    "https://www.unred.it/signalconverter/api/login",
)
SIGNALCONVERTER_URL = os.getenv(
    "SIGNALCONVERTER_URL",
    "https://www.unred.it/signalconverter/api/convert-send",
)
TOKEN_TTL = int(os.getenv("SIGNALCONVERTER_TOKEN_TTL", "540"))  # seconds

STATE_FILE = "unred_state.json"
SESSION_NAME = "unred_local_bridge"

LOCK_FILE = os.path.join(tempfile.gettempdir(), "unred_local_bridge.lock")
_LOCK_HANDLE = None
_converter_token: str = ""
_token_ts: float = 0.0


def acquire_single_instance() -> None:
    global _LOCK_HANDLE
    if os.name != "nt" or msvcrt is None:
        return
    try:
        _LOCK_HANDLE = open(LOCK_FILE, "a+")
        _LOCK_HANDLE.seek(0)
        msvcrt.locking(_LOCK_HANDLE.fileno(), msvcrt.LK_NBLCK, 1)
    except Exception:
        log("Another tg_listener_local_bridge instance is already running. Exit.")
        raise SystemExit(0)

    def _release_lock() -> None:
        global _LOCK_HANDLE
        try:
            if _LOCK_HANDLE:
                _LOCK_HANDLE.seek(0)
                msvcrt.locking(_LOCK_HANDLE.fileno(), msvcrt.LK_UNLCK, 1)
                _LOCK_HANDLE.close()
        except Exception:
            pass
        finally:
            _LOCK_HANDLE = None

    atexit.register(_release_lock)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_state() -> Dict[str, int]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}


def save_state(state: Dict[str, int]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


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
        if re.fullmatch(r"\d+", last) or re.fullmatch(r"\d{1,2}:\d{2}", last):
            lines.pop()
            while lines and lines[-1].strip() == "":
                lines.pop()
            continue
        break

    return "\n".join(lines).strip()


def normalize_for_converter(text: str) -> str:
    t = text
    t = re.sub(r"(?im)^\s*sell\s+limit\s*$", "Sell", t)
    t = re.sub(r"(?im)^\s*buy\s+limit\s*$", "Buy", t)
    t = re.sub(r"(?im)^\s*sell!+\s*$", "Sell", t)
    t = re.sub(r"(?im)^\s*buy!+\s*$", "Buy", t)

    # Accept either `Entry: 1.2345` or `Entry 1.2345`, with optional trailing notes.
    t = re.sub(r"(?im)^\s*(entry|entry\s*price|e)\b\s*[:=]?\s*([0-9][0-9\.,]*)\b.*$", r"E: \2", t)
    t = re.sub(r"(?im)^\s*(tp|take\s*profit)\b\s*[:=]?\s*([0-9][0-9\.,]*)\b.*$", r"TP: \2", t)
    t = re.sub(r"(?im)^\s*(sl|stop\s*loss|stop|si)\b\s*[:=]?\s*([0-9][0-9\.,]*)\b.*$", r"SL: \2", t)

    # Build a canonical payload for the converter when all key fields are present,
    # even if the original message has mixed formatting (e.g. "EUR/CHF – sell limit").
    direction_match = re.search(r"(?i)\b(buy|sell)(?:\s+limit)?\b", t)
    entry_match = re.search(r"(?im)^\s*e\s*:\s*([0-9][0-9\.,]*)\s*$", t)
    tp_match = re.search(r"(?im)^\s*tp\s*:\s*([0-9][0-9\.,]*)\s*$", t)
    sl_match = re.search(r"(?im)^\s*sl\s*:\s*([0-9][0-9\.,]*)\s*$", t)
    pair_match = re.search(r"(?i)\b([A-Z]{3})\s*/\s*([A-Z]{3})\b", t)

    if direction_match and entry_match and tp_match and sl_match:
        direction = direction_match.group(1).capitalize()
        entry = entry_match.group(1)
        tp = tp_match.group(1)
        sl = sl_match.group(1)

        lines = []
        if pair_match:
            lines.append(f"{pair_match.group(1).upper()}{pair_match.group(2).upper()}")
        lines.extend([
            direction,
            f"E: {entry}",
            f"TP: {tp}",
            f"SL: {sl}",
        ])
        t = "\n".join(lines)

    # Fallback: signals that use "@" to indicate the entry price
    if re.search(r"(?im)^\s*e\s*:", t) is None:
        m = re.search(r"(?i)@\s*([0-9][0-9\.,]*)", t)
        if m:
            t = f"E: {m.group(1)}\n" + t

    return t.strip()


def looks_like_signal(text: str) -> bool:
    t = (text or "").lower()
    compact = re.sub(r"\s+", " ", t).strip()

    has_direction = re.search(r"\b(buy|sell)(?:\s+limit)?\b", compact) is not None
    has_entry = (
        re.search(r"\b(entry|entry\s*price|e)\b(?:\s*[:=]\s*|\s+)[0-9]", compact) is not None
        or re.search(r"@\s*[0-9]", compact) is not None
    )
    has_tp = re.search(r"\b(tp|take\s*profit)\b(?:\s*[:=]\s*|\s+)[0-9]", compact) is not None
    has_sl = re.search(r"\b(sl|stop\s*loss|stop|si)\b(?:\s*[:=]\s*|\s+)[0-9]", compact) is not None

    return has_direction and has_entry and has_tp and has_sl


async def ensure_converter_token(session: aiohttp.ClientSession, force: bool = False) -> str:
    global _converter_token, _token_ts
    if not force and _converter_token and (time.time() - _token_ts) < TOKEN_TTL:
        return _converter_token
    if not APP_PIN:
        log("[WARN] APP_PIN non impostato: impossibile autenticarsi")
        return ""

    try:
        async with session.post(
            SIGNALCONVERTER_LOGIN_URL,
            json={"pin": APP_PIN},
            timeout=15,
        ) as resp:
            if resp.status != 200:
                log(f"[WARN] login converter HTTP {resp.status}")
                return ""
            data = await resp.json()
    except Exception as exc:
        log(f"[WARN] login converter errore: {exc}")
        return ""

    token = data.get("token")
    if not token:
        log(f"[WARN] login converter risposta senza token: {data}")
        return ""

    _converter_token = token
    _token_ts = time.time()
    return token


async def send_to_converter(session: aiohttp.ClientSession, payload: Dict[str, Any]) -> bool:
    token = await ensure_converter_token(session)
    if not token:
        return False

    async def _post(current_token: str) -> aiohttp.ClientResponse:
        return await session.post(
            SIGNALCONVERTER_URL,
            json={
                "token": current_token,
                "text": payload.get("text"),
                "room": payload.get("room_hint"),
            },
            timeout=30,
        )

    try:
        resp = await _post(token)
        if resp.status == 200:
            data = await resp.json()
            if data.get("ok"):
                return True
            log(f"[WARN] converter risposta non ok: {data}")
        elif resp.status in (401, 403):
            log("[INFO] token scaduto, rinnovo")
            token = await ensure_converter_token(session, force=True)
            if not token:
                return False
            resp = await _post(token)
            if resp.status == 200:
                data = await resp.json()
                if data.get("ok"):
                    return True
                log(f"[WARN] converter risposta non ok (retry): {data}")
        else:
            text = await resp.text()
            log(f"[WARN] converter HTTP {resp.status}: {text}")
    except Exception as exc:
        log(f"[WARN] converter errore: {exc}")
    return False


async def run_telethon_forever() -> None:
    if API_ID == 0 or not API_HASH:
        raise SystemExit("Missing TG_API_ID / TG_API_HASH in .env")
    if not ALLOWED_CHAT_IDS:
        raise SystemExit("Missing ALLOWED_CHAT_IDS in .env")

    state = load_state()
    log(f"Telethon starting. Whitelist chatIds: {sorted(ALLOWED_CHAT_IDS)}")
    log("Login Telegram: se richiesto inserisci codice sul telefono.")
    log(f"Session file: {SESSION_NAME}.session")

    backoff = 5
    while True:
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        session_http = aiohttp.ClientSession()
        try:
            await client.start()
            me = await client.get_me()
            log(f"Telegram login OK: @{(me.username or '').strip() or me.id}")

            @client.on(events.NewMessage)
            async def on_new_message(event):
                try:
                    chat_id = event.chat_id
                    if chat_id not in ALLOWED_CHAT_IDS:
                        return

                    master_hint = CHAT_MASTER_MAP.get(int(chat_id))
                    if not master_hint:
                        return
                    room_hint = CHAT_ROOM_MAP.get(int(chat_id))
                    if not room_hint:
                        room_hint = re.sub(r"^master_", "room", master_hint)

                    msg_id = event.message.id
                    key_state = str(chat_id)
                    last_id = int(state.get(key_state, 0))
                    if msg_id <= last_id:
                        return

                    raw = event.raw_text or ""
                    normalized = normalize_telegram_signal(raw)
                    if not normalized or not looks_like_signal(normalized):
                        return

                    normalized = normalize_for_converter(normalized)
                    normalized = re.sub(r"(?im)^\s*Master\s*:\s*master_\d+\s*$\n?", "", normalized).strip()
                    normalized = f"Master : {master_hint}\n" + normalized

                    payload = {
                        "chat_id": chat_id,
                        "message_id": msg_id,
                        "master_hint": master_hint,
                        "room_hint": room_hint,
                        "text": normalized,
                    }

                    if await send_to_converter(session_http, payload):
                        state[key_state] = msg_id
                        save_state(state)
                        log(f"SENT chat={chat_id} msg={msg_id}\n{normalized}\n---")
                    else:
                        log(f"[WARN] invio fallito chat={chat_id} msg={msg_id}")

                except Exception as exc:
                    log(f"handler error: {exc}")

            async with client:
                await client.run_until_disconnected()

            backoff = 5
        except TypeNotFoundError as exc:
            log(f"TypeNotFoundError: {exc}")
            backoff = min(60, backoff * 2)
            await asyncio.sleep(backoff)
        except Exception as exc:
            log(f"Telethon fatal: {exc}")
            backoff = min(60, backoff * 2)
            await asyncio.sleep(backoff)
        finally:
            await session_http.close()
            try:
                await client.disconnect()
            except Exception:
                pass


def main() -> None:
    asyncio.run(run_telethon_forever())


if __name__ == "__main__":
    acquire_single_instance()
    main()
