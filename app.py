import argparse
import asyncio
import json
import mimetypes
import os
import re
import secrets
import time
from dataclasses import dataclass
from typing import Dict, Optional

from aiohttp import WSMsgType, web

MAX_FILE_SIZE = 200 * 1024 * 1024
TRANSFER_TTL_SECONDS = 10 * 60
MAX_NAME_LENGTH = 32


@dataclass
class Peer:
    peer_id: str
    name: str
    device: str
    ws: web.WebSocketResponse


@dataclass
class Transfer:
    transfer_id: str
    sender_id: str
    recipient_id: str
    filename: str
    size: int
    mime: str
    token: str
    created_at: float
    accepted: bool = False
    content: Optional[bytes] = None


PEERS: Dict[str, Peer] = {}
TRANSFERS: Dict[str, Transfer] = {}
LOCK = asyncio.Lock()


def sanitize_name(name: str) -> str:
    cleaned = re.sub(r"[^\w\- .ğüşöçıİĞÜŞÖÇ]", "", (name or "").strip())
    if not cleaned:
        cleaned = "Guest"
    return cleaned[:MAX_NAME_LENGTH]


def sanitize_filename(filename: str) -> str:
    base = os.path.basename(filename or "file")
    base = re.sub(r"[^\w\-. ]", "_", base)
    return base[:128] or "file"


def device_icon(device: str) -> str:
    low = (device or "").lower()
    if "ipad" in low or "iphone" in low or "ios" in low:
        return "🍎"
    if "android" in low:
        return "🤖"
    if "windows" in low:
        return "🪟"
    if "mac" in low:
        return "💻"
    return "📱"


def peer_payload(peer: Peer) -> dict:
    return {
        "id": peer.peer_id,
        "name": peer.name,
        "device": peer.device,
        "icon": device_icon(peer.device),
    }


async def send_json(ws: web.WebSocketResponse, payload: dict) -> None:
    await ws.send_str(json.dumps(payload))


async def broadcast_peers() -> None:
    peers = [peer_payload(peer) for peer in PEERS.values()]
    stale = []
    for peer in PEERS.values():
        try:
            await send_json(peer.ws, {"type": "peers", "peers": peers, "selfId": peer.peer_id})
        except Exception:
            stale.append(peer.peer_id)
    for peer_id in stale:
        PEERS.pop(peer_id, None)


async def cleanup_expired_transfers() -> None:
    now = time.time()
    expired = [
        transfer_id
        for transfer_id, transfer in TRANSFERS.items()
        if now - transfer.created_at > TRANSFER_TTL_SECONDS
    ]
    for transfer_id in expired:
        TRANSFERS.pop(transfer_id, None)


async def index(_: web.Request) -> web.Response:
    return web.Response(text=INDEX_HTML, content_type="text/html")


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    peer_id = secrets.token_urlsafe(6)
    peer = Peer(peer_id=peer_id, name="Guest", device="Unknown", ws=ws)

    async with LOCK:
        PEERS[peer_id] = peer
        await send_json(ws, {"type": "welcome", "id": peer_id})
        await broadcast_peers()

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                await send_json(ws, {"type": "error", "message": "Geçersiz istek"})
                continue
            msg_type = data.get("type")

            if msg_type == "hello":
                async with LOCK:
                    peer.name = sanitize_name(data.get("name", "Guest"))
                    peer.device = sanitize_name(data.get("device", "Unknown"))
                    await broadcast_peers()

            elif msg_type == "rename":
                async with LOCK:
                    peer.name = sanitize_name(data.get("name", peer.name))
                    await broadcast_peers()

            elif msg_type == "offer":
                async with LOCK:
                    await cleanup_expired_transfers()
                    recipient_id = data.get("to")
                    recipient = PEERS.get(recipient_id)
                    if not recipient:
                        await send_json(ws, {"type": "error", "message": "Kullanıcı çevrimdışı"})
                        continue
                    size = int(data.get("size", 0))
                    if size <= 0 or size > MAX_FILE_SIZE:
                        await send_json(ws, {"type": "error", "message": "Dosya boyutu limit dışında"})
                        continue
                    transfer = Transfer(
                        transfer_id=secrets.token_urlsafe(12),
                        sender_id=peer_id,
                        recipient_id=recipient_id,
                        filename=sanitize_filename(data.get("filename", "file")),
                        size=size,
                        mime=data.get("mime") or "application/octet-stream",
                        token=secrets.token_urlsafe(18),
                        created_at=time.time(),
                    )
                    TRANSFERS[transfer.transfer_id] = transfer
                    await send_json(
                        recipient.ws,
                        {
                            "type": "file_offer",
                            "transferId": transfer.transfer_id,
                            "from": peer_payload(peer),
                            "filename": transfer.filename,
                            "size": transfer.size,
                            "mime": transfer.mime,
                        },
                    )
                    await send_json(ws, {"type": "offer_sent", "transferId": transfer.transfer_id})

            elif msg_type == "transfer_response":
                transfer_id = data.get("transferId")
                accept = bool(data.get("accept"))
                async with LOCK:
                    transfer = TRANSFERS.get(transfer_id)
                    if not transfer or transfer.recipient_id != peer_id:
                        continue
                    sender = PEERS.get(transfer.sender_id)
                    if not sender:
                        TRANSFERS.pop(transfer_id, None)
                        continue
                    if not accept:
                        TRANSFERS.pop(transfer_id, None)
                        await send_json(sender.ws, {"type": "transfer_rejected", "transferId": transfer_id})
                        continue
                    transfer.accepted = True
                    await send_json(
                        sender.ws,
                        {
                            "type": "transfer_ready",
                            "transferId": transfer.transfer_id,
                            "uploadUrl": f"/api/upload/{transfer.transfer_id}?token={transfer.token}",
                        },
                    )

            elif msg_type == "cancel_transfer":
                transfer_id = data.get("transferId")
                async with LOCK:
                    transfer = TRANSFERS.pop(transfer_id, None)
                    if not transfer:
                        continue
                    target_id = transfer.recipient_id if transfer.sender_id == peer_id else transfer.sender_id
                    target = PEERS.get(target_id)
                    if target:
                        await send_json(target.ws, {"type": "transfer_cancelled", "transferId": transfer_id})

    finally:
        async with LOCK:
            PEERS.pop(peer_id, None)
            await broadcast_peers()
    return ws


async def upload_handler(request: web.Request) -> web.Response:
    transfer_id = request.match_info["transfer_id"]
    token = request.query.get("token", "")
    async with LOCK:
        transfer = TRANSFERS.get(transfer_id)
        if not transfer or not transfer.accepted or transfer.token != token:
            return web.Response(status=403, text="Unauthorized transfer")

    body = await request.read()
    if len(body) != transfer.size or len(body) > MAX_FILE_SIZE:
        return web.Response(status=400, text="Invalid file size")

    async with LOCK:
        transfer.content = body
        recipient = PEERS.get(transfer.recipient_id)
        if recipient:
            await send_json(
                recipient.ws,
                {
                    "type": "file_available",
                    "transferId": transfer.transfer_id,
                    "filename": transfer.filename,
                    "size": transfer.size,
                    "downloadUrl": f"/api/download/{transfer.transfer_id}?token={transfer.token}",
                },
            )
    return web.Response(text="OK")


async def download_handler(request: web.Request) -> web.Response:
    transfer_id = request.match_info["transfer_id"]
    token = request.query.get("token", "")
    async with LOCK:
        transfer = TRANSFERS.get(transfer_id)
        if not transfer or transfer.token != token or transfer.content is None:
            return web.Response(status=404, text="File not found")
        content = transfer.content
        filename = transfer.filename
        mime = transfer.mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        TRANSFERS.pop(transfer_id, None)

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return web.Response(body=content, content_type=mime, headers=headers)


def make_app() -> web.Application:
    app = web.Application(client_max_size=MAX_FILE_SIZE)
    app.router.add_get("/", index)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_post("/api/upload/{transfer_id}", upload_handler)
    app.router.add_get("/api/download/{transfer_id}", download_handler)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="FileTran - Tabletler arası hızlı dosya paylaşımı")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    web.run_app(make_app(), host=args.host, port=args.port)


INDEX_HTML = """<!doctype html>
<html lang=\"tr\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>FileTran</title>
  <style>
    :root { color-scheme: dark; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(circle at top, #1f2a44, #0b1020 60%);
      color: #ecf0ff;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 16px;
    }
    .app { width: min(980px, 100%); display: grid; gap: 16px; }
    .card {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.15);
      border-radius: 14px;
      padding: 16px;
      backdrop-filter: blur(8px);
    }
    h1 { margin: 0 0 8px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    input, button, select {
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(9, 14, 28, 0.85);
      color: #fff;
      padding: 10px 12px;
      font-size: 14px;
    }
    button {
      cursor: pointer;
      background: linear-gradient(135deg, #4f6cff, #3b4fd9);
      border: none;
      font-weight: 600;
    }
    button.secondary { background: rgba(255,255,255,0.18); }
    ul { list-style: none; padding: 0; margin: 8px 0 0; display: grid; gap: 8px; }
    li { padding: 10px; border-radius: 10px; background: rgba(255,255,255,0.05); }
    .muted { color: #b3bbd9; font-size: 13px; }
    .badge { margin-left: auto; background: rgba(79,108,255,0.25); padding: 4px 8px; border-radius: 999px; font-size: 12px; }
    .peer { display: flex; align-items: center; gap: 10px; }
    a.download {
      display: inline-block;
      margin-top: 8px;
      color: #9fc4ff;
      text-decoration: none;
      font-weight: 600;
    }
  </style>
</head>
<body>
  <main class=\"app\">
    <section class=\"card\">
      <h1>📡 FileTran</h1>
      <p class=\"muted\">Aynı ağdaki cihazlarla güvenli ve hızlı dosya paylaşımı</p>
      <div class=\"row\">
        <input id=\"nameInput\" maxlength=\"32\" placeholder=\"Cihaz adın\" />
        <button id=\"renameBtn\">İsmi Güncelle</button>
        <span id=\"status\" class=\"muted\">Bağlanıyor...</span>
      </div>
    </section>

    <section class=\"card\">
      <div class=\"row\"><strong>Aktif kullanıcılar</strong> <span class=\"badge\" id=\"peerCount\">0</span></div>
      <ul id=\"peerList\"></ul>
    </section>

    <section class=\"card\">
      <strong>Dosya gönder</strong>
      <div class=\"row\" style=\"margin-top: 8px\">
        <select id=\"peerSelect\"><option value=\"\">Alıcı seç</option></select>
        <input id=\"fileInput\" type=\"file\" />
        <button id=\"sendBtn\">Gönder</button>
      </div>
      <p class=\"muted\">Maksimum dosya boyutu: 200MB • Bağlantılar tek kullanımlık token ile korunur.</p>
    </section>

    <section class=\"card\">
      <strong>Gelen istekler</strong>
      <ul id=\"incomingList\"></ul>
    </section>

    <section class=\"card\">
      <strong>İndirilebilir dosyalar</strong>
      <ul id=\"downloadList\"></ul>
    </section>
  </main>

  <script>
    const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${wsProto}://${location.host}/ws`);
    const peers = new Map();
    const incomingOffers = new Map();

    const statusEl = document.getElementById('status');
    const peerListEl = document.getElementById('peerList');
    const peerSelectEl = document.getElementById('peerSelect');
    const peerCountEl = document.getElementById('peerCount');
    const incomingListEl = document.getElementById('incomingList');
    const downloadListEl = document.getElementById('downloadList');
    const nameInputEl = document.getElementById('nameInput');

    const guessedDevice = (() => {
      const ua = navigator.userAgent.toLowerCase();
      if (ua.includes('ipad') || ua.includes('iphone')) return 'iPadOS/iOS';
      if (ua.includes('android')) return 'Android';
      if (ua.includes('windows')) return 'Windows';
      if (ua.includes('mac os')) return 'macOS';
      return 'Tablet/Web';
    })();

    const storedName = localStorage.getItem('filetran-name') || '';
    if (storedName) nameInputEl.value = storedName;

    function formatSize(size) {
      if (size > 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
      if (size > 1024) return `${(size / 1024).toFixed(1)} KB`;
      return `${size} B`;
    }

    function updatePeers(list, selfId) {
      peers.clear();
      peerListEl.innerHTML = '';
      peerSelectEl.innerHTML = '<option value="">Alıcı seç</option>';
      list.forEach(peer => {
        peers.set(peer.id, peer);
        if (peer.id === selfId) return;

        const li = document.createElement('li');
        li.innerHTML = `<div class="peer"><span>${peer.icon}</span><strong>${peer.name}</strong><span class="muted">${peer.device}</span></div>`;
        peerListEl.appendChild(li);

        const option = document.createElement('option');
        option.value = peer.id;
        option.textContent = `${peer.icon} ${peer.name} (${peer.device})`;
        peerSelectEl.appendChild(option);
      });
      peerCountEl.textContent = String(Math.max(0, list.length - 1));
    }

    function renderIncoming() {
      incomingListEl.innerHTML = '';
      if (!incomingOffers.size) {
        const li = document.createElement('li');
        li.textContent = 'Bekleyen istek yok';
        li.className = 'muted';
        incomingListEl.appendChild(li);
        return;
      }
      incomingOffers.forEach((offer, transferId) => {
        const li = document.createElement('li');
        li.innerHTML = `<strong>${offer.from.icon} ${offer.from.name}</strong> → ${offer.filename} (${formatSize(offer.size)})`;
        const row = document.createElement('div');
        row.className = 'row';
        row.style.marginTop = '8px';

        const accept = document.createElement('button');
        accept.textContent = 'Kabul Et';
        accept.onclick = () => {
          ws.send(JSON.stringify({ type: 'transfer_response', transferId, accept: true }));
          incomingOffers.delete(transferId);
          renderIncoming();
        };

        const reject = document.createElement('button');
        reject.className = 'secondary';
        reject.textContent = 'Reddet';
        reject.onclick = () => {
          ws.send(JSON.stringify({ type: 'transfer_response', transferId, accept: false }));
          incomingOffers.delete(transferId);
          renderIncoming();
        };

        row.appendChild(accept);
        row.appendChild(reject);
        li.appendChild(row);
        incomingListEl.appendChild(li);
      });
    }

    function addDownload(item) {
      const li = document.createElement('li');
      li.innerHTML = `<strong>${item.filename}</strong> (${formatSize(item.size)})`;
      const a = document.createElement('a');
      a.className = 'download';
      a.href = item.downloadUrl;
      a.textContent = 'İndir';
      a.download = item.filename;
      li.appendChild(a);
      downloadListEl.prepend(li);
    }

    ws.onopen = () => {
      statusEl.textContent = 'Bağlandı';
    };

    ws.onclose = () => {
      statusEl.textContent = 'Bağlantı koptu';
    };

    ws.onmessage = async (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'welcome') {
        const customName = (nameInputEl.value || guessedDevice).trim().slice(0, 32);
        localStorage.setItem('filetran-name', customName);
        ws.send(JSON.stringify({ type: 'hello', name: customName, device: guessedDevice }));
      } else if (msg.type === 'peers') {
        updatePeers(msg.peers, msg.selfId);
      } else if (msg.type === 'file_offer') {
        incomingOffers.set(msg.transferId, msg);
        renderIncoming();
      } else if (msg.type === 'transfer_ready') {
        const file = document.getElementById('fileInput').files[0];
        if (!file) return;
        const body = await file.arrayBuffer();
        const result = await fetch(msg.uploadUrl, {
          method: 'POST',
          body,
          headers: { 'Content-Type': file.type || 'application/octet-stream' }
        });
        statusEl.textContent = result.ok ? 'Dosya gönderildi' : 'Yükleme hatası';
      } else if (msg.type === 'transfer_rejected') {
        statusEl.textContent = 'Alıcı isteği reddetti';
      } else if (msg.type === 'file_available') {
        addDownload(msg);
        statusEl.textContent = 'Yeni dosya hazır';
      } else if (msg.type === 'error') {
        statusEl.textContent = msg.message;
      }
    };

    document.getElementById('renameBtn').onclick = () => {
      const value = (nameInputEl.value || guessedDevice).trim().slice(0, 32);
      nameInputEl.value = value;
      localStorage.setItem('filetran-name', value);
      ws.send(JSON.stringify({ type: 'rename', name: value }));
    };

    document.getElementById('sendBtn').onclick = () => {
      const to = peerSelectEl.value;
      const file = document.getElementById('fileInput').files[0];
      if (!to || !file) {
        statusEl.textContent = 'Alıcı ve dosya seçmelisiniz';
        return;
      }
      ws.send(JSON.stringify({
        type: 'offer',
        to,
        filename: file.name,
        size: file.size,
        mime: file.type || 'application/octet-stream'
      }));
      statusEl.textContent = 'İstek gönderildi, karşı taraf bekleniyor';
    };

    renderIncoming();
  </script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
