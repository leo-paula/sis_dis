"""
P2P Chat Relay — Distributed Systems Demo
==========================================

Distributed Systems Concepts Demonstrated
------------------------------------------
1. Stateless relay
   The server holds no disk-backed state. All message history lives in RAM
   only, making horizontal scaling straightforward: spin up more relay nodes
   behind a load balancer and partition rooms across them.

2. Message passing (no shared memory)
   Clients never touch each other's memory directly. Every interaction is
   encoded as a discrete event (join / message / leave / switch_room) that
   travels over the network — a textbook message-passing system.

3. Room-based multicast
   A single 'message' emission fans out to every member of a room
   (one-to-many). This mirrors multicast group communication in overlay
   networks like Scribe or Bayou.

4. Fault isolation per room
   Each room owns an independent history buffer and membership set. A bug
   or overload in one room cannot corrupt the state of another.

5. Autonomous peer identity
   Each browser client generates its own UUID on startup without consulting
   the server, demonstrating self-managed node identity — a core property of
   fully decentralized P2P systems (BitTorrent, Kademlia, etc.).

Run
---
    pip install -r requirements.txt
    python app.py
"""

import eventlet
eventlet.monkey_patch()

import logging
from datetime import datetime

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.config["SECRET_KEY"] = "p2p-distributed-systems-demo"

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
    logger=False,
    engineio_logger=False,
)

# ── In-memory state (intentionally ephemeral — no persistence) ────────────────

ROOMS = ["General", "P2P Networks", "Distributed Systems"]
MAX_HISTORY = 50

# {room: [msg, ...]}  — bounded ring per room
message_history: dict[str, list] = {r: [] for r in ROOMS}

# {sid: {"username": str, "room": str}}
connected_users: dict[str, dict] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def room_usernames(room: str) -> list[str]:
    return [u["username"] for u in connected_users.values() if u["room"] == room]


def broadcast_user_list(room: str) -> None:
    emit("user_list", {"room": room, "users": room_usernames(room)}, to=room)


def store(room: str, msg: dict) -> None:
    buf = message_history[room]
    buf.append(msg)
    if len(buf) > MAX_HISTORY:
        buf.pop(0)


def sys_msg(text: str) -> dict:
    return {"type": "system", "text": text, "timestamp": now()}


def chat_msg(username: str, text: str) -> dict:
    return {"type": "chat", "username": username, "text": text, "timestamp": now()}


# ── HTTP routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── WebSocket events ──────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    log.info(f"[CONNECT]  sid={request.sid}")


@socketio.on("disconnect")
def on_disconnect():
    user = connected_users.pop(request.sid, None)
    if not user:
        return
    username, room = user["username"], user["room"]
    msg = sys_msg(f"{username} disconnected")
    store(room, msg)
    emit("message", msg, to=room)
    broadcast_user_list(room)
    log.info(f"[DISCONNECT]  {username} ← {room}")


@socketio.on("join")
def on_join(data: dict):
    username = str(data.get("username", "anon")).strip()[:32] or "anon"
    room = data.get("room", "General")
    if room not in ROOMS:
        room = "General"

    connected_users[request.sid] = {"username": username, "room": room}
    join_room(room)

    # Send existing history before announcing arrival so the join message
    # appears after the backlog on the client side.
    emit("history", message_history[room])

    msg = sys_msg(f"{username} joined the channel")
    store(room, msg)
    emit("message", msg, to=room)
    broadcast_user_list(room)
    log.info(f"[JOIN]  {username} → {room}")


@socketio.on("leave")
def on_leave(data: dict):
    user = connected_users.pop(request.sid, None)
    if not user:
        return
    username, room = user["username"], user["room"]
    leave_room(room)
    msg = sys_msg(f"{username} left the channel")
    store(room, msg)
    emit("message", msg, to=room)
    broadcast_user_list(room)
    log.info(f"[LEAVE]  {username} ← {room}")


@socketio.on("switch_room")
def on_switch_room(data: dict):
    user = connected_users.get(request.sid)
    if not user:
        return
    old_room = data.get("old_room", "")
    new_room = data.get("new_room", "General")
    if new_room not in ROOMS:
        return
    username = user["username"]

    # Depart old room
    if old_room and old_room in ROOMS and old_room != new_room:
        leave_room(old_room)
        msg = sys_msg(f"{username} left the channel")
        store(old_room, msg)
        emit("message", msg, to=old_room)
        broadcast_user_list(old_room)

    # Enter new room
    connected_users[request.sid]["room"] = new_room
    join_room(new_room)
    emit("history", message_history[new_room])
    msg = sys_msg(f"{username} joined the channel")
    store(new_room, msg)
    emit("message", msg, to=new_room)
    broadcast_user_list(new_room)
    log.info(f"[SWITCH]  {username}: {old_room} → {new_room}")


@socketio.on("message")
def on_message(data: dict):
    user = connected_users.get(request.sid)
    if not user:
        return
    text = str(data.get("text", "")).strip()[:1000]
    if not text:
        return
    username, room = user["username"], user["room"]
    msg = chat_msg(username, text)
    store(room, msg)
    emit("message", msg, to=room)
    log.info(f"[MSG]  {username}@{room}: {text[:72]}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("P2P Chat relay starting on http://0.0.0.0:5000")
    log.info(f"Rooms: {', '.join(ROOMS)}")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
