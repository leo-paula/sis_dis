# P2P Chat Relay

A real-time chat application built for a Distributed Systems course. Demonstrates core distributed concepts — message passing, room-based multicast, and autonomous peer identity — using a Python relay server and a browser-based client.

## Stack

| Layer | Technology |
|-------|-----------|
| Server | Python, Flask, Flask-SocketIO (eventlet) |
| Transport | WebSocket (Socket.IO) |
| Client | Vanilla JS, single HTML file |

## Quick start

```bash
pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000`, pick a username and channel, and start chatting.

## Distributed systems concepts

- **Message passing** — clients never share memory; all interaction is discrete socket events (`join`, `message`, `leave`).
- **Room-based multicast** — one `message` event fans out to every peer in the room (one-to-many).
- **Stateless relay** — no database; history lives in RAM only, making horizontal scaling straightforward.
- **Fault isolation** — each room owns an independent history buffer; a failure in one room cannot affect others.
- **Autonomous peer identity** — each client generates its own UUID via `crypto.randomUUID()` without consulting the server, mirroring self-managed node identity in systems like BitTorrent or Kademlia.

## Rooms

- General
- P2P Networks
- Distributed Systems

## Project structure

```
.
├── app.py               # Relay server — WebSocket events + in-memory state
├── requirements.txt
└── templates/
    └── index.html       # Single-file SPA (CSS + JS inline, dark terminal UI)
```
