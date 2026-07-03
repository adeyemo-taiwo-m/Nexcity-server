# NexCity Realtime Server — Full Implementation

> **Instructions for the coding agent:** Build this exactly as specified, in order. This is a small,
> standalone Node/Express + Socket.io service, separate from the NexCity React app. After it's built and
> tested, explain fully what was built: what each file does, how the pieces connect, and why each design
> decision was made — before considering this phase done.

---

## 0. What This Service Is, In One Paragraph

Supabase's database can notify listeners when rows change (`postgres_changes`). This server subscribes to
those changes using a privileged Supabase client, reshapes them into small, frontend-friendly events, and
re-broadcasts them over Socket.io to any connected browser. It exists as its own deployable service — not
inside the React app — because Socket.io needs a long-lived server process to hold open connections, which
a static frontend hosting platform (Vercel/Netlify) cannot do.

```
Supabase (Postgres + Realtime)
        │  row inserted (properties / transactionDetails / customersDetails)
        ▼
THIS SERVER (Express + Socket.io + Supabase client)
        │  io.emit('notification:new', { type, message, data, timestamp })
        │  io.emit('stat:update', { key, delta })
        ▼
Browser (Socket.io client) → Redux store → UI
```

---

## 1. Project Setup

```bash
mkdir nexcity-realtime-server
cd nexcity-realtime-server
npm init -y
npm install express socket.io @supabase/supabase-js dotenv cors
```

Why each dependency:
- **express** — minimal HTTP server; mainly exists to give Socket.io something to attach to, plus a
  `/health` endpoint.
- **socket.io** — the real-time bidirectional connection to the browser.
- **@supabase/supabase-js** — subscribes to Postgres changes via Supabase Realtime.
- **dotenv** — loads environment variables from `.env` in local development.
- **cors** — restricts which frontend origin(s) may connect to this server.

No other dependencies are needed. Do not add a process manager, logging library, or ORM for this — it's
intentionally a thin relay.

### 1.1 `package.json` — add a start script

```json
{
  "name": "nexcity-realtime-server",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js",
    "dev": "node --watch index.js"
  }
}
```

`npm run dev` uses Node's built-in `--watch` flag to restart on file changes — no `nodemon` dependency
needed for that.

### 1.2 `.gitignore`

```
node_modules/
.env
```

### 1.3 `.env` (local only — never commit)

```
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
PORT=4000
CLIENT_ORIGIN=http://localhost:5173
```

> **Why the service role key, not the anon key:** this process runs entirely server-side and is never
> shipped to a browser, so it's safe to hold a privileged key that bypasses Row Level Security. The anon key
> would be subject to RLS and is meant for client-side use. Mixing these up — putting the service role key
> in frontend code — is one of the most common real Supabase security mistakes, and explicitly avoiding it
> is worth a sentence in the main project's README security notes.

### 1.4 `.env.example` (commit this one, so reviewers know what to set)

```
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
PORT=4000
CLIENT_ORIGIN=
```

---

## 2. `index.js` — Full Implementation

```js
require('dotenv').config();

const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { createClient } = require('@supabase/supabase-js');

// ---------------------------------------------------------------------------
// 1. Basic server setup
// ---------------------------------------------------------------------------

const app = express();
app.use(cors({ origin: process.env.CLIENT_ORIGIN }));

// Socket.io needs a raw http.Server to attach to — Express's own app.listen()
// wraps this internally, but we need the raw server object ourselves so both
// Express routes and Socket.io can share the same underlying server/port.
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: process.env.CLIENT_ORIGIN,
    methods: ['GET', 'POST'],
  },
});

// ---------------------------------------------------------------------------
// 2. Supabase client (privileged — server-side only, see .env notes above)
// ---------------------------------------------------------------------------

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ---------------------------------------------------------------------------
// 3. Track connected clients (simple in-memory counter — fine for a single
//    server instance; would need a shared store like Redis if this were ever
//    scaled to multiple server instances)
// ---------------------------------------------------------------------------

let onlineCount = 0;

io.on('connection', (socket) => {
  onlineCount++;
  console.log(`[socket] client connected (${socket.id}). Total online: ${onlineCount}`);

  socket.on('disconnect', (reason) => {
    onlineCount--;
    console.log(`[socket] client disconnected (${socket.id}, reason: ${reason}). Total online: ${onlineCount}`);
  });
});

// ---------------------------------------------------------------------------
// 4. Core relay logic
//    Subscribe to Postgres changes via Supabase Realtime, reshape each change
//    into a small frontend-friendly event, and broadcast it over Socket.io.
//    We deliberately reshape rather than forwarding the raw payload, so the
//    frontend never depends on exact database column names.
// ---------------------------------------------------------------------------

const channel = supabase.channel('db-changes');

channel
  .on(
    'postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'properties' },
    (payload) => {
      io.emit('notification:new', {
        type: 'property',
        message: `New property listed: ${payload.new.title}`,
        data: payload.new,
        timestamp: new Date().toISOString(),
      });
      io.emit('stat:update', { key: 'properties', delta: 1 });
    }
  )
  .on(
    'postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'transactionDetails' },
    (payload) => {
      io.emit('notification:new', {
        type: 'transaction',
        message: `New transaction recorded: ₦${payload.new.amount}`,
        data: payload.new,
        timestamp: new Date().toISOString(),
      });
      io.emit('stat:update', { key: 'transactions', delta: 1 });
    }
  )
  .on(
    'postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'customersDetails' },
    (payload) => {
      io.emit('notification:new', {
        type: 'customer',
        message: `New customer lead: ${payload.new.name}`,
        data: payload.new,
        timestamp: new Date().toISOString(),
      });
      io.emit('stat:update', { key: 'customers', delta: 1 });
    }
  )
  .subscribe((status) => {
    console.log(`[supabase] realtime channel status: ${status}`);
  });

// ---------------------------------------------------------------------------
// 5. Health check — lets a reviewer or uptime monitor confirm the service
//    and its Supabase subscription are alive, without needing a socket client.
// ---------------------------------------------------------------------------

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    onlineClients: onlineCount,
    supabaseChannelState: channel.state, // 'joined' once subscribed successfully
  });
});

// ---------------------------------------------------------------------------
// 6. Start server
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Realtime server running on port ${PORT}`);
});

// ---------------------------------------------------------------------------
// 7. Graceful shutdown — closes the Supabase channel and socket connections
//    cleanly instead of just killing the process, so reconnecting clients
//    get a clean disconnect event rather than a hung connection.
// ---------------------------------------------------------------------------

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

function shutdown() {
  console.log('Shutting down gracefully...');
  supabase.removeChannel(channel);
  io.close(() => {
    server.close(() => {
      process.exit(0);
    });
  });
}
```

---

## 3. Local Testing

1. Start the server:
   ```bash
   npm run dev
   ```
   You should see:
   ```
   Realtime server running on port 4000
   [supabase] realtime channel status: SUBSCRIBED
   ```

2. Confirm the health check works:
   ```bash
   curl http://localhost:4000/health
   ```
   Expected: `{"status":"ok","onlineClients":0,"supabaseChannelState":"joined"}`

3. In the Supabase dashboard's Table Editor, manually insert a row into `properties`. Watch the terminal —
   nothing prints from this server by default on the emit itself, so also connect a quick test client:

   ```bash
   node -e "
   const { io } = require('socket.io-client');
   const socket = io('http://localhost:4000');
   socket.on('connect', () => console.log('connected:', socket.id));
   socket.on('notification:new', (data) => console.log('received:', data));
   "
   ```

   Insert a row again with this test client running — you should see the `notification:new` payload printed
   immediately.

4. Confirm `onlineClients` increments/decrements correctly by checking `/health` before and after
   connecting/disconnecting the test client.

**Do not move to deployment until step 3 has actually produced a printed event.** This is the single most
important thing to be able to explain and demonstrate live.

---

## 4. Deployment (Render, free tier)

1. Push this folder to its own GitHub repository (separate from the NexCity frontend repo).
2. In Render: **New → Web Service** → connect the repo.
3. Build command: `npm install`. Start command: `npm start`.
4. Add environment variables in Render's dashboard (same keys as `.env`):
   `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `PORT` (Render sets this automatically, but keep the
   fallback in code), `CLIENT_ORIGIN` — set this to your deployed frontend's actual URL, not localhost.
5. Deploy. Confirm the deployed `/health` endpoint responds.
6. Update the frontend's `VITE_SOCKET_SERVER_URL` to the deployed Render URL.

> **Free-tier note worth mentioning in your README:** Render's free web services spin down after
> inactivity and take a few seconds to wake on the next request — the first socket connection after a period
> of idleness may take a moment to establish. This is a real, honest limitation to name rather than hide.

---

## 5. Security Notes (for the main project README)

- Service role key lives only in this server's environment variables — never in frontend code, never
  committed to git (`.env` is gitignored, `.env.example` has no real values).
- CORS is restricted to a single known frontend origin (`CLIENT_ORIGIN`), not `*`.
- This server only ever reads from Supabase and reshapes/broadcasts — it never writes back to the database,
  which limits the blast radius if the relay logic itself had a bug.
- Not yet implemented, worth naming as a future improvement: authenticating socket connections (e.g.
  verifying a Supabase JWT on `connection`) so only logged-in users receive notifications, rather than
  anyone with the server URL.

---

## 6. Explain Fully

Before considering this phase complete, explain:
1. What each of the 7 numbered sections in `index.js` does and why it's separated that way.
2. Why the raw `http.createServer` is needed instead of just `app.listen()`.
3. Why the payload is reshaped before emitting, rather than forwarding Supabase's raw payload.
4. What the graceful shutdown handler protects against, concretely.
5. Confirm the local test in Section 3 actually printed a received event, and describe what you saw.
