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
