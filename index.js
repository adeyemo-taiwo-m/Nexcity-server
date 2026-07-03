require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// Configure CORS
app.use(cors({ origin: process.env.CLIENT_ORIGIN || 'http://localhost:5173' }));

// Express needs a raw HTTP server to attach Socket.io to
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.CLIENT_ORIGIN || 'http://localhost:5173',
    methods: ['GET', 'POST']
  }
});

// Initialize Supabase Client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceRoleKey) {
  console.warn('WARNING: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables are missing!');
}

const supabase = createClient(supabaseUrl || '', supabaseServiceRoleKey || '');

// Keep track of connected clients
let onlineCount = 0;

io.on('connection', (socket) => {
  onlineCount++;
  console.log(`Client connected. Total online: ${onlineCount}`);

  socket.on('disconnect', () => {
    onlineCount--;
    console.log(`Client disconnected. Total online: ${onlineCount}`);
  });
});

// Subscribe to Supabase Postgres changes and relay to socket clients
supabase
  .channel('db-changes')
  .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'properties' }, (payload) => {
    console.log('Received property INSERT from Supabase:', payload.new.title);
    io.emit('notification:new', {
      id: crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substring(2, 9),
      type: 'property',
      message: `New property listed: ${payload.new.title}`,
      data: payload.new,
      timestamp: new Date().toISOString(),
    });
  })
  .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'transactionDetails' }, (payload) => {
    console.log('Received transaction INSERT from Supabase:', payload.new.amount);
    io.emit('notification:new', {
      id: crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substring(2, 9),
      type: 'transaction',
      message: `New transaction recorded: ₦${Number(payload.new.amount).toLocaleString()}`,
      data: payload.new,
      timestamp: new Date().toISOString(),
    });
    io.emit('stat:update', { key: 'transactions', delta: 1 });
  })
  .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'customersDetails' }, (payload) => {
    console.log('Received customer INSERT from Supabase:', payload.new.name);
    io.emit('notification:new', {
      id: crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substring(2, 9),
      type: 'customer',
      message: `New customer lead: ${payload.new.name}`,
      data: payload.new,
      timestamp: new Date().toISOString(),
    });
  })
  .subscribe((status) => {
    console.log(`Supabase replication subscription status: ${status}`);
  });

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    onlineClients: onlineCount
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Realtime server running on port ${PORT}`);
});
