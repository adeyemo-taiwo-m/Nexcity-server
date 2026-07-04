# NexCity Realtime Server (Backend Relay)

A standalone Node/Express + Socket.io microservice that listens to PostgreSQL database changes from Supabase via replication, reshapes the payloads, and broadcasts them to connected browsers.

## 🏗️ Architecture & Role
Vercel/Netlify hosting environments for static frontends are serverless and cannot hold open, persistent WebSocket connections. This service exists as a persistent, stateful relay server to manage active Socket.io client tunnels and act as a secure intermediary.

* **Supabase Webhooks / Realtime**: The server uses a privileged client (`service_role` key) to safely subscribe to table row modifications without exposure to client code.
* **CORS Restrictions**: Connections are restricted to a defined whitelist (e.g. localhost and your production Vercel frontend URL).
* **Payload Reshaping**: Converts raw database row formats into clean, client-friendly event messages.

## 📁 Project Structure
* `index.js` — Scaffolds Express server, CORS config, Socket.io attachment, Supabase subscriptions, and graceful shutdown handling.
* `.env` — Contains private environment variables (`SUPABASE_SERVICE_ROLE_KEY`, etc. — gitignored).
* `.env.example` — Reference variables sheet for setups.

## 🚀 Setup & Deployment
1. Set up environment variables in `.env`:
   ```env
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
   PORT=4000
   CLIENT_ORIGIN=https://nex-city-reality-dashboard.vercel.app, http://localhost:5173
   ```
2. Install dependencies: `npm install`
3. Run the development server: `npm run dev`
4. Deploy to Render as a **Web Service** using `npm install` (Build) and `npm start` (Start command), adding the environment variables above to Render's configuration.
