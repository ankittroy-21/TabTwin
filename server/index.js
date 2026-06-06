// Starts the TabTwin Express API and WebSocket signaling server.
import express from 'express';
import cors from 'cors';
import http from 'node:http';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';
import { createSessionManager } from './sessionManager.js';
import { createSignalingHandler } from './signalingHandler.js';

const PORT = Number(process.env.PORT || 3001);
const CLIENT_URL = process.env.CLIENT_URL || 'http://localhost:5173';
const REDIS_URL = process.env.REDIS_URL;

if (!REDIS_URL) {
  console.error(
    '[TabTwin] REDIS_URL is not set.\n' +
    'Start Redis locally and add REDIS_URL=redis://localhost:6379 to your .env file.\n' +
    'Quick start: docker run -p 6379:6379 redis:7-alpine'
  );
  process.exit(1);
}

const redisClient = new Redis(REDIS_URL, {
  // Retry up to 3 times with a 500 ms delay before giving up on startup.
  maxRetriesPerRequest: 3,
  lazyConnect: false
});

redisClient.on('error', (err) => {
  console.error('[TabTwin] Redis connection error:', err.message);
});

const app = express();
const server = http.createServer(app);
const sessions = createSessionManager({ clientUrl: CLIENT_URL, redisClient });

app.use(cors({ origin: true }));
app.use(express.json({ limit: '1mb' }));

app.get('/api/health', async (_req, res) => {
  res.json({ ok: true, service: 'tabtwin-server', sessions: await sessions.count() });
});

app.post('/api/session/create', async (req, res) => {
  const hostName = req.body?.hostName || 'Host';
  const session = await sessions.createSession({ hostName });
  res.status(201).json({
    session_id: session.id,
    link: session.link,
    permissions: session.permissions
  });
});

app.get('/api/session/:id', async (req, res) => {
  const session = await sessions.getSession(req.params.id);
  if (!session) {
    res.status(404).json({ exists: false, message: 'Session not found or expired.' });
    return;
  }

  res.json({
    exists: true,
    session_id: session.id,
    guests: session.guests.map((guest) => ({
      id: guest.id,
      name: guest.name,
      color: guest.color,
      permissions: guest.permissions
    })),
    createdAt: session.createdAt
  });
});

app.delete('/api/session/:id', async (req, res) => {
  const ended = await sessions.endSession(req.params.id);
  res.status(ended ? 200 : 404).json({ ended });
});

const wss = new WebSocketServer({ server });
const signaling = createSignalingHandler({ sessions });
wss.on('connection', signaling.handleConnection);

server.listen(PORT, () => {
  console.log(`TabTwin server listening on http://localhost:${PORT}`);
});
