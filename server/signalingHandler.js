// Routes TabTwin WebSocket events between hosts, guests, CRDT peers, and the AI agent bridge.
import { publicGuests, safeSend } from './sessionManager.js';

export function createSignalingHandler({ sessions, redisClient, redisSub, serverId }) {
  const SERVER_CHANNEL = `tabtwin:server:${serverId}`;

  // Listen for messages from other server instances.
  redisSub.subscribe(SERVER_CHANNEL, (err) => {
    if (err) {
      console.error(`[TabTwin] Failed to subscribe to ${SERVER_CHANNEL}:`, err.message);
    }
  });

  redisSub.on('message', (channel, message) => {
    if (channel !== SERVER_CHANNEL) return;

    try {
      const { sessionId, guestId, event, payload } = JSON.parse(message);
      deliverLocally(sessionId, guestId, event, payload);
    } catch (err) {
      console.error('[TabTwin] Error processing Pub/Sub message:', err.message);
    }
  });

  async function deliverLocally(sessionId, guestId, event, payload) {
    const session = await sessions.getSession(sessionId);
    if (!session) return;

    if (guestId) {
      // Send to specific guest or broadcast to all guests if guestId is 'broadcast'
      if (guestId === 'broadcast') {
        broadcastGuests(session, { event, payload });
      } else {
        const target = findGuestSocket(session, guestId);
        if (target) safeSend(target, { event, payload });
      }
    } else {
      // Send to host
      if (session.hostSocket) safeSend(session.hostSocket, { event, payload });
    }
  }

  function handleConnection(socket) {
    socket.tabTwin = { role: 'unknown', sessionId: null, guestId: null };

    socket.on('message', async (raw) => {
      let message;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        safeSend(socket, { event: 'error', payload: { message: 'Invalid JSON message.' } });
        return;
      }

      await routeMessage(socket, message);
    });

    socket.on('close', () => sessions.removeSocket(socket));
  }

  async function routeMessage(socket, { event, payload = {} }) {
    const sessionId = payload.sessionId || socket.tabTwin.sessionId;
    const session = sessionId ? await sessions.getSession(sessionId) : null;

    switch (event) {
      case 'host:connect': {
        const nextSession = await sessions.attachHost(payload.sessionId, socket);
        if (!nextSession) {
          safeSend(socket, { event: 'error', payload: { message: 'Session not found.' } });
          return;
        }

        socket.tabTwin = { role: 'host', sessionId: nextSession.id, guestId: null };
        safeSend(socket, {
          event: 'host:connected',
          payload: { sessionId: nextSession.id, guests: publicGuests(nextSession) }
        });
        return;
      }

      case 'session:join': {
        const joined = await sessions.addGuest(payload.sessionId, socket, { name: payload.name });
        if (!joined) {
          safeSend(socket, { event: 'error', payload: { message: 'Session not found.' } });
          return;
        }

        socket.tabTwin = {
          role: 'guest',
          sessionId: joined.session.id,
          guestId: joined.guest.id
        };

        safeSend(socket, {
          event: 'session:joined',
          payload: {
            sessionId: joined.session.id,
            guest: publicGuest(joined.guest),
            permissions: joined.guest.permissions
          }
        });

        // Notify host (might be remote)
        const hostTarget = joined.session.hostSocket;
        const hostMessage = {
          event: 'session:joined',
          payload: { guest: publicGuest(joined.guest), guests: publicGuests(joined.session) }
        };

        if (hostTarget) {
          safeSend(hostTarget, hostMessage);
        } else if (joined.session.hostServerId) {
          publishToRemote(joined.session.hostServerId, {
            sessionId: joined.session.id,
            event: hostMessage.event,
            payload: hostMessage.payload
          });
        }
        return;
      }

      case 'cursor:move':
      case 'action:request':
      case 'crdt:update':
      case 'webrtc:offer':
      case 'webrtc:answer':
      case 'webrtc:ice-candidate': {
        if (!session) return;
        const isHost = socket.tabTwin.role === 'host';
        const targetGuestId = payload.guestId;

        if (isHost) {
          const guest = session.guests.find(g => g.id === targetGuestId);
          if (!guest) return;

          if (guest.socket) {
            safeSend(guest.socket, { event, payload: withSender(socket, payload) });
          } else if (guest.serverId) {
            publishToRemote(guest.serverId, {
              sessionId: session.id,
              guestId: guest.id,
              event,
              payload: withSender(socket, payload)
            });
          }
        } else {
          if (session.hostSocket) {
            safeSend(session.hostSocket, { event, payload: withSender(socket, payload) });
          } else if (session.hostServerId) {
            publishToRemote(session.hostServerId, {
              sessionId: session.id,
              event,
              payload: withSender(socket, payload)
            });
          }
        }
        return;
      }

      case 'cursor:update':
      case 'action:approved':
      case 'agent:action':
      case 'control:revoke': {
        if (!session) return;
        const msg = { event, payload: withSender(socket, payload) };
        const targetGuestId = payload.guestId;

        if (targetGuestId) {
          const guest = session.guests.find(g => g.id === targetGuestId);
          if (guest?.socket) {
            safeSend(guest.socket, msg);
          } else if (guest?.serverId) {
            publishToRemote(guest.serverId, {
              sessionId: session.id,
              guestId: guest.id,
              event,
              payload: msg.payload
            });
          }
        } else {
          // Broadcast to all guests across all servers
          const servers = new Set();
          for (const guest of session.guests) {
            if (guest.socket) {
              safeSend(guest.socket, msg);
            } else if (guest.serverId) {
              servers.add(guest.serverId);
            }
          }

          for (const remoteServerId of servers) {
            publishToRemote(remoteServerId, {
              sessionId: session.id,
              guestId: 'broadcast',
              event,
              payload: msg.payload
            });
          }
        }
        return;
      }

      case 'agent:command': {
        if (!session) return;
        const msg = {
          event: 'agent:action',
          payload: {
            command: payload.command,
            actions: payload.actions || [],
            summary: payload.summary || 'Agent command received.'
          }
        };

        if (session.hostSocket) {
          safeSend(session.hostSocket, msg);
        } else if (session.hostServerId) {
          publishToRemote(session.hostServerId, {
            sessionId: session.id,
            event: msg.event,
            payload: msg.payload
          });
        }
        return;
      }

      default:
        safeSend(socket, { event: 'error', payload: { message: `Unknown event: ${event}` } });
    }
  }

  function publishToRemote(targetServerId, message) {
    redisClient.publish(`tabtwin:server:${targetServerId}`, JSON.stringify(message));
  }

  return { handleConnection };
}

function publicGuest(guest) {
  return {
    id: guest.id,
    name: guest.name,
    color: guest.color,
    permissions: guest.permissions
  };
}

function withSender(socket, payload) {
  return {
    ...payload,
    senderRole: socket.tabTwin.role,
    guestId: payload.guestId || socket.tabTwin.guestId
  };
}

function findGuestSocket(session, guestId) {
  return session.guests.find((guest) => guest.id === guestId)?.socket || null;
}

function broadcastGuests(session, message, guestId = null) {
  for (const guest of session.guests) {
    if (!guestId || guest.id === guestId) {
      safeSend(guest.socket, message);
    }
  }
}
