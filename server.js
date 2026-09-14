import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import * as Y from 'yjs';
import * as sync from 'y-protocols/sync';
import * as awarenessServices from 'y-protocols/awareness';
import * as encoding from 'lib0/encoding';
import * as decoding from 'lib0/decoding';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static files from the 'public' folder
app.use(express.static('public'));

// Store active document instances and connected clients per room
const rooms = new Map(); // roomName -> { doc: Y.Doc, awareness: Awareness, clients: Set<WebSocket> }

function getRoom(roomName) {
  let room = rooms.get(roomName);
  if (!room) {
    const doc = new Y.Doc();
    const awareness = new awarenessServices.Awareness(doc);
    room = { doc, awareness, clients: new Set() };
    rooms.set(roomName, room);

    // Broadcast incremental document updates to all connected peers
    doc.on('update', (update, origin) => {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, 0); // 0 = messageSync
      sync.writeUpdate(encoder, update);
      const message = encoding.toUint8Array(encoder);

      for (const client of room.clients) {
        if (client !== origin && client.readyState === 1) { // 1 = OPEN
          client.send(message);
        }
      }
    });

    // Broadcast awareness updates (remote cursors / selection) to peers
    awareness.on('update', ({ added, updated, removed }, origin) => {
      const changedClients = added.concat(updated, removed);
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, 1); // 1 = messageAwareness
      encoding.writeVarUint8Array(
        encoder,
        awarenessServices.encodeAwarenessUpdate(awareness, changedClients)
      );
      const message = encoding.toUint8Array(encoder);

      for (const client of room.clients) {
        if (client !== origin && client.readyState === 1) {
          client.send(message);
        }
      }
    });
  }
  return room;
}

wss.on('connection', (conn, req) => {
  const roomName = req.url.replace(/^\//, '') || 'demo-room';
  const room = getRoom(roomName);
  room.clients.add(conn);

  // 1. Send SyncStep1 to negotiate initial document state with the new client
  {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0); // messageSync
    sync.writeSyncStep1(encoder, room.doc);
    conn.send(encoding.toUint8Array(encoder));
  }

  // 2. Send active presence/awareness state to the new client
  const awarenessStates = room.awareness.getStates();
  if (awarenessStates.size > 0) {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 1); // messageAwareness
    encoding.writeVarUint8Array(
      encoder,
      awarenessServices.encodeAwarenessUpdate(
        room.awareness,
        Array.from(awarenessStates.keys())
      )
    );
    conn.send(encoding.toUint8Array(encoder));
  }

  // 3. Process incoming messages from the client
  conn.on('message', (data) => {
    try {
      const message = new Uint8Array(data);
      const encoder = encoding.createEncoder();
      const decoder = decoding.createDecoder(message);
      const messageType = decoding.readVarUint(decoder);

      if (messageType === 0) { // messageSync
        encoding.writeVarUint(encoder, 0);
        sync.readSyncMessage(decoder, encoder, room.doc, conn);

        // Send generated response (e.g. SyncStep2) back to the sender
        if (encoding.length(encoder) > 1) {
          conn.send(encoding.toUint8Array(encoder));
        }
      } else if (messageType === 1) { // messageAwareness
        awarenessServices.applyAwarenessUpdate(
          room.awareness,
          decoding.readVarUint8Array(decoder),
          conn
        );
      }
    } catch (err) {
      console.error('Error parsing binary frame:', err);
    }
  });

  conn.on('close', () => {
    room.clients.delete(conn);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n==================================================`);
  console.log(`🟢 Real-Time Server Running!`);
  console.log(`Open in browser: http://127.0.0.1:${PORT}`);
  console.log(`==================================================\n`);
});