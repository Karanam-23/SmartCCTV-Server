const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');
 
const app = express();
app.use(cors());
app.use(express.json());
 
const server = http.createServer(app);
 
// ===============================
// FIREBASE ADMIN INIT
// ===============================
 
// serviceAccountKey.json must be in same directory as server.js on Render
// Add it as a Render Secret File at path: /etc/secrets/serviceAccountKey.json
// OR set env var FIREBASE_SERVICE_ACCOUNT with the JSON string
 
let firebaseInitialized = false;
 
try {
  let serviceAccount;
 
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    // Preferred: env var contains the full JSON string
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    // Fallback: file on disk (Render Secret File)
    serviceAccount = require('/etc/secrets/serviceAccountKey.json');
  }
 
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
 
  firebaseInitialized = true;
  console.log('[FCM] Firebase Admin SDK initialized');
} catch (err) {
  console.error('[FCM] Firebase Admin init failed:', err.message);
  console.warn('[FCM] Push notifications will be disabled');
}
 
// ===============================
// FCM v1 SEND FUNCTION
// ===============================
 
async function sendMotionNotification(fcmToken, roomCode, labels, confidence) {
  if (!firebaseInitialized) {
    console.warn('[FCM] Skipped — Firebase not initialized');
    return;
  }
  if (!fcmToken) {
    console.warn('[FCM] Skipped — no viewer FCM token');
    return;
  }
 
  const labelText = Array.isArray(labels)
    ? labels.join(', ')
    : (labels || 'Motion');
 
  const confidencePct = confidence
    ? ` (${Math.round(confidence * 100)}%)`
    : '';
 
  const message = {
    token: fcmToken,
    notification: {
      title: '⚠️ Motion Detected',
      body: `${labelText}${confidencePct} detected at your Aveksha camera`
    },
    data: {
      type: 'motion_alert',
      roomCode: roomCode,
      labels: Array.isArray(labels) ? labels.join(',') : (labels || ''),
      confidence: String(confidence || 0),
      timestamp: String(Date.now())
    },
    android: {
      priority: 'high',
      notification: {
        channelId: 'motion_alerts',
        priority: 'max',
        defaultSound: true,
        defaultVibrateTimings: true,
        clickAction: 'FLUTTER_NOTIFICATION_CLICK'
      }
    }
  };
 
  try {
    const response = await admin.messaging().send(message);
    console.log(`[FCM] Notification sent to viewer. MessageId: ${response}`);
  } catch (err) {
    console.error('[FCM] Send failed:', err.message);
    // If token invalid, log it — camera should refresh token
    if (err.code === 'messaging/registration-token-not-registered') {
      console.warn('[FCM] Viewer token expired/invalid for room:', roomCode);
    }
  }
}
 
// ===============================
// SOCKET.IO
// ===============================
 
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket'],
  maxHttpBufferSize: 5e6,
  pingTimeout: 60000,
  pingInterval: 25000
});
 
// ===============================
// ROOM STORE
// ===============================
 
const rooms = {};
 
// ===============================
// HEALTH CHECK
// ===============================
 
app.get('/', (req, res) => {
  res.json({
    status: 'AVEKSHA Server Running',
    fcmEnabled: firebaseInitialized,
    activeRooms: Object.keys(rooms).length,
    rooms: Object.entries(rooms).map(([code, r]) => ({
      code,
      hasCamera: !!r.cameraSocketId,
      hasViewer: !!r.viewerSocketId,
      hasFcmToken: !!r.viewerFcmToken,
      cameraId: r.cameraSocketId,
      viewerId: r.viewerSocketId
    })),
    timestamp: new Date().toISOString()
  });
});
 
io.on('connection', (socket) => {
  console.log(`[CONNECT] ${socket.id}`);
 
  // ===============================
  // JOIN ROOM
  // ===============================
 
  socket.on('join-room', ({ roomCode, role }) => {
    if (!roomCode || !role) {
      console.log('[ERROR] Invalid join-room payload');
      return;
    }
 
    const code = roomCode.toUpperCase().trim();
 
    if (!rooms[code]) {
      rooms[code] = {
        cameraSocketId: null,
        viewerSocketId: null,
        viewerFcmToken: null,
        createdAt: Date.now()
      };
    }
 
    if (socket.currentRoom && socket.currentRoom !== code) {
      socket.leave(socket.currentRoom);
      const oldRoom = rooms[socket.currentRoom];
      if (oldRoom) {
        if (socket.currentRole === 'camera') {
          oldRoom.cameraSocketId = null;
        } else if (socket.currentRole === 'viewer') {
          oldRoom.viewerSocketId = null;
          oldRoom.viewerFcmToken = null;
        }
      }
    }
 
    socket.join(code);
    socket.currentRoom = code;
    socket.currentRole = role;
 
    console.log(`[JOIN] ${role} joined room: ${code} | socket: ${socket.id}`);
 
    if (role === 'camera') {
      if (rooms[code].cameraSocketId && rooms[code].cameraSocketId !== socket.id) {
        console.log(`[JOIN] Evicting old camera from ${code}`);
        io.to(rooms[code].cameraSocketId).emit('session-ended', { reason: 'replaced' });
      }
 
      rooms[code].cameraSocketId = socket.id;
 
      if (rooms[code].viewerSocketId) {
        console.log(`[WEBRTC] Viewer waiting in ${code} — sending create-offer to camera`);
        socket.emit('create-offer', { roomCode: code });
      }
 
      socket.to(code).emit('camera-connected', { roomCode: code });
    }
 
    if (role === 'viewer') {
      if (rooms[code].viewerSocketId && rooms[code].viewerSocketId !== socket.id) {
        console.log(`[JOIN] Evicting old viewer from ${code}`);
        io.to(rooms[code].viewerSocketId).emit('session-ended', { reason: 'replaced' });
        rooms[code].viewerFcmToken = null;
      }
 
      rooms[code].viewerSocketId = socket.id;
 
      if (rooms[code].cameraSocketId) {
        console.log(`[WEBRTC] Camera in ${code} — sending create-offer to camera`);
        io.to(rooms[code].cameraSocketId).emit('create-offer', { roomCode: code });
        socket.emit('camera-connected', { roomCode: code });
      } else {
        console.log(`[WEBRTC] No camera in ${code} — viewer waiting`);
        socket.emit('waiting-for-camera', { roomCode: code });
      }
    }
 
    console.log(`[ROOMS]`, Object.keys(rooms).map(k => ({
      code: k,
      cam: !!rooms[k].cameraSocketId,
      viewer: !!rooms[k].viewerSocketId
    })));
  });
 
  // ===============================
  // WEBRTC SIGNALING
  // ===============================
 
  socket.on('offer', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    console.log(`[OFFER] From camera in room: ${code}, SDP length: ${data.sdp?.length}`);
    const room = rooms[code];
    if (!room?.viewerSocketId) {
      console.log(`[OFFER] No viewer in room ${code}`);
      return;
    }
    io.to(room.viewerSocketId).emit('offer', {
      roomCode: code,
      sdp: data.sdp,
      type: data.type
    });
    console.log(`[OFFER] Forwarded to viewer: ${room.viewerSocketId}`);
  });
 
  socket.on('answer', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    console.log(`[ANSWER] From viewer in room: ${code}`);
    const room = rooms[code];
    if (!room?.cameraSocketId) {
      console.log(`[ANSWER] No camera in room ${code}`);
      return;
    }
    io.to(room.cameraSocketId).emit('answer', {
      roomCode: code,
      sdp: data.sdp,
      type: data.type
    });
    console.log(`[ANSWER] Forwarded to camera: ${room.cameraSocketId}`);
  });
 
  socket.on('ice-candidate', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    const room = rooms[code];
    if (!room) return;
    const targetId = socket.currentRole === 'camera'
      ? room.viewerSocketId
      : room.cameraSocketId;
    if (targetId) {
      io.to(targetId).emit('ice-candidate', {
        roomCode: code,
        sdpMid: data.sdpMid,
        sdpMLineIndex: data.sdpMLineIndex,
        candidate: data.candidate
      });
    }
  });
 
  // ===============================
  // FRAME RELAY (MJPEG fallback)
  // ===============================
 
  socket.on('frame', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code || !data.frameData) return;
    const room = rooms[code];
    if (!room?.viewerSocketId) return;
    io.to(room.viewerSocketId).volatile.emit('frame', {
      frameData: data.frameData,
      timestamp: Date.now()
    });
  });
 
  // ===============================
  // MOTION ALERTS + FCM v1
  // ===============================
 
  socket.on('motion-detected', async (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
 
    console.log(`[MOTION] Room ${code}: ${data.labels}`);
 
    // Real-time in-app alert to viewer socket (if connected)
    socket.to(code).emit('motion-alert', {
      roomCode: code,
      labels: data.labels || 'Person',
      confidence: data.confidence || 0,
      snapshot: data.snapshot || null,
      timestamp: new Date().toISOString()
    });
 
    // FCM push notification via Admin SDK — works even if viewer app is closed
    const room = rooms[code];
    if (room?.viewerFcmToken) {
      await sendMotionNotification(
        room.viewerFcmToken,
        code,
        data.labels,
        data.confidence
      );
    } else {
      console.warn(`[MOTION] No viewer FCM token for room ${code} — push skipped`);
    }
  });
 
  // ===============================
  // FCM TOKEN (viewer registers)
  // ===============================
 
  socket.on('save-fcm-token', ({ roomCode, token }) => {
    if (!roomCode || !token) return;
    const code = roomCode.toUpperCase().trim();
    if (rooms[code]) {
      rooms[code].viewerFcmToken = token;
      console.log(`[FCM] Token saved for room: ${code} | token: ${token.substring(0, 20)}...`);
    } else {
      console.warn(`[FCM] Room ${code} not found — token not saved`);
    }
  });
 
  // ===============================
  // PUSH-TO-TALK AUDIO
  // ===============================
 
  socket.on('audio-chunk', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    const room = rooms[code];
    if (!room?.cameraSocketId) return;
    io.to(room.cameraSocketId).volatile.emit('play-audio', {
      audioData: data.audioData,
      timestamp: Date.now()
    });
  });
 
  // ===============================
  // FLASH CONTROL
  // ===============================
 
  socket.on('flash-control', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    const room = rooms[code];
    if (!room?.cameraSocketId) return;
    io.to(room.cameraSocketId).emit('set-flash', { enabled: data.enabled });
    console.log(`[FLASH] Room ${code}: ${data.enabled ? 'ON' : 'OFF'}`);
  });
 
  // ===============================
  // FLIP CAMERA
  // ===============================
 
  socket.on('flip-camera', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;
    const room = rooms[code];
    if (!room?.cameraSocketId) return;
    io.to(room.cameraSocketId).emit('flip-camera');
    console.log(`[FLIP] Room ${code}: flip-camera sent to camera`);
  });
 
  // ===============================
  // HEARTBEAT
  // ===============================
 
  socket.on('ping-check', () => {
    socket.emit('pong-check', { timestamp: Date.now() });
  });
 
  // ===============================
  // DISCONNECT
  // ===============================
 
  socket.on('disconnect', () => {
    const code = socket.currentRoom;
    const role = socket.currentRole;
 
    console.log(`[DISCONNECT] ${socket.id} (${role} in ${code})`);
 
    if (code && rooms[code]) {
      if (role === 'camera') {
        if (rooms[code].cameraSocketId === socket.id) {
          rooms[code].cameraSocketId = null;
          io.to(code).emit('camera-disconnected');
          console.log(`[DISCONNECT] Camera left ${code}`);
        }
      } else if (role === 'viewer') {
        if (rooms[code].viewerSocketId === socket.id) {
          rooms[code].viewerSocketId = null;
          rooms[code].viewerFcmToken = null;
          io.to(code).emit('viewer-disconnected');
          console.log(`[DISCONNECT] Viewer left ${code}`);
        }
      }
 
      if (!rooms[code].cameraSocketId && !rooms[code].viewerSocketId) {
        delete rooms[code];
        console.log(`[ROOM] Deleted empty room: ${code}`);
      }
    }
 
    console.log(`[ROOMS] Active:`, Object.keys(rooms));
  });
});
 
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('================================');
  console.log('AVEKSHA Signaling Server');
  console.log(`Port: ${PORT}`);
  console.log(`FCM: ${firebaseInitialized ? 'ENABLED' : 'DISABLED'}`);
  console.log(`Health: http://localhost:${PORT}`);
  console.log('================================');
});