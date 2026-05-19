const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

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
    activeRooms: Object.keys(rooms).length,
    rooms: Object.entries(rooms).map(([code, r]) => ({
      code,
      hasCamera: !!r.cameraSocketId,
      hasViewer: !!r.viewerSocketId,
      cameraId: r.cameraSocketId,
      viewerId: r.viewerSocketId
    })),
    timestamp: new Date().toISOString()
  });
});

// ===============================
// SOCKET.IO
// ===============================

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

    // Leave any previous room
    if (socket.currentRoom && socket.currentRoom !== code) {
      socket.leave(socket.currentRoom);
    }

    socket.join(code);
    socket.currentRoom = code;
    socket.currentRole = role;

    console.log(`[JOIN] ${role} joined room: ${code} (socket: ${socket.id})`);
    console.log(`[ROOM] ${code} state: camera=${rooms[code].cameraSocketId} viewer=${rooms[code].viewerSocketId}`);

    if (role === 'camera') {
      rooms[code].cameraSocketId = socket.id;

      // If viewer already waiting — tell camera to create offer
      if (rooms[code].viewerSocketId) {
        console.log(`[WEBRTC] Viewer already in room ${code} — telling camera to create offer`);
        socket.emit('create-offer', { roomCode: code });
      }

      // Tell viewer camera is ready
      socket.to(code).emit('camera-connected', { roomCode: code });
    }

    if (role === 'viewer') {
      rooms[code].viewerSocketId = socket.id;

      if (rooms[code].cameraSocketId) {
        console.log(`[WEBRTC] Camera already in room ${code} — telling camera to create offer`);
        // Tell CAMERA to create offer now that viewer is ready
        io.to(rooms[code].cameraSocketId).emit('create-offer', { roomCode: code });
        // Tell viewer camera is available
        socket.emit('camera-connected', { roomCode: code });
      } else {
        console.log(`[WEBRTC] Camera not in room ${code} yet — viewer waiting`);
        socket.emit('waiting-for-camera', { roomCode: code });
      }
    }

    console.log(`[ROOMS] Active:`, Object.keys(rooms));
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
      console.log(`[OFFER] No viewer in room ${code} to send offer to`);
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

    const role = socket.currentRole;
    const targetId = role === 'camera'
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
  // Only used if WebRTC fails
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
  // MOTION ALERTS
  // ===============================

  socket.on('motion-detected', (data) => {
    const code = data.roomCode?.toUpperCase().trim();
    if (!code) return;

    console.log(`[MOTION] Room ${code}: ${data.labels}`);

    // Real-time socket alert
    socket.to(code).emit('motion-alert', {
      roomCode: code,
      labels: data.labels || 'Person',
      confidence: data.confidence || 0,
      snapshot: data.snapshot || null,
      timestamp: new Date().toISOString()
    });

    // Return viewer FCM token to camera for push notification
    const room = rooms[code];
    if (room?.viewerFcmToken) {
      socket.emit('viewer-fcm-token', {
        token: room.viewerFcmToken
      });
    }
  });

  // ===============================
  // FCM TOKEN
  // ===============================

  socket.on('save-fcm-token', ({ roomCode, token }) => {
    if (!roomCode || !token) return;
    const code = roomCode.toUpperCase().trim();
    if (rooms[code]) {
      rooms[code].viewerFcmToken = token;
      console.log(`[FCM] Token saved for room: ${code}`);
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
    // Relay audio from viewer to camera speaker
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
    io.to(room.cameraSocketId).emit('set-flash', {
      enabled: data.enabled
    });
    console.log(`[FLASH] Room ${code}: ${data.enabled ? 'ON' : 'OFF'}`);
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
        rooms[code].cameraSocketId = null;
        socket.to(code).emit('camera-disconnected');
      } else if (role === 'viewer') {
        rooms[code].viewerSocketId = null;
        rooms[code].viewerFcmToken = null;
        socket.to(code).emit('viewer-disconnected');
      }

      if (!rooms[code].cameraSocketId && !rooms[code].viewerSocketId) {
        delete rooms[code];
        console.log(`[ROOM] Deleted empty room: ${code}`);
      }
    }

    console.log('[ROOMS] Active:', Object.keys(rooms));
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('================================');
  console.log('AVEKSHA Signaling Server');
  console.log(`Port: ${PORT}`);
  console.log(`Health: http://localhost:${PORT}`);
  console.log('================================');
});