const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 5e6 // 5MB max frame size
});

// Track rooms
// rooms[roomCode] = { cameraSocketId, viewerSocketId }
const rooms = {};

// Health check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'SmartCCTV Server Running',
    activeRooms: Object.keys(rooms).length,
    timestamp: new Date().toISOString()
  });
});

app.get('/rooms', (req, res) => {
  const roomSummary = Object.entries(rooms).map(([code, data]) => ({
    roomCode: code,
    hasCamera: !!data.cameraSocketId,
    hasViewer: !!data.viewerSocketId
  }));
  res.json(roomSummary);
});

io.on('connection', (socket) => {
  console.log(`Device connected: ${socket.id}`);

  // Camera device joins a room
  socket.on('join-room', ({ roomCode, role }) => {
    if (!roomCode || !role) return;

    const code = roomCode.toUpperCase().trim();

    if (!rooms[code]) {
      rooms[code] = { cameraSocketId: null, viewerSocketId: null };
    }

    socket.join(code);
    socket.currentRoom = code;
    socket.currentRole = role;

    if (role === 'camera') {
      rooms[code].cameraSocketId = socket.id;
      console.log(`Camera joined room: ${code}`);

      // Notify viewer if already waiting
      socket.to(code).emit('camera-connected', { roomCode: code });

      // Notify camera if viewer already in room
      if (rooms[code].viewerSocketId) {
        socket.emit('viewer-connected', { roomCode: code });
      }
    }

    if (role === 'viewer') {
      rooms[code].viewerSocketId = socket.id;
      console.log(`Viewer joined room: ${code}`);

      // Notify camera that viewer connected
      socket.to(code).emit('viewer-connected', { roomCode: code });

      // Notify viewer if camera already streaming
      if (rooms[code].cameraSocketId) {
        socket.emit('camera-connected', { roomCode: code });
      }
    }
  });

  // Camera sends a frame — relay to viewer instantly
  socket.on('frame', (data) => {
    const roomCode = data.roomCode?.toUpperCase().trim()
    const frameData = data.frame || data.frameData

    console.log(`Frame received for room: ${roomCode}, size: ${frameData?.length}`)

    if (!roomCode || !frameData) {
        console.log('Frame missing roomCode or frameData')
        return
    }

    // Relay to viewer
    socket.to(roomCode).emit('frame', { frameData: frameData })
    console.log(`Frame relayed to room: ${roomCode}`)
  });

  // Motion detected — notify viewer
  socket.on('motion-detected', ({ roomCode }) => {
    if (!roomCode) return;
    const code = roomCode.toUpperCase().trim();
    socket.to(code).emit('motion-alert', {
      roomCode: code,
      timestamp: new Date().toISOString()
    });
    console.log(`Motion detected in room: ${code}`);
  });

  // Handle disconnection
  socket.on('disconnect', () => {
    const code = socket.currentRoom;
    const role = socket.currentRole;

    if (code && rooms[code]) {
      if (role === 'camera') {
        rooms[code].cameraSocketId = null;
        // Notify viewer that camera disconnected
        socket.to(code).emit('camera-disconnected');
        console.log(`Camera left room: ${code}`);
      }

      if (role === 'viewer') {
        rooms[code].viewerSocketId = null;
        // Notify camera that viewer left
        socket.to(code).emit('viewer-disconnected');
        console.log(`Viewer left room: ${code}`);
      }

      // Clean up empty rooms
      if (!rooms[code].cameraSocketId && 
          !rooms[code].viewerSocketId) {
        delete rooms[code];
        console.log(`Room deleted: ${code}`);
      }
    }

    console.log(`Device disconnected: ${socket.id}`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`SmartCCTV Server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}`);
});