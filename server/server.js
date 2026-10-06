import { createServer } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const port = Number(process.env.PORT || 8080);
const httpServer = createServer((request, response) => {
  if (request.url === '/health') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ ok: true, service: 'cifrarchat1-relay' }));
    return;
  }
  response.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  response.end('CifraChat WebSocket relay');
});

const wss = new WebSocketServer({ server: httpServer });
const online = new Map();
const publicKeys = new Map();
const pending = new Map();

function safeSend(ws, payload) {
  if (ws.readyState !== WebSocket.OPEN) return false;
  ws.send(JSON.stringify(payload));
  return true;
}

function queueFor(user, payload) {
  const list = pending.get(user) || [];
  list.push(payload);
  if (list.length > 100) list.shift();
  pending.set(user, list);
}

function deliverPending(user, ws) {
  for (const payload of pending.get(user) || []) safeSend(ws, payload);
  pending.delete(user);
}

wss.on('connection', (ws) => {
  let registeredUser = null;
  ws.on('message', (buffer) => {
    try {
      const data = JSON.parse(buffer.toString('utf8'));
      if (data.type === 'register') {
        const user = String(data.user || '').trim();
        const publicKey = String(data.publicKey || '').trim();
        if (user.length < 3 || !publicKey) {
          safeSend(ws, { type: 'error', message: 'Registro inválido' });
          return;
        }
        registeredUser = user;
        online.set(user, ws);
        publicKeys.set(user, publicKey);
        safeSend(ws, { type: 'registered', user });
        deliverPending(user, ws);
        return;
      }
      if (data.type === 'getKey') {
        const user = String(data.user || '').trim();
        safeSend(ws, { type: 'key', user, publicKey: publicKeys.get(user) || null, requestId: data.requestId || null });
        return;
      }
      if (data.type === 'send') {
        const from = String(data.from || '').trim();
        const to = String(data.to || '').trim();
        if (!registeredUser || from !== registeredUser || !to || !data.box) {
          safeSend(ws, { type: 'error', message: 'Mensaje inválido' });
          return;
        }
        const payload = { type: 'message', from, box: data.box, receivedAt: Date.now() };
        const recipient = online.get(to);
        const delivered = recipient ? safeSend(recipient, payload) : false;
        if (!delivered) queueFor(to, payload);
        safeSend(ws, { type: 'delivery', to, ok: delivered });
        return;
      }
      safeSend(ws, { type: 'error', message: 'Tipo de mensaje desconocido' });
    } catch {
      safeSend(ws, { type: 'error', message: 'JSON inválido' });
    }
  });
  ws.on('close', () => {
    if (registeredUser && online.get(registeredUser) === ws) online.delete(registeredUser);
  });
});

httpServer.listen(port, '0.0.0.0', () => {
  console.log('CifraChat relay escuchando en el puerto ' + port);
});
