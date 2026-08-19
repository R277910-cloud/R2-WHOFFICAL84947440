const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { OAuth2Client } = require('google-auth-library');
const { Pool } = require('pg');

// Production-oriented defaults. Tune these with the host size.
const MAX_CONNECTIONS = Number(process.env.MAX_CONNECTIONS || 60000);
const MAX_ROOMS = Number(process.env.MAX_ROOMS || 30000);
const MAX_MSG_BYTES = 64 * 1024;
const HEARTBEAT_MS = 25000;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;

const PORT = Number(process.env.PORT || 8080);
const ROOT = path.resolve(__dirname, '..');
const rooms = new Map();
const tournaments = new Map();
const tournamentCodes = new Map();
const users = new Map();
let totalConnections = 0;
let totalMessages = 0;

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const googleClient = GOOGLE_CLIENT_ID ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;
const pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined, max: 20 }) : null;
const sessions = new Map();
async function initDatabase(){
  if(!pool) return;
  await pool.query(`CREATE TABLE IF NOT EXISTS r2_users (google_sub TEXT PRIMARY KEY,email TEXT,name TEXT,picture TEXT,state JSONB,created_at TIMESTAMPTZ DEFAULT NOW(),updated_at TIMESTAMPTZ DEFAULT NOW())`);
}
function cookieToken(req){const h=req.headers.cookie||'';const m=h.match(/r2_session=([^;]+)/);return m?decodeURIComponent(m[1]):null;}
function sessionUser(req){const t=cookieToken(req);return t?sessions.get(t):null;}
function setSession(res,user){const token=makeToken()+makeToken();sessions.set(token,user);res.setHeader('Set-Cookie',`r2_session=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=2592000`);}
function clearSession(res,req){const t=cookieToken(req);if(t)sessions.delete(t);res.setHeader('Set-Cookie','r2_session=; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=0');}
async function readBody(req,limit=2*1024*1024){return await new Promise((resolve,reject)=>{let body='';req.on('data',c=>{body+=c;if(body.length>limit){req.destroy();reject(new Error('payload too large'));}});req.on('end',()=>resolve(body));req.on('error',reject);});}


function json(res, code, body) {
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}
function makeId() { return crypto.randomBytes(8).toString('hex'); }
function makeRoomCode() {
  let c;
  do c = String(Math.floor(100000 + Math.random() * 900000)); while (rooms.has(c));
  return c;
}
function roomInfo(r) {
  return { code: r.code, game: r.game, players: [...r.players.values()].map(u => ({ id: u.id, name: u.name, host: u.host })), createdAt: r.createdAt, lastActivity: r.lastActivity };
}
function broadcast(room, msg, except) {
  const data = JSON.stringify(msg);
  for (const u of room.players.values()) {
    if (u.ws !== except && u.ws.readyState === 1) u.ws.send(data);
  }
}
function sendRoomState(room) { broadcast(room, { type: 'roomState', room: roomInfo(room) }); }
function createRoom(ownerName) {
  const code = makeRoomCode();
  const room = { code, players: new Map(), createdAt: Date.now(), lastActivity: Date.now(), game: null, seq: 0 };
  rooms.set(code, room);
  return room;
}


function makeToken(){return crypto.randomBytes(18).toString('hex');}
function makeInviteCode(){let c;do c=crypto.randomBytes(4).toString('hex').toUpperCase();while(tournamentCodes.has(c));return c;}
const GAME_LABELS={auction:'🔨 مزاد برو ماكس',five:'⚽ الملعب الخماسي',deal:'💼 DEAL OR NO DEAL'};
const FORMAT_LABELS={single:'مباراة واحدة','home-away':'ذهاب وإياب'};
function tournamentView(t){
  return {id:t.id,name:t.name,size:t.size,game:t.game,gameLabel:GAME_LABELS[t.game]||t.game,format:t.format,formatLabel:FORMAT_LABELS[t.format]||t.format,status:t.status,round:t.round,players:[...t.players.values()].map(p=>({id:p.id,name:p.name,number:p.number,opponentId:p.opponentId,eliminated:p.eliminated,advanced:p.advanced})),championName:t.championName||null,createdAt:t.createdAt};
}
function randomShuffle(a){return a.slice().sort(()=>Math.random()-0.5);}
function prepareRound(t){
  const active=[...t.players.values()].filter(p=>!p.eliminated);
  active.forEach(p=>{p.opponentId=null;p.advanced=false;p.byePending=false;});
  if(active.length===1){t.championId=active[0].id;t.championName=active[0].name;t.status='finished';return;}
  const shuffled=randomShuffle(active);
  while(shuffled.length>=2){const a=shuffled.shift(),b=shuffled.shift();a.opponentId=b.id;b.opponentId=a.id;}
  if(shuffled.length===1){const bye=shuffled[0];bye.advanced=true;bye.byePending=false;}
  t.round++;t.status='active';
}
function finishMatch(t,winnerId){
  const winner=t.players.get(winnerId); if(!winner||winner.eliminated)return {ok:false,error:'المتسابق غير صالح.'};
  const opp=winner.opponentId?t.players.get(winner.opponentId):null;
  winner.advanced=true;winner.opponentId=null;
  if(opp){opp.eliminated=true;opp.opponentId=null;opp.advanced=false;}
  const active=[...t.players.values()].filter(p=>!p.eliminated);
  const unresolved=active.some(p=>p.opponentId && !p.advanced);
  if(!unresolved){
    if(active.length===1){t.championId=active[0].id;t.championName=active[0].name;t.status='finished';}
    else {active.forEach(p=>{p.opponentId=null;p.advanced=false;});t.status='draw-ready';}
  }
  return {ok:true};
}
function createTournament(data){
  const size=Number(data.size); if(![6,8,16,32,64,128].includes(size)) throw new Error('عدد البطولة يجب أن يكون 6 أو 8 أو 16 أو 32 أو 64 أو 128.');
  const game=String(data.game||'auction'); if(!GAME_LABELS[game]) throw new Error('اللعبة غير صالحة.');
  const format=String(data.format||'single'); if(!FORMAT_LABELS[format]) throw new Error('نظام البطولة غير صالح.');
  const id=makeToken(); const adminToken=makeToken(); const players=new Map();
  const adminId=makeToken(); players.set(adminId,{id:adminId,name:'المنظم',number:1,admin:true,token:adminToken,opponentId:null,eliminated:false,advanced:false,byePending:false});
  const codes=[]; for(let i=1;i<size;i++){const code=makeInviteCode();codes.push(code);tournamentCodes.set(code,{tournamentId:id,used:false});}
  const t={id,adminToken,name:String(data.name||'R2 Tournament').slice(0,50),size,game,format,players,codes,round:0,status:'waiting',createdAt:Date.now(),championId:null,championName:null}; tournaments.set(id,t); return {t,codes};
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    return json(res, 200, {
      ok: true,
      service: 'R2 GAMES Multiplayer Server',
      rooms: rooms.size,
      online: [...users.values()].filter(u => u.ws?.readyState === 1).length,
      connectionsAccepted: totalConnections,
      messages: totalMessages
    });
  }
  if (url.pathname === '/api/config' && req.method === 'GET') return json(res,200,{ok:true,googleClientId:GOOGLE_CLIENT_ID||null,databaseReady:!!pool});
  if (url.pathname === '/api/me' && req.method === 'GET') { const u=sessionUser(req); return u?json(res,200,{ok:true,user:{sub:u.sub,email:u.email,name:u.name,picture:u.picture}}):json(res,401,{ok:false}); }
  if (url.pathname === '/api/auth/logout' && req.method === 'POST') { clearSession(res,req); return json(res,200,{ok:true}); }
  if (url.pathname === '/api/auth/google' && req.method === 'POST') {
    try{
      if(!googleClient) return json(res,503,{ok:false,error:'Google Login غير مُهيأ: أضف GOOGLE_CLIENT_ID في Railway.'});
      const d=JSON.parse(await readBody(req,1024*1024)); const credential=String(d.credential||''); if(!credential)return json(res,400,{ok:false,error:'Google credential مفقود.'});
      const ticket=await googleClient.verifyIdToken({idToken:credential,audience:GOOGLE_CLIENT_ID}); const p=ticket.getPayload(); if(!p?.sub)return json(res,401,{ok:false,error:'تعذر التحقق من حساب Google.'});
      const user={sub:p.sub,email:p.email||'',name:p.name||'R2 Player',picture:p.picture||''};
      if(pool) await pool.query(`INSERT INTO r2_users(google_sub,email,name,picture) VALUES($1,$2,$3,$4) ON CONFLICT(google_sub) DO UPDATE SET email=EXCLUDED.email,name=EXCLUDED.name,picture=EXCLUDED.picture,updated_at=NOW()`,[user.sub,user.email,user.name,user.picture]);
      setSession(res,user); return json(res,200,{ok:true,user});
    }catch(e){return json(res,401,{ok:false,error:'فشل التحقق من Google.'});}
  }
  if (url.pathname === '/api/progress' && req.method === 'GET') {
    const u=sessionUser(req); if(!u)return json(res,401,{ok:false,error:'سجّل الدخول أولًا.'});
    if(!pool)return json(res,200,{ok:true,state:null,databaseReady:false});
    const q=await pool.query('SELECT state FROM r2_users WHERE google_sub=$1',[u.sub]); return json(res,200,{ok:true,state:q.rows[0]?.state||null,databaseReady:true});
  }
  if (url.pathname === '/api/progress' && req.method === 'PUT') {
    const u=sessionUser(req); if(!u)return json(res,401,{ok:false,error:'سجّل الدخول أولًا.'});
    if(!pool)return json(res,503,{ok:false,error:'قاعدة البيانات غير مضافة إلى Railway بعد.'});
    const d=JSON.parse(await readBody(req,2*1024*1024)); const state=d.state; if(!state||typeof state!=='object')return json(res,400,{ok:false,error:'حالة تقدم غير صالحة.'});
    await pool.query('UPDATE r2_users SET state=$1,updated_at=NOW() WHERE google_sub=$2',[state,u.sub]); return json(res,200,{ok:true,saved:true});
  }
  if (url.pathname === '/api/room/create' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 4096) req.destroy(); });
    req.on('end', () => {
      let data = {}; try { data = JSON.parse(body || '{}'); } catch {}
      if (rooms.size >= MAX_ROOMS) return json(res, 503, { ok: false, error: 'الخادم ممتلئ مؤقتًا، حاول لاحقًا.' });
      const room = createRoom(String(data.name || 'Player').slice(0, 24));
      json(res, 200, { ok: true, code: room.code });
    });
    return;
  }
  if (url.pathname.startsWith('/api/room/') && req.method === 'GET') {
    const code = url.pathname.split('/').pop();
    const r = rooms.get(code);
    return r ? json(res, 200, { ok: true, room: roomInfo(r), game: r.game }) : json(res, 404, { ok: false, error: 'Room not found' });
  }

  if (url.pathname === '/api/tournament/create' && req.method === 'POST') {
    let body=''; req.on('data',c=>{body+=c;if(body.length>8192)req.destroy();}); req.on('end',()=>{try{const d=JSON.parse(body||'{}');const out=createTournament(d);json(res,200,{ok:true,tournament:tournamentView(out.t),codes:out.codes,adminToken:out.t.adminToken});}catch(e){json(res,400,{ok:false,error:e.message||'تعذر إنشاء البطولة.'});}}); return;
  }
  if (url.pathname === '/api/tournament/join' && req.method === 'POST') {
    let body=''; req.on('data',c=>{body+=c;if(body.length>4096)req.destroy();}); req.on('end',()=>{try{const d=JSON.parse(body||'{}');const code=String(d.code||'').trim().toUpperCase();const ref=tournamentCodes.get(code);if(!ref)return json(res,404,{ok:false,error:'الكود غير موجود.'});if(ref.used)return json(res,409,{ok:false,error:'هذا الكود مستخدم بالفعل.'});const t=tournaments.get(ref.tournamentId);if(!t)return json(res,404,{ok:false,error:'البطولة غير موجودة.'});if(t.players.size>=t.size)return json(res,409,{ok:false,error:'البطولة اكتمل عددها.'});ref.used=true;const id=makeToken(),token=makeToken();const player={id,name:String(d.name||'Player').slice(0,24),number:t.players.size+1,admin:false,token,opponentId:null,eliminated:false,advanced:false,byePending:false};t.players.set(id,player);if(t.players.size===t.size)t.status='draw-ready';json(res,200,{ok:true,tournament:tournamentView(t),player:{id,name:player.name,number:player.number},playerToken:token});}catch(e){json(res,400,{ok:false,error:'بيانات الانضمام غير صالحة.'});}}); return;
  }
  if (url.pathname.startsWith('/api/tournament/') && req.method === 'GET') {
    const id=url.pathname.split('/').pop(); const t=tournaments.get(id); return t?json(res,200,{ok:true,tournament:tournamentView(t)}):json(res,404,{ok:false,error:'البطولة غير موجودة.'});
  }
  if (url.pathname === '/api/tournament/draw' && req.method === 'POST') {
    let body='';req.on('data',c=>body+=c);req.on('end',()=>{try{const d=JSON.parse(body||'{}');const t=tournaments.get(d.tournamentId);if(!t)return json(res,404,{ok:false,error:'البطولة غير موجودة.'});if(d.adminToken!==t.adminToken)return json(res,403,{ok:false,error:'القرعة للمنظم فقط.'});if(t.players.size<t.size)return json(res,409,{ok:false,error:'لسه العدد ما اكتملش.'});if(t.status==='active')return json(res,409,{ok:false,error:'الدور الحالي لم ينتهِ بعد.'});if(t.status==='finished')return json(res,409,{ok:false,error:'البطولة انتهت.'});prepareRound(t);json(res,200,{ok:true,tournament:tournamentView(t)});}catch(e){json(res,400,{ok:false,error:'تعذر إجراء القرعة.'});}});return;
  }
  if (url.pathname === '/api/tournament/result' && req.method === 'POST') {
    let body='';req.on('data',c=>body+=c);req.on('end',()=>{try{const d=JSON.parse(body||'{}');const t=tournaments.get(d.tournamentId);if(!t)return json(res,404,{ok:false,error:'البطولة غير موجودة.'});const p=t.players.get(d.playerId);if(!p||p.token!==d.playerToken)return json(res,403,{ok:false,error:'جلسة المتسابق غير صالحة.'});if(t.status!=='active')return json(res,409,{ok:false,error:'لا توجد مباراة نشطة الآن.'});if(p.advanced||p.eliminated)return json(res,409,{ok:false,error:'هذه المباراة تم حسمها.'});if(!p.opponentId)return json(res,409,{ok:false,error:'لا يوجد خصم حالي.'});const done=finishMatch(t,p.id);if(!done.ok)return json(res,400,done);const reward=t.status==='finished'&&t.championId===p.id?{xp:200,points:9000}:null;json(res,200,{ok:true,tournament:tournamentView(t),reward});}catch(e){json(res,400,{ok:false,error:'تعذر تسجيل النتيجة.'});}});return;
  }
  if (url.pathname === '/api/tournament/reset' && req.method === 'POST') {
    let body='';req.on('data',c=>body+=c);req.on('end',()=>{try{const d=JSON.parse(body||'{}');const t=tournaments.get(d.tournamentId);if(!t)return json(res,404,{ok:false,error:'البطولة غير موجودة.'});if(d.adminToken!==t.adminToken)return json(res,403,{ok:false,error:'إعادة الضبط للمنظم فقط.'});for(const code of t.codes)tournamentCodes.delete(code);const players=new Map();const adminId=[...t.players.values()].find(p=>p.admin)?.id||makeToken();players.set(adminId,{id:adminId,name:'المنظم',number:1,admin:true,token:t.adminToken,opponentId:null,eliminated:false,advanced:false,byePending:false});const codes=[];for(let i=1;i<t.size;i++){const code=makeInviteCode();codes.push(code);tournamentCodes.set(code,{tournamentId:t.id,used:false});}t.players=players;t.codes=codes;t.round=0;t.status='waiting';t.championId=null;t.championName=null;json(res,200,{ok:true,tournament:tournamentView(t),codes});}catch(e){json(res,400,{ok:false,error:'تعذر إعادة الضبط.'});}});return;
  }
  if (url.pathname === '/api/stats') {
    return json(res, 200, { ok: true, online: users.size, rooms: rooms.size, activeRooms: [...rooms.values()].filter(r => r.players.size).length, connectionsAccepted: totalConnections, messages: totalMessages });
  }
  // Serve the game itself from the same server, so phones use one public address.
  const requested = url.pathname === '/' ? '/index.html' : url.pathname;
  const file = path.normalize(path.join(ROOT, requested));
  if (!file.startsWith(ROOT) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) return json(res, 404, { ok: false, error: 'Not found' });
  const ext = path.extname(file).toLowerCase();
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8' };
  res.writeHead(200, { 'content-type': types[ext] || 'application/octet-stream' });
  fs.createReadStream(file).pipe(res);
});

const wss = new WebSocketServer({ server, clientTracking: false, maxPayload: MAX_MSG_BYTES, perMessageDeflate: false });

wss.on('connection', (ws, req) => {
  if (users.size >= MAX_CONNECTIONS) { ws.close(1013, 'Server busy'); return; }
  totalConnections++;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const user = { id: makeId(), ws, room: null, name: 'Player', host: false, rateWindow: Date.now(), rateCount: 0 };
  users.set(user.id, user);
  ws.send(JSON.stringify({ type: 'connected', userId: user.id }));

  ws.on('message', raw => {
    totalMessages++;
    if (Buffer.byteLength(raw) > MAX_MSG_BYTES) return ws.close(1009, 'Message too large');
    const now = Date.now();
    if (now - user.rateWindow > 1000) { user.rateWindow = now; user.rateCount = 0; }
    if (++user.rateCount > 40) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.type !== 'string' || m.type.length > 40) return;
    if (m.type === 'joinRoom') {
      const code = String(m.code || '').replace(/\D/g, '').slice(0, 6);
      const room = rooms.get(code);
      if (!room) return ws.send(JSON.stringify({ type: 'error', message: 'الغرفة غير موجودة. أنشئ غرفة جديدة أولًا.' }));
      if (room.players.size >= 2 && !room.players.has(user.id)) return ws.send(JSON.stringify({ type: 'error', message: 'الغرفة ممتلئة.' }));
      if (user.room && user.room !== code) leaveRoom(user);
      user.room = code;
      user.name = String(m.name || 'Player').slice(0, 24);
      user.host = room.players.size === 0;
      room.players.set(user.id, user);
      room.lastActivity = Date.now();
      ws.send(JSON.stringify({ type: 'roomJoined', code, you: { id: user.id, name: user.name, host: user.host }, room: roomInfo(room) }));
      sendRoomState(room);
      return;
    }
    if (m.type === 'leaveRoom' && user.room) { leaveRoom(user); return; }
    if (m.type === 'setGame' && user.room) {
      const room = rooms.get(user.room); if (!room) return;
      if (room.players.get(user.id)?.host !== true) return ws.send(JSON.stringify({ type: 'error', message: 'فقط منشئ الغرفة يختار اللعبة.' }));
      room.game = String(m.game || '').slice(0, 40);
      room.lastActivity = Date.now();
      broadcast(room, { type: 'gameSelected', game: room.game, by: user.id });
      return;
    }
    if (m.type === 'gameEvent' && user.room) {
      const room = rooms.get(user.room); if (!room) return;
      if (!m.event || typeof m.event !== 'object') return;
      room.lastActivity = Date.now();
      const seq = ++room.seq;
      broadcast(room, { type: 'gameEvent', from: user.id, seq, event: m.event }, ws);
      return;
    }
    if (m.type === 'ping') ws.send(JSON.stringify({ type: 'pong', t: Date.now() }));
  });

  ws.on('close', () => { leaveRoom(user); users.delete(user.id); });
});

function leaveRoom(user) {
  if (!user.room) return;
  const room = rooms.get(user.room);
  if (room) {
    room.players.delete(user.id);
    if (room.players.size) {
      const next = [...room.players.values()][0]; next.host = true;
      sendRoomState(room);
      broadcast(room, { type: 'playerLeft', playerId: user.id, players: room.players.size });
    } else rooms.delete(room.code);
  }
  user.room = null;
}

const heartbeat = setInterval(() => {
  const now = Date.now();
  for (const [id, u] of users) {
    if (u.ws.readyState !== 1) continue;
    if (u.ws.isAlive === false) { u.ws.terminate(); continue; }
    u.ws.isAlive = false;
    u.ws.ping();
  }
  for (const [code, room] of rooms) {
    if (!room.players.size && now - room.lastActivity > ROOM_TTL_MS) rooms.delete(code);
  }
}, HEARTBEAT_MS);

function shutdown(signal) {
  console.log(`R2 GAMES: ${signal}`);
  clearInterval(heartbeat);
  for (const u of users.values()) u.ws.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;
initDatabase().then(()=>server.listen(PORT, '0.0.0.0', () => console.log(`R2 GAMES Multiplayer Server listening on ${PORT}`))).catch(err=>{console.error('Database init failed:',err);process.exit(1);});
