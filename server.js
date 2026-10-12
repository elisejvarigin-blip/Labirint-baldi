// Сервер-ретранслятор для «Лабиринта кристаллов». Без зависимостей: node server.js
// Раздаёт index.html и пересылает игровые сообщения между игроками по WebSocket (/ws).
// Порт: переменная PORT (по умолчанию 3000). Для HTTPS поставьте перед ним nginx/Caddy с проксированием /ws.
var http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
var rooms={},links={},GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function rnd(){return crypto.randomBytes(4).toString('hex');}
function frame(s){var b=Buffer.from(s),n=b.length,h;
  if(n<126)h=Buffer.from([0x81,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(n,2);}
  else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(n),2);}
  return Buffer.concat([h,b]);}
function send(ws,o){if(ws&&!ws.sock.destroyed)ws.sock.write(frame(JSON.stringify(o)));}
function unlink(ws,cid,notify){var l=links[cid];if(!l)return;delete links[cid];
  var other=l.h===ws?l.c:l.h;if(other){other.cids.delete(cid);if(notify)send(other,{t:'x',cid:cid});}ws.cids.delete(cid);}
function onMsg(ws,m){var l,t;
  if(m.t==='reg'){var id=m.id?String(m.id).slice(0,40):null;
    if(id){if(rooms[id]&&rooms[id]!==ws)return send(ws,{t:'err',e:'unavailable-id'});rooms[id]=ws;ws.id=id;}
    else ws.id='c'+rnd();
    send(ws,{t:'open',id:ws.id});}
  else if(m.t==='conn'){var hs=rooms[m.to],cid=String(m.cid||'').slice(0,40);
    if(!hs||!cid||links[cid])return send(ws,{t:'nf',cid:cid});
    links[cid]={h:hs,c:ws};hs.cids.add(cid);ws.cids.add(cid);send(hs,{t:'conn',cid:cid});send(ws,{t:'ok',cid:cid});}
  else if(m.t==='d'){l=links[m.cid];if(!l)return;t=l.h===ws?l.c:l.c===ws?l.h:null;if(t)send(t,{t:'d',cid:m.cid,m:m.m});}
  else if(m.t==='x'){unlink(ws,m.cid,1);}}
function onClose(ws){if(ws.dead)return;ws.dead=1;ws.cids.forEach(function(c){unlink(ws,c,1);});
  if(ws.id&&rooms[ws.id]===ws)delete rooms[ws.id];}
var srv=http.createServer(function(req,res){var u=req.url.split('?')[0];
  if(u==='/health'){res.end('ok');return;}
  if(u==='/'||u==='/index.html'){var html;try{html=fs.readFileSync(path.join(__dirname,'index.html'),'utf8');}catch(e){res.writeHead(500);res.end('index.html не найден рядом с server.js');return;}
    var p=(req.headers['x-forwarded-proto']==='https'||req.socket.encrypted)?'wss':'ws';
    html=html.replace("window.RELAY_URL=''","window.RELAY_URL='"+p+"://"+req.headers.host+"/ws'");
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});res.end(html);return;}
  res.writeHead(404);res.end('not found');});
srv.on('upgrade',function(req,sock){var key=req.headers['sec-websocket-key'];
  if(req.url.split('?')[0]!=='/ws'||!key){sock.destroy();return;}
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(key+GUID).digest('base64')+'\r\n\r\n');
  sock.setNoDelay(true);sock.setKeepAlive(true,30000);
  var ws={sock:sock,cids:new Set(),id:null},buf=Buffer.alloc(0),frag=null;
  sock.on('data',function(d){buf=Buffer.concat([buf,d]);
    for(;;){if(buf.length<2)return;var op=buf[0]&15,fin=buf[0]&128,ln=buf[1]&127,off=2;
      if(ln===126){if(buf.length<4)return;ln=buf.readUInt16BE(2);off=4;}
      else if(ln===127){if(buf.length<10)return;ln=Number(buf.readBigUInt64BE(2));off=10;}
      if(ln>262144){sock.destroy();return;}
      if(!(buf[1]&128)){sock.destroy();return;}
      if(buf.length<off+4+ln)return;
      var mk=buf.slice(off,off+4),pl=Buffer.from(buf.slice(off+4,off+4+ln));
      for(var i=0;i<pl.length;i++)pl[i]^=mk[i&3];
      buf=buf.slice(off+4+ln);
      if(op===8){sock.end(Buffer.from([0x88,0]));return;}
      if(op===9){sock.write(Buffer.concat([Buffer.from([0x8a,pl.length]),pl]));continue;}
      if(op===1||op===0){frag=op===1?pl:Buffer.concat([frag||Buffer.alloc(0),pl]);
        if(fin){var m;try{m=JSON.parse(frag.toString('utf8'));}catch(e){m=null;}frag=null;if(m)onMsg(ws,m);}}}});
  sock.on('close',function(){onClose(ws);});sock.on('error',function(){sock.destroy();});});
if(require.main===module)srv.listen(process.env.PORT||3000,function(){console.log('Игра: http://localhost:'+(process.env.PORT||3000));});
module.exports=srv;
