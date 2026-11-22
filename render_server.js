import http from 'node:http';
import crypto from 'node:crypto';
import {WebSocketServer, WebSocket} from 'ws';
import admin from 'firebase-admin';

const PORT = Number(process.env.PORT || 10000);
const deviceToken = process.env.DEVICE_TOKEN;
const dashboardToken = process.env.DASHBOARD_TOKEN;
const dbUrl = process.env.FIREBASE_DATABASE_URL;
const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
if (!deviceToken || !dashboardToken || !dbUrl || !sa) {
  throw new Error('Missing DEVICE_TOKEN, DASHBOARD_TOKEN, FIREBASE_DATABASE_URL, or FIREBASE_SERVICE_ACCOUNT_JSON');
}
admin.initializeApp({credential:admin.credential.cert(JSON.parse(sa)), databaseURL:dbUrl});
const db = admin.database();
const server = http.createServer((req,res)=>{
  if (req.url === '/health') {res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true}));return;}
  res.writeHead(404);res.end();
});
const wss = new WebSocketServer({server, maxPayload:128*1024, perMessageDeflate:false});
const devices = new Map();
const dashboards = new Set();
// Per-station detector state. Configuration is persisted in experimental RTDB.
const detectors = new Map();
const allowedSignals = new Set(['x','y','z','sound','pir','sw','button']);
function validDetector(d) {
  return d && typeof d.enabled==='boolean' && allowedSignals.has(d.signal)
    && Number.isFinite(d.threshold) && Math.abs(d.threshold)<=100000
    && ['above','below'].includes(d.operator)
    && Number.isInteger(d.cooldownMs) && d.cooldownMs>=500 && d.cooldownMs<=600000
    && Array.isArray(d.rgb) && d.rgb.length===3
    && d.rgb.every(x=>Number.isInteger(x)&&x>=0&&x<=255);
}
function evaluate(station,sample) {
  const entry=detectors.get(station);
  if(!entry?.enabled || !sample || !Number.isFinite(sample[entry.signal]))return;
  const value=sample[entry.signal];
  const active=entry.operator==='above'?value>entry.threshold:value<entry.threshold;
  const now=Date.now();
  // Edge-triggered; cooldown prevents repeated flashing during sustained events.
  if(active && !entry.wasActive && now-entry.lastAlert>=entry.cooldownMs){
    entry.lastAlert=now;
    const event={type:'detector_alert',station,signal:entry.signal,value,
      threshold:entry.threshold,operator:entry.operator,serverMs:now,sampleSeq:sample.seq??null};
    emitToDashboards(event);
    const device=devices.get(station);
    if(device)send(device,{type:'command',action:'alert_rgb',r:entry.rgb[0],g:entry.rgb[1],b:entry.rgb[2],durationMs:1500});
    db.ref(`experimentalAlerts/${station}`).push(event).catch(e=>console.error('alert_write_failed',String(e)));
  }
  entry.wasActive=active;
}
// Reload saved rules after service restart; alert history stays in Firebase.
db.ref('experimentalDetectors').once('value').then(snap=>{
  const all=snap.val()||{};
  for(const [station,d] of Object.entries(all))if(safeId(station)&&validDetector(d))
    detectors.set(station,{...d,wasActive:false,lastAlert:0});
}).catch(e=>console.error('detector_load_failed',String(e)));

const safeId = s => typeof s === 'string' && /^[A-Za-z0-9_-]{1,48}$/.test(s);
const tokenEq = (a,b) => {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const aa=Buffer.from(a), bb=Buffer.from(b);
  return aa.length === bb.length && crypto.timingSafeEqual(aa,bb);
};
function emitToDashboards(obj) {
  const payload=JSON.stringify(obj);
  for (const ws of dashboards) if(ws.readyState===WebSocket.OPEN && ws.bufferedAmount<256*1024) ws.send(payload);
}
function send(ws,obj) {if(ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(obj));}
function validBatch(m) {
  return safeId(m.station) && safeId(m.session) && [1,2,3].includes(m.tier)
    && Number.isSafeInteger(m.batch) && m.batch>=0 && m.batch<=10000000
    && Array.isArray(m.samples) && m.samples.length>0 && m.samples.length<=100
    && m.samples.every(s=>s && Number.isSafeInteger(s.seq) && s.seq>=0 && Number.isFinite(s.ms));
}
// Bounded, process-local persistence queue. Receipt ACK means accepted into this
// volatile queue, NOT durable Firebase persistence. Restart-loss risk is explicit.
const MAX_PENDING_BATCHES=256;
const pending=new Map();
const persisted=new Map(); // bounded dedup cache; Firebase paths are also idempotent
const MAX_PERSISTED_CACHE=4096;
const recordingStatus=new Map();
let workerActive=false;
function batchKey(m){return `${m.station}/${m.session}/${m.tier}/${m.batch}`;}
function reportRecording(station, patch){
  const previous=recordingStatus.get(station)||{station,receivedSamples:0,persistedSamples:0,receivedBatches:0,persistedBatches:0,failedAttempts:0};
  const next={...previous,...patch,station,serverMs:Date.now()};
  recordingStatus.set(station,next);
  emitToDashboards({type:'recording_status',...next});
}
function queueSnapshot(station){
  let queued=0;
  for(const item of pending.values())if(item.station===station)queued++;
  return queued;
}
function startWorker(){
  if(workerActive)return;
  workerActive=true;
  const pump=async()=>{
    try{
      while(pending.size){
        const now=Date.now();
        let item=null, earliest=Infinity;
        for(const candidate of pending.values()){
          if(candidate.nextAttemptMs<=now){item=candidate;break;}
          earliest=Math.min(earliest,candidate.nextAttemptMs);
        }
        if(!item){await new Promise(resolve=>setTimeout(resolve,Math.max(10,Math.min(1000,earliest-now))));continue;}
        const path=`experimentalRecordings/${item.station}/${item.session}/tier${item.tier}/batches/b${String(item.batch).padStart(8,'0')}`;
        try{
          await db.ref(path).set(item.payload);
          const persistedMs=Date.now();
          pending.delete(item.key);
          persisted.set(item.key,persistedMs);
          if(persisted.size>MAX_PERSISTED_CACHE)persisted.delete(persisted.keys().next().value);
          const state=recordingStatus.get(item.station);
          if(state?.session===item.session){reportRecording(item.station,{persistedSamples:state.persistedSamples+item.samples,persistedBatches:state.persistedBatches+1,queuedBatches:queueSnapshot(item.station)});}
          emitToDashboards({type:'batch_persisted',station:item.station,session:item.session,tier:item.tier,batch:item.batch,count:item.samples,persistedMs});
          process.stdout.write(JSON.stringify({event:'stored',station:item.station,session:item.session,tier:item.tier,batch:item.batch,count:item.samples,receivedMs:item.receivedMs,persistedMs,storeMs:persistedMs-item.receivedMs})+'\n');
        }catch(e){
          item.attempts++;
          item.nextAttemptMs=Date.now()+Math.min(30000,500*2**Math.min(item.attempts,6));
          const state=recordingStatus.get(item.station);
          if(state?.session===item.session)reportRecording(item.station,{failedAttempts:state.failedAttempts+1,queuedBatches:queueSnapshot(item.station),lastPersistenceError:String(e).slice(0,180)});
          emitToDashboards({type:'batch_retry',station:item.station,session:item.session,tier:item.tier,batch:item.batch,attempt:item.attempts,nextAttemptMs:item.nextAttemptMs});
          process.stderr.write(JSON.stringify({event:'persist_retry',station:item.station,session:item.session,batch:item.batch,attempt:item.attempts,error:String(e)})+'\n');
        }
      }
    }finally{workerActive=false;if(pending.size)startWorker();}
  };
  void pump();
}
wss.on('connection', (ws,req)=>{
  let role=null, station=null, authenticated=false;
  ws.on('message',raw=>{
    let m; try {m=JSON.parse(raw.toString());} catch {send(ws,{type:'error',error:'invalid_json'});return;}
    if (!authenticated) {
      if (m.type!=='hello' || !['device','dashboard'].includes(m.role)) {ws.close(1008,'auth required');return;}
      if(m.role==='device' && safeId(m.station) && tokenEq(m.token,deviceToken)) {
        authenticated=true;role='device';station=m.station;
        const old=devices.get(station); if(old && old!==ws) old.close(1000,'replaced');
        devices.set(station,ws);
      } else if(m.role==='dashboard' && tokenEq(m.token,dashboardToken)) {
        authenticated=true;role='dashboard';dashboards.add(ws);
      } else {ws.close(1008,'unauthorized');return;}
      send(ws,{type:'hello_ack',serverMs:Date.now()});if(role==='dashboard')for(const status of recordingStatus.values())send(ws,{type:'recording_status',...status});return;
    }
    if (role==='dashboard') {
      if(m.type==='get_recording_status' && safeId(m.station)){send(ws,{type:'recording_status',...(recordingStatus.get(m.station)||{station:m.station,state:'unknown',queuedBatches:queueSnapshot(m.station)})});return;}
      if(m.type==='set_detector' && safeId(m.station) && validDetector(m.detector)) {
        const d=m.detector;
        db.ref(`experimentalDetectors/${m.station}`).set(d).then(()=>{
          detectors.set(m.station,{...d,wasActive:false,lastAlert:0});
          send(ws,{type:'detector_saved',station:m.station,detector:d});
        }).catch(()=>send(ws,{type:'error',error:'detector_save_failed'}));
        return;
      }

      if(m.type==='command' && safeId(m.station) && ['start','stop','rgb'].includes(m.action) && [1,2,3].includes(m.tier)) {
        const device=devices.get(m.station);
        if(!device || device.readyState!==WebSocket.OPEN) {send(ws,{type:'error',error:'device_offline'});return;}
        if(m.action==='rgb'){
          if(![m.r,m.g,m.b].every(x=>Number.isInteger(x)&&x>=0&&x<=255)){send(ws,{type:'error',error:'invalid_rgb'});return;}
          send(device,{type:'command',action:'rgb',r:m.r,g:m.g,b:m.b});
        }else{
          send(device,{type:'command',action:m.action,tier:m.tier,durationMs:Math.min(m.tier===1?21600000:(m.tier===3?300000:7200000),Math.max(1000,Number(m.durationMs)||300000))});
          send(ws,{type:'recording_command_sent',station:m.station,action:m.action,serverMs:Date.now()});
        }
      }
      return;
    }
    if (role==='device' && m.type==='live_sample') {
      if (m.station===station && [0,1,3].includes(m.tier) && safeId(m.session) && m.sample && Number.isSafeInteger(m.sample.seq)) {
        emitToDashboards({type:'live_sample',station,session:m.session,tier:m.tier,sample:m.sample,serverReceivedMs:Date.now()});
        evaluate(station,m.sample);
      }
      return;
    }
    if (role==='device' && m.type==='batch') {
      if (!validBatch(m) || m.station!==station) {send(ws,{type:'error',error:'invalid_batch'});return;}
      const receivedMs=Date.now();
      // Forward to live dashboard on receipt, without waiting for Firebase.
      // Tier 1 and Tier 3 already forward individual live samples.
      // Tier 2 forwards completed batches; no duplicate live forwarding.
      // Detection uses the independent 10 Hz live_sample stream for every tier.
      // Do not re-evaluate delayed Tier 2 recording batches: doing so could
      // generate stale or duplicate alerts after the live event has passed.
      if(m.tier===2)emitToDashboards({type:'live_batch',station,session:m.session,tier:m.tier,batch:m.batch,
        samples:m.samples,serverReceivedMs:receivedMs,persistence:'pending'});
      const key=batchKey(m);
      if(pending.has(key)||persisted.has(key)){
        send(ws,{type:'ack',session:m.session,tier:m.tier,batch:m.batch,receivedMs,acceptedMs:Date.now(),persistence:'queued_or_persisted'});
        return;
      }
      if(pending.size>=MAX_PENDING_BATCHES){
        send(ws,{type:'nack',session:m.session,tier:m.tier,batch:m.batch,error:'server_queue_full'});
        emitToDashboards({type:'recording_queue_full',station,session:m.session,tier:m.tier,queuedBatches:pending.size});
        return;
      }
      const payload={station,session:m.session,tier:m.tier,batch:m.batch,samples:m.samples,
        deviceSentMs:m.deviceSentMs??null,serverReceivedMs:receivedMs,serverWrittenMs:null};
      pending.set(key,{key,station,session:m.session,tier:m.tier,batch:m.batch,samples:m.samples.length,payload,receivedMs,attempts:0,nextAttemptMs:0});
      // A valid accepted batch establishes the session even when the device
      // has not yet emitted recording_event (legacy or reconnecting firmware).
      // Do not infer authoritative completion or device sample losses here.
      const state=recordingStatus.get(station);
      if(!state || state.session!==m.session){
        reportRecording(station,{session:m.session,tier:m.tier,state:'receiving',
          durationMs:null,startedMs:null,receivedSamples:m.samples.length,
          persistedSamples:0,receivedBatches:1,persistedBatches:0,
          failedAttempts:0,queuedBatches:queueSnapshot(station),
          deviceSamples:null,deviceLost:null});
      }else{
        reportRecording(station,{receivedSamples:state.receivedSamples+m.samples.length,
          receivedBatches:state.receivedBatches+1,queuedBatches:queueSnapshot(station)});
      }
      send(ws,{type:'ack',session:m.session,tier:m.tier,batch:m.batch,receivedMs,acceptedMs:Date.now(),persistence:'queued'});
      emitToDashboards({type:'batch_received',station,session:m.session,tier:m.tier,batch:m.batch,count:m.samples.length,receivedMs});
      startWorker();
    } else if (role==='device' && m.type==='recording_event' && m.station===station && safeId(m.session) && [1,2,3].includes(m.tier) && ['started','draining','completed'].includes(m.state)) {
      const old=recordingStatus.get(station);
      if(old?.session!==m.session)reportRecording(station,{session:m.session,tier:m.tier,state:m.state,durationMs:m.durationMs??null,startedMs:Date.now(),receivedSamples:0,persistedSamples:0,receivedBatches:0,persistedBatches:0,failedAttempts:0,queuedBatches:queueSnapshot(station),deviceSamples:null,deviceLost:null});
      else reportRecording(station,{state:m.state,startedMs:old.startedMs??Date.now(),durationMs:m.durationMs??old.durationMs,deviceSamples:m.state==='completed'?m.samples:(old.deviceSamples??null),deviceLost:m.state==='completed'?m.lost:(old.deviceLost??null),queuedBatches:queueSnapshot(station)});
    } else if (role==='device' && m.type==='diag' && m.station===station) {
      process.stdout.write(JSON.stringify({event:'device_diag',station,serverMs:Date.now(),data:m})+'\n');
      emitToDashboards({type:'diag',station,serverMs:Date.now(),data:m});
    }
  });
  ws.on('close',()=>{if(role==='dashboard') dashboards.delete(ws);if(role==='device'&&devices.get(station)===ws)devices.delete(station);});
});
server.listen(PORT,()=>process.stdout.write(`ENP162 experimental relay listening on ${PORT}\n`));
