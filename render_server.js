import http from 'node:http';
import crypto from 'node:crypto';
import {WebSocketServer, WebSocket} from 'ws';
import admin from 'firebase-admin';

const PORT = Number(process.env.PORT || 10000);
const deviceToken = process.env.DEVICE_TOKEN;
const instructorUIDs = new Set((process.env.INSTRUCTOR_UIDS || '').split(',').map(x=>x.trim()).filter(Boolean));
const dbUrl = process.env.FIREBASE_DATABASE_URL;
const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
const authSa = process.env.FIREBASE_AUTH_SERVICE_ACCOUNT_JSON;
if (!deviceToken || !instructorUIDs.size || !dbUrl || !sa || !authSa) {
  throw new Error('Missing DEVICE_TOKEN, INSTRUCTOR_UIDS, FIREBASE_DATABASE_URL, FIREBASE_SERVICE_ACCOUNT_JSON, or FIREBASE_AUTH_SERVICE_ACCOUNT_JSON');
}
admin.initializeApp({credential:admin.credential.cert(JSON.parse(sa)), databaseURL:dbUrl});
const db = admin.database();
const authApp = admin.initializeApp({credential:admin.credential.cert(JSON.parse(authSa)), projectId:'iot-fall-2026', databaseURL:'https://iot-fall-2026-default-rtdb.firebaseio.com'}, 'student-auth');
const authDb = admin.database(authApp);

// v0.16: Read-only, authenticated recording discovery and bounded retrieval.
// The existing recording/persistence pipeline is intentionally unchanged.
const SERVER_REVISION='v0.28.7_EXPORT_ELIGIBILITY'; // 2026-11-27_2237 ET
const MAX_LIST_SESSIONS=150;
const MAX_PAGE_BATCHES=20;
const MAX_PAGE_SAMPLES=2000;
function recordingRoot(station,session,tier){return `experimentalRecordings/${station}/${session}/tier${tier}`;}
async function shallowKeys(path){
  // RTDB Admin SDK has no shallow listing; authenticated REST avoids downloading
  // entire multi-hour recordings just to discover their keys.
  const credential=admin.app().options.credential;
  const access=await credential.getAccessToken();
  const url=`${dbUrl.replace(/\/$/,'')}/${path}.json?shallow=true`;
  const response=await fetch(url,{headers:{Authorization:`Bearer ${access.access_token}`},signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw new Error(`Firebase key listing HTTP ${response.status}`);
  const obj=await response.json();
  return obj && typeof obj==='object' && !Array.isArray(obj)?Object.keys(obj):[];
}
async function listRecordings(station){
  const sessions=(await shallowKeys(`experimentalRecordings/${station}`)).filter(safeId);
  if(sessions.length>MAX_LIST_SESSIONS)throw new Error(`Too many sessions (${sessions.length}) for bounded discovery; use session ID lookup`);
  const output=[];
  // Limit concurrent Firebase operations; do not read the full sample payload.
  for(let i=0;i<sessions.length;i+=6){
    const group=await Promise.all(sessions.slice(i,i+6).map(async session=>{
      const entries=[];
      try{
        const tierNames=await shallowKeys(`experimentalRecordings/${station}/${session}`);
        for(const tierName of tierNames){
          if(!/^tier[123]$/.test(tierName))continue;
          const tier=Number(tierName.slice(4));
          const root=recordingRoot(station,session,tier);
          const snap=await db.ref(`${root}/batches/b00000000`).once('value');
          const first=snap.val();
          if(!first || !Array.isArray(first.samples))continue;
          const time=Number(first.serverReceivedMs)||Number(first.deviceSentMs)||null;
          entries.push({station,session,tier,recordedMs:time,
            label:time?new Date(time).toISOString():`Undated recording (${session})`,
            firstBatchSamples:first.samples.length, completed:(await db.ref(`${root}/completion`).once('value')).val()?.completed===true});
        }
      }catch(e){console.error('recording_discovery_skipped',station,session,String(e));}
      return entries;
    }));
    for(const entries of group)output.push(...entries);
  }
  output.sort((a,b)=>(b.recordedMs||0)-(a.recordedMs||0));
  return output;
}
async function recordingPage(station,session,tier,afterBatch,limit){
  const root=recordingRoot(station,session,tier);
  const take=Math.min(MAX_PAGE_BATCHES,Math.max(1,Number(limit)||10));
  let query=db.ref(`${root}/batches`).orderByKey();
  if(afterBatch!==null){
    if(!Number.isInteger(afterBatch)||afterBatch<0||afterBatch>10000000)throw new Error('invalid batch cursor');
    query=query.startAt(`b${String(afterBatch+1).padStart(8,'0')}`);
  }
  const snap=await query.limitToFirst(take+1).once('value');
  const items=Object.entries(snap.val()||{}).sort(([a],[b])=>a.localeCompare(b));
  const hasMore=items.length>take;
  const page=items.slice(0,take).map(([key,b])=>({batch:Number(key.slice(1)),...b}));
  if(page.some(b=>!Array.isArray(b.samples)) || page.reduce((n,b)=>n+b.samples.length,0)>MAX_PAGE_SAMPLES)throw new Error('invalid or oversized recording page');
  return {station,session,tier,batches:page,nextBatch:hasMore?page.at(-1).batch:null,hasMore};
}

const server = http.createServer((req,res)=>{
  if (req.url === '/health') {res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true}));return;}
  res.writeHead(404);res.end();
});
const wss = new WebSocketServer({server, maxPayload:128*1024, perMessageDeflate:false});
const devices = new Map();
const dashboards = new Set();
const dashboardAuth = new WeakMap();
const studentStation = s => typeof s==='string' && /^ES-(0[1-9]|[12][0-9]|3[0-2])$/.test(s);
// Authentication diagnostics: never emit raw exception text or credentials.
function authFailure(ws,phase,e){
  const rawCode=e?.code;
  const code=typeof rawCode==='string' && /^(auth|app|database)\/[a-z-]+$/.test(rawCode)?rawCode:'unclassified';
  const messages={
    'auth/insufficient-permission':'Firebase Authentication permission denied',
    'auth/invalid-credential':'Firebase credential rejected',
    'auth/argument-error':'Firebase token argument or claims rejected',
    'auth/invalid-id-token':'Firebase identity token invalid',
    'auth/id-token-expired':'Firebase identity token expired',
    'auth/id-token-revoked':'Firebase identity token revoked',
    'auth/user-not-found':'Firebase user not found',
    'auth/user-disabled':'Firebase user disabled',
    'auth/internal-error':'Firebase Authentication internal error',
    'app/invalid-credential':'Firebase application credential invalid',
    'app/network-error':'Firebase application network error'
  };
  const local=['invalid token','invalid assignment','expired token','authorization changed'];
  const message=messages[code] || (local.includes(e?.message)?e.message:'Unclassified authentication exception; raw message withheld');
  netLog('net_auth_rejected',ws,{phase,errorCode:code,errorMessage:message});
}
async function verifyDashboard(token,ws,phase){
  netLog('net_auth_verify_begin',ws,{phase});
  if(typeof token!=='string')throw Error('invalid token');
  const decoded=await admin.auth(authApp).verifyIdToken(token,true);
  netLog('net_auth_verify_ok',ws,{phase});
  const instructor=instructorUIDs.has(decoded.uid);
  netLog('net_auth_instructor',ws,{phase,instructor});
  const station=instructor?null:(await authDb.ref(`studentAssignments/${decoded.uid}/stationID`).once('value')).val();
  if(!instructor&&!studentStation(station))throw Error('invalid assignment');
  if(decoded.exp*1000<=Date.now())throw Error('expired token');
  return {uid:decoded.uid,instructor,station,expiresMs:decoded.exp*1000};
}
function authorized(ws,station){
  const a=dashboardAuth.get(ws);
  return !!a && a.expiresMs>Date.now() && safeId(station) && (a.instructor||a.station===station);
}
// Read-only experiment association summary; never reads full recordings.
async function associatedRecordings(station,records){
  const annotations=(await db.ref(`experimentalRecordingAnnotations/${station}`).once('value')).val()||{};
  const byId=new Map(records.map(r=>[r.id,[]]));
  for(const r of records)r.associatedRecordings=byId.get(r.id);
  if(!Object.values(annotations).some(tiers=>Object.values(tiers||{}).some(a=>byId.has(a?.experimentId)))){for(const r of records)r.exportComplete=true;return;}
  const library=await listRecordings(station);
  for(const r of library){
    const a=annotations[r.session]?.[`tier${r.tier}`];
    if(byId.has(a?.experimentId))byId.get(a.experimentId).push({name:a.name||'',notes:a.notes||'',tier:r.tier,session:r.session,recordedMs:r.recordedMs,completed:r.completed});
  }
  for(const r of records){r.associatedRecordings=byId.get(r.id);const expected=Object.values(annotations).flatMap(t=>Object.values(t||{})).filter(a=>a?.experimentId===r.id).length;r.exportComplete=expected===r.associatedRecordings.length;}
}

// Read-only library enrichment; recording discovery and storage are unchanged.
async function libraryNames(station,records){
  const [a,e]=await Promise.all([db.ref(`experimentalRecordingAnnotations/${station}`).once('value'),db.ref(`experimentalExperiments/${station}`).once('value')]);
  const annotations=a.val()||{}, experiments=e.val()||{};
  return records.map(r=>{const ann=annotations[r.session]?.[`tier${r.tier}`];const id=ann?.experimentId;return {...r,recordingName:ann?.name||'',experimentName:id?(experiments[id]?.name||'Unavailable experiment'):'No experiment'};});
}

const experimentWrites=new Map();
async function serializeExperiment(station,work){
  const previous=experimentWrites.get(station)||Promise.resolve();
  const task=previous.catch(()=>{}).then(work);
  experimentWrites.set(station,task);
  try{return await task;}finally{if(experimentWrites.get(station)===task)experimentWrites.delete(station);}
}
function experimentDeletionUpdates(station,id,annotations){
 const updates={[`experimentalExperiments/${station}/${id}`]:null};
 for(const [session,tiers] of Object.entries(annotations)){
  if(!safeId(session))continue;
  for(const tier of ['tier1','tier2','tier3'])if(tiers?.[tier]?.experimentId===id)updates[`experimentalRecordingAnnotations/${station}/${session}/${tier}/experimentId`]='';
 }
 return updates;
}
function validateExperiment(m){
  const fields={};
  for(const [key,max] of [['name',200],['question',2000],['description',4000]]){
    if(typeof m[key]!=='string'||m[key].length>max)throw Error('Invalid '+key);
    fields[key]=m[key].trim();
  }
  if(!fields.name)throw Error('Experiment name is required');
  return fields;
}
function dashboardReply(ws,station,obj){if(authorized(ws,station))send(ws,obj);}

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
  for (const ws of dashboards) if(authorized(ws,obj.station) && ws.readyState===WebSocket.OPEN && ws.bufferedAmount<256*1024) diagnosticSend(ws,payload,"broadcast");
}
// Live observations are disposable; never queue behind pending dashboard writes.
const liveWrites=new WeakSet();
const LIVE_MAX_AGE_MS=1000;
function freshLive(m,now){return Number.isSafeInteger(m.acquiredServerMs) && m.acquiredServerMs<=now && now-m.acquiredServerMs<LIVE_MAX_AGE_MS;}
function emitLive(obj){
 const payload=JSON.stringify(obj);
 for(const ws of dashboards){
  if(!authorized(ws,obj.station) || ws.readyState!==WebSocket.OPEN || ws.bufferedAmount!==0 || liveWrites.has(ws))continue;
  liveWrites.add(ws);
  ws.send(payload,err=>{liveWrites.delete(ws);if(err)netLog('net_send_error',ws,{kind:'live',error:String(err.message)});});
 }
}
const diagConnections=new WeakMap();
let diagSerial=0;
function netLog(event,ws,extra={}){const c=diagConnections.get(ws);console.log(JSON.stringify({event,serverMs:Date.now(),connection:c?.id,role:c?.role,station:c?.station,...extra}));}
function diagnosticSend(ws,payload,kind){
 const pending=ws.bufferedAmount;
 if(pending>65536)netLog('net_backpressure',ws,{kind,pending});
 ws.send(payload,err=>{if(err)netLog('net_send_error',ws,{kind,error:String(err.message).slice(0,180),pending:ws.bufferedAmount});});
}
function send(ws,obj) {if(ws.readyState===WebSocket.OPEN) diagnosticSend(ws,JSON.stringify(obj),obj.type);}
let diagTick=Date.now();
setInterval(()=>{const now=Date.now(),lag=now-diagTick-1000;diagTick=now;if(lag>100)console.log(JSON.stringify({event:'net_event_loop_delay',serverMs:now,lagMs:lag}));},1000).unref();
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
const deleting=new Set(), deletionKeys=new Set(), completionWrites=new Map(), idleProof=new Map(), idleRequests=new Map(), deferredCompletion=new Map();
const deletionReady=db.ref('experimentalDeletions').once('value').then(snap=>{
 for(const [st,sessions] of Object.entries(snap.val()||{}))for(const [se,tiers] of Object.entries(sessions||{}))for(const t of Object.keys(tiers||{}))deletionKeys.add(`${st}/${se}/${t}`);
});
// Fail closed on startup if deletion protection cannot be loaded.
deletionReady.catch(e=>console.error('deletion_index_load_failed',String(e)));
const deletionKey=(st,se,t)=>`${st}/${se}/tier${t}`;
function deletionSafe(st){const d=devices.get(st), proof=idleProof.get(st);return !!d&&d.readyState===WebSocket.OPEN&&proof===d&&!queueSnapshot(st)&&!completionWrites.get(st)&&!deferredCompletion.has(st)&&!deleting.has(st);}
// Read-only export eligibility; historical recordings require existing confirmed-idle proof.
async function exportEligibility(st,session,tier){
 const root=recordingRoot(st,session,tier);
 const [completion,first]=await Promise.all([db.ref(`${root}/completion`).once('value'),db.ref(`${root}/batches/b00000000`).once('value')]);
 const state=recordingStatus.get(st);
 if(state?.session===session&&state.tier===tier&&['started','receiving','draining'].includes(state.state))throw Error('Recording is active or draining');
 if(queueSnapshot(st)||completionWrites.get(st)||deferredCompletion.has(st))throw Error('Recording persistence is pending; try again after saving finishes');
 if(deleting.has(st)||deletionKeys.has(deletionKey(st,session,tier))||!first.exists())throw Error('Recording is missing or being deleted');
 const c=completion.val();
 if(c?.completed===true)return {completed:true};
 if(c?.completed===false)throw Error('Recording completion was explicitly unverified');
 if(!deletionSafe(st))throw Error('Historical recording requires confirmed idle station; request a snapshot in Devices and retry');
 return {completed:false};
}
function deletionStatus(st){emitToDashboards({type:'deletion_status',station:st,safe:deletionSafe(st)});}
function invalidateIdle(st){idleProof.delete(st);idleRequests.delete(st);deletionStatus(st);}
async function deleteRecordings(ws,m){
 return serializeExperiment(m.station,()=>deleteRecordingsLocked(ws,m));
}
async function deleteRecordingsLocked(ws,m){
 const st=m.station, a=dashboardAuth.get(ws);
 if(!authorized(ws,st)||!Array.isArray(m.records)||!m.records.length||m.records.length>20||!m.records.every(r=>r&&safeId(r.session)&&[1,2,3].includes(r.tier)))throw Error('invalid deletion request');
 if(!deletionSafe(st))throw Error('Station must be confirmed idle with no pending writes; request a management snapshot');
 deleting.add(st);deletionStatus(st);
 const results=[];
 try{
  await deletionReady;
  for(const r of m.records){
   try{
    if(!authorized(ws,st))throw Error('authorization expired');
    const key=deletionKey(st,r.session,r.tier),root=recordingRoot(st,r.session,r.tier);
    if(deletionKeys.has(key)){if((await db.ref(`experimentalDeletions/${key}`).once('value')).exists()){await db.ref(`experimentalRecordingAnnotations/${key}`).remove();results.push({...r,ok:true});continue;}throw Error('Previous deletion not confirmed; instructor review required');}
    // Historical recordings use the same confirmed-idle and persistence guards.
    if(!(await db.ref(`${root}/batches/b00000000`).once('value')).exists())throw Error('Recording not found');
    if(!authorized(ws,st)||idleProof.get(st)!==devices.get(st)||queueSnapshot(st)||completionWrites.get(st)||deferredCompletion.has(st))throw Error('Station no longer safe');
    // Reserve tombstone before awaiting atomic update; late messages cannot race it.
    deletionKeys.add(key);
    try{await db.ref().update({[root]:null,[`experimentalRecordingAnnotations/${key}`]:null,[`experimentalDeletions/${key}`]:{uid:a.uid,deletedMs:Date.now()}});}
    catch(e){ // Keep protection in memory on ambiguous commit; require inventory refresh.
      throw Error('Deletion not confirmed; refresh inventory');
    }
    results.push({...r,ok:true});
   }catch(e){results.push({...r,ok:false,error:e.message});}
  }
 }finally{deleting.delete(st);deletionStatus(st);}
 return results;
}
function askIdle(st,ws){const id="del-"+crypto.randomUUID();idleRequests.set(st,id);send(ws,{type:"command",action:"manage",operation:"snapshot",requestId:id});}
function saveCompletion(st,m){
 completionWrites.set(st,(completionWrites.get(st)||0)+1);deletionStatus(st);
 const root=recordingRoot(st,m.session,m.tier);
 const state=recordingStatus.get(st);
 const verified=state?.session===m.session&&state.persistedSamples===state.receivedSamples&&state.persistedSamples===m.samples-m.lost;
 const update={completion:{completed:verified,deviceSamples:m.samples??null,deviceLost:m.lost??null,completedMs:Date.now()}};
 if(m.tier===3&&m.transportDiag&&typeof m.transportDiag==='object')update.transportSummary=m.transportDiag;
 return db.ref(root).update(update).finally(()=>{completionWrites.set(st,completionWrites.get(st)-1);deletionStatus(st);});
}
function batchKey(m){return `${m.station}/${m.session}/${m.tier}/${m.batch}`;}
function reportRecording(station, patch){
  const previous=recordingStatus.get(station)||{station,receivedSamples:0,persistedSamples:0,receivedBatches:0,persistedBatches:0,failedAttempts:0};
  const next={...previous,...patch,station,serverMs:Date.now()};
  recordingStatus.set(station,next);
  deletionStatus(station);
  emitToDashboards({type:'recording_status',...next});
}
function queueSnapshot(station){
  let queued=0;
  for(const item of pending.values())if(item.station===station)queued++;
  return queued;
}
function finishCompletion(st){
 const m=deferredCompletion.get(st);if(!m||queueSnapshot(st))return;deferredCompletion.delete(st);
 void saveCompletion(st,m).then(()=>{const d=devices.get(st);if(d)askIdle(st,d);}).catch(e=>console.error("completion_write_failed",String(e)));
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
          finishCompletion(item.station);
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
  let role=null, station=null, authenticated=false, authPending=false;
  const dc={id:++diagSerial,opened:Date.now(),lastRx:Date.now(),messages:0,role:null,station:null};diagConnections.set(ws,dc);
  netLog('net_open',ws);
  ws.on('error',err=>netLog('net_socket_error',ws,{error:String(err.message).slice(0,180)}));
  ws.on('message',async raw=>{dc.lastRx=Date.now();dc.messages++;
    let m; try {m=JSON.parse(raw.toString());} catch {send(ws,{type:'error',error:'invalid_json'});return;}
    if(!m || typeof m!=='object' || Array.isArray(m))return;
    if(authPending&&!authenticated)return;
    if (!authenticated) {
      if (m.type!=='hello' || !['device','dashboard'].includes(m.role)) {ws.close(1008,'auth required');return;}
      if(m.role==='device' && safeId(m.station) && tokenEq(m.token,deviceToken)) {
        authPending=true;try{await deletionReady;}catch{ws.close(1011,'deletion protection unavailable');return;}finally{authPending=false;}if(ws.readyState!==WebSocket.OPEN)return;
        authenticated=true;role='device';station=m.station;invalidateIdle(station);
        const old=devices.get(station); if(old && old!==ws) {netLog('net_replaced',old,{replacement:dc.id,station});old.close(1000,'replaced');}
        devices.set(station,ws);
      } else if(m.role==='dashboard') {
        authPending=true;
        try{
          const a=await verifyDashboard(m.token,ws,'initial');
          if(ws.readyState!==WebSocket.OPEN)return;
          dashboardAuth.set(ws,a);authenticated=true;role='dashboard';station=a.station;dashboards.add(ws);
        }catch(e){authFailure(ws,'initial',e);ws.close(1008,'unauthorized');return;}
        finally{authPending=false;}
      } else {ws.close(1008,'unauthorized');return;}
      dc.role=role;dc.station=station;netLog('net_auth',ws);send(ws,{type:'hello_ack',serverMs:Date.now(),...(role==='dashboard'?{instructor:dashboardAuth.get(ws).instructor,station,expiresMs:dashboardAuth.get(ws).expiresMs}:{})});if(role==='dashboard')for(const status of recordingStatus.values())dashboardReply(ws,status.station,{type:'recording_status',...status});return;
    }
    if (role==='dashboard') {
      if(m.type==='clock_sync'){
        const a=dashboardAuth.get(ws);
        if(!a||a.expiresMs<=Date.now()){ws.close(1008,'expired token');return;}
        if(Number.isSafeInteger(m.requestId)&&m.requestId>0)send(ws,{type:'clock_sync_ack',requestId:m.requestId,serverMs:Date.now()});
        return;
      }
      if(m.type==='renew_auth'){
        if(authPending)return;
        authPending=true;
        try{
          const previous=dashboardAuth.get(ws), a=await verifyDashboard(m.token,ws,'renewal');
          if(ws.readyState!==WebSocket.OPEN)return;
          if(a.uid!==previous.uid||a.station!==previous.station||a.instructor!==previous.instructor)throw Error('authorization changed');
          dashboardAuth.set(ws,a);netLog('net_auth_renewed',ws);send(ws,{type:'auth_renewed',expiresMs:a.expiresMs});
        }catch(e){dashboardAuth.delete(ws);authFailure(ws,'renewal',e);ws.close(1008,'renewal rejected');}
        finally{authPending=false;}return;
      }
      if(['annotation_get','annotation_save'].includes(m.type)){
        const reply=obj=>dashboardReply(ws,m.station,{...obj,station:m.station,session:m.session,tier:m.tier,requestId:m.requestId});
        if(!authorized(ws,m.station)){send(ws,{type:'annotation_error',station:m.station,session:m.session,tier:m.tier,requestId:m.requestId,error:'station_not_authorized'});return;}
        try{
          if(!safeId(m.session)||![1,2,3].includes(m.tier))throw Error('Invalid recording identifier');
          const key=deletionKey(m.station,m.session,m.tier);
          const ref=db.ref(`experimentalRecordingAnnotations/${key}`);
          if(!(await db.ref(`${recordingRoot(m.station,m.session,m.tier)}/batches/b00000000`).once('value')).exists())throw Error('Recording not found');
          if(m.type==='annotation_save'){
            await serializeExperiment(m.station,async()=>{
            if(typeof m.name!=='string'||m.name.length>200||typeof m.notes!=='string'||m.notes.length>4000)throw Error('Invalid annotation fields');
            if(!authorized(ws,m.station)||deleting.has(m.station)||deletionKeys.has(key))throw Error('Recording unavailable');
            const experimentId=m.experimentId??'';
            if(typeof experimentId!=='string'||(experimentId&&!/^[-A-Za-z0-9_]{1,128}$/.test(experimentId)))throw Error('Invalid experiment ID');
            if(experimentId&&!(await db.ref(`experimentalExperiments/${m.station}/${experimentId}`).once('value')).exists())throw Error('Experiment not found for this station');
            if(!authorized(ws,m.station)||deleting.has(m.station)||deletionKeys.has(key))throw Error('Recording unavailable');
            const value={name:m.name.trim(),notes:m.notes.trim(),experimentId};
            await ref.set(value);
            const confirmed=(await ref.once('value')).val();
            if(!confirmed||confirmed.name!==value.name||confirmed.notes!==value.notes||confirmed.experimentId!==value.experimentId)throw Error('Annotation save not confirmed');
            reply({type:'annotation_result',annotation:confirmed});
            });
          }else{
            const value=(await ref.once('value')).val();
            reply({type:'annotation_result',annotation:value||{name:'',notes:''}});
          }
        }catch(e){reply({type:'annotation_error',error:String(e.message||e).slice(0,160)});}
        return;
      }
      if(['experiment_create','experiment_list','experiment_edit','experiment_delete'].includes(m.type)){
        const reply=(obj)=>dashboardReply(ws,m.station,{...obj,station:m.station,requestId:m.requestId});
        if(!authorized(ws,m.station)){send(ws,{type:'experiment_error',requestId:m.requestId,station:m.station,error:'station_not_authorized'});return;}
        try{
          const ref=db.ref(`experimentalExperiments/${m.station}`);
          if(m.type!=='experiment_list'){
            await serializeExperiment(m.station,async()=>{
              if(!authorized(ws,m.station))throw Error('station_not_authorized');
              if(m.type==='experiment_create'){
                const fields=validateExperiment(m);
                const record={...fields,createdMs:Date.now(),createdBy:dashboardAuth.get(ws).uid};
                const child=ref.push();await child.set(record);
                reply({type:'experiment_created',id:child.key});return;
              }
              if(typeof m.id!=='string'||!/^[-A-Za-z0-9_]{1,128}$/.test(m.id))throw Error('Invalid experiment ID');
              const editing=m.type==='experiment_edit',fields=editing?validateExperiment(m):null;
              const child=ref.child(m.id);
              const existing=await child.once('value');
              if(!existing.exists())throw Error('Experiment not found before modification');
              if(!authorized(ws,m.station))throw Error('station_not_authorized');
              if(editing)await child.update(fields);else {
                const annotations=(await db.ref(`experimentalRecordingAnnotations/${m.station}`).once('value')).val()||{};
                const updates=experimentDeletionUpdates(m.station,m.id,annotations);
                if(!authorized(ws,m.station))throw Error('station_not_authorized');
                await db.ref().update(updates);
              }
              const confirmed=await child.once('value');
              if(editing){
                const value=confirmed.val();
                if(!confirmed.exists()||!value||Object.entries(fields).some(([k,v])=>value[k]!==v))throw Error('Experiment edit not confirmed by readback');
              }else if(confirmed.exists())throw Error('Experiment deletion not confirmed by readback');
              reply({type:editing?'experiment_edited':'experiment_deleted',id:m.id});
            });
          }else{
            const snap=await ref.once('value');const records=[];
            snap.forEach(child=>{const v=child.val();records.push({id:child.key,name:v.name,question:v.question,description:v.description,createdMs:v.createdMs});});
            try{await associatedRecordings(m.station,records);}catch(e){for(const r of records)delete r.associatedRecordings;console.error('experiment_associations_read_failed',String(e));}
            reply({type:'experiment_list_result',records});
          }
        }catch(e){reply({type:'experiment_error',error:'Experiment request failed: '+String(e.message||e).slice(0,160)});}
        return;
      }
      if(!authorized(ws,m.station)){
        if(dashboardAuth.get(ws)?.expiresMs<=Date.now()){ws.close(1008,'expired token');return;}
        send(ws,{type:['list_recordings','get_recording_page','delete_recordings','export_eligibility'].includes(m.type)?'recordings_error':'error',requestId:m.requestId??null,error:'station_not_authorized'});return;
      }
      if(m.type==='delete_recordings'){try{const results=await deleteRecordings(ws,m);dashboardReply(ws,m.station,{type:'deletion_result',requestId:m.requestId,results});}catch(e){dashboardReply(ws,m.station,{type:'recordings_error',requestId:m.requestId,error:e.message});}return;}
      if(m.type==='manage' && safeId(m.station) && safeId(m.requestId) && ['snapshot','application','transport','combined','restart','monitor_on','monitor_off','live_on','live_off'].includes(m.operation)){
        if(deleting.has(m.station)&&m.operation!=='snapshot'){send(ws,{type:'error',error:'deletion_in_progress'});return;}
        const device=devices.get(m.station);
        if(!device || device.readyState!==WebSocket.OPEN){send(ws,{type:'management_status',station:m.station,requestId:m.requestId,phase:'offline'});return;}
        if(m.operation==='snapshot')idleRequests.set(m.station,m.requestId);else invalidateIdle(m.station);
        send(device,{type:'command',action:'manage',operation:m.operation,requestId:m.requestId});return;
      }

      if(m.type==='export_eligibility' && safeId(m.station)){
        try{if(!safeId(m.session)||![1,2,3].includes(m.tier))throw Error('Invalid recording identifier');
          const result=await exportEligibility(m.station,m.session,m.tier);
          dashboardReply(ws,m.station,{type:'export_eligibility_result',requestId:m.requestId,station:m.station,session:m.session,tier:m.tier,...result});
        }catch(e){dashboardReply(ws,m.station,{type:'recordings_error',requestId:m.requestId,error:e.message});}return;
      }
      if(m.type==='list_recordings' && safeId(m.station)){
        void listRecordings(m.station).then(records=>libraryNames(m.station,records)).then(records=>dashboardReply(ws,m.station,{type:'recordings_list',station:m.station,requestId:m.requestId??null,records,safe:deletionSafe(m.station)}))
          .catch(e=>dashboardReply(ws,m.station,{type:'recordings_error',requestId:m.requestId??null,error:String(e.message||e).slice(0,180)}));return;
      }
      if(m.type==='get_recording_page' && safeId(m.station) && safeId(m.session) && [1,2,3].includes(m.tier)){
        const after=m.afterBatch===null||m.afterBatch===undefined?null:Number(m.afterBatch);
        void recordingPage(m.station,m.session,m.tier,after,m.limit)
          .then(page=>dashboardReply(ws,m.station,{type:'recording_page',requestId:m.requestId??null,...page}))
          .catch(e=>dashboardReply(ws,m.station,{type:'recordings_error',requestId:m.requestId??null,error:String(e.message||e).slice(0,180)}));return;
      }
      if(m.type==='get_recording_status' && safeId(m.station)){send(ws,{type:'recording_status',...(recordingStatus.get(m.station)||{station:m.station,state:'unknown',queuedBatches:queueSnapshot(m.station)})});return;}
      if(m.type==='set_detector' && safeId(m.station) && validDetector(m.detector)) {
        const d=m.detector;
        db.ref(`experimentalDetectors/${m.station}`).set(d).then(()=>{
          detectors.set(m.station,{...d,wasActive:false,lastAlert:0});
          dashboardReply(ws,m.station,{type:'detector_saved',station:m.station,detector:d});
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
          if(deleting.has(m.station)||(m.action==='start'&&(deferredCompletion.has(m.station)||completionWrites.get(m.station)))){send(ws,{type:'error',error:'recording_finalization_in_progress'});return;}
          invalidateIdle(m.station);
          send(device,{type:'command',action:m.action,tier:m.tier,durationMs:Math.min(m.tier===1?21600000:(m.tier===3?300000:7200000),Math.max(1000,Number(m.durationMs)||300000))});
          send(ws,{type:'recording_command_sent',station:m.station,action:m.action,serverMs:Date.now()});
        }
      }
      return;
    }
    if(role==='device' && m.type==='management_status'){if(m.station===station&&devices.get(station)===ws){if(m.requestId===idleRequests.get(station)){idleRequests.delete(station);if(m.safe===true&&m.mode===0)idleProof.set(station,ws);else idleProof.delete(station);}else if(m.safe!==true||m.mode!==0)idleProof.delete(station);deletionStatus(station);emitToDashboards({...m,station,serverMs:Date.now()});}return;}
    if(role==='device'&&devices.get(station)!==ws)return;
    if (role==='device' && m.type==='live_sample') {
      if (m.station===station && [0,1,3].includes(m.tier) && safeId(m.session) && m.sample && Number.isSafeInteger(m.sample.seq)) {
        const now=Date.now();
        if(devices.get(station)!==ws || !freshLive(m,now))return;
        emitLive({type:'live_sample',station,session:m.session,tier:m.tier,sample:m.sample,acquiredServerMs:m.acquiredServerMs,serverReceivedMs:now});
        evaluate(station,m.sample);
      }
      return;
    }
    if (role==='device' && m.type==='batch') {
      if (!validBatch(m) || m.station!==station) {send(ws,{type:'error',error:'invalid_batch'});return;}
      if(deletionKeys.has(deletionKey(station,m.session,m.tier))){send(ws,{type:'ack',session:m.session,tier:m.tier,batch:m.batch,persistence:'deleted'});return;}
      invalidateIdle(station);
      const receivedMs=Date.now();
      // Durable recording batches are never forwarded as current monitoring.
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
        deviceSentMs:m.deviceSentMs??null,serverReceivedMs:receivedMs,serverWrittenMs:null,
        ...(m.tier===3 && m.transportDiag && typeof m.transportDiag==='object' ? {transportDiag:m.transportDiag} : {})};
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
        reportRecording(station,{state:state.state==='started'?'receiving':state.state,receivedSamples:state.receivedSamples+m.samples.length,
          receivedBatches:state.receivedBatches+1,queuedBatches:queueSnapshot(station)});
      }
      send(ws,{type:'ack',session:m.session,tier:m.tier,batch:m.batch,receivedMs,acceptedMs:Date.now(),persistence:'queued'});
      emitToDashboards({type:'batch_received',station,session:m.session,tier:m.tier,batch:m.batch,count:m.samples.length,receivedMs});
      startWorker();
    } else if (role==='device' && m.type==='recording_event' && m.station===station && safeId(m.session) && [1,2,3].includes(m.tier) && ['started','draining','completed'].includes(m.state)) {
      if(deletionKeys.has(deletionKey(station,m.session,m.tier))){if(m.state==='completed')send(ws,{type:'recording_event_ack',station,session:m.session,tier:m.tier,state:'completed'});return;}
      invalidateIdle(station);
      const old=recordingStatus.get(station);
      if(old?.session!==m.session)reportRecording(station,{session:m.session,tier:m.tier,state:m.state,durationMs:m.durationMs??null,startedMs:Date.now(),receivedSamples:0,persistedSamples:0,receivedBatches:0,persistedBatches:0,failedAttempts:0,queuedBatches:queueSnapshot(station),deviceSamples:null,deviceLost:null});
      else if(old.state!=='completed' || m.state==='completed'){
        // A reconnect may replay 'started' after batches arrived or after draining began.
        // Never move an existing session backward in its lifecycle.
        const rank={started:1,receiving:2,draining:3,completed:4};
        const nextState=(rank[m.state]||0)>=(rank[old.state]||0)?m.state:old.state;
        reportRecording(station,{state:nextState,startedMs:old.startedMs??Date.now(),durationMs:m.durationMs??old.durationMs,deviceSamples:m.state==='completed'?m.samples:(old.deviceSamples??null),deviceLost:m.state==='completed'?m.lost:(old.deviceLost??null),queuedBatches:queueSnapshot(station)});
      }
      // Completion acknowledgement is explicit; the ES may retry safely.
      if(m.state==='completed'){
        deferredCompletion.set(station,m);
        send(ws,{type:'recording_event_ack',station,session:m.session,tier:m.tier,state:'completed'});
        finishCompletion(station);
      }
    } else if (role==='device' && m.type==='diag' && m.station===station) {
      process.stdout.write(JSON.stringify({event:'device_diag',station,serverMs:Date.now(),data:m})+'\n');
      emitToDashboards({type:'diag',station,serverMs:Date.now(),data:m});
    }
  });
  ws.on('close',(code,reason)=>{netLog('net_close',ws,{code,reason:reason.toString().slice(0,120),lifetimeMs:Date.now()-dc.opened,lastRxAgeMs:Date.now()-dc.lastRx,messages:dc.messages,pending:ws.bufferedAmount});if(role==='dashboard') dashboards.delete(ws);if(role==='device'&&devices.get(station)===ws){devices.delete(station);invalidateIdle(station);}});
});
server.listen(PORT,()=>process.stdout.write(`ENP162 experimental relay ${SERVER_REVISION} listening on ${PORT}\n`));
