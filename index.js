// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import FFT from 'fft.js';

const CFG = { sr:22050, ch:1, win:4096, hop:512, top:24, zone:55, pairs:6, sec:85, fq:6, dtq:2, bucket:250 };

const hann = (() => {
  const w = new Float32Array(CFG.win);
  for (let i=0;i<CFG.win;i++) w[i] = 0.5*(1-Math.cos((2*Math.PI*i)/(CFG.win-1)));
  return w;
})();

const ffmpegF32 = file => new Promise((res, rej) => {
  const p = spawn('ffmpeg',['-hide_banner','-loglevel','error','-t',String(CFG.sec),'-i',file,'-ac',String(CFG.ch),'-ar',String(CFG.sr),'-f','f32le','-'],{stdio:['ignore','pipe','pipe']});
  const chunks=[]; let err='';
  p.stdout.on('data',c=>chunks.push(c));
  p.stderr.on('data',c=>err+=c);
  p.on('close',c=> c===0 ? res(new Float32Array(Buffer.concat(chunks).buffer, Buffer.concat(chunks).byteOffset, (Buffer.concat(chunks).length/4)|0)) : rej(new Error('ffmpeg failed: '+err.trim())));
});

function stftMags(x){
  const half = CFG.win>>1;
  const frames = Math.max(0,(((x.length-CFG.win)/CFG.hop|0)+1));
  const fft = new FFT(CFG.win);
  const complex = fft.createComplexArray();
  const inbuf = new Float64Array(CFG.win);
  const out = new Array(frames);
  for (let pos=0,t=0; pos+CFG.win<=x.length; pos+=CFG.hop, t++){
    for (let i=0;i<CFG.win;i++) inbuf[i]=x[pos+i]*hann[i];
    fft.realTransform(complex,inbuf);
    const mags = new Float32Array(half);
    for (let b=0;b<half;b++){ const re=complex[b*2]||0, im=complex[b*2+1]||0; mags[b]=Math.log1p(Math.hypot(re,im)); }
    out[t]=mags;
  }
  return out;
}

function topK(row,K){
  const ids=new Int32Array(K).fill(-1), vals=new Float32Array(K).fill(-Infinity);
  for (let i=0;i<row.length;i++){
    const v=row[i]; if(!(v>0)) continue;
    let mi=0; for (let j=1;j<K;j++) if (vals[j]<vals[mi]) mi=j;
    if (v>vals[mi]) { vals[mi]=v; ids[mi]=i; }
  }
  const r=[];
  for (let j=0;j<K;j++) if (ids[j]!==-1) r.push(ids[j]);
  return r;
}

function hashes(mags){
  const pk = mags.map(f=>topK(f,CFG.top));
  const q = pk.map(r=>r.map(f=>Math.round(f/CFG.fq)));
  const out=[];
  for (let t=0;t<q.length;t++){
    const a=q[t]; if(!a.length) continue;
    for (const f1q of a){
      const bestF=new Int32Array(CFG.pairs).fill(-1), bestDt=new Int32Array(CFG.pairs), bestMag=new Float32Array(CFG.pairs).fill(-Infinity);
      for (let dt=1; dt<=CFG.zone; dt++){
        const t2=t+dt; if (t2>=q.length) break;
        for (const f2q of q[t2]){
          const b1=Math.min(mags[t].length-1,Math.max(0,f1q)), b2=Math.min(mags[t2].length-1,Math.max(0,f2q));
          const score=(mags[t][b1]||1e-9)*(mags[t2][b2]||1e-9);
          for (let s=0;s<CFG.pairs;s++){
            if (score>bestMag[s]){
              for (let k=CFG.pairs-1;k>s;k--){ bestMag[k]=bestMag[k-1]; bestF[k]=bestF[k-1]; bestDt[k]=bestDt[k-1]; }
              bestMag[s]=score; bestF[s]=f2q; bestDt[s]=dt; break;
            }
          }
        }
      }
      for (let p=0;p<CFG.pairs;p++) if (bestF[p]!==-1) out.push({ key:`${f1q}-${bestF[p]}-${Math.round(bestDt[p]/CFG.dtq)}`, t });
    }
  }
  return out;
}

function fingerprintMap(mags,fileId){
  const m=Object.create(null);
  for (const {key,t} of hashes(mags)){
    const a = (m[key] ||= []);
    if (a.length < CFG.bucket) a.push([fileId,t]);
  }
  return m;
}

async function fingerprint(filePath,fileId){
  const x = await ffmpegF32(filePath);
  const mags = stftMags(x);
  return fingerprintMap(mags,fileId);
}

async function atomicWrite(p,s){
  const tmp = p + '.' + process.pid + '.' + Date.now() + '.' + Math.random().toString(36).slice(2) + '.tmp';
  await fs.writeFile(tmp,s,'utf8');
  await fs.rename(tmp,p);
  try { await fs.unlink(tmp); } catch {}
}

const runWorker = (filePath,fileId) => new Promise((res,rej)=>{
  const w = new Worker(new URL(import.meta.url),{workerData:{filePath,fileId}});
  w.once('message',m => m?.ok ? res(m.map) : rej(new Error(m?.error||'worker failed')));
  w.once('error',rej);
  w.once('exit',c => { if(c!==0) rej(new Error('worker exit '+c)); });
});

export async function buildIndex(dir,outFile='index.json'){
  let files = (await fs.readdir(dir)).filter(n=>/\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  let merged = Object.create(null), meta = [];
  try { const parsed = JSON.parse(await fs.readFile(outFile,'utf8')); if (parsed?.index) merged = parsed.index; if (Array.isArray(parsed?.meta)) meta = parsed.meta.slice(); } catch {}
  if (meta.length){ const done=new Set(meta); files = files.filter(f=>!done.has(f)); }
  const conc = Math.max(1, Math.min(os.cpus().length||4, files.length));
  const merge = map => { for (const k in map){ const dst = (merged[k] ||= []), src = map[k]; for (let j=0;j<src.length && dst.length<CFG.bucket; j++) dst.push(src[j]); } };
  let idx=0, done=0;
  const workerLoop = async ()=>{
    for (;;){
      const name = files[idx++]; if (!name) return;
      const fileId = meta.length;
      try { const map = await runWorker(path.join(dir,name), fileId); merge(map); meta.push(name); } catch {}
      done++; await atomicWrite(outFile, JSON.stringify({ index: merged, meta })); console.log(`progress: ${done}/${files.length} - ${name}`);
    }
  };
  await Promise.all(Array.from({length:conc}, workerLoop));
  for (const k of Object.keys(merged)){
    const perFile = Object.create(null);
    for (const it of merged[k]){
      if (!Array.isArray(it) || it.length<2) continue;
      const [fid,t] = it;
      (perFile[fid] ||= []).push(t);
    }
    const outList=[];
    for (const fidStr of Object.keys(perFile)){
      const fid = Number(fidStr);
      const times = perFile[fid].sort((a,b)=>a-b);
      if (!times.length) continue;
      const deltas = new Array(times.length);
      deltas[0]=times[0];
      for (let i=1;i<times.length;i++) deltas[i]=times[i]-times[i-1];
      outList.push([fid,deltas]);
    }
    merged[k]=outList;
  }
  await atomicWrite(outFile, JSON.stringify({ index: merged, meta }));
}

if (!isMainThread){
  (async ()=>{
    try {
      const { filePath, fileId } = workerData;
      parentPort.postMessage({ ok:true, map: await fingerprint(filePath, fileId) });
    } catch (e) { parentPort.postMessage({ ok:false, error: e?.message || String(e) }); }
  })();
}

if (isMainThread && import.meta.url === `file://${process.argv[1]}`){
  (async ()=>{
    const [, , cmd, dir, out] = process.argv;
    if (cmd !== 'build' || !dir) { console.error('usage: node fingerprint.js build <dir> [out.json]'); process.exit(2); }
    await buildIndex(dir, out || 'index.json');
  })().catch(e=>{ console.error(e); process.exit(1); });
}
