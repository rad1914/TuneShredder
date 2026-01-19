import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import FFT from 'fft.js';

const CFG = {
  sr:22050, ch:1, win:4096, hop:512,
  top:24, zone:55, pairs:6, sec:85,
  fq:6, dtq:2, bucket:250
};

const hann = Float32Array.from(
  { length: CFG.win },
  (_, i) => 0.5 * (1 - Math.cos(2 * Math.PI * i / (CFG.win - 1)))
);

const ffmpegF32 = file => new Promise((res, rej) => {
  const p = spawn('ffmpeg', [
    '-loglevel','error','-t',CFG.sec,'-i',file,
    '-ac',CFG.ch,'-ar',CFG.sr,'-f','f32le','-'
  ]);
  const out = [], err = [];
  p.stdout.on('data', d => out.push(d));
  p.stderr.on('data', d => err.push(d));
  p.on('close', c =>
    c ? rej(new Error(Buffer.concat(err).toString() || 'ffmpeg'))
      : res(new Float32Array(Buffer.concat(out).buffer))
  );
});

function stft(x){
  const fft = new FFT(CFG.win);
  const buf = new Float64Array(CFG.win);
  const cmp = fft.createComplexArray();
  const half = CFG.win >> 1;
  const out = [];

  for(let p=0; p+CFG.win<=x.length; p+=CFG.hop){
    for(let i=0;i<CFG.win;i++) buf[i]=x[p+i]*hann[i];
    fft.realTransform(cmp, buf);
    const m = new Float32Array(half);
    for(let i=0;i<half;i++){
      const re=cmp[i*2]||0, im=cmp[i*2+1]||0;
      m[i]=Math.log1p(Math.hypot(re,im));
    }
    out.push(m);
  }
  return out;
}

const topK = (row, k) => {
  const idx=[], val=[];
  for(let i=0;i<row.length;i++){
    const v=row[i]; if(v<=0) continue;
    let j=val.findIndex(x=>v>x);
    if(j<0 && val.length<k) j=val.length;
    if(j>=0 && j<k){ idx.splice(j,0,i); val.splice(j,0,v); }
    if(idx.length>k){ idx.pop(); val.pop(); }
  }
  return idx;
};

function hashes(m){
  const q = m.map(r => topK(r, CFG.top).map(f=>Math.round(f/CFG.fq)));
  const out = [];
  for(let t=0;t<q.length;t++) for(const f1 of q[t]){
    const best=[];
    for(let dt=1;dt<=CFG.zone && t+dt<q.length;dt++){
      for(const f2 of q[t+dt]){
        const s=(m[t][f1]||1e-9)*(m[t+dt][f2]||1e-9);
        best.push([s,f2,dt]);
      }
    }
    best.sort((a,b)=>b[0]-a[0]);
    for(const [,f2,dt] of best.slice(0,CFG.pairs))
      out.push({ key:`${f1}-${f2}-${Math.round(dt/CFG.dtq)}`, t });
  }
  return out;
}

async function fingerprint(file, id){
  const mags = stft(await ffmpegF32(file));
  const map = Object.create(null);
  for(const {key,t} of hashes(mags)){
    const a = map[key] ||= [];
    if(a.length < CFG.bucket) a.push([id,t]);
  }
  return map;
}

const atomicWrite = async (p,s) => {
  const tmp = `${p}.${process.pid}.tmp`;
  await fs.writeFile(tmp,s);
  await fs.rename(tmp,p);
};

export async function buildIndex(dir, out='index.json'){
  let meta=[], index=Object.create(null);
  try{
    const j=JSON.parse(await fs.readFile(out,'utf8'));
    index=j.index||index; meta=j.meta||meta;
  }catch{}

  const done=new Set(meta);
  const files=(await fs.readdir(dir))
    .filter(f=>/\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(f))
    .filter(f=>!done.has(f));

  for(const name of files){
    const id=meta.length;
    try{
      const map=await fingerprint(path.join(dir,name), id);
      // write shard (one JSON object per line) for incremental processing
      await fs.appendFile('shard.json', JSON.stringify(map) + '\n');
      
      for(const k in map){
        const d=index[k] ||= [];
        for(const x of map[k]) if(d.length<CFG.bucket) d.push(x);
      }
      meta.push(name);
    }catch(e){ console.error('fail', name, e.message); }
    await atomicWrite(out, JSON.stringify({ index, meta }));
    console.log('done', name);
  }
}

if(import.meta.url===`file://${process.argv[1]}`){
  const [, , , dir, out] = process.argv;
  if(!dir) process.exit(console.error('node fp.js build <dir> [out]'));
  buildIndex(dir, out).catch(e=>{console.error(e);process.exit(1);});
}