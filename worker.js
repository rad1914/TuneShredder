import { parentPort, workerData as C } from "worker_threads";
import { spawn } from "child_process";
import FFT from "fft.js";

const WIN = (() => {
  const w = new Float32Array(C.win), n = w.length - 1;
  for (let i = 0; i < w.length; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / n));
  return w;
})();

const H = (f1, f2, dt) => (((f1 & 2047) << 17) | ((f2 & 2047) << 6) | (dt & 63)) | 0;

const makePeaks = () => {
  const fft = new FFT(C.win), out = fft.createComplexArray();
  const mags = new Float32Array(C.win / 2);
  const bins = new Int32Array(C.top);
  const mag = new Float32Array(C.top);
  const inp = new Float32Array(C.win);

  return (frame) => {
    for (let i = 0; i < C.win; i++) inp[i] = frame[i] * WIN[i];
    fft.realTransform(out, inp);
    fft.completeSpectrum(out);

    for (let k = 0; k < mags.length; k++) {
      const re = out[2 * k], im = out[2 * k + 1];
      mags[k] = re * re + im * im;
    }

    bins.fill(0);
    mag.fill(0);

    for (let k = 2; k < mags.length - 2; k++) {
      const a = mags[k];
      if (a < C.min) continue;
      if (!(a > mags[k - 1] && a > mags[k + 1] && a > mags[k - 2] && a > mags[k + 2])) continue;

      let ins = -1;
      for (let i = 0; i < C.top; i++) if (a > mag[i]) { ins = i; break; }
      if (ins < 0) continue;

      for (let i = C.top - 1; i > ins; i--) { mag[i] = mag[i - 1]; bins[i] = bins[i - 1]; }
      mag[ins] = a; bins[ins] = k;
    }

    let n = 0;
    while (n < C.top && mag[n] > 0) n++;
    return { bins, n };
  };
};

const peaks = makePeaks();

const eachFrame = (file, cb) =>
  new Promise((res, rej) => {
    const threads = Math.max(1, Math.floor(C.win / 1024));
    const p = spawn(
      "ffmpeg",
      [
        "-v", "error", "-nostdin",
        "-threads", String(threads),
        "-i", file,
        "-t", String(C.dur),
        "-ac", String(C.ch),
        "-ar", String(C.sr),
        "-vn", "-sn", "-dn",
        "-f", "f32le",
        "-acodec", "pcm_f32le",
        "pipe:1",
      ],
      { stdio: ["ignore", "pipe", "pipe"] }
    );

    p.on("error", (err) => rej(err));

    p.stderr.on("data", () => {});

    const bps = 4, { win, hop } = C, hopBytes = hop * bps;
    const ringBuf = Buffer.allocUnsafe(2 * win * bps);
    const ring = new Float32Array(ringBuf.buffer, ringBuf.byteOffset, 2 * win);
    const hopBuf = Buffer.allocUnsafe(hopBytes);
    const frame = new Float32Array(win);

    let hopFill = 0, filled = 0, head = 0, t = 0;

    const emit = () => {
      const a = head, b = win - a;
      for (let i = 0; i < b; i++) frame[i] = ring[a + i];
      for (let i = 0; i < a; i++) frame[b + i] = ring[i];
      cb(frame, t++);
    };

    p.stdout.on("data", (chunk) => {
      for (let off = 0; off < chunk.length;) {
        const take = Math.min(hopBytes - hopFill, chunk.length - off);
        chunk.copy(hopBuf, hopFill, off, off + take);
        hopFill += take;
        off += take;
        if (hopFill !== hopBytes) continue;

        hopFill = 0;
        const v = new Float32Array(hopBuf.buffer, hopBuf.byteOffset, hop);
        ring.set(v, head);
        ring.set(v, head + win);
        head = (head + hop) % win;
        filled = Math.min(win, filled + hop);
        if (filled === win) emit();
      }
    });

    p.on("close", (c) => (c ? rej(Error("ffmpeg decode failed")) : res(t)));
  });

parentPort.on("message", async (m) => {
  if (m.type === "drain") return setTimeout(() => process.exit(0), 200);
  if (m.type !== "file") return;

  const { path: file, id, name } = m;

  try {
    parentPort.postMessage({ type: "log", msg: `\r${name}\x1b[K` });

    const B = 50_000, Hh = new Int32Array(B), Tt = new Int32Array(B);
    let bn = 0;

    const flush = () => {
      if (!bn) return;
      parentPort.postMessage({
        type: "batches",
        id,
        Hh: Array.from(Hh.subarray(0, bn)),
        Tt: Array.from(Tt.subarray(0, bn)),
        n: bn,
      });
      bn = 0;
    };

    const add = (h, t) => {
      Hh[bn] = h;
      Tt[bn] = t;
      if (++bn === B) flush();
    };

    const ringBins = Array.from({ length: C.zone + 1 }, () => new Int32Array(C.top));
    const ringN = new Int8Array(C.zone + 1);
    let rp = 0;

    await eachFrame(file, (frame, t) => {
      const p = peaks(frame);

      ringBins[rp].fill(0);
      if (p.n) ringBins[rp].set(p.bins.subarray(0, p.n));
      ringN[rp] = p.n;

      if (p.n && t % C.anchorEvery === 0) {
        const f2n = Math.min(C.fan, p.n), len = ringBins.length;
        for (let back = 1; back <= C.zone; back++) {
          const pos = (rp - back + len) % len, nPrev = ringN[pos];
          if (!nPrev) continue;
          const prev = ringBins[pos], tt = t - back;
          for (let i = 0; i < nPrev; i++)
            for (let j = 0; j < f2n; j++)
              add(H(prev[i], p.bins[j], back), tt);
        }
      }

      rp = (rp + 1) % ringBins.length;
    });

    flush();

    parentPort.postMessage({ type: "done", name });
  } catch (e) {
    parentPort.postMessage({ type: "log", msg: `\rERROR ${name}: ${String(e)}\n` });
  }
});
