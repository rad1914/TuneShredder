// @path: progress.js
export const createProgress = (total, label = "progress") => {
  let n = 0;
  const render = () => {
    const pct = total ? Math.floor((n / total) * 100) : 0;
    process.stderr.write(`\r${label}: ${n}/${total} (${pct}%)`);
    if (n === total) process.stderr.write("\n");
  };
  return {
    tick: () => { n++; render(); },
    set: (x) => { n = x; render(); }
  };
};
