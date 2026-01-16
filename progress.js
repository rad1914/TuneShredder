// @path: progress.js
export function createProgress(total, prefix = "") {
  let i = 0;
  const width = 28;
  const redraw = () => {
    const pct = total ? (i / total) : 0;
    const filled = Math.round(pct * width);
    const bar = "[" + "=".repeat(filled) + " ".repeat(Math.max(0, width - filled)) + "]";
    const pctText = total ? ` ${Math.round(pct * 100)}%` : "";
    process.stderr.write(`\r${prefix}${bar}${pctText} ${i}/${total}`);
  };
  return {
    tick(n = 1) {
      i += n;
      if (i > total) i = total;
      redraw();
    },
    done() {
      i = total;
      redraw();
      process.stderr.write("\n");
    }
  };
}
