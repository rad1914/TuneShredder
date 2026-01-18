export async function renderProgress(done, total, label = '') {
  const width = 30;
  // Clamp ratio between 0 and 1 to prevent visual glitches
  const ratio = total ? Math.min(Math.max(done / total, 0), 1) : 0;
  const filled = Math.round(ratio * width);

  // ANSI Color Codes
  const c = {
    green: '\x1b[32m',
    dim: '\x1b[90m',
    bold: '\x1b[1m',
    reset: '\x1b[0m'
  };

  // Build the bar using block characters
  const barFilled = '█'.repeat(filled);
  const barEmpty = '░'.repeat(width - filled);
  const bar = `${c.green}${barFilled}${c.dim}${barEmpty}${c.reset}`;
  
  const pct = String(Math.round(ratio * 100)).padStart(3, ' ');

  process.stdout.write(`\r${bar} ${c.bold}${pct}%${c.reset} ${c.dim}(${done}/${total})${c.reset} ${label}`);
  
  if (done >= total) process.stdout.write('\n');
}