import { promises as fs } from "fs";
import { resolve, relative, extname } from "path";
import fg from "fast-glob";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const cwd = process.cwd();
const scriptRelPath = relative(cwd, __filename).replace(/\\/g, '/');

function toPosixPath(path) {
  return path.replace(/\\/g, '/');
}

function getFileType(ext) {
  ext = ext.toLowerCase();
  if (ext === '.js' || ext === '.kt' || ext === '.sh') return 'code';
  if (ext === '.xml') return 'xml';
  if (ext === '.html') return 'html';
  return null;
}

function buildExactPathCommentRegex(commentLine) {
  const escaped = commentLine
    .trim()
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/-->$/, '-->\\s*$');
  return new RegExp(`^\\s*${escaped}`, 'm');
}

async function processFile(filePath) {
  const absPath = resolve(filePath);
  const relPath = toPosixPath(relative(cwd, absPath));
  const ext = extname(filePath);
  const fileType = getFileType(ext);
  if (!fileType) return;

  let content;
  try {
    content = await fs.readFile(absPath, 'utf8');
  } catch (err) {
    console.error(`Failed to read: ${relPath}`, err);
    return;
  }

  // Choose the correct comment prefix
  let commentLine;
  if (fileType === 'xml' || fileType === 'html') {
    commentLine = `<!-- @path: ${relPath} -->\n`;
  } else if (ext === '.sh') {
    commentLine = `# @path: ${relPath}\n`;
  } else {
    commentLine = `// @path: ${relPath}\n`;
  }

  const pathCommentRegex = buildExactPathCommentRegex(commentLine);
  const header = content.slice(0, 500);

  if (!pathCommentRegex.test(header)) {
    // XML declaration handling
    if ((fileType === 'xml' || fileType === 'html') && content.startsWith('<?xml')) {
      const endDecl = content.indexOf('?>');
      if (endDecl !== -1) {
        const before = content.slice(0, endDecl + 2);
        const after = content.slice(endDecl + 2).replace(/^\r?\n/, '');
        content = `${before}\n${commentLine}${after}`;
      } else {
        content = `${commentLine}${content}`;
      }
    } else {
      content = `${commentLine}${content}`;
    }
    console.log(`Prepended @path to: ${relPath}`);
  } else {
    console.log(`Skipping (already has @path): ${relPath}`);
  }

  if (fileType === 'code') {
    content = content
      // Keep only /*…*/ blocks if they contain @path:
      .replace(/\/\*[\s\S]*?\*\//g, m => m.includes('@path:') ? m : '')
      // Keep only // lines if they contain @path:
      .replace(/^\s*\/\/.*$/gm, line => line.includes('@path:') ? line : '')
      // Keep only # lines if they contain @path (for .sh):
      .replace(/^\s*#.*$/gm, line => line.includes('@path:') ? line : '')
      // Remove inline // comments that don’t contain @path:
      .replace(/([^:"'\n])\/\/(?!.*@path:).*$/gm, (_, p) => p.trimEnd())
      // Remove citation marks:
      .replace(/\[cite\s*:\s*\d+(?:\s*,\s*\d+)*\]/g, '')
      .replace(/\[cite(?:_start|_end)?\]/g, '')
      // Remove all span markers (start or end), even if nested/misaligned:
      .replace(/\[span_\d+\]\((?:start|end)_span\)/g, '')
      // Drop any now-empty lines:
      .replace(/^\s*$/gm, '');
  } else if (fileType === 'xml' || fileType === 'html') {
    content = content
      // Keep only <!--…--> comments with @path:
      .replace(/<!--[\s\S]*?-->/g, m => m.includes('@path:') ? m : '');
  }

  // Debug: warn if any span markers still exist
  if (content.includes('[span_')) {
    console.warn(`⚠️ Unremoved spans in ${relPath}`);
  }

  // Collapse excessive blank lines to at most two in a row
  content = content.replace(/\n{3,}/g, '\n\n');

  // Ensure final newline
  if (!content.endsWith('\n')) content += '\n';

  try {
    await fs.writeFile(absPath, content, 'utf8');
    console.log(`Cleaned: ${relPath}`);
  } catch (err) {
    console.error(`Failed to write: ${relPath}`, err);
  }
}

async function main() {
  const pattern = process.argv[2] || '**/*.{js,kt,xml,html,sh}';
  let entries = await fg(pattern, {
    dot: true,
    ignore: ['node_modules/**'],
  });

  entries = entries
    .map(toPosixPath)
    .filter(f => f !== scriptRelPath);

  if (!entries.length) {
    console.warn('No files found for pattern:', pattern);
    return;
  }

  await Promise.allSettled(entries.map(processFile));

  console.log('All done!');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});