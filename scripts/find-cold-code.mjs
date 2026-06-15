// Find slides that drop into a code fence with little/no prose, AND whose
// previous slide isn't a prose concept lead-in. Helps enforce "explain before
// you show code" across the deck.
//
//   node scripts/find-cold-code.mjs [deckName]   (default: temporal-fundamentals)
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const deck = process.argv[2] || 'temporal-fundamentals';
const slides = readFileSync(join(ROOT, 'slides', deck, 'slides.md'), 'utf8').split(/^---$/m);

function info(s) {
  const lines = s.split('\n');
  const hi = lines.findIndex((l) => /^#{1,3}\s/.test(l));
  const heading = hi >= 0 ? lines[hi].replace(/^#+\s/, '').trim() : '';
  let prose = 0, fence = false, fenceFirst = false, hasTable = false;
  for (let i = hi < 0 ? 0 : hi + 1; i < lines.length; i++) {
    const l = lines[i].trim();
    if (l.startsWith('```')) { fence = true; if (prose <= 1) fenceFirst = true; break; }
    if (l.startsWith('|')) { hasTable = true; continue; }
    if (!l || l.startsWith('<!--') || l.startsWith('![') || l.startsWith('<') || l.startsWith('>')) continue;
    prose++;
  }
  const isLab = /_class: (lab|section|day|takeaway)/.test(s);
  const isBash = /```(bash|sh|console)/.test(s);
  // A concept slide = prose-rich OR a table, and not itself code-first.
  return { heading, prose, fence, fenceFirst, isProse: !fence && (prose >= 2 || hasTable), isLab, isBash };
}

let cold = 0;
for (let i = 0; i < slides.length; i++) {
  const cur = info(slides[i]);
  if (!cur.fenceFirst || cur.isLab || cur.isBash) continue; // skip labs / bash slides
  const prev = i > 0 ? info(slides[i - 1]) : { isProse: false, isLab: true, heading: '' };
  const covered = prev.isProse && !prev.isLab; // previous slide is a concept lead-in
  if (!covered) { cold++; console.log(`COLD: "${cur.heading}"  (prev: "${prev.heading}")`); }
}
console.log(`\n${cold} code-first slide(s) without a concept lead-in.`);
