#!/usr/bin/env node
// Slide overflow linter for the Marp decks.
//
// Marp renders every slide into a fixed 1280x720 box and silently lets content
// spill past the bottom edge - no error, no clip. This script renders each deck
// headlessly (reusing the Chromium that marp-cli already bundles) and measures
// the real rendered height of every <section>. Any slide whose content is taller
// than the slide box is reported, and the process exits non-zero.
//
// Usage:
//   node scripts/check-slides.mjs [deck ...]   # default: every deck under slides/
//   node scripts/check-slides.mjs why-temporal
//
// Env:
//   SLIDE_OVERFLOW_TOLERANCE   px of slack before a slide counts as overflowing
//                              (default 2)
//   PUPPETEER_EXECUTABLE_PATH / CHROME_PATH   override the browser binary

import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir, homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const SLIDES_DIR = join(ROOT, 'slides');
const THEME = join(SLIDES_DIR, 'themes', 'base.css');
const TOLERANCE = Number(process.env.SLIDE_OVERFLOW_TOLERANCE ?? 2);
const SLIDE_HEIGHT = 720;

function listDecks() {
  // A deck is any slides/<name>/ directory that has a slides.md.
  return readdirSync(SLIDES_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(SLIDES_DIR, d.name, 'slides.md')))
    .map((d) => d.name);
}

async function resolveBrowser(deckDir) {
  const override = process.env.PUPPETEER_EXECUTABLE_PATH || process.env.CHROME_PATH;
  if (override && existsSync(override)) return override;

  // Prefer the Chromium that @puppeteer/browsers has already downloaded for marp-cli.
  try {
    const { getInstalledBrowsers } = require(
      join(deckDir, 'node_modules', '@puppeteer', 'browsers'),
    );
    const cacheDir = join(homedir(), '.cache', 'puppeteer');
    const installed = existsSync(cacheDir) ? await getInstalledBrowsers({ cacheDir }) : [];
    const pick =
      installed.find((b) => b.browser === 'chrome-headless-shell') ||
      installed.find((b) => b.browser === 'chrome') ||
      installed[0];
    if (pick?.executablePath && existsSync(pick.executablePath)) return pick.executablePath;
  } catch {
    /* fall through to system browsers */
  }

  const candidates = [
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ];
  const found = candidates.find((p) => existsSync(p));
  if (found) return found;

  throw new Error(
    'No Chromium found. Set PUPPETEER_EXECUTABLE_PATH, or run `npm install` in the deck so marp-cli downloads one.',
  );
}

async function checkDeck(deck) {
  const deckDir = join(SLIDES_DIR, deck);
  const marpBin = join(deckDir, 'node_modules', '.bin', 'marp');
  if (!existsSync(marpBin)) {
    throw new Error(`marp not installed for "${deck}". Run: make slides-deps DECK=${deck}`);
  }

  const tmp = mkdtempSync(join(tmpdir(), `slidecheck-${deck}-`));
  const html = join(tmp, 'bare.html');
  try {
    // Render with the "bare" template so every slide is laid out and visible at once.
    execFileSync(
      marpBin,
      ['slides.md', '--html', '--template', 'bare', '--theme', THEME, '-o', html],
      { cwd: deckDir, stdio: 'pipe' },
    );

    const executablePath = await resolveBrowser(deckDir);
    const puppeteer = require(join(deckDir, 'node_modules', 'puppeteer-core'));
    const browser = await puppeteer.launch({
      executablePath,
      headless: true,
      args: ['--no-sandbox', '--disable-gpu', '--hide-scrollbars'],
    });
    try {
      const page = await browser.newPage();
      await page.goto(`file://${html}`, { waitUntil: 'networkidle0' });
      // Let webfonts settle - text height depends on the loaded font.
      await page.evaluate(() => document.fonts?.ready);

      const results = await page.evaluate((slideHeight) => {
        return [...document.querySelectorAll('section')].map((s, i) => {
          const heading = s.querySelector('h1, h2, h3')?.textContent?.trim() ?? '';

          // overflow: scrollHeight grows past the box only when content spills.
          const overflow = Math.round(s.scrollHeight - slideHeight);

          // fill: how much of the usable content area the real content occupies.
          // scrollHeight is clamped to the box, so measure the in-flow children's
          // bounding box instead (skip absolutely-positioned footer/pagination).
          const cs = getComputedStyle(s);
          const padTop = parseFloat(cs.paddingTop);
          const padBottom = parseFloat(cs.paddingBottom);
          const rect = s.getBoundingClientRect();
          const contentTop = rect.top + padTop;
          const avail = rect.height - padTop - padBottom;
          let maxBottom = contentTop;
          for (const child of s.children) {
            const ccs = getComputedStyle(child);
            if (ccs.position === 'absolute' || ccs.position === 'fixed') continue;
            if (ccs.display === 'none') continue;
            const r = child.getBoundingClientRect();
            if (r.bottom > maxBottom) maxBottom = r.bottom;
          }
          const fill = Math.round(((maxBottom - contentTop) / avail) * 100);

          // Classify content so tables/images (which rarely split well) are
          // reported separately from text/code (which usually do). Ignore the
          // footer logo, which is an absolutely-positioned <img> on every slide.
          const contentImg = [...s.querySelectorAll('img')].some((img) => {
            if (img.closest('footer')) return false;
            // Inline emoji (Marp renders them as Twemoji <img class="emoji">) are
            // text, not a content image.
            if (img.classList.contains('emoji')) return false;
            const ics = getComputedStyle(img);
            return ics.position !== 'absolute' && ics.position !== 'fixed';
          });
          let kind = 'text';
          if (s.querySelector('table')) kind = 'table';
          else if (contentImg) kind = 'image';
          // Marp's bare template renders code as <marp-pre>, not <pre>.
          else if (s.querySelector('pre, marp-pre')) kind = 'code';

          return { id: s.id || String(i + 1), heading, overflow, fill, kind };
        });
      }, SLIDE_HEIGHT);
      return results;
    } finally {
      await browser.close();
    }
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

async function main() {
  const args = process.argv.slice(2);
  // --dense[=PCT]: also list slides that fit but are visually crammed (advisory,
  // does not affect exit code). PCT is % of the usable content box; 100% reaches
  // the bottom padding, which is fine - 105%+ is genuinely tight. Default 105.
  const denseArg = args.find((a) => a === '--dense' || a.startsWith('--dense='));
  const denseThreshold = denseArg?.includes('=') ? Number(denseArg.split('=')[1]) : 105;
  const decks = args.filter((a) => !a.startsWith('--'));
  const targets = decks.length ? decks : listDecks();

  let bad = 0;
  let dense = 0;
  for (const deck of targets) {
    process.stdout.write(`\nChecking ${deck} ... `);
    const slides = await checkDeck(deck);
    const over = slides.filter((s) => s.overflow > TOLERANCE);
    if (over.length) {
      console.log(`${over.length} of ${slides.length} slide(s) overflow:`);
      for (const s of over) {
        const title = s.heading ? ` "${s.heading}"` : '';
        console.log(`  - slide ${s.id}${title}: +${s.overflow}px past the bottom edge`);
      }
      bad += over.length;
    } else {
      console.log(`OK (${slides.length} slides)`);
    }

    if (denseArg) {
      // Crammed but not overflowing - candidates for splitting.
      const crammed = slides
        .filter((s) => s.overflow <= TOLERANCE && s.fill >= denseThreshold)
        .sort((a, b) => b.fill - a.fill);
      // Tables and images rarely split well - list them apart so they aren't
      // mistaken for split candidates.
      const splittable = crammed.filter((s) => s.kind === 'text' || s.kind === 'code');
      const review = crammed.filter((s) => s.kind === 'table' || s.kind === 'image');
      const line = (s) =>
        `    - slide ${s.id} [${s.kind}]${s.heading ? ` "${s.heading}"` : ''}: ${s.fill}% full`;

      if (splittable.length) {
        console.log(`  dense, splittable (text/code >=${denseThreshold}% full):`);
        splittable.forEach((s) => console.log(line(s)));
      }
      if (review.length) {
        console.log(`  dense, review (table/image - splitting rarely helps):`);
        review.forEach((s) => console.log(line(s)));
      }
      dense += splittable.length;
    }
  }

  if (bad) {
    console.log(`\n${bad} overflowing slide(s). Trim content or shrink the code font.`);
    process.exit(1);
  }
  console.log(`\nAll slides fit.${denseArg ? ` ${dense} dense slide(s) flagged.` : ''}`);
}

main().catch((err) => {
  console.error(`\nslide check failed: ${err.message}`);
  process.exit(2);
});
