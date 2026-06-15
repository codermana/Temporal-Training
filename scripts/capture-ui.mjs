// One-off helper: screenshot a Temporal Web UI view for the slide deck.
// Requires a dev server (:8233) with the target execution already run.
//
//   node scripts/capture-ui.mjs <outName.png> <urlPath> [waitText] [clickText]
// clickText: optional exact text of an element to click before shooting
// (e.g. "Descending" to flip the Event History to ascending order).
//
// urlPath is relative to http://127.0.0.1:8233, e.g.
//   /namespaces/default/workflows
//   /namespaces/default/workflows/<wfId>/<runId>/history
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const deckDir = join(ROOT, 'slides', 'temporal-fundamentals');
const ASSETS = join(deckDir, 'assets');
const UI = 'http://127.0.0.1:8233';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

const [outName, urlPath, waitText, clickText] = process.argv.slice(2);
if (!outName || !urlPath) {
  console.error('usage: node scripts/capture-ui.mjs <outName.png> <urlPath> [waitText]');
  process.exit(2);
}

const puppeteer = require(join(deckDir, 'node_modules', 'puppeteer-core'));
const browser = await puppeteer.launch({
  executablePath: CHROME,
  headless: 'new',
  args: ['--no-sandbox', '--disable-gpu', '--hide-scrollbars', '--force-color-profile=srgb'],
});
try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
  // domcontentloaded (not networkidle0): running Workflows poll forever, so the
  // network never goes idle and networkidle0 would time out. waitText below
  // gates on the actual content being present.
  await page.goto(`${UI}${urlPath}`, { waitUntil: 'domcontentloaded', timeout: 30000 });
  if (waitText) {
    try {
      await page.waitForFunction((t) => document.body.innerText.includes(t), { timeout: 15000 }, waitText);
    } catch {
      console.warn(`  (warning) "${waitText}" not found on ${urlPath}`);
    }
  }
  if (clickText) {
    const clicked = await page.evaluate((t) => {
      const el = [...document.querySelectorAll('button, [role=button], a, span, label')]
        .find((e) => e.textContent?.trim() === t);
      if (el) { el.click(); return true; }
      return false;
    }, clickText);
    if (!clicked) console.warn(`  (warning) clickable "${clickText}" not found`);
    await new Promise((r) => setTimeout(r, 800));
  }
  await new Promise((r) => setTimeout(r, 1500));
  await page.screenshot({ path: join(ASSETS, outName) });
  console.log('wrote', outName);
} finally {
  await browser.close();
}
