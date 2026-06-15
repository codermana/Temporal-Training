// Catch the transient Pending Activities view for the retries lab.
// Pre-warms the browser/SPA, starts a run, then polls the pending-activities
// tab fast (inside the ChargeCard retry backoff) and keeps the frame that
// actually shows a pending activity. Requires a FRESH worker (attempt counter
// resets per worker process) on task queue retries-heartbeats.
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';

const require = createRequire(import.meta.url);
const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const deckDir = join(ROOT, 'slides', 'temporal-fundamentals');
const ASSETS = join(deckDir, 'assets');
const UI = 'http://127.0.0.1:8233';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const wfId = `retries-pending-${process.argv[2] || '1'}`;

const puppeteer = require(join(deckDir, 'node_modules', 'puppeteer-core'));
const browser = await puppeteer.launch({
  executablePath: CHROME, headless: 'new',
  args: ['--no-sandbox', '--disable-gpu', '--hide-scrollbars', '--force-color-profile=srgb'],
});
try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
  // Pre-warm the SPA so later navigations are fast (assets cached).
  await page.goto(`${UI}/namespaces/default/workflows`, { waitUntil: 'networkidle0' });

  // Start the run now; ChargeCard fails attempts 1-2 (~1s + 2s backoff) then succeeds.
  const out = execFileSync('temporal', [
    'workflow', 'start', '--task-queue', 'retries-heartbeats', '--type', 'ProcessingWorkflow',
    '--workflow-id', wfId, '--input', '"order-42"', '--address', '127.0.0.1:7233', '-o', 'json',
  ]).toString();
  const runId = JSON.parse(out).runId;
  console.log('started', wfId, runId);

  const url = `${UI}/namespaces/default/workflows/${wfId}/${runId}/pending-activities`;
  let best = false;
  for (let i = 0; i < 6; i++) {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await new Promise((r) => setTimeout(r, 500));
    const txt = await page.evaluate(() => document.body.innerText);
    const pending = txt.includes('Attempt') || (txt.includes('ChargeCard') && !txt.includes('No Pending Activities'));
    console.log(`  frame ${i}: pending=${pending}`);
    if (pending) {
      await page.screenshot({ path: join(ASSETS, 'ui-pending-activities.png') });
      best = true;
      break;
    }
  }
  if (!best) console.warn('  never caught a pending activity — rerun with a fresh worker');
} finally {
  await browser.close();
}
