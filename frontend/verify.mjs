import { chromium } from "playwright";
import { mkdirSync } from "node:fs";

const BASE = process.env.BASE || "http://localhost:4173";
const OUT = "/tmp/fe-shots";
mkdirSync(OUT, { recursive: true });

const errors = [];
const steps = [];
const ok = (m) => steps.push(`PASS ${m}`);
const fail = (m) => { steps.push(`FAIL ${m}`); process.exitCode = 1; };

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1200, height: 1400 } });
page.on("console", (m) => { if (m.type() === "error") errors.push(m.text()); });
page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));

async function shot(name) { await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true }); }
const count = (sel) => page.locator(sel).count();

try {
  await page.goto(BASE, { waitUntil: "networkidle" });

  // 1. Leaderboard loads with rows
  await page.waitForSelector('[data-testid="leaderboard"]', { timeout: 8000 });
  await page.waitForSelector('[data-testid="row-rg-01"]', { timeout: 8000 });
  const rowCount = await count("tbody tr");
  rowCount >= 8 ? ok(`leaderboard rendered ${rowCount} rows`) : fail(`only ${rowCount} rows`);
  (await page.locator('[data-testid="mode-pill"]').innerText()).includes("offline")
    ? ok("mode pill shows offline/fixtures") : fail("mode pill wrong");
  await shot("01-leaderboard");

  // 2. Sort by p99 ascending (click header), verify the order actually changes
  const firstTeamBefore = await page.locator("tbody tr").first().locator("td").nth(1).innerText();
  await page.locator('[data-testid="th-p99_ns_at_peak_tps"]').click();
  await page.waitForTimeout(200);
  const firstTeamAfter = await page.locator("tbody tr").first().locator("td").nth(1).innerText();
  firstTeamBefore !== firstTeamAfter
    ? ok(`sort changed top row (${firstTeamBefore} -> ${firstTeamAfter})`)
    : fail("sort did not reorder rows");
  await shot("02-sorted-by-p99");

  // 3. Live SSE: wait for a delta arrow to appear (mock pushes every 2s)
  await page.waitForFunction(
    () => !!document.querySelector(".delta-up, .delta-down"),
    { timeout: 6000 },
  ).then(() => ok("live update produced a rank-delta arrow")).catch(() => fail("no live delta arrow"));
  await shot("03-live-deltas");

  // 4. Open a DQ run (Flow Traders = rg-08) -> run detail with violations
  await page.locator('[data-testid="row-rg-08"]').click();
  await page.waitForSelector('[data-testid="run-detail"]', { timeout: 8000 });
  await page.waitForSelector('[data-testid="session-ramp"]', { timeout: 8000 });
  const canvases = await count(".uplot-host canvas");
  canvases >= 6 ? ok(`run detail rendered ${canvases} chart canvases`) : fail(`only ${canvases} canvases`);
  const violRows = await page.locator('[data-testid="violations"] tbody tr').count();
  violRows >= 1 ? ok(`DQ run shows ${violRows} violations`) : fail("no violations on DQ run");
  (await count('[data-testid="run-detail"] .badge.dq')) > 0 ? ok("DQ badge shown") : fail("no DQ badge");
  await shot("04-run-detail-dq");

  // 5. Back to leaderboard
  await page.locator('[data-testid="back"]').click();
  await page.waitForSelector('[data-testid="leaderboard"]', { timeout: 8000 });
  ok("back button returned to leaderboard");

  // 6. Open a clean run (rg-01) -> no violations
  await page.locator('[data-testid="row-rg-01"]').click();
  await page.waitForSelector('[data-testid="run-detail"]', { timeout: 8000 });
  const cleanViol = await page.locator('[data-testid="violations"] .empty').count();
  cleanViol === 1 ? ok("clean run shows empty violation log") : fail("clean run violations wrong");
  await shot("05-run-detail-clean");
  await page.locator('[data-testid="back"]').click();
  await page.waitForSelector('[data-testid="leaderboard"]', { timeout: 8000 });

  // 7. Platform Health tab
  await page.locator('[data-testid="tab-health"]').click();
  await page.waitForSelector('[data-testid="health-panel"]', { timeout: 8000 });
  const panels = await count('[data-testid^="panel-"]');
  panels >= 4 ? ok(`health panel shows ${panels} panels`) : fail(`only ${panels} panels`);
  await shot("06-health-panel");

  // 8. Back to leaderboard tab
  await page.locator('[data-testid="tab-leaderboard"]').click();
  await page.waitForSelector('[data-testid="leaderboard"]', { timeout: 8000 });
  ok("leaderboard tab works");
} catch (e) {
  fail(`exception: ${e.message}`);
} finally {
  await shot("99-final");
  await browser.close();
}

console.log("\n=== STEPS ===");
for (const s of steps) console.log(s);
console.log(`\n=== CONSOLE ERRORS (${errors.length}) ===`);
for (const e of errors.slice(0, 20)) console.log(e);
if (errors.length) process.exitCode = 1;
console.log(`\nscreenshots in ${OUT}`);
