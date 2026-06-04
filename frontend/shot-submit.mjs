import { chromium } from "playwright";
const errs = [];
const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1150, height: 1000 } });
p.on("pageerror", (e) => errs.push("pageerror: " + e.message));
p.on("console", (m) => { if (m.type() === "error") errs.push("console: " + m.text()); });

await p.goto("http://localhost:8088", { waitUntil: "domcontentloaded" });

// Submit tab is the default landing.
await p.waitForSelector('[data-testid="submit-view"]', { timeout: 10000 });
const tabs = await p.locator('nav.tabs button').allTextContents();
const dropzone = await p.locator('[data-testid="dropzone"]').count();

// Track the existing echo1 submission (build-worker not deployed, so we use a
// ready submission rather than a fresh upload).
await p.locator('[data-testid="track-input"]').fill("echo1");
await p.locator('[data-testid="track-btn"]').click();
await p.waitForSelector('[data-testid="submission-card"]', { timeout: 10000 });
await p.waitForTimeout(500);
const subStatus = await p.locator('[data-testid="sub-status"]').textContent().catch(() => null);
const runBtn = p.locator('[data-testid="run-btn"]');
const runEnabled = await runBtn.isEnabled().catch(() => false);

await p.screenshot({ path: "/tmp/fe-submit.png", fullPage: true });

// Click through the other tabs to confirm no regression.
let tabErrs = 0;
for (const t of ["tab-leaderboard", "tab-live", "tab-submit"]) {
  await p.locator(`[data-testid="${t}"]`).click().catch(() => tabErrs++);
  await p.waitForTimeout(400);
}

console.log("tabs:", JSON.stringify(tabs));
console.log("dropzone present:", dropzone);
console.log("echo1 tracked status:", subStatus, "| Run button enabled:", runEnabled);
console.log("tab nav errors:", tabErrs);
console.log("page errors:", errs.length);
errs.slice(0, 6).forEach((e) => console.log("  " + e));
await b.close();
