import { chromium } from "playwright";
const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1180, height: 760 } });
await p.goto("http://localhost:18090", { waitUntil: "domcontentloaded" });
await p.locator('[data-testid="tab-live"]').click();
await p.waitForSelector('.live-session canvas', { timeout: 100000 });
await p.waitForTimeout(4000); // accumulate a few seconds of both curves
await p.screenshot({ path: "/tmp/fe-rtt-live.png", fullPage: true });
const txt = await p.locator('[data-testid="live-runs"]').innerText();
console.log("has 'p99 service':", txt.includes("p99 service"));
console.log("has 'p99 round-trip':", txt.includes("p99 round-trip"));
await b.close();
