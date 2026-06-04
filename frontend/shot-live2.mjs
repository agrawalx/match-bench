import { chromium } from "playwright";
const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1150, height: 900 } });
await p.goto("http://localhost:18090", { waitUntil: "domcontentloaded" });
await p.locator('[data-testid="tab-live"]').click();
await p.waitForSelector('[data-testid="live-runs"]', { timeout: 8000 });
// wait until at least one live session chart canvas renders (metrics arrived)
await p.waitForSelector('.live-session canvas', { timeout: 90000 });
await p.waitForTimeout(2500); // let a few seconds of p99 accumulate
await p.screenshot({ path: "/tmp/fe-live-tests.png", fullPage: true });
const txt = await p.locator('[data-testid="live-runs"]').innerText();
console.log("sessions shown:", ["constant","spike","ramp"].filter(s=>txt.includes(s)).join(", "));
console.log("has live p99 label:", txt.includes("p99 (live)"));
await b.close();
