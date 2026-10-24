// Usage: (cd bi/metabase && npm install) && node bi/metabase/screenshot.mjs bi/metabase/dashboards.json docs/images
// Set CHROME_PATH if Chrome is not in the default macOS location.
// One fresh browser per public dashboard; split into parts at dashcard boundaries (<= ~1000px).
import puppeteer from "puppeteer-core";
import fs from "node:fs";

const [, , jsonPath, outDir] = process.argv;
const dashboards = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

for (const d of dashboards) {
  const slug = d.name.toLowerCase().replace(/ & /g, "-").replace(/ /g, "-");
  const browser = await puppeteer.launch({
    executablePath: process.env.CHROME_PATH || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    headless: true,
    defaultViewport: { width: 1440, height: 2600, deviceScaleFactor: 2 },
  });
  const page = await browser.newPage();
  await page.goto(d.public_url, { waitUntil: "networkidle0", timeout: 90000 });
  await page.waitForSelector(".react-grid-item", { timeout: 60000 });
  // wait until every dashcard has rendered something (svg, canvas, table or scalar text)
  await page.waitForFunction(() => {
    const items = [...document.querySelectorAll(".react-grid-item")];
    return items.length > 0 && items.every((el) => el.querySelector("svg, canvas, table, [data-testid='scalar-value']") || el.innerText.trim().length > 20);
  }, { timeout: 60000 }).catch(() => console.error("render wait timed out for", d.name));
  await sleep(4000);
  const geo = await page.evaluate(() => {
    const items = [...document.querySelectorAll(".react-grid-item")].map((el) => el.getBoundingClientRect());
    return {
      left: Math.min(...items.map((r) => r.left)),
      right: Math.max(...items.map((r) => r.right)),
      bottoms: [...new Set(items.map((r) => Math.round(r.bottom)))].sort((a, b) => a - b),
    };
  });
  const cuts = [];
  let start = 0, last = 0;
  for (const b of geo.bottoms) {
    if (b - start > 1000 && last > start) { cuts.push([start, last]); start = last; }
    last = b;
  }
  cuts.push([start, last]);
  let i = 1;
  for (const [y0, y1] of cuts) {
    const file = `${outDir}/${slug}-${i++}.png`;
    await page.screenshot({ path: file, clip: { x: geo.left - 24, y: y0, width: geo.right - geo.left + 48, height: y1 - y0 + 12 }, captureBeyondViewport: false });
    console.log(file);
  }
  await browser.close();
}
