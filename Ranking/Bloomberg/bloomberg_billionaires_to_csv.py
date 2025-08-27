# bloomberg_billionaires_to_csv.py
from playwright.sync_api import sync_playwright
import pandas as pd
import re, time

URL = "https://www.bloomberg.com/billionaires/"
OUTPUT = "bloomberg_billionaires.csv"

def clean(text):
    return re.sub(r'\s+', ' ', text).strip()

records = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page    = browser.new_page()
    page.goto(URL, timeout=0)
    # 等待首行加载
    page.wait_for_selector("div.table-cell.t-rank")
    # Bloomberg 默认一次性渲染 500 行，无需 “Load More” 按钮
    rows = page.query_selector_all("div.table-row")
    for r in rows:
        cells = r.query_selector_all("div.table-cell")
        if not cells:                  # 过滤标题栏等
            continue
        rank, name, net, last, ytd, country, industry = [clean(c.inner_text()) for c in cells[:7]]
        records.append({
            "Rank": int(rank),
            "Name": name,
            "NetWorth": net.replace('$',''),
            "LastChange": last.replace('$',''),
            "YTDChange": ytd.replace('$',''),
            "Country": country,
            "Industry": industry
        })
    browser.close()

df = pd.DataFrame(records)
df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
print(f"✅  Saved {len(df)} rows → {OUTPUT}")
