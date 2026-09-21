import os
import sys
import time
import json
import csv
import sqlite3
import base64
import urllib.request
import urllib.parse
import http.cookiejar
from bs4 import BeautifulSoup
from openai import OpenAI
import asyncio
import aiohttp

ROUTER_URL = "http://localhost:20128/v1"
ROUTER_KEY = "sk-a5be1fc2a203d8a3-q7rbod-9d98d295"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "enamad_data.db")
JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domains.json")
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domains.csv")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY,
            domain_address TEXT UNIQUE,
            persian_name TEXT,
            business_type INTEGER,
            province TEXT,
            city TEXT,
            rating INTEGER,
            code TEXT,
            approve_date TEXT,
            expire_date TEXT,
            owner TEXT,
            phone TEXT,
            email TEXT,
            address TEXT,
            working_hours TEXT,
            detail_scraped INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def solve_captcha(b64_img):
    client = OpenAI(base_url=ROUTER_URL, api_key=ROUTER_KEY)
    resp = client.chat.completions.create(
        model="ag/gemini-3.8-flash",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract only the numbers and letters shown in this captcha image. Return ONLY the alphanumeric code without spaces or punctuation."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64_img}"}}
                ]
            }
        ]
    )
    return resp.choices[0].message.content.strip().replace(" ", "")

class EnamadSession:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.init_session()

    def init_session(self):
        req = urllib.request.Request("https://www.enamad.ir/DomainListForMIMT", headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        self.opener.open(req, timeout=20)

    def fetch_page(self, page_num):
        for attempt in range(4):
            try:
                # 1. Refresh Captcha
                req_cpt = urllib.request.Request(
                    "https://www.enamad.ir/refreshCapt",
                    data=b"{}",
                    headers={
                        "Content-Type": "application/json; charset=UTF-8",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "Referer": "https://www.enamad.ir/DomainListForMIMT"
                    }
                )
                cpt_resp = self.opener.open(req_cpt, timeout=15)
                cpt_data = json.loads(cpt_resp.read().decode())
                cptToken = cpt_data.get("cptToken")
                b64_img = cpt_data.get("captha")

                code = solve_captcha(b64_img)
                # 2. Query page
                post_data = urllib.parse.urlencode({
                    "s#ms-domain-address": "",
                    "s#ms-persian-name": "",
                    "s#ms-product-service-id-enc": "",
                    "s#mi-rating": "",
                    "s#ms-province-id-enc": "",
                    "s#ms-city-id-enc": "",
                    "Capt": code,
                    "Csearch": "",
                    "page": str(page_num),
                    "token": cptToken,
                    "cptToken": cptToken,
                    "checkcapga": "0"
                }).encode("utf-8")

                req_search = urllib.request.Request(
                    "https://www.enamad.ir/getDomainList",
                    data=post_data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "Referer": "https://www.enamad.ir/DomainListForMIMT",
                        "X-Requested-With": "XMLHttpRequest"
                    }
                )
                res = self.opener.open(req_search, timeout=20)
                res_data = json.loads(res.read().decode())
                if res_data.get("result") == 1:
                    return res_data.get("applicantDomainsList", []), res_data.get("count", 0)
                else:
                    time.sleep(1)
            except Exception as e:
                time.sleep(1)
                self.init_session()
        return [], 0

def parse_trustseal_html(html):
    soup = BeautifulSoup(html, "html.parser")
    info = {
        "owner": "",
        "phone": "",
        "email": "",
        "address": "",
        "working_hours": ""
    }
    for box in soup.find_all("div", class_="person_details"):
        for r in box.find_all("div", recursive=True):
            if "txtbold" in r.get("class", []):
                label = r.get_text(strip=True).replace(":", "").strip()
                sib = r.find_next_sibling()
                val = sib.get_text(strip=True) if sib else ""
                if "صاحب امتیاز" in label:
                    info["owner"] = val
                elif "تلفن" in label:
                    info["phone"] = val
                elif "پست" in label or "الکترونیک" in label:
                    info["email"] = val.replace("[at]", "@")
                elif "آدرس" in label:
                    info["address"] = val
                elif "ساعت" in label:
                    info["working_hours"] = val
    return info

async def fetch_one_detail(session, item, sem):
    fid, code, domain = item
    if not code or not fid:
        return fid, None
    url = f"https://trustseal.enamad.ir/?id={fid}&code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": f"https://{domain}/"
    }
    async with sem:
        for _ in range(3):
            try:
                async with session.get(url, headers=headers, ssl=False, timeout=12) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        return fid, parse_trustseal_html(text)
            except Exception:
                await asyncio.sleep(1)
    return fid, None

async def batch_fetch_details(items, concurrency=10):
    sem = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [fetch_one_detail(session, item, sem) for item in items]
        return await asyncio.gather(*tasks)

def save_domains_batch(domains_list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for d in domains_list:
        cur.execute("""
            INSERT INTO domains (
                id, domain_address, persian_name, business_type, province, city,
                rating, code, approve_date, expire_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(domain_address) DO UPDATE SET
                code=excluded.code,
                province=coalesce(excluded.province, domains.province),
                city=coalesce(excluded.city, domains.city),
                rating=excluded.rating,
                approve_date=coalesce(excluded.approve_date, domains.approve_date),
                expire_date=coalesce(excluded.expire_date, domains.expire_date),
                updated_at=CURRENT_TIMESTAMP
        """, (
            d.get("id"),
            d.get("domain_address"),
            d.get("persian_name"),
            d.get("business_type"),
            d.get("province"),
            d.get("city"),
            d.get("rating"),
            d.get("code"),
            d.get("approve_date"),
            d.get("expire_date")
        ))
    conn.commit()
    conn.close()

def update_domain_details(details_dict):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for fid, details in details_dict.items():
        if details:
            cur.execute("""
                UPDATE domains SET
                    owner = ?,
                    phone = ?,
                    email = ?,
                    address = ?,
                    working_hours = ?,
                    detail_scraped = 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                details.get("owner", ""),
                details.get("phone", ""),
                details.get("email", ""),
                details.get("address", ""),
                details.get("working_hours", ""),
                fid
            ))
    conn.commit()
    conn.close()

def export_all():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT id, domain_address, persian_name, province, city, rating,
               code, approve_date, expire_date, owner, phone, email, address,
               working_hours, detail_scraped
        FROM domains
        ORDER BY id DESC
    """).fetchall()

    data_list = []
    for r in rows:
        d = dict(r)
        d["enamad_url"] = f"https://trustseal.enamad.ir/?id={d['id']}&code={d['code']}" if d.get("code") else ""
        data_list.append(d)

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)

    with open(CSV_PATH, "w", encoding="utf-8-sig", newline="") as f:
        if data_list:
            writer = csv.DictWriter(f, fieldnames=[
                "id", "domain_address", "persian_name", "province", "city",
                "rating", "phone", "email", "owner", "address", "working_hours",
                "approve_date", "expire_date", "enamad_url"
            ], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data_list)

    conn.close()
    print(f"Exported {len(data_list)} records to {JSON_PATH} and {CSV_PATH}")

def run_scraper(target_pages=5):
    init_db()
    session = EnamadSession()
    print(f"Starting Enamad scraper for {target_pages} pages...")
    total_found = 0

    for p in range(1, target_pages + 1):
        print(f"Scraping page {p}...")
        domains, total = session.fetch_page(p)
        if not domains:
            print(f"Page {p} empty or failed.")
            continue
        save_domains_batch(domains)
        total_found += len(domains)
        print(f"Page {p}: Saved {len(domains)} domains (Total registered: {total})")

        # Fetch details for domains in this page
        items_to_fetch = [(d["id"], d["code"], d["domain_address"]) for d in domains if d.get("code")]
        if items_to_fetch:
            print(f"Fetching details (phone, email, owner) for {len(items_to_fetch)} domains...")
            results = asyncio.run(batch_fetch_details(items_to_fetch, concurrency=8))
            details_map = {fid: dt for fid, dt in results if dt}
            update_domain_details(details_map)
            print(f"Details updated for {len(details_map)} domains.")

    export_all()
    print("Scraper completed successfully.")

if __name__ == "__main__":
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run_scraper(pages)
