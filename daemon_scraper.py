import os
import sys
import time
import json
import csv
import sqlite3
import urllib.request
import urllib.parse
import http.cookiejar
from bs4 import BeautifulSoup
from openai import OpenAI
import asyncio
import aiohttp

ROUTER_URL = "http://localhost:20128/v1"
ROUTER_KEY = "sk-a5be1fc2a203d8a3-q7rbod-9d98d295"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "enamad_data.db")
JSON_PATH = os.path.join(BASE_DIR, "domains.json")
CSV_PATH = os.path.join(BASE_DIR, "domains.csv")
STATE_PATH = os.path.join(BASE_DIR, ".scraper_state.json")

def get_last_page():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                return json.load(f).get("last_page", 1)
        except Exception:
            pass
    return 1

def save_last_page(p):
    with open(STATE_PATH, "w") as f:
        json.dump({"last_page": p, "timestamp": time.time()}, f)

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
        for _ in range(3):
            try:
                req_cpt = urllib.request.Request(
                    "https://www.enamad.ir/refreshCapt",
                    data=b"{}",
                    headers={
                        "Content-Type": "application/json; charset=UTF-8",
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://www.enamad.ir/DomainListForMIMT"
                    }
                )
                cpt_resp = self.opener.open(req_cpt, timeout=15)
                cpt_data = json.loads(cpt_resp.read().decode())
                cptToken = cpt_data.get("cptToken")
                b64_img = cpt_data.get("captha")

                code = solve_captcha(b64_img)
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
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://www.enamad.ir/DomainListForMIMT",
                        "X-Requested-With": "XMLHttpRequest"
                    }
                )
                res = self.opener.open(req_search, timeout=20)
                res_data = json.loads(res.read().decode())
                if res_data.get("result") == 1:
                    return res_data.get("applicantDomainsList", []), res_data.get("count", 0)
                time.sleep(1)
            except Exception:
                time.sleep(1)
                self.init_session()
        return [], 0

def parse_trustseal_html(html):
    soup = BeautifulSoup(html, "html.parser")
    info = {"owner": "", "phone": "", "email": "", "address": "", "working_hours": ""}
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

async def fetch_detail(session, item, sem):
    fid, code, domain = item
    if not code or not fid:
        return fid, None
    url = f"https://trustseal.enamad.ir/?id={fid}&code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": f"https://{domain}/"
    }
    async with sem:
        for _ in range(2):
            try:
                async with session.get(url, headers=headers, ssl=False, timeout=10) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        return fid, parse_trustseal_html(text)
            except Exception:
                await asyncio.sleep(0.5)
    return fid, None

async def batch_fetch_details(items, concurrency=10):
    sem = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [fetch_detail(session, it, sem) for it in items]
        return await asyncio.gather(*tasks)

def save_domains(domains):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for d in domains:
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
            d.get("id"), d.get("domain_address"), d.get("persian_name"),
            d.get("business_type"), d.get("province"), d.get("city"),
            d.get("rating"), d.get("code"), d.get("approve_date"), d.get("expire_date")
        ))
    conn.commit()
    conn.close()

def update_details(details_map):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for fid, det in details_map.items():
        if det:
            cur.execute("""
                UPDATE domains SET
                    owner = ?, phone = ?, email = ?, address = ?, working_hours = ?,
                    detail_scraped = 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                det.get("owner", ""), det.get("phone", ""), det.get("email", ""),
                det.get("address", ""), det.get("working_hours", ""), fid
            ))
    conn.commit()
    conn.close()

def export():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT id, domain_address, persian_name, province, city, rating,
               code, approve_date, expire_date, owner, phone, email, address,
               working_hours, detail_scraped
        FROM domains ORDER BY id DESC
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

def run_continuous(batch_count=10):
    session = EnamadSession()
    cur_page = get_last_page()
    print(f"Resuming scraping from page {cur_page} for {batch_count} pages...")

    for p in range(cur_page, cur_page + batch_count):
        print(f"Fetching page {p}...")
        domains, total = session.fetch_page(p)
        if domains:
            save_domains(domains)
            items = [(d["id"], d["code"], d["domain_address"]) for d in domains if d.get("code")]
            if items:
                res = asyncio.run(batch_fetch_details(items, concurrency=10))
                det_map = {fid: dt for fid, dt in res if dt}
                update_details(det_map)
            save_last_page(p + 1)
        time.sleep(0.5)

    export()
    print("Batch finished and exported.")

if __name__ == "__main__":
    b = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    run_continuous(b)
