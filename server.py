import re
import json
import sqlite3
import pandas as pd
import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

RATES_URL = "https://exam.sanand.workers.dev/questionData?email=24f2005955%40ds.study.iitm.ac.in&quizSign=qaDMHhDfPMnBJcU%2BFeBuxXtwY7xWzn3xG%2F3dOYEufQcegy95Ub5yeb2%2FcMsgy1taz0jK9uOI9%2FLP49TGZHoOvo04ZWv7CpLwoYiKXSq4h1nBHTQfKO1%2BtDiuqYcmj%2B6wrkOl80aCLFD9B6R7n2PScSGSOeOMsUWcrlzyq0qZ0gSLHMT%2BUpendMsv3NCJy%2FATctMHijhyYtfQpmXa4tQicu9XC3%2FjXGTt4G1OYjw6VOEZCdv3a948waETDJTLl164m6ZyTKd2KU16ha1kCflXNERb4Lpfx66566JYAKaXe0zSgFP0xS9Xl%2FP1ENC0C8qiB1vagsgvfENaUfLkKf0m7w%3D%3D&questionId=q-ledger-agent-server&path=%2Frates"
EXPORT_URL = "https://exam.sanand.workers.dev/questionData?email=24f2005955%40ds.study.iitm.ac.in&quizSign=qaDMHhDfPMnBJcU%2BFeBuxXtwY7xWzn3xG%2F3dOYEufQcegy95Ub5yeb2%2FcMsgy1taz0jK9uOI9%2FLP49TGZHoOvo04ZWv7CpLwoYiKXSq4h1nBHTQfKO1%2BtDiuqYcmj%2B6wrkOl80aCLFD9B6R7n2PScSGSOeOMsUWcrlzyq0qZ0gSLHMT%2BUpendMsv3NCJy%2FATctMHijhyYtfQpmXa4tQicu9XC3%2FjXGTt4G1OYjw6VOEZCdv3a948waETDJTLl164m6ZyTKd2KU16ha1kCflXNERb4Lpfx66566JYAKaXe0zSgFP0xS9Xl%2FP1ENC0C8qiB1vagsgvfENaUfLkKf0m7w%3D%3D&questionId=q-ledger-agent-server&path=%2Fexport"

HEADERS = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

db_conn = sqlite3.connect(":memory:", check_same_thread=False)

def init_database():
    with httpx.Client(timeout=30.0) as client:
        print("Fetching currency rates from /rates...")
        resp_rates = client.get(RATES_URL, headers=HEADERS)
        resp_rates.raise_for_status()
        usd_rates = resp_rates.json()["usd_per_unit"]
        print("Currency Rates:", usd_rates)

        print("Fetching raw order ledger from /export...")
        resp_export = client.get(EXPORT_URL, headers=HEADERS)
        resp_export.raise_for_status()
        raw_lines = resp_export.text.strip().splitlines()
        rows = [json.loads(line) for line in raw_lines if line.strip()]
        print(f"Total raw records downloaded: {len(rows)}")

        df = pd.DataFrame(rows)

        # 1. Parse timestamps into timezone-aware UTC
        df["created_at_utc"] = pd.to_datetime(df["created_at"], format="ISO8601", utc=True)
        df["updated_at_utc"] = pd.to_datetime(df["updated_at"], format="ISO8601", utc=True)

        # 2. Deduplicate: sort by updated_at ascending and keep the last updated row per order id
        df = df.sort_values("updated_at_utc", kind="mergesort").drop_duplicates(subset=["id"], keep="last").copy()
        print(f"Total verified active orders: {len(df)}")

        # 3. Convert business dates to Asia/Kolkata timezone
        ist_created = df["created_at_utc"].dt.tz_convert("Asia/Kolkata")
        df["created_date"] = ist_created.dt.strftime("%Y-%m-%d")
        df["created_month"] = ist_created.dt.strftime("%Y-%m")
        df["created_year"] = ist_created.dt.year

        # 4. Standardize all transactions into USD
        df["rate_to_usd"] = df["currency"].map(usd_rates)
        df["amount_usd"] = df["amount"] * df["rate_to_usd"]

        df_sql = df.drop(columns=["created_at_utc", "updated_at_utc"])
        df_sql.to_sql("orders", db_conn, index=False, if_exists="replace")
        print("In-memory SQLite database ready.")

init_database()

MONTH_MAP = {
"january": "01", "february": "02", "march": "03", "april": "04",
"may": "05", "june": "06", "july": "07", "august": "08",
"september": "09", "october": "10", "november": "11", "december": "12"
}

PRODUCTS = [
"Air Fryer", "Rice Cooker", "Juicer", "Toaster",
"Mixer", "Kettle", "Blender", "Grinder"
]

REGIONS = ["North", "South", "East", "West", "Central"]

def parse_question_to_sql(question: str):    
    q_low = question.lower()
    conditions = []

    # Status classification
    if re.search(r"\brefund(ed|s)?\b", q_low):
        conditions.append("status = 'refunded'")
    elif re.search(r"\b(void|voided|cancel|cancelled)\b", q_low):
        conditions.append("status = 'void'")
    else:
        conditions.append("status = 'paid'")

    # Region filter
    for reg in REGIONS:
        if re.search(rf"\b{reg.lower()}\b", q_low):
            conditions.append(f"region = '{reg}'")
            break

    # Product filter
    for prod in PRODUCTS:
        if re.search(rf"\b{re.escape(prod.lower())}s?\b", q_low):
            conditions.append(f"product = '{prod}'")
            break

    # Customer ID filter
    cust_match = re.search(r"\b(c\d{4})\b", q_low)
    if cust_match:
        conditions.append(f"customer = '{cust_match.group(1).upper()}'")

    # Date / Month / Year filter
    year_match = re.search(r"\b(202\d)\b", q_low)
    year_str = year_match.group(1) if year_match else "2026"

    month_num = None
    for m_name, m_val in MONTH_MAP.items():
        if re.search(rf"\b{m_name}\b", q_low):
            month_num = m_val
            break

    if month_num:
        conditions.append(f"created_month = '{year_str}-{month_num}'")
    elif year_match and "month" not in q_low:
        conditions.append(f"created_year = {year_str}")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Route by question intent
    if any(p in q_low for p in ["which product", "what product", "top-selling product", "best-selling product", "product earned the most"]):
        order_metric = "SUM(qty)" if any(w in q_low for w in ["qty", "quantity", "units"]) else "SUM(amount_usd)"
        sql = f"SELECT product FROM orders WHERE {where_clause} GROUP BY product ORDER BY {order_metric} DESC LIMIT 1"
        return sql, "string"

    if any(p in q_low for p in ["which customer", "what customer", "top customer", "customer spent the most"]):
        sql = f"SELECT customer FROM orders WHERE {where_clause} GROUP BY customer ORDER BY SUM(amount_usd) DESC LIMIT 1"
        return sql, "string"

    if any(p in q_low for p in ["which region", "what region", "top region", "region earned the most"]):
        sql = f"SELECT region FROM orders WHERE {where_clause} GROUP BY region ORDER BY SUM(amount_usd) DESC LIMIT 1"
        return sql, "string"

    if any(p in q_low for p in ["distinct customer", "different customer", "unique customer", "how many customer"]):
        sql = f"SELECT COUNT(DISTINCT customer) FROM orders WHERE {where_clause}"
        return sql, "int"

    if "average" in q_low:
        sql = f"SELECT ROUND(COALESCE(AVG(amount_usd), 0), 2) FROM orders WHERE {where_clause}"
        return sql, "money"

    if any(p in q_low for p in ["how many orders", "count the", "number of orders", "how many refunded"]):
        sql = f"SELECT COUNT(*) FROM orders WHERE {where_clause}"
        return sql, "int"

    if any(p in q_low for p in ["how many units", "total quantity", "total units"]):
        sql = f"SELECT COALESCE(SUM(qty), 0) FROM orders WHERE {where_clause}"
        return sql, "int"

    # Default: Total Revenue / Earnings / Sales
    sql = f"SELECT ROUND(COALESCE(SUM(amount_usd), 0), 2) FROM orders WHERE {where_clause}"
    return sql, "money"

app = FastAPI()
app.add_middleware(
CORSMiddleware,
allow_origins=[""],
allow_methods=[""],
allow_headers=["*"],
)

@app.post("/")
async def answer_question(request: Request):
    body = await request.json()
    question = body.get("question", "")
    print(f"\n[INCOMING QUESTION] {question}")

    sql_query, ans_type = parse_question_to_sql(question)
    print(f"[SQL EXECUTED] {sql_query}")

    cursor = db_conn.cursor()
    cursor.execute(sql_query)
    row = cursor.fetchone()
    val = row[0] if row and row[0] is not None else 0

    if ans_type == "string":
        result = str(val)
    elif ans_type == "int":
        result = int(val)
    else:
        result = round(float(val), 2)

    print(f"[RESPONSE] {result}")
    return {"answer": result}
