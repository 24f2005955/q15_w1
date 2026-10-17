python
```

import os
import re
import json
import logging
import sqlite3
import threading
from contextlib import asynccontextmanager

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

RATES_URL = os.getenv("RATES_URL", "")
EXPORT_URL = os.getenv("EXPORT_URL", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", ":memory:")
DEFAULT_STATUS = os.getenv("DEFAULT_STATUS", "paid").strip().lower()

CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ledger_agent")

db_lock = threading.RLock()
db_conn = None

REQUIRED_COLUMNS = {
    "id", "created_at", "updated_at", "currency", "amount",
    "status", "region", "product", "customer", "qty",
}

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11,
    "december": 12,
}

PRODUCTS = [
    "Air Fryer", "Rice Cooker", "Juicer", "Toaster",
    "Mixer", "Kettle", "Blender", "Grinder",
]

REGIONS = ["North", "South", "East", "West", "Central"]

STATUS_PATTERNS = [
    (r"\b(refunded|refunds|refund)\b", "refunded"),
    (r"\b(voided|void)\b", "void"),
    (r"\b(cancelled|canceled|cancel)\b", "void"),
    (r"\b(pending|unpaid)\b", "pending"),
    (r"\b(paid|successful|completed)\b", "paid"),
]

def fetch_order_records(client):
    if not EXPORT_URL:
        raise ValueError("Set the EXPORT_URL environment variable.")

    response = client.get(EXPORT_URL, headers=HEADERS)
    response.raise_for_status()

    content = response.text.strip()
    if not content:
        raise ValueError("The order export is empty.")

    try:
        payload = response.json()

        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
            records = payload["data"]
        elif isinstance(payload, dict) and isinstance(payload.get("orders"), list):
            records = payload["orders"]
        elif isinstance(payload, dict):
            records = [payload]
        else:
            raise ValueError("Unexpected export format.")

    except (json.JSONDecodeError, ValueError):
        records = [
            json.loads(line)
            for line in content.splitlines()
            if line.strip()
        ]

    if not records or not all(isinstance(item, dict) for item in records):
        raise ValueError("Export must contain JSON order records.")

    return records

def fetch_currency_rates(client):
    if not RATES_URL:
        raise ValueError("Set the RATES_URL environment variable.")

    response = client.get(RATES_URL, headers=HEADERS)
    response.raise_for_status()

    rates = response.json().get("usd_per_unit")

    if not isinstance(rates, dict) or not rates:
        raise ValueError("Rates response must contain usd_per_unit.")

    normalized = {}

    for currency, rate in rates.items():
        try:
            value = float(rate)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid rate for {currency}.") from None

        if not pd.notna(value) or value <= 0:
            raise ValueError(f"Invalid rate for {currency}.")

        normalized[str(currency).strip().upper()] = value

    return normalized

def build_dataframe(records, rates):
    df = pd.DataFrame(records)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(sorted(missing))
        )

    for column in [
        "id", "currency", "status", "region", "product", "customer"
    ]:
        df[column] = df[column].astype("string").str.strip()

    if df["id"].isna().any() or df["id"].eq("").any():
        raise ValueError("Order IDs cannot be empty.")

    df["currency"] = df["currency"].str.upper()
    df["status"] = df["status"].str.lower()

    df["created_at_utc"] = pd.to_datetime(
        df["created_at"], format="ISO8601", utc=True, errors="raise"
    )
    df["updated_at_utc"] = pd.to_datetime(
        df["updated_at"], format="ISO8601", utc=True, errors="raise"
    )

    df = (
        df.sort_values("updated_at_utc", kind="mergesort")
        .drop_duplicates(subset=["id"], keep="last")
        .copy()
    )

    df["amount"] = pd.to_numeric(df["amount"], errors="raise")
    df["qty"] = pd.to_numeric(df["qty"], errors="raise")

    if df["amount"].isna().any() or df["qty"].isna().any():
        raise ValueError("Amounts and quantities cannot be missing.")

    ist_created = df["created_at_utc"].dt.tz_convert("Asia/Kolkata")
    df["created_date"] = ist_created.dt.strftime("%Y-%m-%d")
    df["created_month"] = ist_created.dt.strftime("%Y-%m")
    df["created_month_num"] = ist_created.dt.month
    df["created_year"] = ist_created.dt.year

    df["rate_to_usd"] = df["currency"].map(rates)

    if df["rate_to_usd"].isna().any():
        missing_currencies = sorted(
            df.loc[df["rate_to_usd"].isna(), "currency"]
            .dropna().unique().tolist()
        )
        raise ValueError(
            "Missing rates for: " + ", ".join(missing_currencies)
        )

    df["amount_usd"] = df["amount"] * df["rate_to_usd"]

    if not df["amount_usd"].map(pd.notna).all():
        raise ValueError("Currency conversion produced invalid amounts.")

    allowed_statuses = {"paid", "refunded", "void", "pending"}
    unknown = set(df["status"].dropna()) - allowed_statuses

    if unknown:
        raise ValueError(
            "Unknown statuses: " + ", ".join(sorted(unknown))
        )

    return df.drop(columns=["created_at_utc", "updated_at_utc"])

def initialize_database():
    global db_conn

    timeout = httpx.Timeout(30.0, connect=10.0)

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        rates = fetch_currency_rates(client)
        records = fetch_order_records(client)

    df = build_dataframe(records, rates)

    new_conn = sqlite3.connect(
        DATABASE_PATH,
        check_same_thread=False,
        timeout=30.0,
    )

    try:
        df.to_sql("orders", new_conn, index=False, if_exists="replace")
        new_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_id ON orders(id)"
        )
        new_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_date "
            "ON orders(created_date)"
        )
        new_conn.commit()
    except Exception:
        new_conn.close()
        raise

    with db_lock:
        old_conn = db_conn
        db_conn = new_conn

        if old_conn is not None:
            old_conn.close()

    logger.info("Database initialized with %d orders.", len(df))

def parse_question_to_sql(question):
    if not isinstance(question, str) or not question.strip():
        raise ValueError("Question must be a non-empty string.")

    q = question.strip().lower()
    conditions = []
    params = []

    all_statuses = bool(re.search(
        r"\b(all statuses|every status|regardless of status|"
        r"including refunds|including refunded|all orders)\b",
        q,
    ))

    if not all_statuses:
        selected_status = None

        for pattern, status in STATUS_PATTERNS:
            if re.search(pattern, q):
                selected_status = status
                break

        if selected_status is None:
            selected_status = DEFAULT_STATUS

        if selected_status not in {"paid", "refunded", "void", "pending"}:
            raise ValueError("Invalid DEFAULT_STATUS configuration.")

        conditions.append("LOWER(status) = ?")
        params.append(selected_status)

    for region in REGIONS:
        if re.search(rf"\b{re.escape(region.lower())}\b", q):
            conditions.append("region = ?")
            params.append(region)
            break

    for product in PRODUCTS:
        if re.search(rf"\b{re.escape(product.lower())}s?\b", q):
            conditions.append("product = ?")
            params.append(product)
            break

    customer_match = re.search(r"\b(c\d{4})\b", q)
    if customer_match:
        conditions.append("UPPER(customer) = ?")
        params.append(customer_match.group(1).upper())

    year_match = re.search(r"\b(20\d{2})\b", q)
    year = int(year_match.group(1)) if year_match else None

    month_num = None
    for month_name, month_number in MONTH_MAP.items():
        if re.search(rf"\b{month_name}\b", q):
            month_num = month_number
            break

    if month_num is not None:
        conditions.append("created_month_num = ?")
        params.append(month_num)

    if year is not None:
        conditions.append("created_year = ?")
        params.append(year)

    where_clause = (
        " WHERE " + " AND ".join(conditions)
        if conditions else ""
    )

    is_count = bool(re.search(
        r"\b(how many|count|number of|count the|order count|total number)\b",
        q,
    ))

    if is_count and re.search(
        r"\b(distinct|unique|different)\s+customers?\b", q
    ):
        return (
            "SELECT COUNT(DISTINCT customer) FROM orders" + where_clause,
            params,
            "int",
        )

    if is_count and re.search(r"\b(customers?|clients?)\b", q):
        return (
            "SELECT COUNT(DISTINCT customer) FROM orders" + where_clause,
            params,
            "int",
        )

    if (
        is_count and re.search(r"\b(units?|quantities|items?)\b", q)
    ) or re.search(
        r"\b(total quantity|total units|units sold|quantity sold|how many units)\b",
        q,
    ):
        return (
            "SELECT COALESCE(SUM(qty), 0) FROM orders" + where_clause,
            params,
            "int",
        )

    if is_count and re.search(r"\borders?\b", q):
        return (
            "SELECT COUNT(*) FROM orders" + where_clause,
            params,
            "int",
        )

    if re.search(r"\b(average|avg)\b", q):
        return (
            "SELECT ROUND(COALESCE(AVG(amount_usd), 0), 2) "
            "FROM orders" + where_clause,
            params,
            "money",
        )

    ranking_words = (
        "top", "most", "highest", "best-selling", "top-selling",
        "best selling", "top selling", "largest", "greatest",
    )
    is_ranking = any(word in q for word in ranking_words)

    product_ranking = (
        "which product" in q
        or "what product" in q
        or ("product" in q and is_ranking)
        or "product earned the most" in q
    )

    if product_ranking:
        metric = (
            "SUM(qty)"
            if re.search(r"\b(units?|quantity|qty)\b", q)
            else "SUM(amount_usd)"
        )
        return (
            "SELECT product FROM orders" + where_clause
            + f" GROUP BY product ORDER BY {metric} DESC, "
              "product ASC LIMIT 1",
            params,
            "string",
        )

    customer_ranking = (
        "which customer" in q
        or "what customer" in q
        or ("customer" in q and is_ranking)
        or "customer spent the most" in q
    )

    if customer_ranking:
        return (
            "SELECT customer FROM orders" + where_clause
            + " GROUP BY customer "
              "ORDER BY SUM(amount_usd) DESC, customer ASC LIMIT 1",
            params,
            "string",
        )

    region_ranking = (
        "which region" in q
        or "what region" in q
        or ("region" in q and is_ranking)
        or "region earned the most" in q
    )

    if region_ranking:
        return (
            "SELECT region FROM orders" + where_clause
            + " GROUP BY region "
              "ORDER BY SUM(amount_usd) DESC, region ASC LIMIT 1",
            params,
            "string",
        )

    return (
        "SELECT ROUND(COALESCE(SUM(amount_usd), 0), 2) "
        "FROM orders" + where_clause,
        params,
        "money",
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield

    global db_conn
    with db_lock:
        if db_conn is not None:
            db_conn.close()
            db_conn = None

app = FastAPI(
    title="Ledger Agent API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials="*" not in CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    with db_lock:
        ready = db_conn is not None

    return {
        "status": "ok" if ready else "not_ready",
        "database_ready": ready,
    }

@app.post("/")
async def answer_question(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Request body must contain valid JSON.",
        ) from None

    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail="Request body must be a JSON object.",
        )

    question = body.get("question")

    if not isinstance(question, str) or not question.strip():
        raise HTTPException(
            status_code=422,
            detail="'question' must be a non-empty string.",
        )

    try:
        sql_query, sql_params, answer_type = parse_question_to_sql(question)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    logger.info("Question: %s", question)
    logger.info("SQL: %s", sql_query)

    try:
        with db_lock:
            if db_conn is None:
                raise HTTPException(
                    status_code=503,
                    detail="Database is not initialized.",
                )

            row = db_conn.execute(sql_query, sql_params).fetchone()

        value = row[0] if row else None

        if value is None:
            result = "" if answer_type == "string" else 0
        elif answer_type == "string":
            result = str(value)
        elif answer_type == "int":
            result = int(value)
        else:
            result = round(float(value), 2)

        logger.info("Answer: %s", result)
        return {"answer": result}

    except HTTPException:
        raise
    except sqlite3.Error:
        logger.exception("Database query failed.")
        raise HTTPException(
            status_code=500,
            detail="Unable to execute the ledger query.",
        ) from None

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
```
