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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = (
    "https://exam.sanand.workers.dev/questionData"
    "?email=24f2005955%40ds.study.iitm.ac.in"
    "&quizSign=Shkp89MVIKfN5ZSTp5dUbH5BigHJottL9rn2lqturXm49uw3pUyUqo2YsvGIgOv6DUkn2AcQuoVqUeaW9SjGj6yQA5u7CNGQwth2UaV2qbYFmRg0dQq%2F%2F09sXCNYOPJoJYm9Oc3G7iL1FpI%2Fsb7r9vDXfZm2%2B8Ewh0hF4xQB2WqvTS061OqWFpqhMqtYiIRuZNgM5c3KyWHgfjrKz99BbLkq39VxhAdqbrD22trz3ZMGV%2F9AhvI9Jr9UNqpqB7mGvnCWGLSxJw3RITxTDZmEZAjwMNeLYCZmCtmmRvbFpi0ZIVsgn%2FEiaZ60xt2%2FN4m9ojqI8XYB%2BwN57kaWKvRZxw%3D%3D"
    "&questionId=q-ledger-agent-server"
)

RATES_URL = BASE_URL + "&path=%2Frates"
EXPORT_URL = BASE_URL + "&path=%2Fexport"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

PRODUCTS = [
    "Air Fryer",
    "Rice Cooker",
    "Juicer",
    "Toaster",
    "Mixer",
    "Kettle",
    "Blender",
    "Grinder",
]

REGIONS = ["North", "South", "East", "West", "Central"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ledger-agent")

db_lock = threading.RLock()
db_conn = None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def fetch_json(client: httpx.Client, url: str):
    response = client.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def fetch_export_rows(client: httpx.Client) -> list[dict]:
    response = client.get(EXPORT_URL, headers=HEADERS)
    response.raise_for_status()

    text = response.text.strip()
    if not text:
        raise ValueError("Export response is empty.")

    try:
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
    except json.JSONDecodeError:
        pass

    return [json.loads(line) for line in text.splitlines() if line.strip()]


def build_dataframe(rows: list[dict], usd_rates: dict) -> pd.DataFrame:
    required = {
        "id", "created_at", "updated_at", "currency", "amount",
        "status", "region", "product", "customer", "qty",
    }

    df = pd.DataFrame(rows)

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in export: {sorted(missing)}")

    df["created_at_utc"] = pd.to_datetime(
        df["created_at"], format="ISO8601", utc=True, errors="raise"
    )
    df["updated_at_utc"] = pd.to_datetime(
        df["updated_at"], format="ISO8601", utc=True, errors="raise"
    )

    # Keep only the latest version of each order.
    df = (
        df.sort_values("updated_at_utc", kind="mergesort")
        .drop_duplicates(subset=["id"], keep="last")
        .copy()
    )

    df["amount"] = pd.to_numeric(df["amount"], errors="raise")
    df["qty"] = pd.to_numeric(df["qty"], errors="raise")

    # Use IST business dates.
    ist_created = df["created_at_utc"].dt.tz_convert("Asia/Kolkata")
    df["created_date"] = ist_created.dt.strftime("%Y-%m-%d")
    df["created_month"] = ist_created.dt.strftime("%Y-%m")
    df["created_month_num"] = ist_created.dt.month
    df["created_year"] = ist_created.dt.year

    # Convert every amount to USD.
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    df["rate_to_usd"] = df["currency"].map(usd_rates)

    missing_currencies = sorted(
        df.loc[df["rate_to_usd"].isna(), "currency"].unique().tolist()
    )
    if missing_currencies:
        raise ValueError(f"Missing USD rates for: {missing_currencies}")

    df["amount_usd"] = df["amount"] * df["rate_to_usd"]

    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["region"] = df["region"].astype(str).str.strip()
    df["product"] = df["product"].astype(str).str.strip()
    df["customer"] = df["customer"].astype(str).str.strip().str.upper()

    return df.drop(columns=["created_at_utc", "updated_at_utc"])


def initialize_database():
    global db_conn

    timeout = httpx.Timeout(30.0, connect=10.0)

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        logger.info("Fetching currency rates...")
        rates_response = fetch_json(client, RATES_URL)
        usd_rates = rates_response["usd_per_unit"]
        logger.info("Currency rates: %s", usd_rates)

        logger.info("Fetching order export...")
        rows = fetch_export_rows(client)
        logger.info("Raw records downloaded: %d", len(rows))

    df = build_dataframe(rows, usd_rates)
    logger.info("Verified active orders: %d", len(df))

    new_conn = sqlite3.connect(
        ":memory:",
        check_same_thread=False,
        timeout=30.0,
    )

    df.to_sql("orders", new_conn, index=False, if_exists="replace")

    new_conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_id ON orders(id)")
    new_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_created_month "
        "ON orders(created_month)"
    )
    new_conn.commit()

    with db_lock:
        old_conn = db_conn
        db_conn = new_conn

        if old_conn is not None:
            old_conn.close()

    logger.info("In-memory SQLite database ready.")


# ---------------------------------------------------------------------------
# Question parsing
# ---------------------------------------------------------------------------

def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def extract_filters(question: str):
    q = question.lower()
    conditions = []
    params = []

    # Status
    if re.search(r"\brefund(ed|s)?\b", q):
        conditions.append("status = ?")
        params.append("refunded")
    elif re.search(r"\b(void|voided|cancel|cancelled|canceled)\b", q):
        conditions.append("status = ?")
        params.append("void")
    elif re.search(r"\bpending|unpaid\b", q):
        conditions.append("status = ?")
        params.append("pending")
    else:
        conditions.append("status = ?")
        params.append("paid")

    # Region
    for region in REGIONS:
        if re.search(rf"\b{re.escape(region.lower())}\b", q):
            conditions.append("region = ?")
            params.append(region)
            break

    # Product
    q_normalized = normalize_text(q)
    for product in PRODUCTS:
        if normalize_text(product) in q_normalized:
            conditions.append("product = ?")
            params.append(product)
            break

    # Customer
    customer_match = re.search(r"\b(c\d{4})\b", q)
    if customer_match:
        conditions.append("customer = ?")
        params.append(customer_match.group(1).upper())

    # Year
    year_match = re.search(r"\b(20\d{2})\b", q)
    year = int(year_match.group(1)) if year_match else None

    # Month
    month = None
    for month_name, month_number in MONTH_MAP.items():
        if re.search(rf"\b{re.escape(month_name)}\b", q):
            month = month_number
            break

    # YYYY-MM format
    year_month_match = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", q)
    if year_month_match:
        conditions.append("created_month = ?")
        params.append(year_month_match.group(0))
    else:
        if month is not None:
            conditions.append("created_month_num = ?")
            params.append(month)

        if year is not None:
            conditions.append("created_year = ?")
            params.append(year)

    return conditions, params


def parse_question_to_sql(question: str):
    if not isinstance(question, str) or not question.strip():
        raise ValueError("Question must be a non-empty string.")

    q = question.strip().lower()
    conditions, params = extract_filters(question)

    where = ""
    if conditions:
        where = " WHERE " + " AND ".join(conditions)

    is_count = bool(re.search(
        r"\b(how many|count|number of|total number)\b", q
    ))

    # Unique customers
    if is_count and re.search(
        r"\b(distinct|unique|different)\s+customers?\b", q
    ):
        return (
            f"SELECT COUNT(DISTINCT customer) FROM orders{where}",
            params,
            "int",
        )

    # Customers
    if is_count and re.search(r"\b(customers?|clients?)\b", q):
        return (
            f"SELECT COUNT(DISTINCT customer) FROM orders{where}",
            params,
            "int",
        )

    # Distinct products
    if is_count and re.search(
        r"\b(distinct|unique|different)\s+products?\b", q
    ):
        return (
            f"SELECT COUNT(DISTINCT product) FROM orders{where}",
            params,
            "int",
        )

    # Units / quantity
    if (
        is_count and re.search(r"\b(units?|quantities|items?)\b", q)
    ) or re.search(
        r"\b(total quantity|total units|units sold|quantity sold)\b", q
    ):
        return (
            f"SELECT COALESCE(SUM(qty), 0) FROM orders{where}",
            params,
            "int",
        )

    # Orders
    if is_count and re.search(r"\borders?\b", q):
        return (
            f"SELECT COUNT(*) FROM orders{where}",
            params,
            "int",
        )

    # Average
    if re.search(r"\b(average|avg)\b", q):
        return (
            f"SELECT ROUND(COALESCE(AVG(amount_usd), 0), 2) "
            f"FROM orders{where}",
            params,
            "money",
        )

    ranking_words = (
        "top", "most", "highest", "best-selling", "best selling",
        "top-selling", "top selling", "largest", "greatest",
    )
    is_ranking = any(word in q for word in ranking_words)

    # Top product
    if (
        "which product" in q
        or "what product" in q
        or ("product" in q and is_ranking)
        or "product earned the most" in q
    ):
        metric = (
            "SUM(qty)"
            if re.search(r"\b(units?|quantity|qty)\b", q)
            else "SUM(amount_usd)"
        )
        return (
            f"SELECT product FROM orders{where} "
            f"GROUP BY product "
            f"ORDER BY {metric} DESC, product ASC LIMIT 1",
            params,
            "string",
        )

    # Top customer
    if (
        "which customer" in q
        or "what customer" in q
        or ("customer" in q and is_ranking)
        or "customer spent the most" in q
    ):
        return (
            f"SELECT customer FROM orders{where} "
            f"GROUP BY customer "
            f"ORDER BY SUM(amount_usd) DESC, customer ASC LIMIT 1",
            params,
            "string",
        )

    # Top region
    if (
        "which region" in q
        or "what region" in q
        or ("region" in q and is_ranking)
        or "region earned the most" in q
    ):
        return (
            f"SELECT region FROM orders{where} "
            f"GROUP BY region "
            f"ORDER BY SUM(amount_usd) DESC, region ASC LIMIT 1",
            params,
            "string",
        )

    # Default: total revenue / sales / earnings
    return (
        f"SELECT ROUND(COALESCE(SUM(amount_usd), 0), 2) "
        f"FROM orders{where}",
        params,
        "money",
    )


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield

    global db_conn
    with db_lock:
        if db_conn is not None:
            db_conn.close()
            db_conn = None


app = FastAPI(title="Ledger Agent", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
        sql, params, answer_type = parse_question_to_sql(question)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    logger.info("Question: %s", question)
    logger.info("SQL: %s", sql)
    logger.info("Params: %s", params)

    try:
        with db_lock:
            if db_conn is None:
                raise HTTPException(
                    status_code=503,
                    detail="Database is not initialized.",
                )

            row = db_conn.execute(sql, params).fetchone()

        value = row[0] if row and row[0] is not None else 0

        if answer_type == "string":
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
            detail="Unable to execute ledger query.",
        ) from None


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
