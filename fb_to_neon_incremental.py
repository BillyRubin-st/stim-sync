# fb_to_neon_incremental.py

import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timedelta
from dateutil import parser

# ========== CONFIG ==========
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")  # long-lived token с правами ads_read
FB_API_VERSION = "v17.0"

# Список рекламных аккаунтов через запятую, без act_
# Пример: FB_AD_ACCOUNTS="1234567890,9876543210"
AD_ACCOUNTS = [a.strip() for a in os.getenv("FB_AD_ACCOUNTS", "").split(",") if a.strip()]

# DSN подключения к Neon (Postgres)
# Пример: PG_DSN="host=... port=5432 dbname=... user=... password=..."
PG_DSN = os.getenv("PG_DSN")

# Сколько дней назад начинать, если watermark ещё нет
DEFAULT_DAYS_BACK = int(os.getenv("FB_DEFAULT_DAYS_BACK", "30"))

# Сколько последних дней пересчитывать каждый запуск
RELOAD_LAST_DAYS = int(os.getenv("FB_RELOAD_LAST_DAYS", "3"))

FIELDS = [
    "date_start",
    "date_stop",
    "account_id",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "ctr",
    "cpc",
    "cpp"
]
# ============================


def get_conn():
    if not PG_DSN:
        raise RuntimeError("PG_DSN is not set")
    return psycopg2.connect(PG_DSN)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fb_insights_daily (
            date        date                NOT NULL,
            account_id  text                NOT NULL,
            campaign_id text,
            campaign_name text,
            adset_id    text,
            adset_name  text,
            ad_id       text                NOT NULL,
            ad_name     text,
            impressions bigint,
            clicks      bigint,
            spend       numeric(18,4),
            reach       bigint,
            frequency   numeric(10,4),
            ctr         numeric(10,4),
            cpc         numeric(18,4),
            cpp         numeric(18,4),
            updated_at  timestamptz DEFAULT now(),
            PRIMARY KEY (date, account_id, ad_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fb_watermark (
            account_id text PRIMARY KEY,
            last_date  date,
            updated_at timestamptz DEFAULT now()
        );
        """)
        conn.commit()


def get_watermark(conn, account_id: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM fb_watermark WHERE account_id = %s", (account_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return None


def set_watermark(conn, account_id: str, last_date: date):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fb_watermark (account_id, last_date, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (account_id) DO UPDATE
                SET last_date = EXCLUDED.last_date,
                    updated_at = now();
        """, (account_id, last_date))
        conn.commit()


def fetch_insights(account_id: str, date_from: date, date_to: date):
    """
    Тянем статистику по объявлениям за период [date_from, date_to]
    с шагом 1 день (time_increment=1).
    """
    print(f"[FB] Fetching {account_id} from {date_from} to {date_to}")

    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}/insights"

    params = {
        "access_token": FB_ACCESS_TOKEN,
        "time_range": json.dumps({
            "since": date_from.isoformat(),
            "until": date_to.isoformat()
        }),
        "time_increment": 1,
        "level": "ad",
        "fields": ",".join(FIELDS),
        "limit": 500
    }

    all_rows = []
    while True:
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"FB API error {resp.status_code}: {resp.text}")

        data = resp.json()

        for item in data.get("data", []):
            row_date = parser.isoparse(item["date_start"]).date() if "date_start" in item else None

            row = {
                "date": row_date,
                "account_id": item.get("account_id"),
                "campaign_id": item.get("campaign_id"),
                "campaign_name": item.get("campaign_name"),
                "adset_id": item.get("adset_id"),
                "adset_name": item.get("adset_name"),
                "ad_id": item.get("ad_id"),
                "ad_name": item.get("ad_name"),
                "impressions": int(item.get("impressions") or 0),
                "clicks": int(item.get("clicks") or 0),
                "spend": float(item.get("spend") or 0),
                "reach": int(item.get("reach") or 0) if item.get("reach") else None,
                "frequency": float(item.get("frequency") or 0) if item.get("frequency") else None,
                "ctr": float(item.get("ctr") or 0) if item.get("ctr") else None,
                "cpc": float(item.get("cpc") or 0) if item.get("cpc") else None,
                "cpp": float(item.get("cpp") or 0) if item.get("cpp") else None,
            }
            all_rows.append(row)

        paging = data.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        # Для next запросов параметры уже внутри URL
        params = {}
        url = next_url

    print(f"[FB] {account_id}: fetched {len(all_rows)} rows")
    return all_rows


def upsert_insights(conn, rows):
    if not rows:
        return

    cols = [
        "date", "account_id", "campaign_id", "campaign_name",
        "adset_id", "adset_name", "ad_id", "ad_name",
        "impressions", "clicks", "spend", "reach",
        "frequency", "ctr", "cpc", "cpp"
    ]
    values = [[r[c] for c in cols] for r in rows]

    with conn.cursor() as cur:
        sql = f"""
        INSERT INTO fb_insights_daily ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (date, account_id, ad_id) DO UPDATE
        SET
            campaign_id = EXCLUDED.campaign_id,
            campaign_name = EXCLUDED.campaign_name,
            adset_id = EXCLUDED.adset_id,
            adset_name = EXCLUDED.adset_name,
            ad_name = EXCLUDED.ad_name,
            impressions = EXCLUDED.impressions,
            clicks = EXCLUDED.clicks,
            spend = EXCLUDED.spend,
            reach = EXCLUDED.reach,
            frequency = EXCLUDED.frequency,
            ctr = EXCLUDED.ctr,
            cpc = EXCLUDED.cpc,
            cpp = EXCLUDED.cpp,
            updated_at = now();
        """
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
    print(f"[DB] Upserted {len(rows)} rows into fb_insights_daily")


def main():
    if not FB_ACCESS_TOKEN:
        raise RuntimeError("FB_ACCESS_TOKEN is not set")
    if not AD_ACCOUNTS:
        raise RuntimeError("FB_AD_ACCOUNTS is not set or empty")

    conn = get_conn()
    ensure_tables(conn)

    today = date.today()
    target_to = today - timedelta(days=1)  # до вчера

    for account_id in AD_ACCOUNTS:
        last = get_watermark(conn, account_id)
        if last:
            date_from = last - timedelta(days=RELOAD_LAST_DAYS)
        else:
            date_from = today - timedelta(days=DEFAULT_DAYS_BACK)

        if date_from > target_to:
            print(f"[SKIP] {account_id}: nothing to load (date_from > date_to)")
            continue

        rows = fetch_insights(account_id, date_from, target_to)
        upsert_insights(conn, rows)
        set_watermark(conn, account_id, target_to)

    conn.close()
    print("[OK] Finished incremental load")


if __name__ == "__main__":
    main()
