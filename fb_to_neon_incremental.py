import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
from dateutil import parser

# ========== CONFIG ==========

FB_API_VERSION = "v17.0"

FB_CONFIG_RAW = os.getenv("FB_CONFIG")  # JSON с токенами/аккаунтами
PG_DSN = os.getenv("PG_DSN") or os.getenv("DATABASE_URL")  # Neon DSN

DEFAULT_DAYS_BACK = int(os.getenv("FB_DEFAULT_DAYS_BACK", "60"))   # история
RELOAD_LAST_DAYS = int(os.getenv("FB_RELOAD_LAST_DAYS", "3"))      # пересчёт
FIELDS = [
    "date_start",
    "account_id",
    "account_name",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "impressions",
    "spend",
    "reach",
    "frequency",
    "ctr",
    "cpc",
    "cpp",
    "actions"
]


# ========== DB HELPERS ==========

def get_conn():
    if not PG_DSN:
        raise RuntimeError("PG_DSN / DATABASE_URL is not set")
    return psycopg2.connect(PG_DSN)


def ensure_tables(conn):
    """Создаёт таблицы, если их нет"""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fb_insights_daily (
            date           date                NOT NULL,
            account_id     text                NOT NULL,
            account_name   text,
            campaign_id    text,
            campaign_name  text,
            adset_id       text,
            adset_name     text,
            ad_id          text                NOT NULL,
            ad_name        text,
            impressions    bigint,
            clicks         bigint,
            leads          bigint,
            spend          numeric(18,4),
            reach          bigint,
            frequency      numeric(10,4),
            ctr            numeric(10,4),
            cpc            numeric(18,4),
            cpp            numeric(18,4),
            targetologist  text,
            updated_at     timestamptz DEFAULT now(),
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


def get_watermark(conn, account_id):
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM fb_watermark WHERE account_id = %s", (account_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_watermark(conn, account_id, last_date):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fb_watermark (account_id, last_date, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (account_id)
            DO UPDATE SET last_date = EXCLUDED.last_date, updated_at = now();
        """, (account_id, last_date))
        conn.commit()


# ========== FB HELPERS ==========

def extract_leads(actions):
    """Извлекает лиды (lead, pixel_lead, grouped_lead)"""
    if not actions:
        return 0
    total = 0
    for a in actions:
        t = a.get("action_type")
        if t in (
            "lead",
            "onsite_conversion.lead_grouped",
            "offsite_conversion.fb_pixel_lead",
            "offsite_conversion.lead"
        ):
            try:
                total += int(a.get("value") or 0)
            except (TypeError, ValueError):
                pass
    return total


def extract_link_clicks(actions):
    """Извлекает клики по ссылке"""
    if not actions:
        return 0
    total = 0
    for a in actions:
        t = a.get("action_type")
        if t in (
            "link_click",
            "offsite_clicks",
            "landing_page_view"
        ):
            try:
                total += int(a.get("value") or 0)
            except (TypeError, ValueError):
                pass
    return total


def fetch_insights_for_account(token, account_id, date_from, date_to, targetologist):
    """Выгрузка данных с FB API"""
    print(f"***FB*** Fetch {account_id} ({targetologist}) {date_from} → {date_to}")
    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}/insights"
    params = {
        "access_token": token,
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
            print(f"⚠️ FB API error for {account_id}: {resp.text}")
            return []

        data = resp.json()
        for item in data.get("data", []):
            dt = parser.isoparse(item["date_start"]).date()
            actions = item.get("actions", [])
            leads = extract_leads(actions)
            link_clicks = extract_link_clicks(actions)

            row = {
                "date": dt,
                "account_id": item.get("account_id"),
                "account_name": item.get("account_name"),
                "campaign_id": item.get("campaign_id"),
                "campaign_name": item.get("campaign_name"),
                "adset_id": item.get("adset_id"),
                "adset_name": item.get("adset_name"),
                "ad_id": item.get("ad_id"),
                "ad_name": item.get("ad_name"),
                "impressions": int(item.get("impressions") or 0),
                "clicks": link_clicks,  # 👈 только клики по ссылке
                "leads": leads,
                "spend": float(item.get("spend") or 0),
                "reach": int(item.get("reach") or 0) if item.get("reach") else None,
                "frequency": float(item.get("frequency") or 0) if item.get("frequency") else None,
                "ctr": float(item.get("ctr") or 0) if item.get("ctr") else None,
                "cpc": float(item.get("cpc") or 0) if item.get("cpc") else None,
                "cpp": float(item.get("cpp") or 0) if item.get("cpp") else None,
                "targetologist": targetologist
            }
            all_rows.append(row)

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url = next_url
        params = {}

    print(f"***FB*** {account_id} ({targetologist}): {len(all_rows)} rows fetched")
    return all_rows


def upsert_insights(conn, rows):
    """Upsert данных в fb_insights_daily"""
    if not rows:
        return
    cols = [
        "date", "account_id", "account_name", "campaign_id", "campaign_name",
        "adset_id", "adset_name", "ad_id", "ad_name",
        "impressions", "clicks", "leads", "spend", "reach",
        "frequency", "ctr", "cpc", "cpp", "targetologist"
    ]
    values = [[r[c] for c in cols] for r in rows]
    with conn.cursor() as cur:
        sql = f"""
        INSERT INTO fb_insights_daily ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (date, account_id, ad_id) DO UPDATE
        SET
            account_name  = EXCLUDED.account_name,
            campaign_id   = EXCLUDED.campaign_id,
            campaign_name = EXCLUDED.campaign_name,
            adset_id      = EXCLUDED.adset_id,
            adset_name    = EXCLUDED.adset_name,
            ad_name       = EXCLUDED.ad_name,
            impressions   = EXCLUDED.impressions,
            clicks        = EXCLUDED.clicks,
            leads         = EXCLUDED.leads,
            spend         = EXCLUDED.spend,
            reach         = EXCLUDED.reach,
            frequency     = EXCLUDED.frequency,
            ctr           = EXCLUDED.ctr,
            cpc           = EXCLUDED.cpc,
            cpp           = EXCLUDED.cpp,
            targetologist = EXCLUDED.targetologist,
            updated_at    = now();
        """
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
    print(f"***DB*** Upserted {len(rows)} rows into fb_insights_daily")


# ========== MAIN ==========

def main():
    if not FB_CONFIG_RAW:
        raise RuntimeError("FB_CONFIG not set")
    config = json.loads(FB_CONFIG_RAW)
    conn = get_conn()
    ensure_tables(conn)

    today = date.today()
    target_to = today  # включая сегодня

    for cfg in config:
        token = cfg["token"]
        targetologist = cfg.get("targetologist", "unknown")
        ids = cfg.get("ids", [])
        for acc in ids:
            account_id = acc.replace("act_", "")
            last = get_watermark(conn, account_id)
            if last:
                date_from = last - timedelta(days=RELOAD_LAST_DAYS)
            else:
                date_from = today - timedelta(days=DEFAULT_DAYS_BACK)
            rows = fetch_insights_for_account(token, account_id, date_from, target_to, targetologist)
            if rows:
                upsert_insights(conn, rows)
                set_watermark(conn, account_id, target_to)

    conn.close()
    print("***OK*** Facebook → Neon load complete (with leads & link_clicks)")


if __name__ == "__main__":
    main()
