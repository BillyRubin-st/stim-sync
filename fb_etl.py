import os
import sys
import json
import logging
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, wait_exponential, stop_after_attempt

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

DATABASE_URL = os.getenv("DATABASE_URL")
FB_ACCESS_TOKENS = [t.strip() for t in os.getenv("FB_ACCESS_TOKENS", "").split(",") if t.strip()]
FB_ACCOUNTS = [a.strip() for a in os.getenv("FB_ACCOUNTS", "").split(",") if a.strip()]
DAYS_BACK = int(os.getenv("DAYS_BACK", "180"))
GRAPH = os.getenv("FB_GRAPH_BASE", "https://graph.facebook.com/v21.0")
LEVEL = os.getenv("FB_LEVEL", "ad")
REPORT_LOCALE = os.getenv("REPORT_LOCALE", "ru_RU")

FIELDS = [
    "date_start","date_stop",
    "account_id",
    "campaign_id","campaign_name",
    "adset_id","adset_name",
    "ad_id","ad_name",
    "account_currency",
    "impressions","clicks","reach","spend",
    "inline_link_clicks",
    "cpc","cpm","cpp","ctr",
    "purchase_roas",
    "actions"
]

BASE_PARAMS = {
    "level": LEVEL,
    "time_increment": 1,
    "limit": 500,
    "fields": ",".join(FIELDS),
    "locale": REPORT_LOCALE
}

def parse_actions(actions, action_type):
    if not actions:
        return 0
    val = 0
    for a in actions:
        if a.get("action_type") == action_type:
            try:
                val += int(float(a.get("value", 0)))
            except Exception:
                try:
                    val += int(a.get("value", 0))
                except Exception:
                    pass
    return val

def parse_roas(purchase_roas):
    if not purchase_roas:
        return None
    try:
        v = purchase_roas[0].get("value")
        return float(v) if v is not None else None
    except Exception:
        return None

@retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(7))
def fb_get(url, params):
    r = requests.get(url, params=params, timeout=60)
    data = r.json()
    if r.status_code >= 500:
        raise RuntimeError(f"FB 5xx {r.status_code} {data}")
    if "error" in data:
        err = data["error"]
        if err.get("code") in (1,2,4,17,32,613):
            raise RuntimeError(f"FB throttled {err}")
        raise RuntimeError(f"FB error {err}")
    return data

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def upsert_rows(conn, rows):
    if not rows:
        return 0
    sql = """
    INSERT INTO fb.insights_daily(
        date, account_id, campaign_id, adset_id, ad_id,
        campaign_name, adset_name, ad_name, currency,
        impressions, clicks, reach, spend,
        purchases, leads, inline_link_clicks,
        cpc, cpm, cpp, ctr, roas
    ) VALUES %s
    ON CONFLICT (date, account_id, campaign_id, adset_id, ad_id)
    DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name,
        adset_name    = EXCLUDED.adset_name,
        ad_name       = EXCLUDED.ad_name,
        currency      = EXCLUDED.currency,
        impressions   = EXCLUDED.impressions,
        clicks        = EXCLUDED.clicks,
        reach         = EXCLUDED.reach,
        spend         = EXCLUDED.spend,
        purchases     = EXCLUDED.purchases,
        leads         = EXCLUDED.leads,
        inline_link_clicks = EXCLUDED.inline_link_clicks,
        cpc           = EXCLUDED.cpc,
        cpm           = EXCLUDED.cpm,
        cpp           = EXCLUDED.cpp,
        ctr           = EXCLUDED.ctr,
        roas          = EXCLUDED.roas,
        pulled_at     = now();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)

def fetch_account_day(token, account_id, day):
    url = f"{GRAPH}/act_{account_id}/insights"
    params = BASE_PARAMS.copy()
    day_str = day.strftime("%Y-%m-%d")
    params.update({
        "access_token": token,
        "time_range": json.dumps({"since": day_str, "until": day_str}),
        "action_attribution_windows": "1d_view,7d_click"
    })

    rows = []
    while True:
        data = fb_get(url, params)
        for item in data.get("data", []):
            d = item.get("date_start")
            actions = item.get("actions")
            purchases = parse_actions(actions, "omni_purchase") or parse_actions(actions, "purchase")
            leads     = parse_actions(actions, "lead")
            roas      = parse_roas(item.get("purchase_roas"))

            rows.append((
                dt.datetime.strptime(d, "%Y-%m-%d").date(),
                item.get("account_id"),
                item.get("campaign_id"),
                item.get("adset_id"),
                item.get("ad_id"),
                item.get("campaign_name"),
                item.get("adset_name"),
                item.get("ad_name"),
                item.get("account_currency"),
                int(item.get("impressions", 0) or 0),
                int(item.get("clicks", 0) or 0),
                int(item.get("reach", 0) or 0),
                float(item.get("spend", 0) or 0),
                int(purchases or 0),
                int(leads or 0),
                int(item.get("inline_link_clicks", 0) or 0),
                float(item.get("cpc", 0) or 0),
                float(item.get("cpm", 0) or 0),
                float(item.get("cpp", 0) or 0),
                float(item.get("ctr", 0) or 0),
                roas if roas is not None else None
            ))

        after = data.get("paging", {}).get("cursors", {}).get("after")
        if after:
            params["after"] = after
        else:
            break
    return rows

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)

def main():
    if not DATABASE_URL or not FB_ACCOUNTS or not FB_ACCESS_TOKENS:
        logging.error("ENV DATABASE_URL, FB_ACCOUNTS, FB_ACCESS_TOKENS must be set")
        sys.exit(1)

    today = dt.date.today()
    start = today - dt.timedelta(days=DAYS_BACK)
    end   = today

    logging.info(f"Range: {start}..{end}")
    inserted = 0
    conn = get_conn()
    try:
        for i, acc in enumerate(FB_ACCOUNTS):
            token = FB_ACCESS_TOKENS[i % len(FB_ACCESS_TOKENS)]
            logging.info(f"Account {acc} with token #{(i % len(FB_ACCESS_TOKENS))+1}")
            for day in daterange(start, end):
                rows = fetch_account_day(token, acc, day)
                if rows:
                    cnt = upsert_rows(conn, rows)
                    inserted += cnt
                    logging.info(f"{acc} {day}: upsert {cnt}")
    finally:
        conn.close()
    logging.info(f"Done. total upserted: {inserted}")

if __name__ == "__main__":
    main()
