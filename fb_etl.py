# fb_etl.py
import os
import sys
import json
import logging
import datetime as dt
import requests
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError

# ---------- ЛОГИ ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
FB_DEBUG = (os.getenv("FB_DEBUG", "0").strip() == "1")

# ---------- ENV ----------
DATABASE_URL = os.getenv("DATABASE_URL")

FB_ACCESS_TOKENS = [t.strip() for t in os.getenv("FB_ACCESS_TOKENS", "").split(",") if t.strip()]
FB_ACCOUNTS = [a.strip().replace("act_", "") for a in os.getenv("FB_ACCOUNTS", "").split(",") if a.strip()]

raw_days = (os.getenv("DAYS_BACK") or "180").strip()
try:
    DAYS_BACK = int(raw_days)
except Exception:
    DAYS_BACK = 180

raw_tail = (os.getenv("REFRESH_TAIL_DAYS") or "2").strip()
try:
    REFRESH_TAIL_DAYS = max(0, int(raw_tail))  # сколько последних дней всегда переобновляем
except Exception:
    REFRESH_TAIL_DAYS = 2

GRAPH = os.getenv("FB_GRAPH_BASE", "https://graph.facebook.com/v21.0")
LEVEL = os.getenv("FB_LEVEL", "ad")  # campaign|adset|ad
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
    "time_increment": 1,   # daily
    "limit": 500,
    "fields": ",".join(FIELDS),
    "locale": REPORT_LOCALE
}

# ---------- HELPERS ----------
def mask_token(tok: str) -> str:
    if not tok:
        return ""
    if len(tok) <= 10:
        return "***"
    return tok[:6] + "..." + tok[-4:]

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

@retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(5))
def fb_get(url, params):
    """Запрос к FB с ретраями на троттлинг/5xx и подробными ошибками."""
    resp = requests.get(url, params=params, timeout=60)

    # иногда приходит не-JSON
    try:
        data = resp.json()
    except Exception:
        txt = (resp.text or "")[:400]
        raise RuntimeError(f"FB non-JSON response [{resp.status_code}]: {txt}")

    if resp.status_code >= 500:
        if FB_DEBUG:
            logging.error("FB 5xx: %s | url=%s | params=%s", resp.status_code, url, params)
        raise RuntimeError(f"FB 5xx: {resp.status_code}")

    if "error" in data:
        err = data["error"]
        code = err.get("code")
        msg  = err.get("message")
        sub  = err.get("error_subcode")
        typ  = err.get("type")
        # троттлинг/лимиты — ретраим
        if code in (1,2,4,17,32,613):
            if FB_DEBUG:
                logging.error("FB throttled | code=%s sub=%s type=%s msg=%s", code, sub, typ, msg)
            raise RuntimeError(f"FB throttled code={code} sub={sub} type={typ} msg={msg}")
        # неверный токен/нет доступа — не ретраим, пробрасываем
        raise RuntimeError(f"FB error code={code} sub={sub} type={typ} msg={msg}")

    return data

# ---------- DB ----------
def get_conn():
    # keepalive, чтобы соединение не отваливалось на долгих запросах
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=20,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

def ensure_fb_tables(conn):
    """Создаёт схему/таблицу для инсайтов, если их нет."""
    sql = """
    CREATE SCHEMA IF NOT EXISTS fb;

    CREATE TABLE IF NOT EXISTS fb.insights_daily (
        date               date        NOT NULL,
        account_id         text        NOT NULL,
        campaign_id        text,
        adset_id           text,
        ad_id              text,
        campaign_name      text,
        adset_name         text,
        ad_name            text,
        currency           text,
        impressions        bigint,
        clicks             bigint,
        reach              bigint,
        spend              numeric(18,6),
        purchases          bigint,
        leads              bigint,
        inline_link_clicks bigint,
        cpc                numeric(18,6),
        cpm                numeric(18,6),
        cpp                numeric(18,6),
        ctr                numeric(18,6),
        roas               numeric(18,6),
        pulled_at          timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT insights_daily_pk PRIMARY KEY (date, account_id, campaign_id, adset_id, ad_id)
    );

    CREATE INDEX IF NOT EXISTS idx_fb_date     ON fb.insights_daily(date);
    CREATE INDEX IF NOT EXISTS idx_fb_campaign ON fb.insights_daily(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_fb_ad       ON fb.insights_daily(ad_id);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def ensure_watermarks(conn):
    """Создаёт/чинит таблицу водяных знаков. Поддерживает твою старую структуру (key/value_utc/last_date/source)."""
    with conn.cursor() as cur:
        # базовая таблица (если когда-то создавалась иначе — просто добавим недостающее)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.sync_watermarks (
              key        TEXT,
              value_utc  timestamptz,
              last_date  DATE,
              source     TEXT
            );
        """)
        # заполняем source, если пусто
        cur.execute("UPDATE public.sync_watermarks SET source = COALESCE(source, key);")
        # удаляем дубликаты по source
        cur.execute("""
            WITH ranked AS (
               SELECT ctid,
                      ROW_NUMBER() OVER (
                        PARTITION BY source
                        ORDER BY COALESCE(value_utc, 'epoch'::timestamptz) DESC,
                                 COALESCE(last_date, DATE '1900-01-01') DESC
                      ) AS rn
               FROM public.sync_watermarks
               WHERE source IS NOT NULL
            )
            DELETE FROM public.sync_watermarks sw
            USING ranked r
            WHERE sw.ctid = r.ctid AND r.rn > 1;
        """)
        # уникальность по source
        cur.execute("""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname='public'
                  AND indexname='uq_sync_watermarks_source'
              ) THEN
                CREATE UNIQUE INDEX uq_sync_watermarks_source
                  ON public.sync_watermarks (source);
              END IF;
            END $$;
        """)
        # стартовая запись для FB (если нет)
        cur.execute("""
          INSERT INTO public.sync_watermarks (source, last_date)
          VALUES ('facebook_insights', CURRENT_DATE - INTERVAL '3 days')
          ON CONFLICT (source) DO NOTHING;
        """)
    conn.commit()

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

def get_last_synced_date(conn, source):
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM public.sync_watermarks WHERE source = %s", (source,))
        row = cur.fetchone()
        return row[0] if row else None

def update_last_synced_date(conn, source, date_):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.sync_watermarks (source, last_date)
            VALUES (%s, %s)
            ON CONFLICT (source) DO UPDATE SET last_date = EXCLUDED.last_date,
                                               value_utc = now();
        """, (source, date_))
    conn.commit()

def safe_update_last_synced_date(conn, source, date_):
    """Обновляет watermark; если коннект умер — переподключаемся и повторяем один раз."""
    try:
        if conn.closed:
            conn = get_conn()
        update_last_synced_date(conn, source, date_)
    except OperationalError:
        conn = get_conn()
        update_last_synced_date(conn, source, date_)
    return conn

# ---------- FB ----------
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
            d = item.get("date_start")  # == date_stop при time_increment=1
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

# ---------- MAIN ----------
def main():
    if not DATABASE_URL or not FB_ACCOUNTS or not FB_ACCESS_TOKENS:
        logging.error("ENV DATABASE_URL, FB_ACCOUNTS, FB_ACCESS_TOKENS must be set")
        sys.exit(1)

    today = dt.date.today()
    end = today

    conn = get_conn()
    try:
        # ensure tables
        ensure_fb_tables(conn)
        ensure_watermarks(conn)

        source_name = "facebook_insights"
        last_synced = get_last_synced_date(conn, source_name)

        default_start = end - dt.timedelta(days=DAYS_BACK)

        if last_synced:
            # инкремент + «хвост» переобновления
            start = max(default_start, last_synced - dt.timedelta(days=max(0, REFRESH_TAIL_DAYS - 1)))
            logging.info(f"Incremental: last_synced={last_synced}, start={start}, end={end}, tail={REFRESH_TAIL_DAYS}")
        else:
            start = default_start
            logging.info(f"First load: start={start}, end={end} (DAYS_BACK={DAYS_BACK})")

        inserted_total = 0
        for i, acc in enumerate(FB_ACCOUNTS):
            token = FB_ACCESS_TOKENS[i % len(FB_ACCESS_TOKENS)]
            logging.info(f"[ACC] {acc} | token={mask_token(token)}")
            for day in daterange(start, end):
                try:
                    rows = fetch_account_day(token, acc, day)
                    cnt = upsert_rows(conn, rows)
                    inserted_total += cnt
                    logging.info(f"[OK] {acc} {day}: upsert {cnt}")
                except RetryError as e:
                    cause = e.last_attempt.exception() if hasattr(e, "last_attempt") else e
                    logging.error(f"[SKIP] Account {acc} day {day} failed: {cause}")
                    continue
                except Exception as e:
                    logging.error(f"[SKIP] Account {acc} day {day} failed: {e}")
                    continue

        # обновляем watermark на сегодняшнюю дату (с защитой от упавшего коннекта)
        conn = safe_update_last_synced_date(conn, source_name, end)
        logging.info(f"Done. total upserted: {inserted_total}")

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
