# fb_to_neon_incremental.py

import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
from dateutil import parser

# ================== CONFIG ==================

FB_API_VERSION = "v17.0"

# JSON-массив с токенами, таргетологами и списком аккаунтов.
# Берётся из переменной окружения FB_CONFIG (GitHub Secret).
FB_CONFIG_RAW = os.getenv("FB_CONFIG")

# Строка подключения к Neon (Postgres).
# Берётся из переменной PG_DSN (в workflow мы подставим secrets.DATABASE_URL).
PG_DSN = os.getenv("PG_DSN")

# Сколько дней истории тянуть при первом запуске (если watermark ещё нет).
DEFAULT_DAYS_BACK = int(os.getenv("FB_DEFAULT_DAYS_BACK", "60"))

# Сколько последних дней пересчитывать при каждом запуске (на случай корректировок в FB).
RELOAD_LAST_DAYS = int(os.getenv("FB_RELOAD_LAST_DAYS", "3"))

# Поля для insights.
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

# ============================================


def get_conn():
    if not PG_DSN:
        raise RuntimeError("PG_DSN is not set")
    return psycopg2.connect(PG_DSN)


def ensure_tables(conn):
    """Создаём таблицы, если их ещё нет."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fb_insights_daily (
            date           date                NOT NULL,
            account_id     text                NOT NULL,
            campaign_id    text,
            campaign_name  text,
            adset_id       text,
            adset_name     text,
            ad_id          text                NOT NULL,
            ad_name        text,
            impressions    bigint,
            clicks         bigint,
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
    """Читаем последнюю загруженную дату для аккаунта."""
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM fb_watermark WHERE account_id = %s", (account_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return None


def set_watermark(conn, account_id, last_date):
    """Обновляем watermark по аккаунту."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fb_watermark (account_id, last_date, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (account_id) DO UPDATE
                SET last_date = EXCLUDED.last_date,
                    updated_at = now();
        """, (account_id, last_date))
        conn.commit()


def fetch_insights_for_account(token, account_id, date_from, date_to, targetologist):
    """
    Тянем статистику по объявлениям за период [date_from, date_to]
    c шагом 1 день (time_increment=1) для конкретного account_id.
    account_id — БЕЗ префикса act_.
    Возвращаем список строк или None, если запрос упал.
    """
    print(f"***FB*** Fetch {account_id} ({targetologist}) from {date_from} to {date_to}")

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
    page = 1

    while True:
        resp = requests.get(url, params=params)

        if resp.status_code != 200:
            # Логируем ошибку и выходим
            try:
                err_json = resp.json()
            except Exception:
                err_json = resp.text
            print(f"***ERROR*** FB API error for account {account_id} ({targetologist}), "
                  f"status={resp.status_code}, response={err_json}")
            return None

        try:
            data = resp.json()
        except Exception as e:
            print(f"***ERROR*** Cannot parse JSON for account {account_id} ({targetologist}): {e}")
            print("Raw response:", resp.text[:500])
            return None

        for item in data.get("data", []):
            dt = parser.isoparse(item["date_start"]).date() if "date_start" in item else None

            row = {
                "date": dt,
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
                "targetologist": targetologist
            }
            all_rows.append(row)

        paging = data.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break

        url = next_url
        params = {}
        page += 1

    print(f"***FB*** {account_id} ({targetologist}): {len(all_rows)} rows fetched")
    return all_rows


def upsert_insights(conn, rows):
    """Upsert строк в fb_insights_daily."""
    if not rows:
        return

    cols = [
        "date", "account_id", "campaign_id", "campaign_name",
        "adset_id", "adset_name", "ad_id", "ad_name",
        "impressions", "clicks", "spend", "reach",
        "frequency", "ctr", "cpc", "cpp",
        "targetologist"
    ]

    values = [[r[c] for c in cols] for r in rows]

    with conn.cursor() as cur:
        sql = f"""
        INSERT INTO fb_insights_daily ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (date, account_id, ad_id) DO UPDATE
        SET
            campaign_id   = EXCLUDED.campaign_id,
            campaign_name = EXCLUDED.campaign_name,
            adset_id      = EXCLUDED.adset_id,
            adset_name    = EXCLUDED.adset_name,
            ad_name       = EXCLUDED.ad_name,
            impressions   = EXCLUDED.impressions,
            clicks        = EXCLUDED.clicks,
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


def main():
    if not FB_CONFIG_RAW:
        raise RuntimeError("FB_CONFIG is not set")

    config = json.loads(FB_CONFIG_RAW)
    if not isinstance(config, list) or not config:
        raise RuntimeError("FB_CONFIG must be a non-empty JSON array")

    conn = get_conn()
    ensure_tables(conn)

    today = date.today()
    target_to = today - timedelta(days=1)  # обычно тянем до вчера

    for cfg in config:
        token = cfg.get("token")
        targetologist = cfg.get("targetologist")
        ids = cfg.get("ids", [])

        if not token or not ids:
            print(f"***WARN*** Skipping config without token or ids: {cfg}")
            continue

        if not targetologist:
            targetologist = "unknown"

        for acc in ids:
            # убираем префикс act_, если есть
            if acc.startswith("act_"):
                account_id = acc[4:]
            else:
                account_id = acc

            try:
                last = get_watermark(conn, account_id)
                if last:
                    date_from = last - timedelta(days=RELOAD_LAST_DAYS)
                else:
                    date_from = today - timedelta(days=DEFAULT_DAYS_BACK)

                if date_from > target_to:
                    print(f"***SKIP*** {account_id} ({targetologist}): date_from > date_to")
                    continue

                rows = fetch_insights_for_account(
                    token=token,
                    account_id=account_id,
                    date_from=date_from,
                    date_to=target_to,
                    targetologist=targetologist
                )

                if rows is None:
                    print(f"***WARN*** {account_id} ({targetologist}): FB returned error, "
                          f"skipping upsert and watermark update")
                    continue

                if not rows:
                    print(f"***WARN*** {account_id} ({targetologist}): no data, "
                          f"skipping upsert and watermark update")
                    continue

                upsert_insights(conn, rows)
                set_watermark(conn, account_id, target_to)

            except Exception as e:
                print(f"***ERROR*** Unexpected error for account {account_id} ({targetologist}): {e}")

    conn.close()
    print("***OK*** Finished FB → Neon incremental load")


if __name__ == "__main__":
    main()
