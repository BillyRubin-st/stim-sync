# fb_sheet_to_neon.py
# Google Sheets (published CSV) -> Neon (Postgres) UPSERT
# Updated for "Актуальные данные 2.0" (ads-level) with creatives:
# - adset, ad_name, ad_id, creative_id, creative_name
# - PRIMARY KEY: (date, account_name, campaign, adset, ad_id)
#
# Robust to:
# - UTF-8 BOM / encoding issues
# - delimiter , or ;
# - header variations (spaces/BOM/newlines)
# - percent values like "3,15%" (stores as 3.15)

import os
import csv
import io
import requests
from dateutil import parser as dateparser
import psycopg2

CSV_URL = os.environ["SHEETS_CSV_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]


def norm(s: str) -> str:
    """Normalize header/value strings: remove BOM, trim spaces/newlines."""
    return (s or "").replace("\ufeff", "").strip()


def to_int(x):
    if x is None:
        return None
    s = str(x).strip().replace(" ", "")
    if s == "":
        return None
    try:
        return int(float(s.replace(",", ".")))
    except Exception:
        return None


def to_num(x):
    """Parse numbers like: 1.23 / 1,23 / 3,15% / 0 / '' -> float or None.
    For percent values, we keep 3.15 (NOT 0.0315).
    """
    if x is None:
        return None
    s = str(x).strip().replace(" ", "")
    if s == "":
        return None

    if s.endswith("%"):
        s = s[:-1]

    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def to_date_iso(x):
    """Parse date like '22.12.2025' -> '2025-12-22'."""
    s = norm(str(x)) if x is not None else ""
    if not s:
        return None
    try:
        dt = dateparser.parse(s, dayfirst=True)
        return dt.date().isoformat()
    except Exception:
        return None


def fetch_csv_text(url: str) -> str:
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    # Decode as UTF-8 reliably (avoid r.text auto-decoding issues)
    content = r.content
    if content.startswith(b"\xef\xbb\xbf"):  # UTF-8 BOM
        content = content[3:]
    return content.decode("utf-8", errors="replace")


def make_reader(csv_text: str) -> csv.DictReader:
    """Try comma delimiter first; if looks wrong, try semicolon."""
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=",")
    if not reader.fieldnames or len(reader.fieldnames) <= 1:
        reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    return reader


def build_header_map(fieldnames):
    """Map normalized header -> original header from CSV."""
    raw = fieldnames or []
    return {norm(h): h for h in raw}


def get_cell(row: dict, header_map: dict, *keys: str):
    """Return cell by trying multiple normalized header keys."""
    for k in keys:
        real = header_map.get(k)
        if real is not None:
            return row.get(real)
    return None


def ensure_table(cur):
    # NOTE:
    # This creates the table with the new schema if it doesn't exist.
    # If you already have fb_ads_daily with an old PRIMARY KEY,
    # run migration SQL in Neon (once):
    #
    # ALTER TABLE fb_ads_daily
    # ADD COLUMN IF NOT EXISTS adset TEXT,
    # ADD COLUMN IF NOT EXISTS ad_name TEXT,
    # ADD COLUMN IF NOT EXISTS ad_id TEXT,
    # ADD COLUMN IF NOT EXISTS creative_id TEXT,
    # ADD COLUMN IF NOT EXISTS creative_name TEXT;
    #
    # ALTER TABLE fb_ads_daily DROP CONSTRAINT IF EXISTS fb_ads_daily_pkey;
    # ALTER TABLE fb_ads_daily
    # ADD CONSTRAINT fb_ads_daily_pkey PRIMARY KEY (date, account_name, campaign, adset, ad_id);
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fb_ads_daily (
          date date NOT NULL,
          account_name text NOT NULL,
          campaign text NOT NULL,

          adset text,
          ad_name text,
          ad_id text,
          creative_id text,
          creative_name text,

          leads integer,
          spend numeric,
          cpl numeric,
          reach integer,
          impressions integer,
          link_clicks integer,
          cpm numeric,
          ctr numeric,
          cr numeric,
          updated_at timestamptz,

          PRIMARY KEY (date, account_name, campaign, adset, ad_id)
        );
        """
    )


def upsert_rows(cur, rows):
    sql = """
    INSERT INTO fb_ads_daily
      (date, account_name, campaign, adset, ad_name, ad_id, creative_id, creative_name,
       leads, spend, cpl, reach, impressions, link_clicks, cpm, ctr, cr, updated_at)
    VALUES
      (%(date)s, %(account_name)s, %(campaign)s, %(adset)s, %(ad_name)s, %(ad_id)s, %(creative_id)s, %(creative_name)s,
       %(leads)s, %(spend)s, %(cpl)s, %(reach)s, %(impressions)s, %(link_clicks)s, %(cpm)s, %(ctr)s, %(cr)s, NOW())
    ON CONFLICT (date, account_name, campaign, adset, ad_id)
    DO UPDATE SET
      ad_name = EXCLUDED.ad_name,
      creative_id = EXCLUDED.creative_id,
      creative_name = EXCLUDED.creative_name,
      leads = EXCLUDED.leads,
      spend = EXCLUDED.spend,
      cpl = EXCLUDED.cpl,
      reach = EXCLUDED.reach,
      impressions = EXCLUDED.impressions,
      link_clicks = EXCLUDED.link_clicks,
      cpm = EXCLUDED.cpm,
      ctr = EXCLUDED.ctr,
      cr = EXCLUDED.cr,
      updated_at = NOW();
    """
    for r in rows:
        cur.execute(sql, r)


def main():
    csv_text = fetch_csv_text(CSV_URL)
    reader = make_reader(csv_text)

    raw_headers = reader.fieldnames or []
    headers_norm = [norm(h) for h in raw_headers]
    header_map = build_header_map(raw_headers)

    print("HEADERS:", headers_norm)

    # Required columns (tolerant mapping) for "Актуальные данные 2.0":
    required_any = {
        "account": ("Название аккаунта",),
        "campaign": ("Кампания",),
        "adset": ("Адсет", "Adset"),
        "ad_id": ("Ad ID", "ad_id"),
        "date": ("Дата", "Дата начала"),
    }

    missing = []
    for name, keys in required_any.items():
        found = any(k in header_map for k in keys)
        if not found:
            missing.append(list(keys))

    if missing:
        raise RuntimeError(f"Нет колонки(ок): {missing}. Реальные заголовки: {headers_norm}")

    rows = []
    for row in reader:
        date_val = get_cell(row, header_map, "Дата", "Дата начала")
        acc_val = get_cell(row, header_map, "Название аккаунта")
        camp_val = get_cell(row, header_map, "Кампания")

        adset_val = get_cell(row, header_map, "Адсет", "Adset")
        ad_name_val = get_cell(row, header_map, "Объявление", "Ad Name", "ad_name")
        ad_id_val = get_cell(row, header_map, "Ad ID", "ad_id")
        creative_id_val = get_cell(row, header_map, "Creative ID", "creative_id")
        creative_name_val = get_cell(row, header_map, "Creative Name", "creative_name")

        date = to_date_iso(date_val)
        account = norm(str(acc_val)) if acc_val is not None else ""
        campaign = norm(str(camp_val)) if camp_val is not None else ""

        adset = norm(str(adset_val)) if adset_val is not None else ""
        ad_name = norm(str(ad_name_val)) if ad_name_val is not None else ""
        ad_id = norm(str(ad_id_val)) if ad_id_val is not None else ""
        creative_id = norm(str(creative_id_val)) if creative_id_val is not None else ""
        creative_name = norm(str(creative_name_val)) if creative_name_val is not None else ""

        # Required minimum for a unique row:
        if not date or not account or not campaign or not ad_id:
            continue

        leads = to_int(get_cell(row, header_map, "Лиды"))
        spend = to_num(get_cell(row, header_map, "Затраты"))
        cpl = to_num(get_cell(row, header_map, "Цена за лид"))
        reach = to_int(get_cell(row, header_map, "Охваты"))
        impressions = to_int(get_cell(row, header_map, "Показы"))

        link_clicks = to_int(
            get_cell(row, header_map, "Клики по ссылке", "Клики по ссыл.", "Клики", "Link clicks", "link_clicks")
        )

        cpm = to_num(get_cell(row, header_map, "CPM", "Cpm", "cpm"))
        ctr = to_num(get_cell(row, header_map, "CTR (%)", "CTR(%)", "CTR %", "CTR", "ctr"))
        cr = to_num(get_cell(row, header_map, "CR (%)", "CR(%)", "CR %", "CR", "cr"))

        rows.append(
            {
                "date": date,
                "account_name": account,
                "campaign": campaign,

                "adset": adset,
                "ad_name": ad_name,
                "ad_id": ad_id,
                "creative_id": creative_id,
                "creative_name": creative_name,

                "leads": leads,
                "spend": spend,
                "cpl": cpl,
                "reach": reach,
                "impressions": impressions,
                "link_clicks": link_clicks,
                "cpm": cpm,
                "ctr": ctr,
                "cr": cr,
            }
        )

    if not rows:
        print("Нет данных для загрузки (0 строк).")
        return

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        ensure_table(cur)
        upsert_rows(cur, rows)
        conn.commit()
        cur.close()
        print(f"OK: upserted {len(rows)} rows")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
