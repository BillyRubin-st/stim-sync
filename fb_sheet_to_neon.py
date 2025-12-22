# fb_sheet_to_neon.py
# Google Sheets (published CSV) -> Neon (Postgres) UPSERT
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

    # remove percent sign
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
    # comma
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=",")
    if not reader.fieldnames or len(reader.fieldnames) <= 1:
        # semicolon (common in RU locales)
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fb_ads_daily (
          date date NOT NULL,
          account_name text NOT NULL,
          campaign text NOT NULL,
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
          PRIMARY KEY (date, account_name, campaign)
        );
        """
    )


def upsert_rows(cur, rows):
    sql = """
    INSERT INTO fb_ads_daily
      (date, account_name, campaign, leads, spend, cpl, reach, impressions, link_clicks, cpm, ctr, cr, updated_at)
    VALUES
      (%(date)s, %(account_name)s, %(campaign)s, %(leads)s, %(spend)s, %(cpl)s, %(reach)s,
       %(impressions)s, %(link_clicks)s, %(cpm)s, %(ctr)s, %(cr)s, NOW())
    ON CONFLICT (date, account_name, campaign)
    DO UPDATE SET
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

    # Required columns (with tolerant mapping):
    # - account: "Название аккаунта" (may have extra spaces/BOM)
    # - campaign: "Кампания"
    # - date: "Дата" OR "Дата начала" (fallback)
    required_any = {
        "account": ("Название аккаунта",),
        "campaign": ("Кампания",),
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

        date = to_date_iso(date_val)
        account = norm(str(acc_val)) if acc_val is not None else ""
        campaign = norm(str(camp_val)) if camp_val is not None else ""

        if not date or not account or not campaign:
            continue

        leads = to_int(get_cell(row, header_map, "Лиды"))
        spend = to_num(get_cell(row, header_map, "Затраты"))
        cpl = to_num(get_cell(row, header_map, "Цена за лид"))
        reach = to_int(get_cell(row, header_map, "Охваты"))
        impressions = to_int(get_cell(row, header_map, "Показы"))

        # Clicks column names might vary
        link_clicks = to_int(
            get_cell(row, header_map, "Клики по ссылке", "Клики по ссыл.", "Клики", "Link clicks", "link_clicks")
        )

        cpm = to_num(get_cell(row, header_map, "CPM", "Cpm", "cpm"))

        # CTR/CR headers may vary; value might contain %
        ctr = to_num(get_cell(row, header_map, "CTR (%)", "CTR(%)", "CTR %", "CTR", "ctr"))
        cr = to_num(get_cell(row, header_map, "CR (%)", "CR(%)", "CR %", "CR", "cr"))

        rows.append(
            {
                "date": date,
                "account_name": account,
                "campaign": campaign,
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
