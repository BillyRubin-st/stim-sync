import os
import csv
import io
import requests
from dateutil import parser as dateparser
import psycopg2

CSV_URL = os.environ["SHEETS_CSV_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]

def to_int(x):
    if x in (None, ""): return None
    try:
        return int(float(str(x).replace(",", ".")))
    except:
        return None

def to_num(x):
    if x in (None, ""): return None
    try:
        return float(str(x).replace(",", "."))
    except:
        return None

def to_date_iso(x):
    if not x: return None
    try:
        dt = dateparser.parse(str(x), dayfirst=True)
        return dt.date().isoformat()
    except:
        return None

def main():
    r = requests.get(CSV_URL, timeout=60)
    r.raise_for_status()

    text = r.text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))

    required = ["Дата", "Название аккаунта", "Кампания"]
    for col in required:
        if col not in reader.fieldnames:
            raise RuntimeError(f"Нет колонки {col}")

    rows = []
    for row in reader:
        date = to_date_iso(row["Дата"])
        acc = row["Название аккаунта"].strip()
        camp = row["Кампания"].strip()
        if not date or not acc or not camp:
            continue

        rows.append({
            "date": date,
            "account_name": acc,
            "campaign": camp,
            "leads": to_int(row.get("Лиды")),
            "spend": to_num(row.get("Затраты")),
            "cpl": to_num(row.get("Цена за лид")),
            "reach": to_int(row.get("Охваты")),
            "impressions": to_int(row.get("Показы")),
            "link_clicks": to_int(row.get("Клики по ссылке")),
            "cpm": to_num(row.get("CPM")),
            "ctr": to_num(row.get("CTR (%)")),
            "cr": to_num(row.get("CR (%)")),
        })

    if not rows:
        print("Нет данных для загрузки")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
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
    """)

    sql = """
    INSERT INTO fb_ads_daily
    (date, account_name, campaign, leads, spend, cpl, reach, impressions,
     link_clicks, cpm, ctr, cr, updated_at)
    VALUES
    (%(date)s,%(account_name)s,%(campaign)s,%(leads)s,%(spend)s,%(cpl)s,
     %(reach)s,%(impressions)s,%(link_clicks)s,%(cpm)s,%(ctr)s,%(cr)s, NOW())
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

    conn.commit()
    cur.close()
    conn.close()
    print(f"OK: {len(rows)} строк обновлено")

if __name__ == "__main__":
    main()
