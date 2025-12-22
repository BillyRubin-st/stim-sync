import os
import csv
import io
import requests
from dateutil import parser as dateparser
import psycopg2

CSV_URL = os.environ["SHEETS_CSV_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]

def norm(s: str) -> str:
    # убираем BOM, пробелы, переносы
    return (s or "").replace("\ufeff", "").strip()

def to_int(x):
    if x in (None, ""): return None
    try:
        return int(float(str(x).replace(" ", "").replace(",", ".")))
    except:
        return None

def to_num(x):
    if x in (None, ""): return None
    try:
        return float(str(x).replace(" ", "").replace(",", "."))
    except:
        return None

def to_date_iso(x):
    s = norm(str(x)) if x is not None else ""
    if not s: return None
    try:
        dt = dateparser.parse(s, dayfirst=True)
        return dt.date().isoformat()
    except:
        return None

def main():
    r = requests.get(CSV_URL, timeout=60)
    r.raise_for_status()

    text = r.text.lstrip("\ufeff")

    # пробуем стандартную запятую, если не распарсилось — пробуем ; (часто в RU локали)
    def parse_with(delim):
        return csv.DictReader(io.StringIO(text), delimiter=delim)

    reader = parse_with(",")
    if not reader.fieldnames or len(reader.fieldnames) <= 1:
        reader = parse_with(";")

    # нормализуем заголовки
    raw_headers = reader.fieldnames or []
    headers = [norm(h) for h in raw_headers]
    print("HEADERS:", headers)

    # создаём мапу "нормализованный заголовок" -> "как в CSV реально"
    header_map = {norm(h): h for h in raw_headers}

    def get(row, key):
        # key — ожидаемое имя (например "Дата"), находим реальное
        real = header_map.get(key)
        if real is None:
            return None
        return row.get(real)

    # проверяем обязательные колонки (по нормализованным именам)
    required = ["Название аккаунта", "Кампания", "Дата"]
    missing = [c for c in required if c not in header_map]
    if missing:
        raise RuntimeError(f"Нет колонки(ок): {missing}. Реальные заголовки: {headers}")

    rows = []
    for row in reader:
        date = to_date_iso(get(row, "Дата"))
        acc = norm(get(row, "Название аккаунта"))
        camp = norm(get(row, "Кампания"))
        if not date or not acc or not camp:
            continue

        rows.append({
            "date": date,
            "account_name": acc,
            "campaign": camp,
            "leads": to_int(get(row, "Лиды")),
            "spend": to_num(get(row, "Затраты")),
            "cpl": to_num(get(row, "Цена за лид")),
            "reach": to_int(get(row, "Охваты")),
            "impressions": to_int(get(row, "Показы")),
            "link_clicks": to_int(get(row, "Клики по ссылке")),
            "cpm": to_num(get(row, "CPM")),
            "ctr": to_num(get(row, "CTR (%)")),
            "cr": to_num(get(row, "CR (%)")),
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
