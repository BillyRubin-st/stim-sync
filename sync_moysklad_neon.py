#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Полная синхронизация из МойСклад → Neon Postgres:
- Заказы + Позиции.
- Инкремент по updated (водяная метка в таблице watermarks).
- Разворачивает имена по meta.href (state, project, custom-entity) с кешем.
- Ретраи 429 с экспоненциальной паузой.

Требует переменные окружения:
  MS_TOKEN            — токен МойСклад (Bearer)
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD — параметры Neon Postgres
или одной строкой:
  DATABASE_URL        — строка подключения PostgreSQL

Таблицы создаются автоматически (если их нет):
  orders, order_positions, watermarks.

Автор: ты :)
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlencode

import requests
import psycopg2
import psycopg2.extras

# ---------- Настройки ----------
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
MS_LIMIT = 1000
FIRST_LOAD_DAYS = 7
ATTR_NAMES = ["Проект", "Таргетолог", "Аккаунт", "Контент", "Сектор"]

# ---------- Логирование ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ---------- Хелперы времени ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_ms_filter_date(dt: datetime) -> str:
    """Формат для filter=updated>=YYYY-MM-DD HH:MM:SS (UTC)."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_iso(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    # МС отдает ISO 8601 с Z. Python понимает с +00:00
    if iso.endswith("Z"):
        iso = iso.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return None

# ---------- МойСклад HTTP ----------
def ms_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;charset=utf-8",
        "User-Agent": "stim-sync/1.0",
    }

def fetch_json(url: str, headers: Dict[str, str], max_attempts=6) -> Dict[str, Any]:
    attempt = 0
    while True:
        attempt += 1
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429 and attempt < max_attempts:
            # троттлинг
            delay = 0.5 * attempt * attempt
            log.warning("429 Too Many Requests → sleep %.1fs", delay)
            time.sleep(delay)
            continue
        if 200 <= resp.status_code < 300:
            return resp.json()
        log.error("HTTP %s for %s\nBody: %s", resp.status_code, url, resp.text)
        resp.raise_for_status()

# ---------- Вытаскиваем id из meta.href ----------
def path_id(obj: Any) -> str:
    try:
        href = obj.get("meta", {}).get("href", "")
        if not href:
            return ""
        return href.rstrip("/").split("/")[-1]
    except Exception:
        return ""

# ---------- Разворачивание имен по meta.href с кэшем ----------
_name_cache: Dict[str, str] = {}

def resolve_name_from_meta(meta: Dict[str, Any], headers: Dict[str, str]) -> str:
    href = meta.get("href")
    if not href:
        return ""
    if href in _name_cache:
        return _name_cache[href]
    data = fetch_json(href, headers=headers)
    nm = str(data.get("name") or "")
    _name_cache[href] = nm
    return nm

# ---------- Достаём кастомные поля по имени ----------
def attr_val_by_name_resolve(attrs: List[Dict[str, Any]], name: str, headers: Dict[str, str]) -> str:
    if not attrs:
        return ""
    n = name.strip().lower()
    a = next((x for x in attrs if str(x.get("name","")).strip().lower() == n), None)
    if not a:
        return ""
    v = a.get("value")
    if v is None:
        return ""
    if isinstance(v, dict):
        if "name" in v:
            return str(v["name"])
        if "meta" in v:
            return resolve_name_from_meta(v["meta"], headers)
        return json.dumps(v, ensure_ascii=False)
    return str(v)

# ---------- БД ----------
def get_conn():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url, sslmode="require")
    # Neon обычно требует SSL
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
        sslmode="require",
    )

DDL = """
CREATE TABLE IF NOT EXISTS orders(
  id          TEXT PRIMARY KEY,
  name        TEXT,
  moment      TIMESTAMPTZ,
  updated     TIMESTAMPTZ,
  sum_rub     NUMERIC(18,2),
  state       TEXT,
  agent_id    TEXT,
  store_id    TEXT,
  project     TEXT,
  targetolog  TEXT,
  account     TEXT,
  content     TEXT,
  sector      TEXT
);

CREATE TABLE IF NOT EXISTS order_positions(
  id          TEXT PRIMARY KEY,
  order_id    TEXT REFERENCES orders(id) ON DELETE CASCADE,
  product     TEXT,
  quantity    NUMERIC,
  price_rub   NUMERIC(18,2),
  discount    NUMERIC,
  vat         TEXT,
  updated     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS watermarks(
  wm_key  TEXT PRIMARY KEY,
  wm_val  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_orders_updated   ON orders(updated);
CREATE INDEX IF NOT EXISTS idx_orders_moment    ON orders(moment);
CREATE INDEX IF NOT EXISTS idx_orders_project   ON orders(project);
CREATE INDEX IF NOT EXISTS idx_orders_state     ON orders(state);
CREATE INDEX IF NOT EXISTS idx_pos_order_id     ON order_positions(order_id);
"""

UPSERT_ORDER = """
INSERT INTO orders(id, name, moment, updated, sum_rub, state, agent_id, store_id,
                   project, targetolog, account, content, sector)
VALUES (%(id)s, %(name)s, %(moment)s, %(updated)s, %(sum_rub)s, %(state)s, %(agent_id)s, %(store_id)s,
        %(project)s, %(targetolog)s, %(account)s, %(content)s, %(sector)s)
ON CONFLICT(id) DO UPDATE SET
  name       = EXCLUDED.name,
  moment     = EXCLUDED.moment,
  updated    = EXCLUDED.updated,
  sum_rub    = EXCLUDED.sum_rub,
  state      = EXCLUDED.state,
  agent_id   = EXCLUDED.agent_id,
  store_id   = EXCLUDED.store_id,
  project    = EXCLUDED.project,
  targetolog = EXCLUDED.targetolog,
  account    = EXCLUDED.account,
  content    = EXCLUDED.content,
  sector     = EXCLUDED.sector
"""

UPSERT_POS = """
INSERT INTO order_positions(id, order_id, product, quantity, price_rub, discount, vat, updated)
VALUES (%(id)s, %(order_id)s, %(product)s, %(quantity)s, %(price_rub)s, %(discount)s, %(vat)s, %(updated)s)
ON CONFLICT(id) DO UPDATE SET
  order_id = EXCLUDED.order_id,
  product  = EXCLUDED.product,
  quantity = EXCLUDED.quantity,
  price_rub= EXCLUDED.price_rub,
  discount = EXCLUDED.discount,
  vat      = EXCLUDED.vat,
  updated  = EXCLUDED.updated
"""

SELECT_WM = "SELECT wm_val FROM watermarks WHERE wm_key = %s"
UPSERT_WM = """
INSERT INTO watermarks(wm_key, wm_val)
VALUES (%s, %s)
ON CONFLICT(wm_key) DO UPDATE SET wm_val = EXCLUDED.wm_val
"""

def ensure_schema(cur):
    cur.execute(DDL)

def get_watermark(cur, key: str) -> Optional[datetime]:
    cur.execute(SELECT_WM, (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_watermark(cur, key: str, val: datetime):
    cur.execute(UPSERT_WM, (key, val))

# ---------- Сбор позиций ----------
def fetch_positions(order_id: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url = f"{MS_BASE}/entity/customerorder/{order_id}/positions"
    params = {"limit": MS_LIMIT, "expand": "assortment"}
    url = url + "?" + urlencode(params)
    while url:
        data = fetch_json(url, headers)
        rows = data.get("rows", []) or []
        for p in rows:
            assortment = p.get("assortment") or {}
            product = assortment.get("name") or ""
            if not product and "meta" in assortment:
                product = resolve_name_from_meta(assortment["meta"], headers)

            out.append({
                "id": p.get("id"),
                "order_id": order_id,
                "product": product,
                "quantity": p.get("quantity") or 0,
                "price_rub": (p.get("price") or 0) / 100.0,
                "discount": p.get("discount"),
                "vat": str(p.get("vat")) if p.get("vat") is not None else None,
            })
        url = (data.get("meta") or {}).get("nextHref")
    return out

# ---------- Основная загрузка ----------
def sync():
    ms_token = os.getenv("MS_TOKEN")
    if not ms_token:
        log.error("MS_TOKEN not set")
        sys.exit(1)

    headers = ms_headers(ms_token)

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            ensure_schema(cur)

            wm_key = "customerorder"
            wm = get_watermark(cur, wm_key)
            if wm is None:
                since = now_utc() - timedelta(days=FIRST_LOAD_DAYS)
            else:
                since = wm

            log.info("Sync since (UTC): %s", since.isoformat())

            params = {
                "limit": MS_LIMIT,
                "filter": f"updated>={to_ms_filter_date(since)}",
                "expand": "attributes,state,project",
            }
            url = f"{MS_BASE}/entity/customerorder?{urlencode(params)}"

            total_orders = 0
            max_updated: Optional[datetime] = wm

            while url:
                data = fetch_json(url, headers=headers)
                rows = data.get("rows", []) or []
                if not rows:
                    break

                for r in rows:
                    total_orders += 1
                    order_id = r.get("id")
                    name = r.get("name") or ""
                    moment = parse_iso(r.get("moment"))
                    updated = parse_iso(r.get("updated") or r.get("moment"))
                    if updated and (max_updated is None or updated > max_updated):
                        max_updated = updated

                    sum_rub = (r.get("sum") or 0) / 100.0

                    # state
                    state = ""
                    if r.get("state"):
                        state = r["state"].get("name") or ""
                        if not state and r["state"].get("meta"):
                            state = resolve_name_from_meta(r["state"]["meta"], headers)

                    agent_id = path_id(r.get("agent") or {})
                    store_id = path_id(r.get("store") or {})

                    attrs = r.get("attributes") or []
                    # Кастомные
                    custom = {nm: attr_val_by_name_resolve(attrs, nm, headers) for nm in ATTR_NAMES}

                    # Проект: стандартное поле как fallback
                    project_name = custom.get("Проект") or ""
                    if not project_name and r.get("project"):
                        project_name = r["project"].get("name") or ""
                        if not project_name and r["project"].get("meta"):
                            project_name = resolve_name_from_meta(r["project"]["meta"], headers)
                    custom["Проект"] = project_name

                    # UPSERT orders
                    cur.execute(
                        UPSERT_ORDER,
                        {
                            "id": order_id,
                            "name": name,
                            "moment": moment,
                            "updated": updated,
                            "sum_rub": sum_rub,
                            "state": state,
                            "agent_id": agent_id,
                            "store_id": store_id,
                            "project": custom.get("Проект") or "",
                            "targetolog": custom.get("Таргетолог") or "",
                            "account": custom.get("Аккаунт") or "",
                            "content": custom.get("Контент") or "",
                            "sector": custom.get("Сектор") or "",
                        },
                    )

                    # позиции
                    positions = fetch_positions(order_id, headers)
                    for p in positions:
                        cur.execute(
                            UPSERT_POS,
                            {
                                **p,
                                "updated": updated,
                            },
                        )

                url = (data.get("meta") or {}).get("nextHref")

            if max_updated:
                set_watermark(cur, wm_key, max_updated)

            conn.commit()
            log.info("Done. Orders processed: %s. New watermark: %s",
                     total_orders, max_updated.isoformat() if max_updated else None)

if __name__ == "__main__":
    from datetime import timedelta
    try:
        sync()
    except Exception as e:
        log.exception("Sync failed: %s", e)
        sys.exit(2)
