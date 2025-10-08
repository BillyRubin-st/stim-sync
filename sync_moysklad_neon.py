#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError

# ================== КОНСТАНТЫ ==================
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
MS_LIMIT = 1000
FIRST_LOAD_DAYS = 9999
ATTR_NAMES = ["Проект", "Таргетолог", "Аккаунт", "Контент", "Сектор"]

# сколько заказов обрабатывать в одной транзакции
BATCH_SIZE = 500

# HTTP таймауты (connect, read)
HTTP_TIMEOUT = (15, 60)

# ================== ЛОГИ ==================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("stim-sync")

# ================== ВРЕМЯ ==================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_ms_filter_date(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_iso(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    if iso.endswith("Z"):
        iso = iso.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return None

# ================== HTTP CLIENT c RETRY ==================
def build_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=7,
        connect=7,
        read=7,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

SESSION = build_session()

def ms_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;charset=utf-8",
        "User-Agent": "stim-sync/1.1",
    }

def fetch_json(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    # все GET идут через Session с retry и таймаутом
    resp = SESSION.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    # если получили нестандартный код — выбросим, чтобы сработал retry выше
    resp.raise_for_status()
    return resp.json()

# ================== УТИЛЫ ПО MS ==================
_name_cache: Dict[str, str] = {}

def resolve_name_from_meta(meta: Dict[str, Any], headers: Dict[str, str]) -> str:
    """Безопасно разворачивает имя по href. В случае таймаута/ошибки — вернёт ''."""
    try:
        href = (meta or {}).get("href") or ""
        if not href:
            return ""
        if href in _name_cache:
            return _name_cache[href]
        data = fetch_json(href, headers=headers)
        nm = str(data.get("name") or "")
        _name_cache[href] = nm
        return nm
    except Exception as e:
        log.warning("resolve_name_from_meta failed: %s", e)
        return ""

def path_id(obj: Optional[dict]) -> Optional[str]:
    try:
        href = ((obj or {}).get("meta") or {}).get("href") or ""
        if not href:
            return None
        return href.rsplit("/", 1)[-1] or None
    except Exception:
        return None

def attr_val_by_name_resolve(attrs: List[Dict[str, Any]], name: str, headers: Dict[str, str]) -> str:
    if not attrs:
        return ""
    n = name.strip().lower()
    a = next((x for x in attrs if str(x.get("name", "")).strip().lower() == n), None)
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

# ================== БАЗА ДАННЫХ ==================
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

CREATE TABLE IF NOT EXISTS sync_watermarks(
  key        TEXT PRIMARY KEY,
  value_utc  TIMESTAMPTZ
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

SELECT_WM = "SELECT value_utc FROM sync_watermarks WHERE key = %s"
UPSERT_WM  = """
INSERT INTO sync_watermarks(key, value_utc)
VALUES (%s, %s)
ON CONFLICT(key) DO UPDATE SET value_utc = EXCLUDED.value_utc
"""

def connect_pg():
    url = os.getenv("DATABASE_URL")
    kwargs = {}
    if url:
        kwargs["dsn"] = url
    else:
        kwargs.update(
            host=os.getenv("PGHOST"),
            port=int(os.getenv("PGPORT", "5432")),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            dbname=os.getenv("PGDATABASE"),
        )
    # Включаем SSL (для Neon) и TCP keepalive, чтобы соединение не засыпало
    return psycopg2.connect(
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        **kwargs,
    )

def ensure_schema(cur):
    cur.execute(DDL)
    # на всякий — ограничим statement_timeout (миллисекунды)
    cur.execute("SET statement_timeout = %s", ("600000",))  # 10 минут

def get_watermark(cur, key: str) -> Optional[datetime]:
    cur.execute(SELECT_WM, (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_watermark(cur, key: str, val: datetime):
    cur.execute(UPSERT_WM, (key, val))

# ================== ПОЗИЦИИ ==================
def fetch_positions(order_id: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url = f"{MS_BASE}/entity/customerorder/{order_id}/positions?{urlencode({'limit': MS_LIMIT, 'expand':'assortment'})}"
    while url:
        data = fetch_json(url, headers)
        rows = data.get("rows") or []
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

# ================== ОСНОВНАЯ СИНХРОНИЗАЦИЯ ==================
def run_batch(cur, batch, headers):
    """Запись пачки заказов + позиций, возвращает max(updated) из пачки."""
    max_upd = None
    for r in batch:
        order_id = r.get("id")
        name     = r.get("name") or ""
        moment   = parse_iso(r.get("moment"))
        updated  = parse_iso(r.get("updated") or r.get("moment"))
        if updated and (max_upd is None or updated > max_upd):
            max_upd = updated

        sum_rub  = (r.get("sum") or 0) / 100.0

        # state
        state = ""
        if r.get("state"):
            state = r["state"].get("name") or ""
            if not state and r["state"].get("meta"):
                state = resolve_name_from_meta(r["state"]["meta"], headers)

        agent_id = path_id(r.get("agent"))
        store_id = path_id(r.get("store"))

        attrs = r.get("attributes") or []
        custom = {nm: attr_val_by_name_resolve(attrs, nm, headers) for nm in ATTR_NAMES}

        # проект: имя из стандартного поля — только если в кастомном пусто
        if not custom.get("Проект"):
            pr = r.get("project") or {}
            project_name = pr.get("name") or ""
            if not project_name and pr.get("meta"):
                project_name = resolve_name_from_meta(pr["meta"], headers)
            custom["Проект"] = project_name

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

        # позиции (быстро, без отдельной транзакции)
        for p in fetch_positions(order_id, headers):
            cur.execute(
                UPSERT_POS,
                {**p, "updated": updated},
            )
    return max_upd

def sync():
    ms_token = os.getenv("MS_TOKEN")
    if not ms_token:
        log.error("MS_TOKEN not set")
        sys.exit(1)
    headers = ms_headers(ms_token)

    # один раз пробуем переподключиться при разрыве соединения
    for attempt_conn in (1, 2):
        try:
            with connect_pg() as conn:
                conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    ensure_schema(cur)

                    wm_key = "customerorder"
                    wm = get_watermark(cur, wm_key)
                    since = wm if wm else (now_utc() - timedelta(days=FIRST_LOAD_DAYS))
                    log.info("Sync since (UTC): %s", since.isoformat())

                    params = {
                        "limit": MS_LIMIT,
                        "filter": f"updated>={to_ms_filter_date(since)}",
                        "expand": "attributes,state,project",
                    }
                    url = f"{MS_BASE}/entity/customerorder?{urlencode(params)}"

                    total_orders = 0
                    max_updated_global: Optional[datetime] = wm

                    batch: List[dict] = []

                    while url:
                        data = fetch_json(url, headers=headers)
                        rows = data.get("rows") or []
                        if not rows:
                            break

                        for r in rows:
                            batch.append(r)
                            if len(batch) >= BATCH_SIZE:
                                # короткая транзакция
                                max_upd = run_batch(cur, batch, headers)
                                total_orders += len(batch)
                                if max_upd:
                                    set_watermark(cur, wm_key, max_upd)
                                    max_updated_global = max_upd
                                conn.commit()
                                batch.clear()
                                log.info("Committed batch, total orders: %d", total_orders)

                        url = (data.get("meta") or {}).get("nextHref")

                    # хвост
                    if batch:
                        max_upd = run_batch(cur, batch, headers)
                        total_orders += len(batch)
                        if max_upd:
                            set_watermark(cur, wm_key, max_upd)
                            max_updated_global = max_upd
                        conn.commit()
                        log.info("Committed final batch, total orders: %d", total_orders)

                    log.info(
                        "Done. Orders processed: %s. Watermark: %s",
                        total_orders,
                        max_updated_global.isoformat() if max_updated_global else None,
                    )
                    return  # успех
        except OperationalError as e:
            log.warning("Postgres connection error (attempt %d/2): %s", attempt_conn, e)
            if attempt_conn == 2:
                raise
            time.sleep(3)
        except requests.exceptions.ReadTimeout as e:
            # это уже обрабатывается retry’ями с backoff, но вдруг дошло сюда
            log.warning("HTTP ReadTimeout (outer): %s", e)
            time.sleep(2)
        except Exception as e:
            log.exception("Sync failed: %s", e)
            raise

if __name__ == "__main__":
    try:
        sync()
    except Exception as e:
        log.exception("Fatal: %s", e)
        sys.exit(2)
