"""SQLite 访问（薄封装，无 ORM）。

表：
- meta(key,value)                       seed 版本
- batches(...)                          已入库批次；status=active/retracted，seq 为入库顺序
- fact_orders(...)                      当前明细事实，仅 active 批次保留
- batch_aggregates(...)                 按 site 聚合快照（撤回后仍保留，作为原始批次证据）
- import_attempts(...)                  每次上传的结论（含回滚记录）/review 用
- batch_retractions(...)                批次撤回记录（含进度、撤回依据、前后汇总）
- batch_retraction_items(...)           被撤回批次的逐行快照 + 后续版本冲突清单
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT NOT NULL,
  bom INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  forced INTEGER NOT NULL DEFAULT 0,
  superseded_by TEXT,
  seq INTEGER,
  status TEXT NOT NULL DEFAULT 'active',
  retracted_at TEXT,
  retraction_id TEXT,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_sha ON batches(sha256);

CREATE TABLE IF NOT EXISTS fact_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  order_date TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_orders(batch_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_id ON fact_orders(order_id);

CREATE TABLE IF NOT EXISTS batch_aggregates (
  batch_id TEXT NOT NULL,
  site TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (batch_id, site)
);

-- 当前站点汇总（与 fact_orders 在同一事务内切换；撤回后立刻反映）
CREATE TABLE IF NOT EXISTS site_totals (
  site TEXT PRIMARY KEY,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS import_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT,
  status TEXT NOT NULL,
  error_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  batch_id TEXT,
  detail TEXT,
  errors_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_retractions (
  retraction_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  batch_id TEXT NOT NULL,
  reason TEXT,
  operator TEXT,
  status TEXT NOT NULL,
  planned_rows INTEGER NOT NULL DEFAULT 0,
  total_rows INTEGER NOT NULL DEFAULT 0,
  removed_rows INTEGER NOT NULL DEFAULT 0,
  conflict_rows INTEGER NOT NULL DEFAULT 0,
  rows_before INTEGER,
  rows_after INTEGER,
  summary_before_json TEXT,
  summary_after_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_retractions_batch ON batch_retractions(batch_id);
CREATE INDEX IF NOT EXISTS idx_retractions_status ON batch_retractions(status);

CREATE TABLE IF NOT EXISTS batch_retraction_items (
  retraction_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT,
  category TEXT,
  order_date TEXT,
  amount_cents INTEGER,
  qty INTEGER,
  memo TEXT,
  action TEXT NOT NULL,
  later_batch_id TEXT,
  later_row_no INTEGER,
  later_count INTEGER NOT NULL DEFAULT 0,
  other_versions_json TEXT,
  PRIMARY KEY (retraction_id, row_no),
  FOREIGN KEY(retraction_id) REFERENCES batch_retractions(retraction_id)
);
CREATE INDEX IF NOT EXISTS idx_retraction_items_action
  ON batch_retraction_items(retraction_id, action);
"""

# 老库升级：CREATE TABLE IF NOT EXISTS 不会给已有表加列，这里逐列补齐。
_COLUMN_MIGRATIONS = (
    ("seq", "INTEGER"),
    ("status", "TEXT NOT NULL DEFAULT 'active'"),
    ("retracted_at", "TEXT"),
    ("retraction_id", "TEXT"),
)

RETRACTION_STATUSES = ("pending", "completed")

# 允许的上传策略（避免路由随意传值）
STATUSES = ("committed", "duplicate_file", "rejected", "encoding_failed", "rolled_back",
            "retracting", "retracted")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path) if path else settings.DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), isolation_level=None, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _migrate_columns(conn)
    conn.execute(
        "UPDATE batches SET seq = rowid WHERE seq IS NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_batches_seq ON batches(seq)"
    )
    # 老库补物化汇总：只有在「汇总为空但明细非空」时才整体重算，避免覆盖正在进行的撤回进度。
    empty = conn.execute("SELECT COUNT(*) AS c FROM site_totals").fetchone()["c"] == 0
    has_facts = conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"] > 0
    if empty and has_facts:
        conn.execute("BEGIN IMMEDIATE")
        try:
            rebuild_site_totals(conn)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _migrate_columns(conn: sqlite3.Connection) -> None:
    """老库补列（幂等）：SQLite 的 ADD COLUMN 只能加常量默认值。"""
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(batches)").fetchall()}
    for name, decl in _COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(f"ALTER TABLE batches ADD COLUMN {name} {decl}")


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]


def find_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    """只认仍计入明细的批次（active / 撤回中的 retracting）。

    已撤回（retracted）的批次不再计入明细，因此同一 sha256 可以重新入库。
    """
    return conn.execute(
        "SELECT * FROM batches WHERE sha256 = ? AND status IN ('active','retracting')"
        " ORDER BY created_at ASC LIMIT 1",
        (sha256,),
    ).fetchone()


def find_retracted_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batches WHERE sha256 = ? AND status = 'retracted'"
        " ORDER BY created_at DESC LIMIT 1",
        (sha256,),
    ).fetchone()


def list_batches(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM batches ORDER BY seq DESC, created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "batch_id": r["batch_id"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "encoding": r["encoding"],
            "bom": bool(r["bom"]),
            "row_count": r["row_count"],
            "total_amount": f"{r['total_amount_cents'] // 100}.{r['total_amount_cents'] % 100:02d}",
            "total_qty": r["total_qty"],
            "forced": bool(r["forced"]),
            "superseded_by": r["superseded_by"],
            "status": r["status"],
            "retracted_at": r["retracted_at"],
            "retraction_id": r["retraction_id"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
    if not row:
        return None
    retracted = row["status"] == "retracted"
    agg = [
        {
            "site": a["site"],
            "amount": f"{a['amount_cents'] // 100}.{a['amount_cents'] % 100:02d}",
            "amount_cents": a["amount_cents"],
            "qty": a["qty"],
            "row_count": a["row_count"],
        }
        for a in conn.execute(
            "SELECT * FROM batch_aggregates WHERE batch_id = ? ORDER BY amount_cents DESC",
            (batch_id,),
        ).fetchall()
    ]
    if retracted:
        sample = [
            dict(r) for r in conn.execute(
                "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo,"
                " action, later_batch_id, later_row_no, later_count"
                " FROM batch_retraction_items WHERE batch_id = ? ORDER BY row_no LIMIT 20",
                (batch_id,),
            ).fetchall()
        ]
    else:
        sample = [
            dict(r) for r in conn.execute(
                "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo "
                "FROM fact_orders WHERE batch_id = ? ORDER BY row_no LIMIT 20",
                (batch_id,),
            ).fetchall()
        ]
    for r in sample:
        r["amount"] = f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}"
        r.setdefault("action", "removed")
        r.setdefault("later_batch_id", None)
        r.setdefault("later_row_no", None)
        r.setdefault("later_count", 0)
    retraction = get_retraction(conn, row["retraction_id"]) if row["retraction_id"] else None
    return {
        "batch_id": row["batch_id"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "encoding": row["encoding"],
        "bom": bool(row["bom"]),
        "row_count": row["row_count"],
        "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
        "total_amount_cents": row["total_amount_cents"],
        "total_qty": row["total_qty"],
        "forced": bool(row["forced"]),
        "superseded_by": row["superseded_by"],
        "seq": row["seq"],
        "status": row["status"],
        "retracted_at": row["retracted_at"],
        "retraction_id": row["retraction_id"],
        "retraction": retraction,
        "created_at": row["created_at"],
        "note": row["note"],
        "aggregates": agg,
        "sample_rows": sample,
    }


def site_summary_from_facts(conn: sqlite3.Connection) -> list[dict]:
    """从明细则算站点汇总（独立口径，用于自证 site_totals 没漂移）。"""
    rows = conn.execute(
        "SELECT site, COUNT(*) AS row_count, COALESCE(SUM(amount_cents), 0) AS amount_cents,"
        " COALESCE(SUM(qty), 0) AS qty"
        " FROM fact_orders GROUP BY site ORDER BY amount_cents DESC, site ASC"
    ).fetchall()
    return [_site_row(r) for r in rows]


def site_summary(conn: sqlite3.Connection) -> list[dict]:
    """当前站点汇总（物化表 site_totals）：撤回时与明细在同一事务内切换。"""
    rows = conn.execute(
        "SELECT site, row_count, amount_cents, qty FROM site_totals"
        " ORDER BY amount_cents DESC, site ASC"
    ).fetchall()
    return [_site_row(r) for r in rows]


def _site_row(r: sqlite3.Row) -> dict:
    return {
        "site": r["site"],
        "row_count": r["row_count"],
        "amount_cents": r["amount_cents"],
        "amount": f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}",
        "qty": r["qty"],
    }


def summary_snapshot(conn: sqlite3.Connection, *, from_facts: bool = False) -> dict:
    sites = site_summary_from_facts(conn) if from_facts else site_summary(conn)
    return {
        "fact_rows": sum(s["row_count"] for s in sites),
        "total_amount_cents": sum(s["amount_cents"] for s in sites),
        "total_qty": sum(s["qty"] for s in sites),
        "sites": sites,
    }


def rebuild_site_totals(conn: sqlite3.Connection) -> None:
    """由当前明细整体重算站点汇总（调用方负责事务边界）。"""
    conn.execute("DELETE FROM site_totals")
    conn.execute(
        "INSERT INTO site_totals(site, amount_cents, qty, row_count)"
        " SELECT site, SUM(amount_cents), SUM(qty), COUNT(*) FROM fact_orders GROUP BY site"
    )


def verify_summary(conn: sqlite3.Connection) -> dict:
    """自证：物化汇总 == 从明细则算（站点、行数、金额、件数逐项一致）。"""
    facts = {s["site"]: (s["row_count"], s["amount_cents"], s["qty"])
             for s in site_summary_from_facts(conn)}
    totals = {s["site"]: (s["row_count"], s["amount_cents"], s["qty"])
              for s in site_summary(conn)}
    return {
        "ok": facts == totals,
        "detail": "站点汇总与明细逐项一致" if facts == totals else (
            f"站点汇总与明细不一致：明细 {facts} vs 汇总 {totals}"
        ),
    }


# ---------------------------------------------------------------- 撤回记录


def get_retraction(conn: sqlite3.Connection, retraction_id: str | None) -> dict | None:
    if not retraction_id:
        return None
    row = conn.execute(
        "SELECT * FROM batch_retractions WHERE retraction_id = ?", (retraction_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    for key in ("summary_before_json", "summary_after_json"):
        raw = data.pop(key, None)
        target = key.replace("_json", "")
        try:
            data[target] = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            data[target] = None
    data["items_total"] = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_retraction_items WHERE retraction_id = ?",
        (retraction_id,),
    ).fetchone()["c"]
    data["items_conflict"] = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_retraction_items"
        " WHERE retraction_id = ? AND action = 'kept_later_version'",
        (retraction_id,),
    ).fetchone()["c"]
    return data


def get_retraction_by_key(conn: sqlite3.Connection, idempotency_key: str) -> dict | None:
    row = conn.execute(
        "SELECT retraction_id FROM batch_retractions WHERE idempotency_key = ?",
        (idempotency_key,),
    ).fetchone()
    return get_retraction(conn, row["retraction_id"]) if row else None


def get_retraction_for_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute(
        "SELECT retraction_id FROM batch_retractions WHERE batch_id = ?"
        " ORDER BY created_at DESC LIMIT 1",
        (batch_id,),
    ).fetchone()
    return get_retraction(conn, row["retraction_id"]) if row else None


def list_retraction_items(conn: sqlite3.Connection, retraction_id: str,
                          *, action: str | None = None, limit: int = 200) -> list[dict]:
    sql = "SELECT * FROM batch_retraction_items WHERE retraction_id = ?"
    params: list = [retraction_id]
    if action:
        sql += " AND action = ?"
        params.append(action)
    sql += " ORDER BY row_no LIMIT ?"
    params.append(limit)
    items = []
    for r in conn.execute(sql, params).fetchall():
        item = dict(r)
        try:
            item["other_versions"] = json.loads(item.pop("other_versions_json") or "[]")
        except json.JSONDecodeError:
            item["other_versions"] = []
        items.append(item)
    return items


def list_retractions(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT retraction_id FROM batch_retractions ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [r for r in (get_retraction(conn, row["retraction_id"]) for row in rows) if r]


def list_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM import_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["errors"] = json.loads(data.get("errors_json") or "[]")
    except json.JSONDecodeError:
        data["errors"] = []
    return data


def record_attempt(conn: sqlite3.Connection, *, filename: str, sha256: str, status: str,
                   encoding: str = "", error_count: int = 0, row_count: int = 0,
                   batch_id: str | None = None, detail: str = "",
                   errors: list[dict] | None = None) -> int:
    payload = json.dumps(errors[:200] if errors else [], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO import_attempts(filename, sha256, encoding, status, error_count,"
        " row_count, batch_id, detail, errors_json, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (filename, sha256, encoding, status, error_count, row_count, batch_id, detail,
         payload, now_iso()),
    )
    return int(cur.lastrowid or 0)


def stats(conn: sqlite3.Connection) -> dict:
    batches = conn.execute(
        "SELECT COUNT(*) AS c FROM batches WHERE status IN ('active','retracting')"
    ).fetchone()["c"]
    retracted = conn.execute(
        "SELECT COUNT(*) AS c FROM batches WHERE status = 'retracted'"
    ).fetchone()["c"]
    fact = count_rows(conn)
    rejected = conn.execute(
        "SELECT COUNT(*) AS c FROM import_attempts WHERE status IN ('rejected','encoding_failed')"
    ).fetchone()["c"]
    sites = conn.execute("SELECT COUNT(DISTINCT site) AS c FROM fact_orders").fetchone()["c"]
    return {"batches": batches, "retracted_batches": retracted, "fact_rows": fact,
            "rejected_attempts": rejected, "sites": sites}
