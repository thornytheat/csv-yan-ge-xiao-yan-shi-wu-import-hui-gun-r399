"""批次撤回：把一个已入账批次从「当前明细 + 站点汇总」中撤下。

背景：值班同学传错文件后，旧批次仍计入明细与汇总，同一订单可能被重复累计。
本模块支持撤回一个已入账批次，规则：

1. **移除该批独有的订单**：只属于被撤回批次的 order_id 从当前明细中消失。
2. **被后续批次修订过的订单保留后来版本**：若同一 order_id 出现在更晚的 active 批次里，
   被撤回批次自己的那行仍撤下（避免重复累计），后来批次的行原样保留；
   这类订单逐项写入冲突清单（action=kept_later_version），附后来批次 / 行号 /
   修订差异 / 其它版本，长期可查。
3. **明细与汇总一次性切换**：每个处理块的「删除明细行 + 扣减站点汇总」在同一事务内完成，
   结束块在同一事务里再翻转批次状态并核对「站点汇总 == 明细逐项复算」；任一时刻两者一致。
4. **原文长期保留**：batches 行只把状态改为 retracted（不删除）、batch_aggregates 原始快照
   不删除、batch_retraction_items 保存被撤回行的逐行快照、撤回依据（原因 / 操作人 /
   幂等键 / 撤回前后汇总 / 时间）长期保留，可追溯、可复核。
5. **中断可恢复 / 重试幂等**：计划与进度先落库再执行；每块提交后更新进度；
   重新发起同一撤回时从剩余行继续，不会重复扣减；已完成的撤回再次发起只返回既有记录。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

from . import store

DEFAULT_CHUNK_SIZE = 200


# ---------------------------------------------------------------- 工具


def retraction_id_for(batch_id: str) -> str:
    """每个批次只有一个撤回记录，ID 由批号确定（长期保留、重试复用）。"""
    return f"R-{batch_id}"


def default_idempotency_key(batch_id: str) -> str:
    return f"retract:{batch_id}"


def delete_hash(batch_id: str) -> str:
    """撤回依据摘要（便于值班同学口头核对；只依赖批号，重试稳定）。"""
    return hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:16]


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _changed_fields(original: sqlite3.Row, later: sqlite3.Row) -> list[str]:
    fields = {
        "site": (original["site"], later["site"]),
        "category": (original["category"], later["category"]),
        "order_date": (original["order_date"], later["order_date"]),
        "amount_cents": (original["amount_cents"], later["amount_cents"]),
        "qty": (original["qty"], later["qty"]),
    }
    return [name for name, (left, right) in fields.items() if left != right]


# ---------------------------------------------------------------- 计划（分析 + 固化依据）


def _plan_items(conn: sqlite3.Connection, target: sqlite3.Row) -> list[dict]:
    """逐行分析被撤回批次的订单：只有更晚入库的 active 批次才算「后来版本」。"""
    seq = int(target["seq"])
    items: list[dict] = []
    rows = conn.execute(
        "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo"
        " FROM fact_orders WHERE batch_id = ? ORDER BY row_no",
        (target["batch_id"],),
    ).fetchall()
    for row in rows:
        later = conn.execute(
            "SELECT l.batch_id, l.row_no, l.site, l.category, l.order_date,"
            " l.amount_cents, l.qty, l.memo, b.seq, b.status"
            " FROM fact_orders l JOIN batches b ON b.batch_id = l.batch_id"
            " WHERE l.order_id = ? AND b.status IN ('active','retracting') AND b.seq > ?"
            " ORDER BY b.seq DESC, l.row_no ASC",
            (row["order_id"], seq),
        ).fetchall()
        latest = later[0] if later else None
        items.append({
            "row_no": int(row["row_no"]),
            "order_id": int(row["order_id"]),
            "site": row["site"],
            "category": row["category"],
            "order_date": row["order_date"],
            "amount_cents": int(row["amount_cents"]),
            "qty": int(row["qty"]),
            "memo": row["memo"],
            "action": "kept_later_version" if latest else "removed",
            "later_batch_id": latest["batch_id"] if latest else None,
            "later_row_no": int(latest["row_no"]) if latest else None,
            "later_count": len(later),
            "other_versions_json": _json([
                {
                    "batch_id": x["batch_id"], "row_no": x["row_no"], "seq": x["seq"],
                    "order_date": x["order_date"], "amount_cents": x["amount_cents"],
                    "qty": x["qty"], "changed_fields": _changed_fields(row, x),
                }
                for x in later
            ]),
        })
    return items


def _persist_plan(conn: sqlite3.Connection, retraction_id: str, idempotency_key: str,
                  target: sqlite3.Row, items: list[dict], *, reason: str, operator: str,
                  snapshot_before: dict) -> None:
    conflicts = sum(1 for i in items if i["action"] == "kept_later_version")
    now = store.now_iso()
    conn.execute(
        "INSERT INTO batch_retractions(retraction_id, idempotency_key, batch_id, reason,"
        " operator, status, planned_rows, total_rows, removed_rows, conflict_rows,"
        " rows_before, rows_after, summary_before_json, summary_after_json, error,"
        " created_at, updated_at, completed_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (retraction_id, idempotency_key, target["batch_id"], reason or "", operator or "",
         "pending", len(items), len(items), 0, conflicts, snapshot_before["fact_rows"],
         None, _json(snapshot_before), None,
         "计划已保存，等待执行（中断后从此进度继续）", now, now, None),
    )
    conn.executemany(
        "INSERT INTO batch_retraction_items(retraction_id, batch_id, row_no, order_id,"
        " site, category, order_date, amount_cents, qty, memo, action, later_batch_id,"
        " later_row_no, later_count, other_versions_json)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (retraction_id, target["batch_id"], i["row_no"], i["order_id"], i["site"],
             i["category"], i["order_date"], i["amount_cents"], i["qty"], i["memo"],
             i["action"], i["later_batch_id"], i["later_row_no"], i["later_count"],
             i["other_versions_json"])
            for i in items
        ],
    )
    conn.execute(
        "UPDATE batches SET status = 'retracting', retraction_id = ?"
        " WHERE batch_id = ? AND status = 'active'",
        (retraction_id, target["batch_id"]),
    )
    store.record_attempt(
        conn, filename=target["filename"], sha256=target["sha256"], status="retracting",
        encoding=target["encoding"], row_count=int(target["row_count"]),
        batch_id=target["batch_id"],
        detail=(f"批次撤回 {retraction_id} 计划已保存（依据 {delete_hash(target['batch_id'])}）："
                f"{len(items)} 行待撤下，其中 {conflicts} 行在更晚批次中有后来版本，"
                "将保留后来版本并列入冲突清单。"),
    )


# ---------------------------------------------------------------- 执行（分块、可恢复）


def _chunk_site_deltas(rows: list[sqlite3.Row]) -> dict[str, tuple[int, int, int]]:
    deltas: dict[str, list[int]] = {}
    for row in rows:
        bucket = deltas.setdefault(row["site"], [0, 0, 0])
        bucket[0] += int(row["amount_cents"])
        bucket[1] += int(row["qty"])
        bucket[2] += 1
    return {site: (v[0], v[1], v[2]) for site, v in deltas.items()}


def _remaining(conn: sqlite3.Connection, batch_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM fact_orders WHERE batch_id = ?", (batch_id,)
    ).fetchone()["c"]


def _apply_chunks(conn: sqlite3.Connection, retraction_id: str, target: sqlite3.Row,
                  *, fail_after: int | None, chunk_size: int) -> dict:
    """分块撤下明细；每块「删明细 + 扣汇总 + 记进度」同一事务，失败块整体回滚。"""
    batch_id = target["batch_id"]
    retraction = store.get_retraction(conn, retraction_id)
    planned = int(retraction["planned_rows"])
    deleted = 0
    while True:
        remaining = _remaining(conn, batch_id)
        if remaining == 0:
            try:
                return _finalize_transaction(conn, retraction_id, target, planned)
            except Exception as exc:  # noqa: BLE001 - 收尾失败同样保留进度
                conn.execute(
                    "UPDATE batch_retractions SET error = ?, updated_at = ?"
                    " WHERE retraction_id = ? AND status = 'pending'",
                    (f"撤回收尾中断：{exc}；明细已全部撤下，重新发起同一撤回会继续收尾，"
                     "不会重复扣减。", store.now_iso(), retraction_id),
                )
                current = store.get_retraction(conn, retraction_id)
                return {
                    "status": "pending", "retraction_id": retraction_id,
                    "batch_id": batch_id, "detail": current["error"] if current else str(exc),
                    "error": str(exc), "planned_rows": planned, "removed_rows": planned,
                    "retraction": current, "batch": store.get_batch(conn, batch_id),
                }

        rows = conn.execute(
            "SELECT row_no, site, amount_cents, qty FROM fact_orders"
            " WHERE batch_id = ? ORDER BY row_no LIMIT ?",
            (batch_id, chunk_size),
        ).fetchall()
        hit_fail_after = False
        try:
            conn.execute("BEGIN IMMEDIATE")
            deltas = _chunk_site_deltas(rows)
            conn.execute(
                "DELETE FROM fact_orders WHERE batch_id = ? AND row_no IN ("
                + ",".join("?" for _ in rows) + ")",
                (batch_id, *(int(r["row_no"]) for r in rows)),
            )
            for site, (amount, qty, count) in deltas.items():
                conn.execute(
                    "UPDATE site_totals SET amount_cents = amount_cents - ?,"
                    " qty = qty - ?, row_count = row_count - ? WHERE site = ?",
                    (amount, qty, count, site),
                )
            conn.execute("DELETE FROM site_totals WHERE row_count <= 0")
            deleted += len(rows)
            remaining_after = _remaining(conn, batch_id)
            if remaining_after == 0:
                result = _finalize(conn, retraction_id, target, planned)
                conn.execute("COMMIT")
                return result
            # 进度随本块一起提交：这是「中断后从已保存进度恢复」的依据。
            conn.execute(
                "UPDATE batch_retractions SET removed_rows = ?, updated_at = ?, error = NULL"
                " WHERE retraction_id = ?",
                (planned - remaining_after, store.now_iso(), retraction_id),
            )
            conn.execute("COMMIT")
            hit_fail_after = (
                fail_after is not None and deleted >= fail_after and remaining_after > 0
            )
        except Exception as exc:  # noqa: BLE001 - 单块失败必须整体回滚
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            progress = planned - _remaining(conn, batch_id)
            conn.execute(
                "UPDATE batch_retractions SET removed_rows = ?, error = ?, updated_at = ?"
                " WHERE retraction_id = ? AND status = 'pending'",
                (progress,
                 f"撤回中断：{exc}；进度 {progress}/{planned} 行已保存，"
                 "重新发起同一撤回会从剩余行继续，不会重复扣减。",
                 store.now_iso(), retraction_id),
            )
            return _pending_payload(conn, retraction_id, target, planned, progress, str(exc))
        if hit_fail_after:
            progress = planned - _remaining(conn, batch_id)
            detail = (f"撤回中断：注入故障，已撤下 {progress}/{planned} 行后停止"
                      "（用于证明进度可恢复）。重新发起同一撤回会从剩余行继续。")
            conn.execute(
                "UPDATE batch_retractions SET removed_rows = ?, error = ?, updated_at = ?"
                " WHERE retraction_id = ? AND status = 'pending'",
                (progress, detail, store.now_iso(), retraction_id),
            )
            return _pending_payload(conn, retraction_id, target, planned, progress, detail)


def _pending_payload(conn: sqlite3.Connection, retraction_id: str, target: sqlite3.Row,
                     planned: int, progress: int, error: str) -> dict:
    current = store.get_retraction(conn, retraction_id)
    return {
        "status": "pending",
        "retraction_id": retraction_id,
        "batch_id": target["batch_id"],
        "detail": current["error"] if current else error,
        "error": error,
        "planned_rows": planned,
        "removed_rows": progress,
        "rows_before": current["rows_before"] if current else None,
        "summary_before": current["summary_before"] if current else None,
        "retraction": current,
        "batch": store.get_batch(conn, target["batch_id"]),
    }


def _finalize_transaction(conn: sqlite3.Connection, retraction_id: str,
                          target: sqlite3.Row, planned: int) -> dict:
    """所有行已撤下但尚未收尾（上次中断在最后一块）→ 单独事务完成收尾。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        result = _finalize(conn, retraction_id, target, planned)
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _unique_orders_gone(conn: sqlite3.Connection, retraction_id: str) -> int:
    """撤回后彻底不在明细里的订单数（被后来版本接住的订单不算）。"""
    return conn.execute(
        "SELECT COUNT(DISTINCT i.order_id) AS c FROM batch_retraction_items i"
        " WHERE i.retraction_id = ? AND NOT EXISTS ("
        "   SELECT 1 FROM fact_orders f WHERE f.order_id = i.order_id)",
        (retraction_id,),
    ).fetchone()["c"]


def _finalize(conn: sqlite3.Connection, retraction_id: str, target: sqlite3.Row,
              planned: int) -> dict:
    """收尾（调用方负责提交）：核对汇总一致 → 翻转批次状态 → 写入后快照与审计。"""
    batch_id = target["batch_id"]
    verify = store.verify_summary(conn)
    if not verify["ok"]:
        raise RuntimeError(f"撤回结束核对失败，本块不提交：{verify['detail']}")
    snapshot_after = store.summary_snapshot(conn)
    rows_before = int(store.get_retraction(conn, retraction_id)["rows_before"])
    rows_after = store.count_rows(conn)
    unique_gone = _unique_orders_gone(conn, retraction_id)
    now = store.now_iso()
    conn.execute(
        "UPDATE batch_retractions SET status = 'completed', removed_rows = ?, rows_after = ?,"
        " summary_after_json = ?, error = NULL, updated_at = ?, completed_at = ?"
        " WHERE retraction_id = ?",
        (planned, rows_after, _json(snapshot_after), now, now, retraction_id),
    )
    conn.execute(
        "UPDATE batches SET status = 'retracted', retracted_at = ?, retraction_id = ?"
        " WHERE batch_id = ? AND status IN ('active','retracting')",
        (now, retraction_id, batch_id),
    )
    retraction = store.get_retraction(conn, retraction_id)
    store.record_attempt(
        conn, filename=target["filename"], sha256=target["sha256"], status="retracted",
        encoding=target["encoding"], row_count=planned, batch_id=batch_id,
        detail=(f"批次撤回完成 {retraction_id}（依据 {delete_hash(batch_id)}）："
                f"撤下 {planned} 行，明细 {rows_before} → {rows_after} 行；"
                f"{unique_gone} 个订单不再出现在明细中，"
                f"{retraction['items_conflict']} 个订单保留后来版本（见冲突清单）。"),
    )
    return {
        "status": "completed",
        "retraction_id": retraction_id,
        "batch_id": batch_id,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "planned_rows": planned,
        "removed_rows": planned,
        "conflict_rows": retraction["items_conflict"],
        "unique_orders_removed": unique_gone,
        "summary_before": retraction["summary_before"],
        "summary_after": retraction["summary_after"],
        "summary_check": verify,
        "retraction": retraction,
        "detail": "撤回完成：明细与汇总已在同一事务内切换，原批次与撤回依据长期保留。",
    }


# ---------------------------------------------------------------- 对外入口


def _completed_payload(conn: sqlite3.Connection, retraction: dict, *, replay: bool) -> dict:
    return {
        "status": "completed",
        "retraction_id": retraction["retraction_id"],
        "batch_id": retraction["batch_id"],
        "idempotent_replay": replay,
        "rows_before": retraction["rows_before"],
        "rows_after": retraction["rows_after"],
        "planned_rows": retraction["planned_rows"],
        "removed_rows": retraction["planned_rows"],
        "conflict_rows": retraction["items_conflict"],
        "unique_orders_removed": _unique_orders_gone(conn, retraction["retraction_id"]),
        "summary_before": retraction["summary_before"],
        "summary_after": retraction["summary_after"],
        "retraction": retraction,
        "batch": store.get_batch(conn, retraction["batch_id"]),
        "detail": ("该撤回此前已完成，本次为幂等重试：未删除任何行、未重复扣减汇总。"
                   if replay else
                   "撤回完成：明细与汇总已在同一事务内切换，原批次与撤回依据长期保留。"),
    }


def retract_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    reason: str = "",
    operator: str = "",
    idempotency_key: str | None = None,
    fail_after: int | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict:
    """撤回一个已入账批次；不抛业务异常，全部以结论返回。"""
    key = idempotency_key or default_idempotency_key(batch_id)
    retraction_id = retraction_id_for(batch_id)

    existing = store.get_retraction_by_key(conn, key)
    if existing:
        if existing["batch_id"] != batch_id:
            return {"status": "idempotency_conflict",
                    "retraction_id": existing["retraction_id"],
                    "detail": (f"幂等键 {key} 已用于批次 {existing['batch_id']}，"
                               "不能用于另一个批次。")}
        if existing["status"] == "completed":
            return _completed_payload(conn, existing, replay=True)
        target = _load_target(conn, batch_id)
        if not target:
            return {"status": "not_found", "batch_id": batch_id,
                    "detail": f"批次 {batch_id} 不存在"}
        return _resume(conn, existing, target, fail_after=fail_after, chunk_size=chunk_size,
                       replay=True)

    target = _load_target(conn, batch_id)
    if not target:
        return {"status": "not_found", "batch_id": batch_id,
                "detail": f"批次 {batch_id} 不存在（批号需与 /api/batches 一致）"}

    # 同一批次换了幂等键重试：复用既有撤回记录，不重做计划。
    other = store.get_retraction(conn, retraction_id)
    if other:
        if other["status"] == "completed":
            return _completed_payload(conn, other, replay=True)
        return _resume(conn, other, target, fail_after=fail_after, chunk_size=chunk_size,
                       replay=False)

    if target["status"] not in ("active", "retracting"):
        return {"status": "not_retractable", "batch_id": batch_id,
                "detail": (f"批次 {batch_id} 当前状态为 {target['status']}，"
                           "只有 active 批次可以撤回。")}

    # ---- 计划阶段：固化撤回依据 + 逐行冲突分析（独立事务，先提交再执行）
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute(
            "SELECT status FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if not current or current["status"] not in ("active", "retracting"):
            conn.execute("ROLLBACK")
            return {"status": "not_retractable", "batch_id": batch_id,
                    "detail": (f"批次 {batch_id} 当前状态为 "
                               f"{current['status'] if current else '不存在'}，"
                               "只有 active 批次可以撤回。")}
        verify = store.verify_summary(conn)
        if not verify["ok"]:
            store.rebuild_site_totals(conn)          # 老数据自愈后再取快照
        snapshot_before = store.summary_snapshot(conn)
        items = _plan_items(conn, target)
        if not items:
            conn.execute("ROLLBACK")
            return {"status": "empty", "batch_id": batch_id,
                    "detail": f"批次 {batch_id} 没有可撤回的明细行。"}
        _persist_plan(conn, retraction_id, key, target, items,
                      reason=reason, operator=operator, snapshot_before=snapshot_before)
        conn.execute("COMMIT")
    except sqlite3.IntegrityError:
        # 并发撤回：另一个请求已保存了同一批次的计划，直接接手它（不重复建计划）。
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        existing = store.get_retraction(conn, retraction_id)
        if existing and existing["batch_id"] == batch_id:
            if existing["status"] == "completed":
                return _completed_payload(conn, existing, replay=True)
            return _resume(conn, existing, target, fail_after=fail_after,
                           chunk_size=chunk_size, replay=True)
        return {"status": "error", "batch_id": batch_id,
                "detail": "并发撤回冲突，未删除任何数据，请重试。"}
    except Exception as exc:  # noqa: BLE001
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        return {"status": "error", "batch_id": batch_id,
                "detail": f"撤回计划阶段失败，未删除任何数据：{exc}"}

    saved = store.get_retraction(conn, retraction_id)
    return _resume(conn, saved, target, fail_after=fail_after, chunk_size=chunk_size,
                   replay=False)


def _resume(conn: sqlite3.Connection, retraction: dict, target: sqlite3.Row,
            *, fail_after: int | None, chunk_size: int, replay: bool) -> dict:
    result = _apply_chunks(
        conn, retraction["retraction_id"], target,
        fail_after=fail_after, chunk_size=max(int(chunk_size), 1),
    )
    if result["status"] == "completed":
        result["idempotent_replay"] = replay
        result["batch"] = store.get_batch(conn, target["batch_id"])
    return result


def _load_target(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()


def conflicts(conn: sqlite3.Connection, retraction_id: str) -> list[dict]:
    """冲突清单：被撤回批次中、被后来版本修订过的订单，逐项附修订依据。"""
    return store.list_retraction_items(
        conn, retraction_id, action="kept_later_version", limit=10_000_000
    )


def conflict_view(conn: sqlite3.Connection, retraction_id: str) -> list[dict]:
    """冲突清单的展示视图：补上后来版本的当前取值与差异，便于逐项复核。"""
    items = conflicts(conn, retraction_id)
    for item in items:
        later = None
        if item["later_batch_id"]:
            row = conn.execute(
                "SELECT batch_id, row_no, site, category, order_date, amount_cents, qty"
                " FROM fact_orders WHERE batch_id = ? AND row_no = ?",
                (item["later_batch_id"], item["later_row_no"]),
            ).fetchone()
            if row:
                later = dict(row)
                later["amount"] = f"{row['amount_cents'] // 100}.{row['amount_cents'] % 100:02d}"
                later["changed_fields"] = [
                    name for name in ("site", "category", "order_date", "amount_cents", "qty")
                    if item[name] != row[name]
                ]
        item["later"] = later
        item["removed_amount"] = (
            f"{item['amount_cents'] // 100}.{item['amount_cents'] % 100:02d}"
            if item["amount_cents"] is not None else "-"
        )
        item["other_versions"] = item["other_versions"] or []
    return items


def preview(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    """只读预演：撤回会发生什么（不写库），用于页面确认。"""
    target = _load_target(conn, batch_id)
    if not target:
        return None
    retraction_id = retraction_id_for(batch_id)
    existing = store.get_retraction(conn, retraction_id)
    if existing:
        return {
            "batch_id": batch_id,
            "retraction": existing,
            "items": store.list_retraction_items(conn, retraction_id, limit=200),
            "conflicts": conflict_view(conn, retraction_id),
            "planned_rows": existing["planned_rows"],
            "unique_rows_removed": _unique_orders_gone(conn, retraction_id),
            "summary_before": existing["summary_before"],
            "already_planned": True,
        }
    if target["status"] != "active":
        return {"batch_id": batch_id, "retraction": None, "conflicts": [],
                "items": [], "not_retractable": True, "status": target["status"]}
    items = _plan_items(conn, target)
    for item in items:
        try:
            item["other_versions"] = json.loads(item.pop("other_versions_json") or "[]")
        except json.JSONDecodeError:
            item["other_versions"] = []
        item["removed_amount"] = f"{item['amount_cents'] // 100}.{item['amount_cents'] % 100:02d}"
    return {
        "batch_id": batch_id,
        "retraction": None,
        "items": items,
        "conflicts": [i for i in items if i["action"] == "kept_later_version"],
        "planned_rows": len(items),
        "unique_rows_removed": sum(1 for i in items if i["action"] == "removed"),
        "summary_before": store.summary_snapshot(conn),
    }
