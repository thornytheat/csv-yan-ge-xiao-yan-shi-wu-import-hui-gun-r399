"""批次撤回：独有订单移除、后来版本保留、冲突清单、明细/汇总同事务切换、
原文与依据长期保留、中断恢复、重试幂等。"""
from __future__ import annotations

import pytest

from app import aggregate, importer, pipeline, retraction, rows as rows_mod, store

HEADER = ["site", "category", "order_id", "order_date", "amount", "qty"]


@pytest.fixture()
def conn(tmp_path):
    connection = store.connect(tmp_path / "t.sqlite3")
    store.init_schema(connection)
    yield connection
    connection.close()


def rows_from(specs: list[tuple]) -> list[dict]:
    """specs 每项 = (order_id, site, amount, qty[, order_date])。"""
    data = []
    for spec in specs:
        oid, site, amount, qty = spec[:4]
        day = spec[4] if len(spec) > 4 else "2026-11-05"
        data.append([site, "grocery", str(oid), day, amount, str(qty)])
    result = rows_mod.validate(data, HEADER, HEADER)
    assert result.ok, result.error_dicts()
    return result.rows


def import_rows(conn, specs, *, filename="a.csv", sha: str, force=False) -> dict:
    rows = rows_from(specs)
    agg = aggregate.summarize(rows)
    return importer.import_batch(
        conn, filename=filename, sha256=sha, rows=rows, aggregates=agg,
        encoding="utf-8", bom=False, force_new_batch=force,
    )


def test_retract_removes_only_that_batch_rows_and_switches_summary(conn):
    import_rows(conn, [(1, "beijing", "10.00", 2), (2, "shanghai", "20.00", 1)],
                filename="seed.csv", sha="1" * 64)
    late = import_rows(conn, [(3, "beijing", "30.00", 3), (4, "beijing", "40.00", 2)],
                       filename="late.csv", sha="2" * 64)
    rows_before = store.count_rows(conn)
    summary_before = store.summary_snapshot(conn)

    outcome = retraction.retract_batch(conn, late["batch_id"], reason="值班传错文件",
                                       operator="ops-1")

    assert outcome["status"] == "completed"
    assert outcome["batch_id"] == late["batch_id"]
    assert outcome["rows_before"] == 4
    assert outcome["rows_after"] == 2
    assert outcome["removed_rows"] == 2
    assert outcome["conflict_rows"] == 0
    assert store.count_rows(conn) == rows_before - 2
    assert store.verify_summary(conn)["ok"] is True

    summary_after = store.summary_snapshot(conn)
    assert summary_after["fact_rows"] == 2
    assert summary_after["total_amount_cents"] == summary_before["total_amount_cents"] - 7000
    assert {s["site"]: s["row_count"] for s in summary_after["sites"]} == {
        "beijing": 1, "shanghai": 1,
    }
    # 明细里只剩 seed 的两行
    remaining = conn.execute(
        "SELECT order_id FROM fact_orders ORDER BY order_id"
    ).fetchall()
    assert [r["order_id"] for r in remaining] == [1, 2]
    # 原批次与原始聚合快照长期保留
    batch = store.get_batch(conn, late["batch_id"])
    assert batch["status"] == "retracted"
    assert batch["row_count"] == 2
    assert batch["aggregates"][0]["site"] == "beijing"
    assert batch["retraction"]["reason"] == "值班传错文件"
    assert batch["retraction"]["operator"] == "ops-1"
    assert batch["retraction"]["summary_before"]["fact_rows"] == 4
    assert batch["retraction"]["summary_after"]["fact_rows"] == 2
    stored_items = store.list_retraction_items(conn, outcome["retraction_id"])
    assert [i["order_id"] for i in stored_items] == [3, 4]
    assert all(i["action"] == "removed" for i in stored_items)


def test_later_version_kept_and_listed_in_conflicts(conn):
    """被后续批次修订过的订单：撤掉旧批的行，保留后来版本，并逐项进冲突清单。"""
    early = import_rows(conn, [(1, "beijing", "10.00", 2), (2, "shanghai", "20.00", 1)],
                        filename="early.csv", sha="3" * 64)
    # 后来的修订批次：订单 1 件数改为 3，并新增订单 3
    later = import_rows(conn, [(1, "beijing", "10.00", 3), (3, "shenzhen", "30.00", 2)],
                        filename="later.csv", sha="4" * 64, force=True)
    assert later["status"] == "committed"
    before = store.count_rows(conn)

    outcome = retraction.retract_batch(conn, early["batch_id"], reason="early 批传错")

    assert outcome["status"] == "completed"
    assert outcome["planned_rows"] == 2
    assert outcome["conflict_rows"] == 1
    # early 批两行都撤下；后来批次的 order 1（修订版）与 order 3 原样保留
    assert store.count_rows(conn) == before - 2
    assert sorted(r["order_id"] for r in conn.execute(
        "SELECT order_id FROM fact_orders")) == [1, 3]
    assert store.verify_summary(conn)["ok"] is True
    # 后来版本仍是修订后的件数 3
    kept = conn.execute(
        "SELECT qty, batch_id FROM fact_orders WHERE order_id = 1"
    ).fetchone()
    assert kept["qty"] == 3 and kept["batch_id"] == later["batch_id"]

    conflicts = retraction.conflicts(conn, outcome["retraction_id"])
    assert [c["order_id"] for c in conflicts] == [1]
    assert conflicts[0]["action"] == "kept_later_version"
    assert conflicts[0]["later_batch_id"] == later["batch_id"]
    assert conflicts[0]["later_row_no"] == 1
    assert conflicts[0]["other_versions"][0]["changed_fields"] == ["qty"]
    stored = store.list_retraction_items(conn, outcome["retraction_id"])
    actions = {i["order_id"]: i["action"] for i in stored}
    assert actions == {1: "kept_later_version", 2: "removed"}
    # 原批次与逐行快照长期保留（快照记录的是被撤下的原值）
    batch = store.get_batch(conn, early["batch_id"])
    assert batch["status"] == "retracted"
    snapshot = {i["order_id"]: i["qty"] for i in stored}
    assert snapshot == {1: 2, 2: 1}


def test_later_version_in_active_batch_survives_when_other_batch_still_active(conn):
    """被撤批次与后来版本批次都留着时：后来版本仍在明细里，只撤下被撤批的那行。"""
    early = import_rows(conn, [(1, "beijing", "10.00", 2), (2, "shanghai", "20.00", 1)],
                        filename="early.csv", sha="5" * 64)
    later = import_rows(conn, [(1, "beijing", "10.00", 2), (3, "shenzhen", "30.00", 2)],
                        filename="later.csv", sha="6" * 64, force=True)

    outcome = retraction.retract_batch(conn, early["batch_id"])
    assert outcome["status"] == "completed"
    assert outcome["conflict_rows"] == 1
    assert store.count_rows(conn) == 2                     # later 批的 2 行都在
    assert store.verify_summary(conn)["ok"] is True
    # order_id=2 只在 early 批里，已被移除；order_id=1 与 3 保留（后来批次）
    remaining = sorted(r["order_id"] for r in conn.execute("SELECT order_id FROM fact_orders"))
    assert remaining == [1, 3]
    conflict = retraction.conflicts(conn, outcome["retraction_id"])[0]
    assert conflict["order_id"] == 1
    assert conflict["later_batch_id"] == later["batch_id"]
    assert conflict["other_versions"][0]["changed_fields"] == []


def test_retraction_is_atomic_per_chunk_and_summary_never_drifts(conn):
    """多块撤回：每块提交后汇总都与明细一致（中途读库不漂移）。"""
    batch = import_rows(conn, [(1000 + i, "beijing", "10.00", 1) for i in range(25)],
                        filename="many.csv", sha="7" * 64, force=True)
    outcome = retraction.retract_batch(conn, batch["batch_id"], chunk_size=5)

    assert outcome["status"] == "completed"
    assert outcome["removed_rows"] == 25
    assert store.count_rows(conn) == 0
    snapshot = store.summary_snapshot(conn)
    assert snapshot == {"fact_rows": 0, "total_amount_cents": 0, "total_qty": 0, "sites": []}
    assert store.verify_summary(conn)["ok"] is True


def test_interrupted_retraction_resumes_without_double_counting(conn):
    batch = import_rows(conn, [(1000 + i, "beijing", "10.00", 1) for i in range(12)],
                        filename="many.csv", sha="8" * 64)
    before = store.count_rows(conn)

    first = retraction.retract_batch(conn, batch["batch_id"], fail_after=4, chunk_size=4)
    assert first["status"] == "pending"
    assert first["removed_rows"] == 4
    assert "恢复" in first["detail"]
    assert store.count_rows(conn) == before - 4
    assert store.verify_summary(conn)["ok"] is True          # 中断后汇总仍与明细一致
    progress = store.get_retraction_by_key(conn, retraction.default_idempotency_key(
        batch["batch_id"]))
    assert progress["status"] == "pending"
    assert progress["removed_rows"] == 4
    summary_mid = store.summary_snapshot(conn)

    second = retraction.retract_batch(conn, batch["batch_id"], chunk_size=4)
    assert second["status"] == "completed"
    assert second["idempotent_replay"] is True
    assert store.count_rows(conn) == 0
    # 只扣一次：金额从 before 直接归零，没有负值或双倍扣减
    assert store.summary_snapshot(conn)["total_amount_cents"] == 0
    assert summary_mid["total_amount_cents"] == 8000
    assert store.verify_summary(conn)["ok"] is True


def test_replaying_completed_retraction_changes_nothing(conn):
    batch = import_rows(conn, [(1, "beijing", "10.00", 2), (2, "beijing", "20.00", 1)],
                        filename="x.csv", sha="9" * 64)
    retraction.retract_batch(conn, batch["batch_id"])
    rows_after = store.count_rows(conn)
    summary = store.summary_snapshot(conn)

    replay = retraction.retract_batch(conn, batch["batch_id"])
    assert replay["status"] == "completed"
    assert replay["idempotent_replay"] is True
    assert replay["rows_after"] == rows_after
    assert store.count_rows(conn) == rows_after
    assert store.summary_snapshot(conn) == summary
    attempts = [a for a in store.list_attempts(conn) if a["status"] == "retracted"]
    assert len(attempts) == 1                                 # 审计只记一次


def test_retracted_sha_can_be_imported_again(conn):
    sha = "a" * 64
    first = import_rows(conn, [(1, "beijing", "10.00", 1)], filename="x.csv", sha=sha)
    retraction.retract_batch(conn, first["batch_id"])
    assert store.find_batch_by_sha(conn, sha) is None

    again = import_rows(conn, [(1, "beijing", "10.00", 1)], filename="x.csv", sha=sha)
    assert again["status"] == "committed"
    assert store.count_rows(conn) == 1
    assert store.get_batch(conn, again["batch_id"])["status"] == "active"


def test_retract_rejects_non_active_or_unknown_batch(conn):
    unknown = retraction.retract_batch(conn, "B-NOPE")
    assert unknown["status"] == "not_found"

    batch = import_rows(conn, [(1, "beijing", "10.00", 1)], filename="x.csv", sha="b" * 64)
    retraction.retract_batch(conn, batch["batch_id"])
    again = retraction.retract_batch(conn, batch["batch_id"], idempotency_key="other-key")
    assert again["status"] == "completed"                     # 同一批号只能撤回一次
    assert again["idempotent_replay"] is True


def test_idempotency_key_cannot_be_reused_across_batches(conn):
    one = import_rows(conn, [(1, "beijing", "10.00", 1)], filename="1.csv", sha="c" * 64)
    two = import_rows(conn, [(2, "beijing", "10.00", 1)], filename="2.csv", sha="d" * 64)
    retraction.retract_batch(conn, one["batch_id"], idempotency_key="k-1")
    clash = retraction.retract_batch(conn, two["batch_id"], idempotency_key="k-1")
    assert clash["status"] == "idempotency_conflict"
    assert store.get_batch(conn, two["batch_id"])["status"] == "active"


def test_pipeline_imports_still_consistent_with_materialized_summary(conn):
    text = "site,category,order_id,order_date,amount,qty\n" + "".join(
        f"beijing,grocery,{1001 + i},2026-11-05,10.00,2\n" for i in range(5)
    )
    result = pipeline.run(text.encode("utf-8"), "ok.csv", conn=conn)
    assert result["status"] == "committed"
    assert store.verify_summary(conn)["ok"] is True
    assert store.summary_snapshot(conn)["fact_rows"] == 5


def test_preview_is_read_only(conn):
    batch = import_rows(conn, [(1, "beijing", "10.00", 1)], filename="x.csv", sha="e" * 64)
    preview = retraction.preview(conn, batch["batch_id"])
    assert preview["planned_rows"] == 1
    assert preview["summary_before"]["fact_rows"] == 1
    assert store.count_rows(conn) == 1
    assert store.get_retraction(conn, retraction.retraction_id_for(batch["batch_id"])) is None


def test_interrupted_at_finalize_resumes_without_duplicate_deduction(conn, monkeypatch):
    """收尾前中断：最后一块整体回滚（不丢数据），已提交的块保留；重试只处理剩余行。"""
    batch = import_rows(conn, [(1000 + i, "beijing", "10.00", 1) for i in range(6)],
                        filename="many.csv", sha="f" * 64)
    original_finalize = retraction._finalize
    calls = {"n": 0}

    def flaky_finalize(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("注入故障：收尾前中断")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(retraction, "_finalize", flaky_finalize)
    first = retraction.retract_batch(conn, batch["batch_id"], chunk_size=2)
    assert first["status"] == "pending"
    # 前两块（4 行）已提交，最后一块随收尾失败一起回滚：明细仍是 2 行，没有丢数据
    assert store.count_rows(conn) == 2
    progress = store.get_retraction(conn, first["retraction_id"])
    assert progress["status"] == "pending"
    assert progress["removed_rows"] == 4
    assert store.verify_summary(conn)["ok"] is True

    monkeypatch.setattr(retraction, "_finalize", original_finalize)
    second = retraction.retract_batch(conn, batch["batch_id"], chunk_size=2)
    assert second["status"] == "completed"
    assert second["removed_rows"] == 6
    assert store.count_rows(conn) == 0                      # 剩余 2 行撤下，没有回撤已扣减的 4 行
    assert store.summary_snapshot(conn)["total_amount_cents"] == 0
    assert store.get_batch(conn, batch["batch_id"])["status"] == "retracted"
    assert len([a for a in store.list_attempts(conn) if a["status"] == "retracted"]) == 1


def test_later_batch_retracted_first_means_no_conflict_for_earlier(conn):
    early = import_rows(conn, [(1, "beijing", "10.00", 2), (2, "shanghai", "20.00", 1)],
                        filename="early.csv", sha="1a" * 32)
    later = import_rows(conn, [(1, "beijing", "10.00", 3), (3, "shenzhen", "30.00", 2)],
                        filename="later.csv", sha="1b" * 32)
    retraction.retract_batch(conn, later["batch_id"])
    outcome = retraction.retract_batch(conn, early["batch_id"])
    assert outcome["status"] == "completed"
    assert outcome["conflict_rows"] == 0          # later 批已撤回，不再有后来版本
    assert store.count_rows(conn) == 0
    assert all(i["action"] == "removed"
               for i in store.list_retraction_items(conn, outcome["retraction_id"]))


def test_forced_reimport_double_count_is_untangled_by_retraction(conn):
    """同一份文件被 force_new_batch 重灌后两批都计入（重复累计）；
    撤回旧批后重复订单只留新批版本，并在冲突清单逐项标出。"""
    specs = [(1, "beijing", "10.00", 1), (2, "shanghai", "20.00", 2), (3, "shenzhen", "30.00", 1)]
    old = import_rows(conn, specs, filename="x.csv", sha="2a" * 32)
    new = import_rows(conn, specs, filename="x.csv", sha="2a" * 32, force=True)
    assert store.count_rows(conn) == 6             # 重复累计
    assert store.get_batch(conn, old["batch_id"])["superseded_by"] == new["batch_id"]

    outcome = retraction.retract_batch(conn, old["batch_id"], reason="撤回误传的旧批")
    assert outcome["status"] == "completed"
    assert outcome["planned_rows"] == 3
    assert outcome["conflict_rows"] == 3
    assert outcome["unique_orders_removed"] == 0
    assert store.count_rows(conn) == 3             # 只留新批的 3 行，重复累计消除
    assert sorted(r["order_id"] for r in conn.execute(
        "SELECT order_id FROM fact_orders")) == [1, 2, 3]
    assert all(r["batch_id"] == new["batch_id"] for r in conn.execute(
        "SELECT batch_id FROM fact_orders"))
    assert [c["order_id"] for c in retraction.conflicts(conn, outcome["retraction_id"])] == [1, 2, 3]
    assert store.verify_summary(conn)["ok"] is True
    # 新批次仍是 active；旧批次保留但标 retracted，superseded 关系不变
    assert store.get_batch(conn, new["batch_id"])["status"] == "active"
    assert store.get_batch(conn, old["batch_id"])["status"] == "retracted"


def test_preview_reports_plan_and_conflicts_without_writing(conn):
    early = import_rows(conn, [(1, "beijing", "10.00", 2), (2, "shanghai", "20.00", 1)],
                        filename="early.csv", sha="3a" * 32)
    later = import_rows(conn, [(1, "beijing", "10.00", 4), (3, "shenzhen", "30.00", 1)],
                        filename="later.csv", sha="3b" * 32)
    verdict = retraction.preview(conn, early["batch_id"])
    assert verdict["planned_rows"] == 2
    assert verdict["unique_rows_removed"] == 1
    assert [c["order_id"] for c in verdict["conflicts"]] == [1]
    assert verdict["items"][0]["removed_amount"] == "10.00"
    assert verdict["summary_before"]["fact_rows"] == 4
    assert store.count_rows(conn) == 4
    assert store.get_retraction(conn, retraction.retraction_id_for(early["batch_id"])) is None
    assert retraction.preview(conn, "B-NOPE") is None


def test_conflict_view_reports_later_values_and_defaults(conn):
    early = import_rows(conn, [(1, "beijing", "10.00", 2)], filename="early.csv", sha="4a" * 32)
    later = import_rows(conn, [(1, "beijing", "10.00", 3)], filename="later.csv", sha="4b" * 32)
    outcome = retraction.retract_batch(conn, early["batch_id"])
    view = retraction.conflict_view(conn, outcome["retraction_id"])
    assert view[0]["later"]["qty"] == 3
    assert view[0]["later"]["changed_fields"] == ["qty"]
    assert view[0]["removed_amount"] == "10.00"
