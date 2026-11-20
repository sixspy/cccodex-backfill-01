import sqlite3

import pytest


@pytest.mark.asyncio
async def test_plan_open_replay_coverage(authed):
    windows = (await authed.get("/api/windows")).json()["items"]
    wid = windows[0]["id"]
    planned = (await authed.post(f"/api/windows/{wid}/plan")).json()
    assert len(planned["slices"]) == 3

    # 未开灌直接重放必须被拒（可写门槛对齐）
    first = planned["slices"][0]["id"]
    denied = await authed.post(f"/api/slices/{first}/replay")
    assert denied.status_code == 409

    opened = (await authed.post(f"/api/windows/{wid}/open")).json()
    assert opened["status"] == "执行中"
    assert opened["mutex_no"].startswith("MX-")

    replayed = (await authed.post(f"/api/slices/{first}/replay")).json()["slice"]
    assert replayed["status"] == "已落库"
    assert replayed["row_count"] > 0

    # 重复重放只能是业务 409，绝不落到主键异常
    again = await authed.post(f"/api/slices/{first}/replay")
    assert again.status_code == 409
    assert "已落库" in again.json()["error"]

    report = (await authed.post("/api/coverage-reports", json={"window_id": wid})).json()["report"]
    assert report["planned"] == 3
    assert report["loaded"] == 1
    assert report["missing"] == 2
    assert report["mixed_rows"] == 0
    assert report["verdict"] == "对账存疑"  # 缺口未清

    # 连跑两次对账都不许撞主键、不许虚绿
    second = (await authed.post("/api/coverage-reports", json={"window_id": wid})).json()["report"]
    assert second["verdict"] == "对账存疑"
    assert second["landing_rows"] == replayed["row_count"]


@pytest.mark.asyncio
async def test_close_requires_all_slices(authed):
    wid = (await authed.get("/api/windows")).json()["items"][0]["id"]
    planned = (await authed.post(f"/api/windows/{wid}/plan")).json()
    await authed.post(f"/api/windows/{wid}/open")

    early = await authed.post(f"/api/windows/{wid}/close")
    assert early.status_code == 409

    for s in planned["slices"]:
        r = await authed.post(f"/api/slices/{s['id']}/replay")
        assert r.status_code == 200

    report = (await authed.post("/api/coverage-reports", json={"window_id": wid})).json()["report"]
    assert report["verdict"] == "通过"
    assert report["missing"] == 0
    assert report["mixed_rows"] == 0

    closed = (await authed.post(f"/api/windows/{wid}/close")).json()
    assert closed["status"] == "已完成"
    assert closed["gate_increment"] == "可写"


@pytest.mark.asyncio
async def test_daily_ingest_rejected_while_open_writes_zero_rows(authed, tmp_path):
    wins = (await authed.get("/api/windows")).json()["items"]
    win = next(w for w in wins if w["table_name"] == "dw_orders_d" and w["status"] == "草稿")
    await authed.post(f"/api/windows/{win['id']}/plan")
    await authed.post(f"/api/windows/{win['id']}/open")

    denied = await authed.post(
        "/api/daily-rows",
        json={"table_name": "dw_orders_d", "biz_date": "2026-11-11", "rows": 5},
    )
    assert denied.status_code == 409
    assert "拒灌" in denied.json()["error"]

    db = tmp_path / "test.db"
    conn = sqlite3.connect(db)
    mixed = conn.execute(
        "SELECT COUNT(*) FROM landing_rows WHERE source='日增量'"
    ).fetchone()[0]
    conn.close()
    assert mixed == 0  # 选 A：拒灌即零写入，不存在“先混入再回滚”

    # 列表与对账都亮拒灌芯片
    listed = (await authed.get("/api/windows")).json()["items"]
    row = next(w for w in listed if w["id"] == win["id"])
    assert row["gate_increment"] == "拒灌"
    assert row["aligned"] is True

    report = (await authed.post(
        "/api/coverage-reports", json={"window_id": win["id"]}
    )).json()["report"]
    assert report["gate"] == "拒灌"
    assert report["mixed_rows"] == 0
    assert report["mutex_no"] == row["mutex_no"]

    logs = (await authed.get("/api/orchestration-log")).json()["items"]
    kinds = {(i["kind"], i["outcome"]) for i in logs}
    assert ("开灌", "成功") in kinds
    assert ("日增量", "拒灌") in kinds


@pytest.mark.asyncio
async def test_daily_ingest_allowed_when_no_mutex(authed):
    ok = await authed.post(
        "/api/daily-rows",
        json={"table_name": "dw_payments_d", "biz_date": "2026-11-11", "rows": 3},
    )
    assert ok.status_code == 200
    assert ok.json()["result"]["written"] == 3

    # 该日已有日增落点，A 路线不允许开灌（否则日增行会在回灌区间里与落点共存）
    wid = next(
        w["id"] for w in (await authed.get("/api/windows")).json()["items"]
        if w["table_name"] == "dw_payments_d"
    )
    await authed.post(f"/api/windows/{wid}/plan")
    blocked = await authed.post(f"/api/windows/{wid}/open")
    assert blocked.status_code == 409
    assert "日增量" in blocked.json()["error"] or "日增" in blocked.json()["error"]

    # 未开灌 → 没有互斥号；重放必须被门槛拦住
    row = next(
        w for w in (await authed.get("/api/windows")).json()["items"] if w["id"] == wid
    )
    assert row["mutex_no"] is None
    assert row["status"] == "已计划"
    slices = (await authed.get(f"/api/windows/{wid}/slices")).json()["items"]
    hit = await authed.post(f"/api/slices/{slices[0]['id']}/replay")
    assert hit.status_code == 409
