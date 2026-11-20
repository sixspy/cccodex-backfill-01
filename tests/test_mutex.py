import sqlite3
import threading

import pytest

from app.services.errors import ServiceError
from app.services.windows import (
    close_window,
    coverage_report,
    daily_ingest,
    open_window,
    plan_slices,
    replay_slice,
)


def _fresh_db(tmp_path) -> str:
    from app.db import init_db

    db = str(tmp_path / "mutex.db")
    init_db(db)
    return db


def _rows(db, sql, *args):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def test_two_managers_open_same_table_only_one_wins(tmp_path):
    """两侧仓管同点开灌：排他锁 + 唯一索引，只许一侧拿到互斥号。"""
    db = _fresh_db(tmp_path)
    windows = _rows(db, "SELECT id, table_name FROM backfill_windows ORDER BY id")
    same_table = [w[0] for w in windows if w[1] == "dw_orders_d"]
    assert len(same_table) == 2
    for wid in same_table:
        plan_slices(wid, db)

    results: list[tuple[int, str]] = []
    barrier = threading.Barrier(len(same_table))

    def try_open(wid: int):
        barrier.wait()
        try:
            open_window(wid, db)
            results.append((wid, "成功"))
        except ServiceError as exc:
            results.append((wid, exc.message))
        except sqlite3.OperationalError as exc:  # 锁等待超时也只能是拒灌
            results.append((wid, f"锁拒: {exc}"))

    threads = [threading.Thread(target=try_open, args=(wid,)) for wid in same_table]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [r for r in results if r[1] == "成功"]
    denied = [r for r in results if r[1] != "成功"]
    assert len(ok) == 1, results
    assert denied, results

    held = _rows(db, "SELECT window_id, mutex_no FROM backfill_mutex WHERE state='持有'")
    assert len(held) == 1
    assert held[0][0] == ok[0][0]

    # 页面芯片与对账目录必须同亮拒灌：赢家与输家都拒日增，日增混入清零。
    winner = ok[0][0]
    loser = next(wid for wid in same_table if wid != winner)
    for wid in (winner, loser):
        report = coverage_report(wid, db)
        assert report["gate"] == "拒灌", report
        assert report["mixed_rows"] == 0, report

    winner_report = coverage_report(winner, db)
    assert winner_report["mutex_no"] == held[0][1]
    with pytest.raises(ServiceError):
        daily_ingest("dw_orders_d", "2026-11-11", 2, db)
    assert _rows(db, "SELECT COUNT(*) FROM landing_rows WHERE source='日增量'")[0][0] == 0

    # 输家窗口在列表里同样是拒灌芯片，且重放被门槛挡住
    from app.services.windows import list_windows

    cards = {w["id"]: w for w in list_windows(db)}
    assert cards[winner]["gate_increment"] == "拒灌"
    assert cards[winner]["mutex_no"] == held[0][1]
    assert cards[loser]["gate_increment"] == "拒灌"
    assert cards[loser]["aligned"] is True


def test_same_table_other_card_flags_gate_mismatch(tmp_path):
    """同表窗口互斥丢失：不仅本窗失配，同表其它卡片也要亮门槛失配。"""
    db = _fresh_db(tmp_path)
    rows = _rows(db, "SELECT id FROM backfill_windows WHERE table_name='dw_orders_d' ORDER BY id")
    a, b = rows[0][0], rows[1][0]
    plan_slices(a, db)
    plan_slices(b, db)
    open_window(a, db)

    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM backfill_mutex WHERE window_id=?", (a,))
    conn.commit()
    conn.close()

    from app.services.windows import list_windows

    cards = {w["id"]: w for w in list_windows(db)}
    assert cards[a]["aligned"] is False
    assert cards[b]["aligned"] is False
    assert cards[b]["gate_increment"] == "拒灌"

    with pytest.raises(ServiceError):
        daily_ingest("dw_orders_d", "2026-11-11", 2, db)
    assert _rows(db, "SELECT COUNT(*) FROM landing_rows WHERE source='日增量'")[0][0] == 0


def test_open_keeps_window_and_mutex_aligned(tmp_path):
    db = _fresh_db(tmp_path)
    wid = _rows(db, "SELECT id FROM backfill_windows ORDER BY id")[0][0]
    plan_slices(wid, db)
    opened = open_window(wid, db)
    assert opened["status"] == "执行中"

    from app.services.windows import _active_mutex, _window

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        mutex = _active_mutex(conn, "dw_orders_d")
        win = _window(conn, wid)
        assert mutex is not None and mutex["window_id"] == wid
        assert win["status"] == "执行中"
    finally:
        conn.close()


def test_mutex_missing_blocks_replay(tmp_path):
    """开灌态与互斥账册拧了就要拦：删掉互斥后重放必须 409。"""
    db = _fresh_db(tmp_path)
    wid = _rows(db, "SELECT id FROM backfill_windows ORDER BY id")[0][0]
    plan_slices(wid, db)
    open_window(wid, db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM backfill_mutex WHERE window_id=?", (wid,))
    conn.commit()
    conn.close()

    sid = _rows(db, "SELECT id FROM backfill_slices WHERE window_id=? ORDER BY id", wid)[0][0]
    with pytest.raises(ServiceError) as ei:
        replay_slice(sid, db)
    assert ei.value.status_code == 409
    assert "不一致" in ei.value.message

    # 对账不抛错，但必须判“对账存疑”，不能虚绿
    report = coverage_report(wid, db)
    assert report["verdict"] == "对账存疑"
    assert "不一致" in report["summary"] or report["gate"] == "拒灌"


def test_concurrent_open_then_daily_burst_writes_nothing(tmp_path):
    """开灌成立后并发打日增量：全部拒灌，落点日增行必须为 0。"""
    db = _fresh_db(tmp_path)
    wid = _rows(db, "SELECT id FROM backfill_windows WHERE id=1")[0][0]
    plan_slices(wid, db)
    open_window(wid, db)

    errors: list[str] = []
    barrier = threading.Barrier(8)

    def hammer(i: int):
        barrier.wait()
        try:
            daily_ingest("dw_orders_d", "2026-11-1" + str(i % 3 + 0), 4, db)
            errors.append("居然写进去了")
        except ServiceError as exc:
            errors.append(exc.message)
        except sqlite3.OperationalError as exc:
            errors.append(f"锁拒: {exc}")

    threads = [threading.Thread(target=hammer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 8, errors
    assert all("拒灌" in e or "锁拒" in e for e in errors), errors
    mixed = _rows(db, "SELECT COUNT(*) FROM landing_rows WHERE source='日增量'")[0][0]
    assert mixed == 0

    report = coverage_report(wid, db)
    assert report["mixed_rows"] == 0
    assert report["gate"] == "拒灌"


def test_replay_and_coverage_concurrent_no_pk_collision(tmp_path):
    """同一窗口多切片并发重放 + 并发对账，不得撞主键。"""
    db = _fresh_db(tmp_path)
    wid = _rows(db, "SELECT id FROM backfill_windows ORDER BY id")[0][0]
    planned = plan_slices(wid, db)
    open_window(wid, db)
    sids = [s["id"] for s in planned["slices"]]

    crashes: list[str] = []
    barrier = threading.Barrier(len(sids) + 2)

    def do_replay(sid):
        barrier.wait()
        try:
            replay_slice(sid, db)
        except ServiceError:
            pass
        except Exception as exc:  # noqa: BLE001 — 只关心是否落到 PK 异常
            crashes.append(f"{type(exc).__name__}: {exc}")

    def do_report():
        barrier.wait()
        try:
            coverage_report(wid, db)
        except ServiceError:
            pass
        except Exception as exc:  # noqa: BLE001
            crashes.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=do_replay, args=(s,)) for s in sids]
    threads += [threading.Thread(target=do_report) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert crashes == []
    loaded = _rows(db, "SELECT COUNT(*) FROM backfill_slices WHERE window_id=? AND status='已落库'", wid)[0][0]
    assert loaded == len(sids)
    dup = _rows(
        db,
        "SELECT table_name, biz_date, row_no, COUNT(*) c FROM landing_rows "
        "GROUP BY table_name, biz_date, row_no HAVING c > 1",
    )
    assert dup == []

    close_window(wid, db)
    final = coverage_report(wid, db)
    assert final["verdict"] == "通过"
    assert final["mixed_rows"] == 0


def test_coverage_not_falsely_green_with_missing_slices(tmp_path):
    db = _fresh_db(tmp_path)
    wid = _rows(db, "SELECT id FROM backfill_windows ORDER BY id")[0][0]
    planned = plan_slices(wid, db)
    open_window(wid, db)
    replay_slice(planned["slices"][1]["id"], db)
    report = coverage_report(wid, db)
    assert report["verdict"] == "对账存疑"
    assert report["missing"] == 2
    assert "缺口2" in report["summary"]
