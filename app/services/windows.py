"""回灌业务。

挡增量的路线只保留一条（A）：
  开灌 / 重放 / 日增量全部走同一个 BEGIN EXCLUSIVE 事务，
  日增量先查互斥账册，命中就直接拒灌并抛出短句 —— 目标表里一行都不写。
不做“先混入再整笔回滚”的旁路，因此两种落库结果不会并存。

落点表 landing_rows 的主键是 (table_name, biz_date, row_no)，
回灌与日增量共用它；写之前必须在同一把锁里验一遍，不允许靠撞主键发现冲突。
"""

from datetime import date, timedelta

from app.db import get_tx
from app.services.errors import ServiceError

BACKFILL_SOURCE = "回灌"
DAILY_SOURCE = "日增量"


# ---------------------------------------------------------------- 通用小件


def _log(
    conn,
    kind: str,
    outcome: str,
    *,
    window_id=None,
    slice_id=None,
    table_name=None,
    mutex_no=None,
    detail="",
) -> None:
    conn.execute(
        "INSERT INTO orchestration_log"
        "(kind, outcome, window_id, slice_id, table_name, mutex_no, detail) "
        "VALUES (?,?,?,?,?,?,?)",
        (kind, outcome, window_id, slice_id, table_name, mutex_no, detail),
    )


def _window(conn, window_id: int):
    win = conn.execute(
        "SELECT * FROM backfill_windows WHERE id=?", (window_id,)
    ).fetchone()
    if not win:
        raise ServiceError("回灌窗口不存在", 404)
    return win


def _reject(conn, message: str, status: int = 409):
    """拒灌要先落留痕再抛：直接 raise 会把刚写的日志一起回滚掉。

    只提交“拒灌”这一笔，落点表在拒灌路径上没有任何写入，所以不存在混入行。
    """
    conn.execute("COMMIT")
    raise ServiceError(message, status)


def _active_mutex(conn, table_name: str):
    return conn.execute(
        "SELECT * FROM backfill_mutex WHERE table_name=? AND state='持有'",
        (table_name,),
    ).fetchone()


def _stuck_window_on_table(conn, table_name: str):
    """状态说在执行中、互斥账册却没有持有记录 —— 同一目标表上任何窗口都算失配。"""
    return conn.execute(
        "SELECT id FROM backfill_windows w WHERE w.table_name=? AND w.status='执行中' "
        "AND NOT EXISTS (SELECT 1 FROM backfill_mutex m "
        "                WHERE m.window_id=w.id AND m.state='持有')",
        (table_name,),
    ).fetchone()


def _gate(conn, win) -> dict:
    """一张目标表的可写门槛，由互斥账册 + 窗口状态共同决定。

    回灌可写  ⇔ 本窗口持有该表的互斥且状态为执行中；
    日增可写  ⇔ 该表没有持有中的互斥，且门槛本身没有失配。
    """
    mutex = _active_mutex(conn, win["table_name"])
    own = bool(mutex and mutex["window_id"] == win["id"])
    opening = win["status"] == "执行中"

    aligned = True
    problem = ""
    if opening and not own:
        aligned, problem = False, "窗口状态为执行中，但互斥账册没有本窗口的持有记录"
    elif not opening and own:
        aligned, problem = False, "互斥账册持有中，但窗口状态不是执行中"
    elif not mutex:
        # 同表另一个窗口状态拧了，也会把整张表的门槛带歪。
        stuck = _stuck_window_on_table(conn, win["table_name"])
        if stuck:
            aligned = False
            problem = f"同表窗口#{stuck['id']} 状态为执行中，但互斥账册无持有记录"

    replay_open = aligned and own
    # 失配时两条路都当拒灌处理：状态说在执行中却没有互斥，日增写进去就会和回灌挤落点。
    increment_open = bool(mutex) or not aligned
    return {
        "aligned": aligned,
        "problem": problem,
        "mutex_no": mutex["mutex_no"] if mutex else None,
        "mutex_window_id": mutex["window_id"] if mutex else None,
        "own_mutex": own,
        "opening": opening,
        "gate_increment": "拒灌" if increment_open else "可写",
        "gate_replay": "可写" if replay_open else "拒灌",
    }


def _daterange(start: str, end: str) -> list[str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    if e < s:
        raise ServiceError("结束日不能早于开始日")
    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


# ---------------------------------------------------------------- 窗口 / 切片


def list_windows(db_path: str | None = None) -> list[dict]:
    with get_tx(db_path, "DEFERRED") as conn:
        rows = conn.execute(
            "SELECT w.*, "
            "(SELECT COUNT(*) FROM backfill_slices s WHERE s.window_id=w.id) AS slice_count, "
            "(SELECT COUNT(*) FROM backfill_slices s WHERE s.window_id=w.id AND s.status='已落库') AS loaded_count "
            "FROM backfill_windows w ORDER BY w.id"
        ).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item.update(_gate(conn, r))
            out.append(item)
        return out


def plan_slices(window_id: int, db_path: str | None = None) -> dict:
    with get_tx(db_path) as conn:
        win = _window(conn, window_id)
        if win["status"] not in ("草稿", "已计划"):
            raise ServiceError("当前状态不可再生成切片", 409)
        if _active_mutex(conn, win["table_name"]):
            raise ServiceError("目标表开灌中，暂不可重排切片", 409)
        days = _daterange(win["start_date"], win["end_date"])
        conn.execute("DELETE FROM backfill_slices WHERE window_id=?", (window_id,))
        for d in days:
            conn.execute(
                "INSERT INTO backfill_slices(window_id, slice_date, status, row_count) "
                "VALUES (?,?, '待重放', 0)",
                (window_id, d),
            )
        conn.execute(
            "UPDATE backfill_windows SET status='已计划' WHERE id=?", (window_id,)
        )
        slices = conn.execute(
            "SELECT * FROM backfill_slices WHERE window_id=? ORDER BY slice_date",
            (window_id,),
        ).fetchall()
        _log(conn, "切片计划", "成功", window_id=window_id, table_name=win["table_name"],
             detail=f"生成{len(days)}片")
        return {"window_id": window_id, "slices": [dict(s) for s in slices]}


def list_slices(window_id: int, db_path: str | None = None) -> dict:
    with get_tx(db_path, "DEFERRED") as conn:
        win = _window(conn, window_id)
        gate = _gate(conn, win)
        rows = conn.execute(
            "SELECT * FROM backfill_slices WHERE window_id=? ORDER BY slice_date",
            (window_id,),
        ).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            item["can_replay"] = (
                r["status"] == "待重放" and gate["gate_replay"] == "可写"
            )
            items.append(item)
        return {"items": items, "window": dict(win), "gate": gate}


# ---------------------------------------------------------------- 开灌 / 收灌


def open_window(window_id: int, db_path: str | None = None) -> dict:
    """开灌：排他锁里先验互斥账册与落点，再一次性写入开灌态。"""
    with get_tx(db_path) as conn:
        win = _window(conn, window_id)
        table = win["table_name"]

        if win["status"] == "执行中":
            raise ServiceError("窗口已开灌，勿重复开灌", 409)
        if win["status"] == "已完成":
            raise ServiceError("窗口已完成，不可再开灌", 409)
        if win["status"] == "草稿":
            raise ServiceError("请先生成切片计划再开灌", 409)

        mutex = _active_mutex(conn, table)
        if mutex:
            if mutex["window_id"] == window_id:
                msg = "开灌态与互斥账册不一致，已拦截"
            else:
                msg = f"目标表已被窗口#{mutex['window_id']} 开灌（{mutex['mutex_no']}），拒灌"
            _log(conn, "开灌", "拒灌", window_id=window_id, table_name=table,
                 mutex_no=mutex["mutex_no"], detail=msg)
            _reject(conn, msg)

        # 开灌要求区间内落点干净：
        #  · 日增行 → A 路线下区间内日增必须为零，有就拒开；
        #  · 回灌行 → 说明与既有窗口重叠，开灌后必然无片可落，提前拒入。
        existing = conn.execute(
            "SELECT source, COUNT(*) AS c FROM landing_rows "
            "WHERE table_name=? AND biz_date BETWEEN ? AND ? GROUP BY source",
            (table, win["start_date"], win["end_date"]),
        ).fetchall()
        counts = {r["source"]: r["c"] for r in existing}
        if counts.get(DAILY_SOURCE):
            msg = f"区间已有 {counts[DAILY_SOURCE]} 行日增量落点，拒开灌，请先清理"
            _log(conn, "开灌", "拒灌", window_id=window_id, table_name=table,
                 detail=msg)
            _reject(conn, msg)
        if counts.get(BACKFILL_SOURCE):
            msg = f"区间已有 {counts[BACKFILL_SOURCE]} 行回灌落点，与其他窗口重叠，拒开灌"
            _log(conn, "开灌", "拒灌", window_id=window_id, table_name=table,
                 detail=msg)
            _reject(conn, msg)

        # 占位号用随机串，避免两张表同时开灌时在 UNIQUE(mutex_no) 上撞车。
        cur = conn.execute(
            "INSERT INTO backfill_mutex(table_name, window_id, mutex_no, state) "
            "VALUES (?,?, 'TMP-' || lower(hex(randomblob(8))), '持有')",
            (table, window_id),
        )
        mutex_no = f"MX-{cur.lastrowid:04d}"
        conn.execute(
            "UPDATE backfill_mutex SET mutex_no=? WHERE id=?", (mutex_no, cur.lastrowid)
        )
        conn.execute(
            "UPDATE backfill_windows SET status='执行中' WHERE id=?", (window_id,)
        )
        _log(conn, "开灌", "成功", window_id=window_id, table_name=table,
             mutex_no=mutex_no, detail="日增量自此刻起拒灌")
        return {
            "window_id": window_id,
            "table_name": table,
            "status": "执行中",
            "mutex_no": mutex_no,
            "gate_increment": "拒灌",
        }


def close_window(window_id: int, db_path: str | None = None) -> dict:
    with get_tx(db_path) as conn:
        win = _window(conn, window_id)
        gate = _gate(conn, win)
        if not gate["aligned"]:
            raise ServiceError("开灌态与可写门槛不一致，已拦截", 409)
        if not gate["own_mutex"]:
            raise ServiceError("窗口未开灌，无可收之灌", 409)
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM backfill_slices WHERE window_id=? AND status!='已落库'",
            (window_id,),
        ).fetchone()["c"]
        if pending:
            raise ServiceError(f"还有 {pending} 片未落库，不能收灌", 409)
        conn.execute(
            "UPDATE backfill_mutex SET state='已释放', closed_at=datetime('now') "
            "WHERE window_id=? AND state='持有'",
            (window_id,),
        )
        conn.execute(
            "UPDATE backfill_windows SET status='已完成' WHERE id=?", (window_id,)
        )
        _log(conn, "收灌", "成功", window_id=window_id, table_name=win["table_name"],
             mutex_no=gate["mutex_no"], detail="日增量恢复可写")
        return {
            "window_id": window_id,
            "status": "已完成",
            "mutex_no": gate["mutex_no"],
            "gate_increment": "可写",
        }


# ---------------------------------------------------------------- 重放


def replay_slice(slice_id: int, db_path: str | None = None) -> dict:
    with get_tx(db_path) as conn:
        sl = conn.execute(
            "SELECT * FROM backfill_slices WHERE id=?", (slice_id,)
        ).fetchone()
        if not sl:
            raise ServiceError("切片不存在", 404)
        win = _window(conn, sl["window_id"])
        gate = _gate(conn, win)
        table = win["table_name"]

        if not gate["aligned"]:
            _log(conn, "重放", "拒灌", window_id=win["id"], slice_id=slice_id,
                 table_name=table, mutex_no=gate["mutex_no"], detail=gate["problem"])
            _reject(conn, "开灌态与可写门槛不一致，已拦截")
        if gate["gate_replay"] != "可写":
            _log(conn, "重放", "拒灌", window_id=win["id"], slice_id=slice_id,
                 table_name=table, mutex_no=gate["mutex_no"],
                 detail="窗口未开灌或互斥号不在本窗口")
            _reject(conn, "窗口未开灌或互斥号不在本窗口，拒灌")
        if sl["status"] == "已落库":
            raise ServiceError("切片已落库", 409)
        if sl["status"] != "待重放":
            raise ServiceError("仅待重放切片可重放", 409)

        # A 路线：回灌区间内不得存在日增行。出现即拒灌，绝不靠主键冲突兜底。
        daily = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_rows "
            "WHERE table_name=? AND biz_date=? AND source=?",
            (table, sl["slice_date"], DAILY_SOURCE),
        ).fetchone()["c"]
        if daily:
            _log(conn, "重放", "拒灌", window_id=win["id"], slice_id=slice_id,
                 table_name=table, mutex_no=gate["mutex_no"],
                 detail=f"该日已有{daily}行日增量")
            _reject(conn, "该日落点已有日增量行，拒灌并拦截")

        already = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_rows "
            "WHERE table_name=? AND biz_date=? AND source=?",
            (table, sl["slice_date"], BACKFILL_SOURCE),
        ).fetchone()["c"]
        if already:
            _log(conn, "重放", "拒灌", window_id=win["id"], slice_id=slice_id,
                 table_name=table, mutex_no=gate["mutex_no"],
                 detail="该日回灌落点已存在")
            _reject(conn, "该日回灌落点已存在，勿重复重放")

        rows = 100 + int(sl["slice_date"].replace("-", "")) % 97
        conn.executemany(
            "INSERT INTO landing_rows(table_name, biz_date, row_no, payload, source) "
            "VALUES (?,?,?,?,?)",
            [
                (table, sl["slice_date"], n, f"{table}:{sl['slice_date']}:{n}", BACKFILL_SOURCE)
                for n in range(1, rows + 1)
            ],
        )
        conn.execute(
            "UPDATE backfill_slices SET status='已落库', row_count=? WHERE id=?",
            (rows, slice_id),
        )
        # 窗口保持“执行中”：收灌要在互斥账册里释放后才会置“已完成”，
        # 这样开灌态与可写门槛始终对齐。
        _log(conn, "重放", "成功", window_id=win["id"], slice_id=slice_id,
             table_name=table, mutex_no=gate["mutex_no"], detail=f"落库{rows}行")
        row = conn.execute(
            "SELECT * FROM backfill_slices WHERE id=?", (slice_id,)
        ).fetchone()
        return dict(row)


# ---------------------------------------------------------------- 日增量


def daily_ingest(
    table_name: str, biz_date: str, rows: int, db_path: str | None = None
) -> dict:
    """日增量落库。与开灌共用一把排他锁；命中互斥账册就整笔拒灌，零行写入。"""
    table_name = (table_name or "").strip()
    if not table_name:
        raise ServiceError("目标表不能为空")
    if rows < 1 or rows > 1000:
        raise ServiceError("行数需在 1~1000 之间")
    date.fromisoformat(biz_date)  # 非法日期直接 400
    with get_tx(db_path) as conn:
        mutex = _active_mutex(conn, table_name)
        if mutex:
            _log(conn, "日增量", "拒灌", window_id=mutex["window_id"],
                 table_name=table_name, mutex_no=mutex["mutex_no"],
                 detail=f"{biz_date} {rows}行未写入")
            _reject(
                conn,
                f"目标表 {table_name} 回灌开灌中（{mutex['mutex_no']}），日增量拒灌",
            )
        backfilled = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_rows "
            "WHERE table_name=? AND biz_date=? AND source=?",
            (table_name, biz_date, BACKFILL_SOURCE),
        ).fetchone()["c"]
        if backfilled:
            _log(conn, "日增量", "拒灌", table_name=table_name,
                 detail=f"{biz_date} 已被回灌占用")
            _reject(conn, "该日落点已被回灌占用，日增量拒灌")

        # 开灌态与互斥账册拧了也要拦：窗口在“执行中”却没有持有互斥，
        # 此时日增量若放行，就会和正在开灌的回灌挤同一落点。
        stuck = _stuck_window_on_table(conn, table_name)
        if stuck:
            _log(conn, "日增量", "拒灌", window_id=stuck["id"], table_name=table_name,
                 detail="开灌态与互斥账册不一致")
            _reject(conn, "开灌态与可写门槛不一致，日增量拒灌")
        conn.execute(
            "DELETE FROM landing_rows WHERE table_name=? AND biz_date=? AND source=?",
            (table_name, biz_date, DAILY_SOURCE),
        )
        conn.executemany(
            "INSERT INTO landing_rows(table_name, biz_date, row_no, payload, source) "
            "VALUES (?,?,?,?,?)",
            [
                (table_name, biz_date, n, f"{table_name}:{biz_date}:{n}", DAILY_SOURCE)
                for n in range(1, rows + 1)
            ],
        )
        _log(conn, "日增量", "成功", table_name=table_name, detail=f"{biz_date} {rows}行")
        return {"table_name": table_name, "biz_date": biz_date, "written": rows}


# ---------------------------------------------------------------- 互斥账册 / 留痕


def list_mutexes(db_path: str | None = None) -> list[dict]:
    with get_tx(db_path, "DEFERRED") as conn:
        rows = conn.execute(
            "SELECT m.*, w.start_date, w.end_date, w.status AS window_status "
            "FROM backfill_mutex m JOIN backfill_windows w ON w.id=m.window_id "
            "ORDER BY m.id DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def list_log(db_path: str | None = None, limit: int = 50) -> list[dict]:
    with get_tx(db_path, "DEFERRED") as conn:
        rows = conn.execute(
            "SELECT * FROM orchestration_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------- 覆盖对账


def coverage_report(window_id: int, db_path: str | None = None) -> dict:
    with get_tx(db_path) as conn:
        win = _window(conn, window_id)
        table = win["table_name"]
        gate = _gate(conn, win)

        planned = conn.execute(
            "SELECT COUNT(*) AS c FROM backfill_slices WHERE window_id=?", (window_id,)
        ).fetchone()["c"]
        loaded = conn.execute(
            "SELECT COUNT(*) AS c FROM backfill_slices WHERE window_id=? AND status='已落库'",
            (window_id,),
        ).fetchone()["c"]
        missing = planned - loaded

        slice_rows = conn.execute(
            "SELECT COALESCE(SUM(row_count),0) AS c FROM backfill_slices "
            "WHERE window_id=? AND status='已落库'",
            (window_id,),
        ).fetchone()["c"]
        landing = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_rows WHERE table_name=? AND source=? "
            "AND biz_date BETWEEN ? AND ?",
            (table, BACKFILL_SOURCE, win["start_date"], win["end_date"]),
        ).fetchone()["c"]
        mixed = conn.execute(
            "SELECT COUNT(*) AS c FROM landing_rows WHERE table_name=? AND source=? "
            "AND biz_date BETWEEN ? AND ?",
            (table, DAILY_SOURCE, win["start_date"], win["end_date"]),
        ).fetchone()["c"]

        reasons = []
        if not gate["aligned"]:
            reasons.append("开灌态与互斥账册不一致")
        if missing:
            reasons.append(f"缺{missing}片")
        if mixed:
            reasons.append(f"日增混入{mixed}行")
        if landing != slice_rows:
            reasons.append(f"落点{landing}行与切片合计{slice_rows}行不符")
        # 主键唯一性复核：同 (表,日,行号) 不可能有两条，这里只确认落点未超记切片行数。
        verdict = "通过" if not reasons else "对账存疑"

        summary = (
            f"窗口#{window_id} {table}：计划{planned}片，已落库{loaded}，缺口{missing}，"
            f"日增混入{mixed}行，{gate['gate_increment']}"
            + (f"（{gate['mutex_no']}）" if gate["mutex_no"] else "")
            + (f"；门槛失配：{gate['problem']}" if not gate["aligned"] else "")
        )
        cur = conn.execute(
            "INSERT INTO coverage_reports"
            "(window_id, planned, loaded, missing, summary, landing_rows, mixed_rows, "
            " mutex_no, gate, verdict) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (window_id, planned, loaded, missing, summary, landing, mixed,
             gate["mutex_no"], gate["gate_increment"], verdict),
        )
        _log(conn, "覆盖对账", verdict, window_id=window_id, table_name=table,
             mutex_no=gate["mutex_no"], detail=summary)
        row = conn.execute(
            "SELECT * FROM coverage_reports WHERE id=?", (cur.lastrowid,)
        ).fetchone()
        return dict(row)


def list_coverage(db_path: str | None = None) -> list[dict]:
    with get_tx(db_path, "DEFERRED") as conn:
        rows = conn.execute(
            "SELECT * FROM coverage_reports ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
