import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from app.config import Config

BUSY_TIMEOUT_MS = int(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "5000"))


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path,
        timeout=BUSY_TIMEOUT_MS / 1000,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    # synchronous 是连接级设置；journal_mode=WAL 持久在库文件里，由 init_db 设一次，
    # 避免每个连接都去抢一次改模式的锁。
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_tx(db_path: str | None = None, mode: str = "EXCLUSIVE"):
    """显式事务；开灌、重放、日增量都在这把排他锁里做判定与落库。

    pysqlite 默认会在 DML 前隐式开事务，显式 BEGIN 前必须先关掉隐式事务，
    否则并发进入时会变成嵌套事务。
    """
    conn = connect(db_path)
    conn.isolation_level = None
    try:
        conn.execute(f"BEGIN {mode}")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS backfill_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('草稿','已计划','执行中','已完成')),
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS backfill_slices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER NOT NULL REFERENCES backfill_windows(id),
  slice_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('待重放','已落库','失败')),
  row_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE(window_id, slice_date)
);

CREATE TABLE IF NOT EXISTS coverage_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER NOT NULL,
  planned INTEGER NOT NULL,
  loaded INTEGER NOT NULL,
  missing INTEGER NOT NULL,
  summary TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 回灌与日增量共用的落点表：同一主键只能被一条来源占住。
CREATE TABLE IF NOT EXISTS landing_rows (
  table_name TEXT NOT NULL,
  biz_date TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  payload TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('回灌','日增量')),
  loaded_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (table_name, biz_date, row_no)
);

-- 互斥账册：一张目标表同时只允许一条“持有”记录。
CREATE TABLE IF NOT EXISTS backfill_mutex (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  window_id INTEGER NOT NULL REFERENCES backfill_windows(id),
  mutex_no TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL CHECK (state IN ('持有','已释放')),
  opened_at TEXT NOT NULL DEFAULT (datetime('now')),
  closed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_mutex_active
  ON backfill_mutex(table_name) WHERE state='持有';

-- 编排留痕：开灌、拒灌、重放、对账都记一笔。
CREATE TABLE IF NOT EXISTS orchestration_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL DEFAULT (datetime('now')),
  actor TEXT NOT NULL DEFAULT 'admin',
  kind TEXT NOT NULL,
  outcome TEXT NOT NULL,
  window_id INTEGER,
  slice_id INTEGER,
  table_name TEXT,
  mutex_no TEXT,
  detail TEXT NOT NULL DEFAULT ''
);
"""

# coverage_reports 是发布过的表，老库要补列。
_COVERAGE_COLS = {
    "landing_rows": "INTEGER NOT NULL DEFAULT 0",
    "mixed_rows": "INTEGER NOT NULL DEFAULT 0",
    "mutex_no": "TEXT",
    "gate": "TEXT NOT NULL DEFAULT '可写'",
    "verdict": "TEXT NOT NULL DEFAULT '对账存疑'",
}


def _migrate(conn: sqlite3.Connection) -> None:
    have = {r["name"] for r in conn.execute("PRAGMA table_info(coverage_reports)")}
    for name, ddl in _COVERAGE_COLS.items():
        if name not in have:
            conn.execute(f"ALTER TABLE coverage_reports ADD COLUMN {name} {ddl}")


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        # WAL 持久在库文件里：读不被写挡，写仍靠 BEGIN EXCLUSIVE 串行化。
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        _migrate(conn)
        if conn.execute("SELECT value FROM meta WHERE key='seeded'").fetchone():
            return
        cur = conn.execute(
            "INSERT INTO backfill_windows(table_name, start_date, end_date, status) "
            "VALUES ('dw_orders_d','2026-11-10','2026-11-12','草稿')"
        )
        wid = cur.lastrowid
        conn.execute(
            "INSERT INTO backfill_windows(table_name, start_date, end_date, status) "
            "VALUES ('dw_payments_d','2026-11-11','2026-11-11','草稿')"
        )
        # 同一目标表的第二个窗口：用来演示“两侧仓管同点开灌”的互斥。
        conn.execute(
            "INSERT INTO backfill_windows(table_name, start_date, end_date, status) "
            "VALUES ('dw_orders_d','2026-11-13','2026-11-14','草稿')"
        )
        # placeholder so list is non-empty; slices created by plan
        _ = wid
        conn.execute("INSERT INTO meta(key, value) VALUES ('seeded','1')")
