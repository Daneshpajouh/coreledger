"""The catalogue: one SQLite file, or a PostgreSQL database with the same tables.

The SQL here is the subset both engines accept. Code writes ``?`` placeholders; the
PostgreSQL connection rewrites them to ``%s``.
"""

import os
import sqlite3
from typing import Any, Iterable, List, Optional, Sequence, Tuple

SCHEMA_VERSION = 1

TABLES = [
    """CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS projects (
        project_id TEXT PRIMARY KEY,
        name TEXT,
        account TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS samples (
        sample_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL REFERENCES projects (project_id),
        external_id TEXT,
        material TEXT,
        received TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS seq_runs (
        run_id TEXT PRIMARY KEY,
        instrument TEXT,
        flowcell TEXT,
        run_date TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS sample_runs (
        sample_id TEXT NOT NULL REFERENCES samples (sample_id),
        run_id TEXT NOT NULL REFERENCES seq_runs (run_id),
        lane TEXT NOT NULL,
        reads BIGINT,
        PRIMARY KEY (sample_id, run_id, lane)
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        prun_id TEXT PRIMARY KEY,
        pipeline TEXT NOT NULL,
        version TEXT,
        commit_sha TEXT,
        profile TEXT,
        params_sha256 TEXT,
        outdir TEXT NOT NULL,
        registered TEXT NOT NULL,
        gate_report TEXT,
        gate_verdict TEXT,
        gate_config_sha256 TEXT,
        released TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS prun_samples (
        prun_id TEXT NOT NULL REFERENCES pipeline_runs (prun_id),
        sample_id TEXT NOT NULL REFERENCES samples (sample_id),
        reads BIGINT,
        PRIMARY KEY (prun_id, sample_id)
    )""",
    """CREATE TABLE IF NOT EXISTS results (
        prun_id TEXT NOT NULL REFERENCES pipeline_runs (prun_id),
        path TEXT NOT NULL,
        sample_id TEXT REFERENCES samples (sample_id),
        sha256 TEXT NOT NULL,
        bytes BIGINT NOT NULL,
        PRIMARY KEY (prun_id, path)
    )""",
    """CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY,
        prun_id TEXT NOT NULL REFERENCES pipeline_runs (prun_id),
        name TEXT,
        tag TEXT,
        state TEXT,
        elapsed_s BIGINT NOT NULL,
        alloc_cpus INTEGER NOT NULL,
        req_mem_bytes BIGINT NOT NULL,
        gpus INTEGER NOT NULL,
        sample_id TEXT REFERENCES samples (sample_id)
    )""",
]


class Ledger:
    """A thin wrapper over a DB-API connection that hides the placeholder style."""

    def __init__(self, conn: Any, dialect: str):
        self.conn = conn
        self.dialect = dialect

    def _sql(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.dialect == "postgres" else sql

    def execute(self, sql: str, args: Sequence[Any] = ()) -> None:
        cur = self.conn.cursor()
        cur.execute(self._sql(sql), tuple(args))
        cur.close()

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        cur = self.conn.cursor()
        cur.executemany(self._sql(sql), [tuple(r) for r in rows])
        cur.close()

    def query(self, sql: str, args: Sequence[Any] = ()) -> List[Tuple[Any, ...]]:
        cur = self.conn.cursor()
        cur.execute(self._sql(sql), tuple(args))
        rows = [tuple(r) for r in cur.fetchall()]
        cur.close()
        return rows

    def one(self, sql: str, args: Sequence[Any] = ()) -> Optional[Tuple[Any, ...]]:
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()


def connect(target: str) -> Ledger:
    """Open a catalogue: a file path for SQLite, or a ``postgresql://`` URL."""
    if target.startswith(("postgresql://", "postgres://")):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL needs the psycopg package: pip install 'coreledger[postgres]'") from exc
        return Ledger(psycopg.connect(target), "postgres")
    conn = sqlite3.connect(target)
    conn.execute("PRAGMA foreign_keys = ON")
    return Ledger(conn, "sqlite")


def integrity_errors() -> Tuple[type, ...]:
    """The constraint errors of both engines, such as a lane entered twice (psycopg's when it is installed)."""
    errors: List[type] = [sqlite3.IntegrityError]
    try:
        import psycopg
    except ImportError:
        pass
    else:
        errors.append(psycopg.IntegrityError)
    return tuple(errors)


def init(ledger: Ledger) -> None:
    """Create the tables if they are missing; refuse a catalogue from another schema version."""
    for ddl in TABLES:
        ledger.execute(ddl)
    row = ledger.one("SELECT value FROM meta WHERE key = ?", ("schema_version",))
    if row is None:
        ledger.execute("INSERT INTO meta (key, value) VALUES (?, ?)", ("schema_version", str(SCHEMA_VERSION)))
    elif int(row[0]) != SCHEMA_VERSION:
        raise RuntimeError(f"catalogue schema version {row[0]}, this coreledger reads version {SCHEMA_VERSION}")
    ledger.commit()


def open_existing(target: str) -> Ledger:
    """Open a catalogue that ``coreledger init`` made, checking its schema version."""
    if not target.startswith(("postgresql://", "postgres://")) and not os.path.isfile(target):
        raise RuntimeError(f"{target} not found; run coreledger init first")
    ledger = connect(target)
    try:
        row = ledger.one("SELECT value FROM meta WHERE key = ?", ("schema_version",))
    except Exception as exc:
        ledger.close()
        raise RuntimeError(f"{target} is not a coreledger catalogue; run coreledger init first") from exc
    if row is None or int(row[0]) != SCHEMA_VERSION:
        ledger.close()
        raise RuntimeError(f"{target}: unsupported catalogue schema version {row[0] if row else 'none'}")
    return ledger
