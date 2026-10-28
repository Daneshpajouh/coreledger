"""The same catalogue on PostgreSQL. Runs only when COREL_PG_URL names a database to use.

Each test works in its own new schema, so the database needs no cleaning.
"""

import json
import os
import uuid
from urllib.parse import quote

import pytest

from coreledger import audit, db
from coreledger.cli import main

from .test_core import COMMIT, FINISHED, RATES, SACCT, TRACE, write

URL = os.environ.get("COREL_PG_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set COREL_PG_URL to run the PostgreSQL tests")


@pytest.fixture
def pg_target():
    import psycopg

    schema = "t_" + uuid.uuid4().hex[:12]
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {schema}")
    sep = "&" if "?" in URL else "?"
    yield f"{URL}{sep}options={quote(f'-csearch_path={schema}')}"
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA {schema} CASCADE")


def test_end_to_end_on_postgres(tmp_path, pg_target, capsys):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\nB,P1\nC,P1\n")
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,1000000\nB,RUN1,1,3000000\nC,RUN1,1,5000000000\n")
    write(tmp_path / "res" / "A.txt", "a")
    write(tmp_path / "res" / "B.txt", "b")
    write(tmp_path / "res2" / "C.txt", "c")
    write(tmp_path / "sacct.txt", SACCT)
    write(tmp_path / "trace.txt", TRACE)
    write(tmp_path / "rates.json", json.dumps(RATES))
    gate = write(tmp_path / "gate.json", json.dumps({"verdict": "PASS"}))
    assert main(["init", pg_target]) == 0
    assert main(["init", pg_target]) == 0
    assert main(["intake", pg_target, str(tmp_path / "i.csv")]) == 0
    assert main(["sequencing", pg_target, str(tmp_path / "l.csv")]) == 0
    assert main([
        "register", pg_target, "R1", "--pipeline", "nf-core/demo", "--outdir", str(tmp_path / "res"),
        "--sample", "A", "--sample", "B", "--commit", COMMIT, "--sacct", str(tmp_path / "sacct.txt"),
        "--trace", str(tmp_path / "trace.txt"), "--gate", str(gate),
    ]) == 0
    r2 = ["register", pg_target, "R2", "--pipeline", "p", "--outdir", str(tmp_path / "res2"), "--sample", "C"]
    assert main(r2) == 0
    assert main(["release", pg_target, "R1"]) == 0
    assert main(["release", pg_target, "R2"]) == 2
    assert main(["bill", pg_target, "--rates", str(tmp_path / "rates.json"), "--outdir", str(tmp_path / "bill")]) == 0
    out = capsys.readouterr().out
    assert "P1: CAD 0.49 (sample lines add up)" in out
    rows = (tmp_path / "bill" / "samples.csv").read_text().splitlines()
    assert rows[1:] == [
        "P1,A,4.5000,9.0000,0.0000,0.315000,0.32,CAD",
        "P1,B,2.5000,5.0000,0.0000,0.175000,0.17,CAD",
    ]
    ledger = db.open_existing(pg_target)
    try:
        assert ledger.one("SELECT reads FROM sample_runs WHERE sample_id = ?", ("C",)) == (5000000000,)
        found = sorted((f.code, f.subject) for f in audit.audit(ledger, check_files=True))
        assert found == [("NO_COMMIT", "R2")]
    finally:
        ledger.close()


def test_rollback_on_postgres(tmp_path, pg_target):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    write(tmp_path / "sacct.txt", FINISHED)
    (tmp_path / "o1").mkdir()
    write(tmp_path / "o2" / "A.txt", "a")
    assert main(["init", pg_target]) == 0
    assert main(["intake", pg_target, str(tmp_path / "i.csv")]) == 0
    base = ["--pipeline", "p", "--sample", "A", "--sacct", str(tmp_path / "sacct.txt")]
    assert main(["register", pg_target, "R1", "--outdir", str(tmp_path / "o1"), *base]) == 0
    # the same Slurm jobs again: refused after the run and its results were written, so all of it is undone
    assert main(["register", pg_target, "R2", "--outdir", str(tmp_path / "o2"), *base]) == 2
    ledger = db.open_existing(pg_target)
    try:
        assert ledger.query("SELECT prun_id FROM pipeline_runs") == [("R1",)]
        assert ledger.one("SELECT COUNT(*) FROM results") == (0,)
    finally:
        ledger.close()
