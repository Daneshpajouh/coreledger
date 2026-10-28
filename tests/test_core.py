import json
import random
from fractions import Fraction
from pathlib import Path

import pytest

from coreledger import audit, billing, db, ingest, slurm
from coreledger.cli import main

COMMIT = "0123456789abcdef0123456789abcdef01234567"

SACCT = """JobID|JobIDRaw|JobName|State|Elapsed|AllocCPUS|NNodes|ReqMem|AllocTRES
101|101|nf-FASTQC_(A)|COMPLETED|01:00:00|4|1|8G|cpu=4,mem=8G,node=1
101.batch|101.batch|batch|COMPLETED|01:00:00|4|1||cpu=4,mem=8G,node=1
102|102|nf-FASTQC_(B)|COMPLETED|00:30:00|2|1|4G|cpu=2,mem=4G,node=1
103|103|nf-MULTIQC|COMPLETED|02:00:00|1|1|2G|cpu=1,mem=2G,node=1
104|104|other_users_job|COMPLETED|10:00:00|32|1|64G|cpu=32,mem=64G,node=1
105|105|nf-FASTQC_(B)|RUNNING|00:10:00|2|1|4G|cpu=2,mem=4G,node=1
"""

TRACE = """task_id\thash\tnative_id\tname\tstatus\texit
1\taa/000001\t101\tNFCORE_DEMO:FASTQC (A)\tCOMPLETED\t0
2\tbb/000002\t102\tNFCORE_DEMO:FASTQC (B)\tCOMPLETED\t0
3\tcc/000003\t103\tNFCORE_DEMO:MULTIQC\tCOMPLETED\t0
"""

RATES = {"currency": "CAD", "rates": {"cpu_hour": "0.05", "mem_gib_hour": "0.01"}}


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.fixture
def ledger(tmp_path):
    lg = db.connect(str(tmp_path / "core.sqlite"))
    db.init(lg)
    yield lg
    lg.close()


@pytest.fixture
def core(tmp_path, ledger):
    """Two samples of one project, one pipeline run with three jobs and a passing gate."""
    write(tmp_path / "intake.csv", "sample_id,project_id,material\nA,P1,RNA\nB,P1,RNA\n")
    write(tmp_path / "lanes.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,1000000\nB,RUN1,1,3000000\n")
    ingest.intake(ledger, tmp_path / "intake.csv")
    ingest.add_sequencing(ledger, tmp_path / "lanes.csv")
    out = tmp_path / "results"
    write(out / "fastqc" / "A_fastqc.html", "report for A\n")
    write(out / "fastqc" / "B_fastqc.html", "report for B\n")
    write(out / "multiqc" / "multiqc_report.html", "all samples\n")
    write(out / "pipeline_info" / "execution_trace.txt", "left out\n")
    gate = write(tmp_path / "gate.json", json.dumps({"tool": "tolgate", "verdict": "PASS", "config_sha256": "ab"}))
    reg = ingest.register_run(
        ledger, "R1", "nf-core/demo", out, ["A", "B"], version="1.0.0", commit=COMMIT,
        sacct=write(tmp_path / "sacct.txt", SACCT), trace=write(tmp_path / "trace.txt", TRACE), gate=gate,
        today="2026-10-28",
    )
    return reg


# sacct and trace parsing


@pytest.mark.parametrize(
    "text,seconds",
    [("01:00:00", 3600), ("20:00", 1200), ("2-03:04:05", 2 * 86400 + 3 * 3600 + 4 * 60 + 5), ("00:00.001", 0)],
)
def test_parse_elapsed(text, seconds):
    assert slurm.parse_elapsed(text) == seconds


@pytest.mark.parametrize("text", ["", "1h", "2-03:04", "1:2:3:4"])
def test_parse_elapsed_rejects(text):
    with pytest.raises(slurm.FormatError):
        slurm.parse_elapsed(text)


@pytest.mark.parametrize(
    "text,cpus,nodes,expected",
    [
        ("32G", 8, 1, 32 * 1024**3),
        ("4000Mc", 4, 1, 4 * 4000 * 1024**2),
        ("16000Mn", 8, 2, 2 * 16000 * 1024**2),
        ("2048", 1, 1, 2048 * 1024**2),
        ("1.5G", 1, 1, 3 * 1024**3 // 2),
    ],
)
def test_parse_reqmem(text, cpus, nodes, expected):
    assert slurm.parse_reqmem(text, cpus, nodes) == expected


def test_parse_gpus():
    assert slurm.parse_gpus("cpu=8,gres/gpu=2,gres/gpu:a100=2,mem=32G") == 2
    assert slurm.parse_gpus("cpu=8,gres/gpu:a100=1,gres/gpu:v100=2") == 3
    assert slurm.parse_gpus("cpu=8,mem=32G") == 0
    assert slurm.parse_gpus("") == 0


def test_read_sacct_keeps_finished_allocations(tmp_path):
    jobs, notes = slurm.read_sacct(write(tmp_path / "s.txt", SACCT))
    assert [j.job_id for j in jobs] == ["101", "102", "103", "104"]
    assert jobs[0].elapsed_s == 3600 and jobs[0].alloc_cpus == 4 and jobs[0].req_mem_bytes == 8 * 1024**3
    assert notes == ["job 105 skipped: state RUNNING"]


def test_read_sacct_array_job_keeps_both_ids(tmp_path):
    text = "JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|ReqMem\n7_1|7010|COMPLETED|60|1|1G\n"
    (job,), _ = slurm.read_sacct(write(tmp_path / "s.txt", text))
    assert (job.job_id, job.alt_id, job.elapsed_s) == ("7010", "7_1", 60)


def test_read_sacct_refuses_ragged_line(tmp_path):
    with pytest.raises(slurm.FormatError, match="line 2"):
        slurm.read_sacct(write(tmp_path / "s.txt", "JobID|Elapsed|AllocCPUS|ReqMem\n1|01:00|1\n"))


def test_read_sacct_needs_columns(tmp_path):
    with pytest.raises(slurm.FormatError, match="ReqMem"):
        slurm.read_sacct(write(tmp_path / "s.txt", "JobID|Elapsed|AllocCPUS\n1|01:00|1\n"))


def test_trace_tags(tmp_path):
    tags = slurm.read_nextflow_trace(write(tmp_path / "t.txt", TRACE))
    assert tags == {"101": "A", "102": "B", "103": ""}
    assert slurm.task_tag("X:Y (S1 (lane 2))") == "S1 (lane 2)"
    assert slurm.task_tag("X:Y") is None
    assert slurm.task_tag("X:Y (open") is None
    assert slurm.task_tag("X:Y close)") is None


# intake and registration


def test_intake_refuses_whole_sheet_on_one_bad_line(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nS1,P1\nS1,P1\n")
    with pytest.raises(ingest.InputError, match="twice"):
        ingest.intake(ledger, tmp_path / "i.csv")
    assert ledger.query("SELECT * FROM samples") == []


@pytest.mark.parametrize("bad", ["S 1", "-S1", "S1/2", ""])
def test_intake_refuses_bad_ids(tmp_path, ledger, bad):
    write(tmp_path / "i.csv", f"sample_id,project_id\n{bad},P1\n")
    with pytest.raises(ingest.InputError):
        ingest.intake(ledger, tmp_path / "i.csv")


def test_intake_refuses_bad_date_and_repeat(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id,received\nS1,P1,28/10/2026\n")
    with pytest.raises(ingest.InputError, match="not a date"):
        ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "j.csv", "sample_id,project_id\nS1,P1\n")
    ingest.intake(ledger, tmp_path / "j.csv")
    with pytest.raises(ingest.InputError, match="already"):
        ingest.intake(ledger, tmp_path / "j.csv")


def test_sequencing_needs_known_sample(tmp_path, ledger):
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nZ,RUN1,1,10\n")
    with pytest.raises(ingest.InputError, match="not in the catalogue"):
        ingest.add_sequencing(ledger, tmp_path / "l.csv")


@pytest.mark.parametrize(
    "path,expected",
    [
        ("fastqc/S1_fastqc.html", "S1"),
        ("fastqc/S10_fastqc.html", "S10"),
        ("star/S1/S1.Aligned.bam", "S1"),
        ("star/S1/S10.Aligned.bam", None),
        ("multiqc/multiqc_report.html", None),
        ("S1x/report.txt", None),
        ("salmon/S1_T2/quant.sf", "S1_T2"),
    ],
)
def test_sample_for_path(path, expected):
    assert ingest.sample_for_path(path, ["S1", "S10", "S1_T2"]) == expected


def test_register_records_results_jobs_and_gate(core, ledger):
    assert (core.results, core.per_sample, core.jobs, core.jobs_per_sample) == (3, 2, 3, 2)
    assert "1 sacct job(s) not in the trace were left out" in core.notes
    rows = ledger.query("SELECT path, sample_id FROM results ORDER BY path")
    assert rows == [
        ("fastqc/A_fastqc.html", "A"), ("fastqc/B_fastqc.html", "B"), ("multiqc/multiqc_report.html", None)
    ]
    assert ledger.query("SELECT job_id, sample_id FROM jobs ORDER BY job_id") == [
        ("101", "A"), ("102", "B"), ("103", None)
    ]
    assert ledger.one("SELECT gate_verdict FROM pipeline_runs") == ("PASS",)


def test_register_refuses_short_commit_and_unknown_sample(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    (tmp_path / "o").mkdir()
    with pytest.raises(ingest.InputError, match="40-character"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit="abc1234")
    with pytest.raises(ingest.InputError, match="not in the catalogue"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A", "B"])
    assert ledger.query("SELECT * FROM pipeline_runs") == []


def test_register_is_all_or_nothing(tmp_path, core, ledger):
    (tmp_path / "o2").mkdir()
    write(tmp_path / "o2" / "A.txt", "x")
    with pytest.raises(ingest.InputError, match="already recorded"):
        ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], sacct=tmp_path / "sacct.txt")
    assert ledger.query("SELECT prun_id FROM pipeline_runs") == [("R1",)]
    assert ledger.one("SELECT COUNT(*) FROM results WHERE prun_id = 'R2'") == (0,)


def test_job_map_overrides_and_checks(tmp_path, core, ledger):
    write(tmp_path / "s2.txt", "JobIDRaw|Elapsed|AllocCPUS|ReqMem\n201|01:00:00|1|1G\n")
    write(tmp_path / "map.csv", "job_id,sample_id\n201,B\n")
    (tmp_path / "o2").mkdir()
    write(tmp_path / "o2" / "B.txt", "x")
    ingest.register_run(
        ledger, "R2", "p", tmp_path / "o2", ["B"], sacct=tmp_path / "s2.txt", job_map=tmp_path / "map.csv"
    )
    assert ledger.one("SELECT sample_id FROM jobs WHERE job_id = '201'") == ("B",)
    write(tmp_path / "bad.csv", "job_id,sample_id\n301,A\n")
    (tmp_path / "o3").mkdir()
    with pytest.raises(ingest.InputError, match="not in this run"):
        ingest.register_run(ledger, "R3", "p", tmp_path / "o3", ["B"], job_map=tmp_path / "bad.csv")


def test_release_needs_pass(tmp_path, core, ledger):
    gate = write(tmp_path / "fail.json", json.dumps({"verdict": "FAIL"}))
    (tmp_path / "o2").mkdir()
    write(tmp_path / "o2" / "A.txt", "x")
    ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], gate=gate)
    with pytest.raises(ingest.InputError, match="refused"):
        ingest.release(ledger, "R2")
    ingest.release(ledger, "R1", today="2026-10-28")
    with pytest.raises(ingest.InputError, match="was released"):
        ingest.release(ledger, "R1")


# billing


def test_bill_hand_checked(tmp_path, core, ledger):
    card = billing.RateCard.load(write(tmp_path / "rates.json", json.dumps(RATES)))
    bill = billing.build_bill(ledger, card)
    # job 101 (A): 4 CPU h x 0.05 + 8 GiB h x 0.01 = 0.28
    # job 102 (B): 1 CPU h x 0.05 + 2 GiB h x 0.01 = 0.07
    # job 103 (shared, reads 1:3): 2 CPU h x 0.05 + 4 GiB h x 0.01 = 0.14 -> A 0.035, B 0.105
    a, b = bill.samples
    assert (a.sample_id, a.exact) == ("A", Fraction("0.315"))
    assert (b.sample_id, b.exact) == ("B", Fraction("0.175"))
    assert bill.projects == {"P1": 49}
    # 31.5 and 17.5 cents: one cent left after the floors; the tie goes to A
    assert (a.cents, b.cents) == (32, 17)
    # rounding each line on its own bills 50 cents for a 49 cent total
    assert billing.naive_cents(bill) == {"P1": 50}
    assert a.cpu_hours == Fraction(4) + Fraction(2, 4)


def test_bill_equal_split_and_states(tmp_path, core, ledger):
    card = billing.RateCard.load(write(tmp_path / "rates.json", json.dumps(dict(RATES, bill_states=["FAILED"]))))
    bill = billing.build_bill(ledger, card, split="equal")
    assert bill.samples == [] and len(bill.skipped) == 3
    card = billing.RateCard.load(write(tmp_path / "rates.json", json.dumps(RATES)))
    bill = billing.build_bill(ledger, card, split="equal")
    assert [s.exact for s in bill.samples] == [Fraction("0.35"), Fraction("0.14")]


def test_bill_reads_split_needs_every_lane(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\nB,P1\n")
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,10\nB,RUN1,1,\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    ingest.add_sequencing(ledger, tmp_path / "l.csv")
    (tmp_path / "o").mkdir()
    write(tmp_path / "s.txt", "JobIDRaw|Elapsed|AllocCPUS|ReqMem\n1|01:00:00|1|1G\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A", "B"], sacct=tmp_path / "s.txt")
    card = billing.RateCard.load(write(tmp_path / "rates.json", json.dumps(RATES)))
    with pytest.raises(ValueError, match="no read count"):
        billing.build_bill(ledger, card)


@pytest.mark.parametrize(
    "card,match",
    [
        ({"rates": {"cpu_hour": "1"}}, "currency"),
        ({"currency": "CAD", "rates": {"cpu_hour": 0.05}}, "string"),
        ({"currency": "CAD", "rates": {"disk": "1"}}, "unknown resource"),
        ({"currency": "CAD", "rates": {"cpu_hour": "-1"}}, "negative"),
        ({"currency": "CAD", "rates": {}}, "no rates"),
    ],
)
def test_rate_card_checks(tmp_path, card, match):
    with pytest.raises(ValueError, match=match):
        billing.RateCard.load(write(tmp_path / "r.json", json.dumps(card)))


def test_split_cents_known_answers():
    assert billing.split_cents(100, [("a", Fraction(1)), ("b", Fraction(1)), ("c", Fraction(1))]) == {
        "a": 34, "b": 33, "c": 33
    }
    assert billing.split_cents(0, [("a", Fraction(1))]) == {"a": 0}
    assert billing.split_cents(5, [("a", Fraction(0)), ("b", Fraction(0))]) == {"a": 3, "b": 2}
    assert billing.split_cents(7, []) == {}


def test_split_cents_always_adds_up():
    rng = random.Random(20261028)
    for _ in range(5000):
        n = rng.randint(1, 40)
        shares = [(f"s{i:02d}", Fraction(rng.randint(0, 10**6), rng.randint(1, 10**4))) for i in range(n)]
        total = rng.randint(0, 10**7)
        out = billing.split_cents(total, shares)
        assert sum(out.values()) == total
        weight = sum(s for _, s in shares)
        for k, s in shares:
            quota = total * s / weight if weight else Fraction(total, n)
            assert abs(out[k] - quota) < 1


@pytest.mark.parametrize(
    "value,expected", [(Fraction(1, 2), 1), (Fraction(3, 2), 2), (Fraction(49, 100), 0), (Fraction(5), 5)]
)
def test_round_half_up(value, expected):
    assert billing.round_half_up(value) == expected


def test_money_and_decimal_text():
    assert billing.money(123456, "CAD") == "CAD 1,234.56"
    assert billing.money(5, "CAD") == "CAD 0.05"
    assert billing.decimal_text(Fraction(1, 3), 4) == "0.3333"
    assert billing.decimal_text(Fraction(2, 3), 2) == "0.67"


# audit


def codes(findings):
    return sorted((f.code, f.subject) for f in findings)


def test_clean_core_has_no_errors(core, ledger):
    ingest.release(ledger, "R1", today="2026-10-28")
    assert codes(audit.audit(ledger, check_files=True)) == []


def test_audit_finds_planted_defects(tmp_path, core, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nC,P2\nD,P2\nE,P2\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    out = tmp_path / "r2"
    write(out / "C.counts.tsv", "gene\tcount\ng1\t5\n")
    write(out / "D.counts.tsv", "gene\tcount\ng1\t5\n")
    ingest.register_run(ledger, "R2", "p", out, ["C", "D", "E"], sacct=None)
    ingest.release(ledger, "R2", force=True, today="2026-10-28")
    (tmp_path / "results" / "fastqc" / "A_fastqc.html").write_text("edited\n")
    (tmp_path / "results" / "fastqc" / "B_fastqc.html").unlink()
    (tmp_path / "gate.json").write_text(json.dumps({"verdict": "FAIL"}))
    found = codes(audit.audit(ledger, check_files=True))
    assert found == sorted([
        ("CHANGED", "fastqc/A_fastqc.html"),
        ("GATE_MOVED", str((tmp_path / "gate.json").resolve())),
        ("MISSING", "fastqc/B_fastqc.html"),
        ("NO_COMMIT", "R2"),
        ("NO_GATE", "R2"),
        ("NO_RESULTS", "E"),
        ("NO_SEQUENCING", "C"),
        ("NO_SEQUENCING", "D"),
        ("NO_SEQUENCING", "E"),
        ("SAME_CONTENT", "C,D"),
    ])


def test_audit_gate_not_pass_and_unbillable(tmp_path, core, ledger):
    gate = write(tmp_path / "err.json", json.dumps({"verdict": "ERROR"}))
    (tmp_path / "o2").mkdir()
    write(tmp_path / "o2" / "A.txt", "x")
    ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], commit=COMMIT, gate=gate)
    ingest.release(ledger, "R2", force=True, today="2026-10-28")
    ledger.execute("DELETE FROM prun_samples WHERE prun_id = 'R1'")
    ledger.commit()
    found = codes(audit.audit(ledger))
    assert ("GATE_NOT_PASS", "R2") in found
    assert ("UNBILLABLE", "R1") in found


def test_same_content_ignores_empty_files_and_other_outputs(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\nB,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    out = tmp_path / "o"
    write(out / "A.empty.txt", "")
    write(out / "B.empty.txt", "")
    write(out / "A.x.txt", "same")
    write(out / "B.y.txt", "same")
    ingest.register_run(ledger, "R1", "p", out, ["A", "B"], commit=COMMIT)
    assert [f.code for f in audit.audit(ledger)] == ["NO_SEQUENCING", "NO_SEQUENCING"]


# command line


def test_cli_end_to_end(tmp_path, capsys):
    dbp = str(tmp_path / "c.sqlite")
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\nB,P1\n")
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,1000000\nB,RUN1,1,3000000\n")
    write(tmp_path / "sheet.csv", "sample,fastq_1\nA,a.fq.gz\nB,b.fq.gz\n")
    write(tmp_path / "res" / "A.txt", "a")
    write(tmp_path / "res" / "B.txt", "b")
    write(tmp_path / "sacct.txt", SACCT)
    write(tmp_path / "trace.txt", TRACE)
    write(tmp_path / "rates.json", json.dumps(RATES))
    gate = write(tmp_path / "gate.json", json.dumps({"verdict": "PASS"}))
    assert main(["init", dbp]) == 0
    assert main(["intake", dbp, str(tmp_path / "i.csv")]) == 0
    assert main(["sequencing", dbp, str(tmp_path / "l.csv")]) == 0
    assert main([
        "register", dbp, "R1", "--pipeline", "nf-core/demo", "--outdir", str(tmp_path / "res"),
        "--samplesheet", str(tmp_path / "sheet.csv"), "--commit", COMMIT, "--sacct", str(tmp_path / "sacct.txt"),
        "--trace", str(tmp_path / "trace.txt"), "--gate", str(gate),
    ]) == 0
    assert main(["release", dbp, "R1"]) == 0
    assert main(["audit", dbp, "--files", "--json", str(tmp_path / "a.json")]) == 0
    assert json.loads((tmp_path / "a.json").read_text()) == []
    assert main(["bill", dbp, "--rates", str(tmp_path / "rates.json"), "--outdir", str(tmp_path / "bill")]) == 0
    text = capsys.readouterr().out
    assert "P1: CAD 0.49 (sample lines add up)" in text
    rows = (tmp_path / "bill" / "samples.csv").read_text().splitlines()
    assert rows[1] == "P1,A,4.5000,9.0000,0.0000,0.315000,0.32,CAD"
    assert rows[2] == "P1,B,2.5000,5.0000,0.0000,0.175000,0.17,CAD"


def test_cli_errors_exit_2(tmp_path, capsys):
    assert main(["intake", str(tmp_path / "none.sqlite"), "x.csv"]) == 2
    assert "run coreledger init first" in capsys.readouterr().err
    assert not (tmp_path / "none.sqlite").exists()
