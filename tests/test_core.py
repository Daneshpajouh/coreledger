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

FINISHED = "".join(line + "\n" for line in SACCT.splitlines() if "RUNNING" not in line)

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
        slurm.read_sacct(write(tmp_path / "s.txt", "JobID|State|Elapsed|AllocCPUS|ReqMem\n1|COMPLETED|01:00|1\n"))


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
    assert "1 sacct job(s) not in the trace were left out (--sacct-run-only keeps them, " \
        "shared across the run's samples)" in core.notes
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
        ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], sacct=write(tmp_path / "done.txt", FINISHED),
                            sacct_run_only=True)
    assert ledger.query("SELECT prun_id FROM pipeline_runs") == [("R1",)]
    assert ledger.one("SELECT COUNT(*) FROM results WHERE prun_id = 'R2'") == (0,)


def test_job_map_overrides_and_checks(tmp_path, core, ledger):
    write(tmp_path / "s2.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n201|201|COMPLETED|01:00:00|1|1G\n")
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
    write(tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n1|1|COMPLETED|01:00:00|1|1G\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A", "B"], sacct=tmp_path / "s.txt", sacct_run_only=True)
    card = billing.RateCard.load(write(tmp_path / "rates.json", json.dumps(RATES)))
    bill = billing.build_bill(ledger, card)
    assert bill.samples == [] and [j for j, _ in bill.unallocated] == ["1"]
    assert "a lane without a read count" in bill.notes[0]


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


# edge cases: array lines, requeued attempts, linked files, contrast files, read snapshots


def test_sacct_skips_array_range_lines_and_bills_requeued(tmp_path):
    text = (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|NNodes|ReqMem\n"
        "300_[3-10]|300|CANCELLED|00:00:00|0|1|4G\n"
        "300_1|301|COMPLETED|01:00:00|2|1|4G\n"
        "302|302|REQUEUED|00:30:00|2|1|4G\n"
    )
    jobs, notes = slurm.read_sacct(write(tmp_path / "s.txt", text))
    assert [(j.job_id, j.state) for j in jobs] == [("301", "COMPLETED"), ("302", "REQUEUED")]
    assert notes == ["job 300_[3-10] skipped: an array line that covers a range of tasks"]


def test_sacct_per_node_memory_needs_nnodes(tmp_path):
    with pytest.raises(slurm.FormatError, match="NNodes"):
        slurm.read_sacct(
            write(
                tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n1|1|COMPLETED|01:00:00|4|16000Mn\n"
            )
        )


def test_reqmem_fraction_is_exact():
    assert slurm.parse_reqmem("0.1G", 1, 1) == round(Fraction(1, 10) * 1024**3)


def test_register_enters_linked_folders(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    store = tmp_path / "work"
    write(store / "A.bam", "bam A")
    write(store / "dir" / "A.bai", "bai A")
    out = tmp_path / "out"
    (out / "align").mkdir(parents=True)
    (out / "align" / "A.bam").symlink_to(store / "A.bam")
    (out / "linked").symlink_to(store / "dir")
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    assert [r[0] for r in ledger.query("SELECT path FROM results ORDER BY path")] == ["align/A.bam", "linked/A.bai"]
    assert reg.notes == ["1 linked folder(s) entered: linked"]
    assert "NO_RESULTS" not in [f.code for f in audit.audit(ledger)]


def test_contrast_file_belongs_to_neither_sample():
    samples = ["KO_REP1", "WT_REP1", "S1", "S1_T2"]
    assert ingest.sample_for_path("de/KO_REP1_vs_WT_REP1.tsv", samples) is None
    assert ingest.sample_for_path("align/S1_T2.bam", samples) == "S1_T2"
    assert ingest.sample_for_path("align/KO_REP1.WT_REP1x.bam", samples) == "KO_REP1"


def test_same_content_pattern_leaves_folder_names_alone(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nqc,P1\nwt,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    out = tmp_path / "o"
    write(out / "fastqc" / "qc_1.html", "same")
    write(out / "fastqc" / "wt_1.html", "same")
    ingest.register_run(ledger, "R1", "p", out, ["qc", "wt"], commit=COMMIT)
    assert audit.name_pattern("fastqc/qc_1.html", "qc") == "fastqc/{sample}_1.html"
    assert ("SAME_CONTENT", "qc,wt") in codes(audit.audit(ledger))


def test_register_notes_jobs_without_trace_or_map(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "A.txt", "a")
    sacct = write(tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n1|1|COMPLETED|01:00:00|4|8G\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct,
                              sacct_run_only=True)
    assert reg.notes == [
        "no --trace or --job-map: every Slurm job is shared across the run's samples",
        "no read total for A (no lanes, or a lane without reads); "
        "this run's shared jobs stay unallocated in a bill split by reads",
    ]


def test_read_split_uses_reads_recorded_at_register(tmp_path, core, ledger):
    card = billing.RateCard(currency="CAD", rates={"cpu_hour": Fraction(1)})
    before = {sl.sample_id: sl.exact for sl in billing.build_bill(ledger, card).samples}
    write(tmp_path / "late.csv", "sample_id,run_id,lane,reads\nA,RUN2,1,9000000\n")
    ingest.add_sequencing(ledger, tmp_path / "late.csv")
    after = {sl.sample_id: sl.exact for sl in billing.build_bill(ledger, card).samples}
    assert after == before


def test_tagged_job_of_a_sample_not_in_the_run_is_unallocated(core, ledger):
    ledger.execute("DELETE FROM prun_samples WHERE prun_id = 'R1'")
    ledger.commit()
    card = billing.RateCard(currency="CAD", rates={"cpu_hour": Fraction(1)})
    bill = billing.build_bill(ledger, card, split="equal")
    assert bill.samples == []
    assert sorted(j for j, _ in bill.unallocated) == ["101", "102", "103"]


def test_cli_exclude_replaces_the_default_and_bill_exits_1_on_unallocated(tmp_path, capsys):
    dbp = str(tmp_path / "c.sqlite")
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    write(tmp_path / "res" / "pipeline_info" / "t.txt", "kept")
    write(tmp_path / "res" / "skip" / "A.txt", "a")
    write(tmp_path / "sacct.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n1|1|COMPLETED|01:00:00|4|8G\n")
    write(tmp_path / "rates.json", json.dumps(RATES))
    assert main(["init", dbp]) == 0
    assert main(["intake", dbp, str(tmp_path / "i.csv")]) == 0
    assert main([
        "register", dbp, "R1", "--outdir", str(tmp_path / "res"), "--pipeline", "p", "--sample", "A",
        "--commit", COMMIT,
        "--sacct", str(tmp_path / "sacct.txt"), "--sacct-run-only", "--exclude", "skip",
    ]) == 0
    lg = db.connect(dbp)
    assert [r[0] for r in lg.query("SELECT path FROM results")] == ["pipeline_info/t.txt"]
    lg.execute("DELETE FROM prun_samples")
    lg.commit()
    lg.close()
    assert main(["bill", dbp, "--rates", str(tmp_path / "rates.json"), "--outdir", str(tmp_path / "b")]) == 1
    assert "unallocated: job 1" in capsys.readouterr().out


# review round 2: Illumina names, vs contrasts, broken links, unfinished jobs, odd gate files, blank reads


def test_r2_illumina_name_and_numeric_sample_stay_with_their_sample():
    assert ingest.sample_for_path("fastq/S1_S1_L001_R1_001.fastq.gz", ["S1", "S10"]) == "S1"
    assert ingest.sample_for_path("fastqc/KO_REP1_1_fastqc.html", ["KO_REP1", "1"]) == "KO_REP1"
    assert ingest.sample_for_path("de/KO_REP1-vs-WT_REP1.tsv", ["KO_REP1", "WT_REP1"]) is None
    assert ingest.sample_for_path("de/KO_REP1.vs.WT_REP1.tsv", ["KO_REP1", "WT_REP1"]) is None
    assert ingest.sample_for_path("qc/S1_vs_S1.tsv", ["S1", "S10"]) == "S1"


def test_r2_broken_link_is_refused(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    out = tmp_path / "out"
    out.mkdir()
    (out / "A.bam").symlink_to(tmp_path / "work" / "gone.bam")
    with pytest.raises(ingest.InputError, match="does not point at a file"):
        ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    assert ledger.one("SELECT COUNT(*) FROM pipeline_runs")[0] == 0


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_r2_unfinished_jobs_of_the_run_are_refused(tmp_path, ledger, state):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "A.txt", "a")
    head = "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
    sacct = write(tmp_path / "s.txt", f"{head}300_[3-10]|300|{state}|00:00:00|0|4G\n")
    with pytest.raises(ingest.InputError, match="have not finished"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, sacct_run_only=True)
    trace = write(tmp_path / "t.txt", "native_id\tname\n300_1\tP (A)\n")
    with pytest.raises(ingest.InputError, match="have not finished"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)
    plain = write(tmp_path / "p.txt", f"{head}7|7|{state}|00:00:00|2|4G\n")
    with pytest.raises(ingest.InputError, match=r"not finished \(7\)"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=plain, sacct_run_only=True)


def test_r2_cancelled_range_and_other_users_running_job_are_not_refused(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "A.txt", "a")
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "300_[3-10]|300|CANCELLED by 1000|00:00:00|0|4G\n"
        "300_1|301|COMPLETED|01:00:00|2|4G\n"
        "999|999|RUNNING|00:10:00|8|4G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n300_1\tP (A)\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)
    assert reg.jobs == 1 and reg.jobs_per_sample == 1


def test_r2_gate_report_that_is_not_an_object(tmp_path, core, ledger):
    gate = tmp_path / "gate.json"
    for text in ("[]", "null"):
        gate.write_text(text)
        assert ("GATE_MOVED", str(gate.resolve())) in codes(audit.audit(ledger))
    write(tmp_path / "i2.csv", "sample_id,project_id\nC,P2\n")
    ingest.intake(ledger, tmp_path / "i2.csv")
    write(tmp_path / "o2" / "C.txt", "c")
    with pytest.raises(ingest.InputError, match="not a JSON object"):
        ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["C"], commit=COMMIT, gate=gate)


def test_r2_folders_naming_two_samples_are_noted(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nWT_REP1,P1\nKO_REP1,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "WT_REP1" / "KO_REP1" / "quant.sf", "q")
    write(tmp_path / "o" / "de" / "KO_REP1_vs_WT_REP1.tsv", "d")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["WT_REP1", "KO_REP1"], commit=COMMIT)
    note = "1 result(s) in folders that name two samples were recorded for the run: WT_REP1/KO_REP1/quant.sf"
    assert note in reg.notes


def test_r2_contrast_job_tag_is_shared(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nWT_REP1,P1\nKO_REP1,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "x.txt", "x")
    sacct = write(tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n7|7|COMPLETED|01:00:00|1|1G\n")
    trace = write(tmp_path / "t.txt", "native_id\tname\n7\tDESEQ2 (KO_REP1_vs_WT_REP1)\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["WT_REP1", "KO_REP1"], commit=COMMIT,
                        sacct=sacct, trace=trace)
    assert ledger.one("SELECT sample_id FROM jobs WHERE job_id = '7'") == (None,)


def test_r2_blank_reads_unallocate_one_run_and_bill_the_rest(tmp_path, capsys):
    dbp = str(tmp_path / "c.sqlite")
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\nB,P1\nC,P2\n")
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,10\nB,RUN1,1,\nC,RUN1,2,5\n")
    write(tmp_path / "o1" / "x.txt", "x")
    write(tmp_path / "o2" / "y.txt", "y")
    write(tmp_path / "s1.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n1|1|COMPLETED|01:00:00|1|1G\n")
    write(tmp_path / "s2.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n2|2|COMPLETED|01:00:00|1|1G\n")
    write(tmp_path / "rates.json", json.dumps(RATES))
    assert main(["init", dbp]) == 0
    assert main(["intake", dbp, str(tmp_path / "i.csv")]) == 0
    assert main(["sequencing", dbp, str(tmp_path / "l.csv")]) == 0
    for run, out, sacct, samples in (("R1", "o1", "s1.txt", ["A", "B"]), ("R2", "o2", "s2.txt", ["C"])):
        args = ["register", dbp, run, "--outdir", str(tmp_path / out), "--pipeline", "p", "--commit", COMMIT,
                "--sacct", str(tmp_path / sacct), "--sacct-run-only"]
        for s in samples:
            args += ["--sample", s]
        assert main(args) == 0
    assert "no read total for B" in capsys.readouterr().out
    assert main(["bill", dbp, "--rates", str(tmp_path / "rates.json"), "--outdir", str(tmp_path / "b")]) == 1
    printed = capsys.readouterr().out
    assert "unallocated: job 1" in printed and "note: R1: shared jobs unallocated" in printed
    assert "P2,C," in (tmp_path / "b" / "samples.csv").read_text()



NOT_ENTERED = "linked folder(s) not entered (a loop, a folder inside the results, a folder that holds the results " \
    "or another linked folder, or a link out of a linked folder): "


def _r3_one_sample(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o" / "A.txt", "a")


def test_r3_resized_job_bills_both_records(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "500|500|RESIZING|01:00:00|8|8G\n"
        "500|500|COMPLETED|02:00:00|4|8G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n500\tP (A)\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)
    assert reg.jobs == 2
    assert ledger.query("SELECT job_id, state, elapsed_s, alloc_cpus, sample_id FROM jobs ORDER BY job_id") == [
        ("500", "RESIZING", 3600, 8, "A"), ("500#2", "COMPLETED", 7200, 4, "A"),
    ]


@pytest.mark.parametrize("state", ["COMPLETING", "CONFIGURING", "STOPPED"])
def test_r3_jobs_that_can_still_add_time_are_held(tmp_path, ledger, state):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", f"JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n9|9|{state}|00:10:00|2|4G\n")
    with pytest.raises(ingest.InputError, match=r"not finished \(9\)"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, sacct_run_only=True)


@pytest.mark.parametrize("state", ["FAILED", "TIMEOUT", "COMPLETED", "CANCELLED"])
def test_r3_finished_array_range_does_not_block(tmp_path, ledger, state):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        f"300_[3-10]|300|{state}|00:00:00|0|4G\n"
        "300_1|301|COMPLETED|00:10:00|2|4G\n"
    ))
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, sacct_run_only=True)
    assert reg.jobs == 1


def test_r4_sacct_without_a_state_is_refused(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    no_column = write(tmp_path / "s.txt", "JobID|JobIDRaw|Elapsed|AllocCPUS|ReqMem\n101|101|00:10:00|4|8G\n")
    with pytest.raises(slurm.FormatError, match="needs a State column"):
        ingest.register_run(
            ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=no_column, sacct_run_only=True
        )
    blank = write(tmp_path / "b.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n300_[3-10]|300||00:00:00|0|4G\n")
    with pytest.raises(slurm.FormatError, match="line 2: State is empty"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=blank, sacct_run_only=True)
    assert ledger.one("SELECT COUNT(*) FROM jobs")[0] == 0


def test_r3_open_range_blocks_when_the_trace_has_raw_task_ids(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "300_1|301|COMPLETED|00:10:00|2|4G\n"
        "300_[3-10]|300|RUNNING|00:00:00|0|4G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n301\tP (A)\n")
    with pytest.raises(ingest.InputError, match=r"not finished \(300_\[3-10\]\)"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)


def test_r3_linked_sample_folders_are_entered_and_loops_end(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "work" / "A"
    write(store / "quant.sf", "quant A")
    out = tmp_path / "o"
    (out / "salmon").mkdir(parents=True)
    (out / "salmon" / "A").symlink_to(store)
    (out / "again").symlink_to(store)
    (out / "salmon" / "loop").symlink_to(out / "salmon")
    (store / "up").symlink_to(store.parent)
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    rows = ledger.query("SELECT path, sample_id, sha256 FROM results ORDER BY path")
    assert [(r[0], r[1]) for r in rows] == [("A.txt", "A"), ("again/quant.sf", None), ("salmon/A/quant.sf", "A")]
    assert rows[2][2] == ingest.sha256_file(store / "quant.sf")
    assert reg.notes == [
        "2 linked folder(s) entered: again, salmon/A",
        "3 " + NOT_ENTERED + "again/up, salmon/loop, salmon/A/up",
    ]


def test_r3_broken_folder_link_is_refused(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    (tmp_path / "o" / "gone").symlink_to(tmp_path / "missing")
    with pytest.raises(ingest.InputError, match="does not point at a file"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT)
    assert ledger.query("SELECT * FROM pipeline_runs") == []


def test_r3_wide_sacct_needs_a_trace_a_job_map_or_run_only(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|JobName|State|Elapsed|AllocCPUS|ReqMem\n"
        "7|7|ours|COMPLETED|01:00:00|2|4G\n"
        "8811299|8811299|someone_elses_job|COMPLETED|10:00:00|32|64G\n"
        "8811300|8811300|someone_elses_job|RUNNING|00:10:00|32|64G\n"
    ))
    with pytest.raises(ingest.InputError, match="would bill every job"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct)
    assert ledger.query("SELECT * FROM pipeline_runs") == []
    jmap = write(tmp_path / "m.csv", "job_id,sample_id\n7,A\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, job_map=jmap)
    assert ledger.query("SELECT job_id, sample_id FROM jobs") == [("7", "A")]
    assert "2 sacct job(s) not in the job map were left out" not in " ".join(reg.notes)
    assert "1 sacct job(s) not in the job map were left out" in " ".join(reg.notes)


def test_r3_run_only_keeps_every_job_and_holds_every_unfinished_one(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "7|7|COMPLETED|01:00:00|2|4G\n"
        "8|8|RUNNING|00:10:00|2|4G\n"
    ))
    jmap = write(tmp_path / "m.csv", "job_id,sample_id\n7,A\n")
    with pytest.raises(ingest.InputError, match=r"not finished \(8\)"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, job_map=jmap,
                            sacct_run_only=True)


def test_r3_cached_task_of_a_resumed_run_is_left_out(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    write(tmp_path / "o1" / "A.txt", "a")
    write(tmp_path / "o2" / "A.txt", "a2")
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "40|40|COMPLETED|01:00:00|2|4G\n"
        "41|41|COMPLETED|00:30:00|2|4G\n"
    ))
    first = write(tmp_path / "t1.txt", "native_id\tname\tstatus\n40\tP (A)\tCOMPLETED\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o1", ["A"], commit=COMMIT, sacct=sacct, trace=first)
    resumed = write(tmp_path / "t2.txt", "native_id\tname\tstatus\n40\tP (A)\tCACHED\n41\tQ (A)\tCOMPLETED\n")
    reg = ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], commit=COMMIT, sacct=sacct, trace=resumed)
    assert ledger.query("SELECT job_id, prun_id FROM jobs ORDER BY job_id") == [("40", "R1"), ("41", "R2")]
    assert "Slurm job 40 is a cached task already recorded for R1; left out" in reg.notes
    write(tmp_path / "o3" / "A.txt", "a3")
    live = write(tmp_path / "t3.txt", "native_id\tname\tstatus\n41\tQ (A)\tCOMPLETED\n")
    with pytest.raises(ingest.InputError, match="already recorded for pipeline run R2"):
        ingest.register_run(ledger, "R3", "p", tmp_path / "o3", ["A"], commit=COMMIT, sacct=sacct, trace=live)


def test_r3_gate_report_that_is_not_json(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    gate = write(tmp_path / "g.json", "{not json")
    with pytest.raises(ingest.InputError, match="not valid JSON"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, gate=gate)


def test_r3_cli_reports_a_duplicate_lane_without_a_traceback(tmp_path, capsys):
    dbp = str(tmp_path / "c.sqlite")
    write(tmp_path / "i.csv", "sample_id,project_id\nA,P1\n")
    write(tmp_path / "l.csv", "sample_id,run_id,lane,reads\nA,RUN1,1,10\nA,RUN1,1,12\n")
    assert main(["init", dbp]) == 0
    assert main(["intake", dbp, str(tmp_path / "i.csv")]) == 0
    assert main(["sequencing", dbp, str(tmp_path / "l.csv")]) == 2
    assert "the catalogue refused the change, so nothing was written" in capsys.readouterr().err
    assert db.connect(dbp).query("SELECT * FROM sample_runs") == []


def test_r4_pending_task_listed_by_raw_id_is_held(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "300_1|301|PENDING|00:00:00|0|4G\n"
        "300_[3-10]|300|PENDING|00:00:00|0|4G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n301\tP (A)\n")
    with pytest.raises(ingest.InputError, match=r"2 Slurm job\(s\) .* not finished \(300_1, 300_\[3-10\]\)"):
        ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)
    # Someone else's pending job is still not ours.
    other = write(
        tmp_path / "s2.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n400_1|401|PENDING|00:00:00|0|4G\n"
    )
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=other, trace=trace)
    assert reg.jobs == 0


def test_r4_link_to_a_folder_holding_ours_is_not_entered(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "work" / "A"
    write(store / "quant.sf", "quant A")
    write(tmp_path / "work" / "B" / "other.sf", "another task's file")
    out = tmp_path / "o"
    (out / "again").symlink_to(store)
    (store / "up").symlink_to(store.parent)
    (out / "top").symlink_to(tmp_path)
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    paths = [r[0] for r in ledger.query("SELECT path FROM results ORDER BY path")]
    assert paths == ["A.txt", "again/quant.sf"]
    assert reg.notes == [
        "1 linked folder(s) entered: again",
        "2 " + NOT_ENTERED + "top, again/up",
    ]


def test_r4_sacct_run_only_keeps_jobs_outside_the_trace(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "101|101|COMPLETED|01:00:00|4|8G\n"
        "103|103|COMPLETED|02:00:00|1|2G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n101\tP (A)\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace,
                              sacct_run_only=True)
    rows = ledger.query("SELECT job_id, sample_id FROM jobs ORDER BY job_id")
    assert [tuple(r) for r in rows] == [("101", "A"), ("103", None)]
    assert "1 sacct job(s) not in the trace or job map were kept, shared across the run's samples " \
        "(--sacct-run-only)" in reg.notes
    # A job of this file still running blocks the run, in the trace or not.
    running = write(
        tmp_path / "s2.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n104|104|RUNNING|00:10:00|1|2G\n"
    )
    with pytest.raises(ingest.InputError, match=r"not finished \(104\)"):
        ingest.register_run(ledger, "R2", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=running, trace=trace,
                            sacct_run_only=True)


@pytest.mark.parametrize("wide", ["a_work", "zz_work"])
def test_r5_link_order_does_not_decide_what_is_entered(tmp_path, ledger, wide):
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "work" / "ab" / "A"
    write(store / "quant.sf", "quant A")
    write(tmp_path / "work" / "cd" / "B" / "other.sf", "another task's file")
    out = tmp_path / "o"
    (out / "m_sample").symlink_to(store)
    (out / wide).symlink_to(tmp_path / "work")
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    paths = [r[0] for r in ledger.query("SELECT path FROM results ORDER BY path")]
    assert paths == ["A.txt", "m_sample/quant.sf"]
    assert reg.notes == ["1 linked folder(s) entered: m_sample", "1 " + NOT_ENTERED + wide]


def test_r5_link_into_the_results_does_not_hide_a_real_folder(tmp_path, ledger):
    write(tmp_path / "i.csv", "sample_id,project_id\nKO,P1\nWT,P1\n")
    ingest.intake(ledger, tmp_path / "i.csv")
    out = tmp_path / "o"
    write(out / "salmon" / "WT" / "quant.sf", "wt")
    write(out / "salmon" / "KO" / "quant.sf", "ko")
    (out / "latest").symlink_to(out / "salmon" / "WT")
    reg = ingest.register_run(ledger, "R1", "p", out, ["WT", "KO"], commit=COMMIT)
    rows = ledger.query("SELECT path, sample_id FROM results ORDER BY path")
    assert [tuple(r) for r in rows] == [("salmon/KO/quant.sf", "KO"), ("salmon/WT/quant.sf", "WT")]
    assert reg.notes == ["1 " + NOT_ENTERED + "latest"]


def test_r5_link_inside_its_own_store_does_not_stop_the_store(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "work" / "A"
    write(store / "quant.sf", "quant A")
    write(store / "sub" / "x.txt", "x")
    (store / "cur").symlink_to(store / "sub")
    out = tmp_path / "o"
    (out / "again").symlink_to(store)
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    paths = [r[0] for r in ledger.query("SELECT path FROM results ORDER BY path")]
    assert paths == ["A.txt", "again/cur/x.txt", "again/quant.sf", "again/sub/x.txt"]
    assert reg.notes == ["2 linked folder(s) entered: again, again/cur"]


def test_r5_jobs_in_the_job_map_count_beside_the_trace(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "101|101|COMPLETED|01:00:00|4|8G\n"
        "102|102|COMPLETED|01:00:00|2|4G\n"
        "103|103|COMPLETED|01:00:00|1|2G\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n101\tP (A)\n")
    job_map = write(tmp_path / "m.csv", "job_id,sample_id\n102,A\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace,
                              job_map=job_map)
    rows = ledger.query("SELECT job_id, sample_id FROM jobs ORDER BY job_id")
    assert [tuple(r) for r in rows] == [("101", "A"), ("102", "A")]
    assert "1 sacct job(s) not in the trace or job map were left out " \
        "(--sacct-run-only keeps them, shared across the run's samples)" in reg.notes


@pytest.mark.parametrize("line, column", [
    ("101|101|COMPLETED|01:00:00||8G", "AllocCPUS"),
    ("101|101|COMPLETED|01:00:00|4|", "ReqMem"),
    ("101|101|COMPLETED|01:00:00| |8G", "AllocCPUS"),
])
def test_r5_blank_cpus_or_memory_is_refused(tmp_path, line, column):
    sacct = write(tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n" + line + "\n")
    with pytest.raises(slurm.FormatError, match=f"line 2: {column} is empty"):
        slurm.read_sacct(sacct)


def test_r5_one_array_task_has_one_key_with_or_without_jobidraw(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    write(tmp_path / "o2" / "A.txt", "a")
    with_raw = write(
        tmp_path / "s1.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n300_1|301|COMPLETED|01:00:00|4|8G\n"
    )
    shown_only = write(tmp_path / "s2.txt", "JobID|State|Elapsed|AllocCPUS|ReqMem\n300_1|COMPLETED|01:00:00|4|8G\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=with_raw, sacct_run_only=True)
    assert [tuple(r) for r in ledger.query("SELECT job_id, prun_id FROM jobs")] == [("300_1", "R1")]
    with pytest.raises(ingest.InputError, match=r"Slurm job 300_1 is already recorded for pipeline run R1"):
        ingest.register_run(ledger, "R2", "p", tmp_path / "o2", ["A"], commit=COMMIT, sacct=shown_only,
                            sacct_run_only=True)


def test_r6_sacct_without_jobid_is_refused(tmp_path):
    # JobIDRaw alone shows 301 for the array task a trace lists as 300_1, so neither the trace nor the key could match.
    sacct = write(tmp_path / "s.txt", "JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n301|COMPLETED|01:00:00|4|8G\n")
    with pytest.raises(slurm.FormatError, match="needs a JobID column"):
        slurm.read_sacct(sacct)


def test_r6_a_staged_link_in_a_linked_work_folder_does_not_stop_a_sample(tmp_path, ledger):
    # Following "work" finds a task's staged link into A's store; only links in the results folder count as targets.
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "ext" / "A"
    write(store / "quant" / "quant.sf", "quant A")
    work = tmp_path / "ext" / "work"
    write(work / "ab" / "log.txt", "other task")
    (work / "ab" / "staged").symlink_to(store / "quant")
    out = tmp_path / "o"
    (out / "A_quant").symlink_to(store)
    (out / "work").symlink_to(work)
    reg = ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    rows = ledger.query("SELECT path, sample_id FROM results ORDER BY path")
    assert [tuple(r) for r in rows] == [("A.txt", "A"), ("A_quant/quant/quant.sf", "A"), ("work/ab/log.txt", None)]
    assert "1 linked folder(s) not entered" in " ".join(reg.notes)


def test_r6_a_link_out_of_a_linked_folder_is_not_entered(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    store = tmp_path / "ext" / "A"
    write(store / "a.txt", "a")
    write(tmp_path / "ext" / "other" / "b.txt", "another task's input")
    (store / "input").symlink_to(tmp_path / "ext" / "other")
    out = tmp_path / "o"
    (out / "A_store").symlink_to(store)
    ingest.register_run(ledger, "R1", "p", out, ["A"], commit=COMMIT)
    paths = [r[0] for r in ledger.query("SELECT path FROM results ORDER BY path")]
    assert paths == ["A.txt", "A_store/a.txt"]


def test_r6_a_blank_size_stops_only_a_job_of_the_run(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", (
        "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
        "101|101|COMPLETED|01:00:00|4|8G\n"
        "909|909|CANCELLED|00:00:00||\n"
    ))
    trace = write(tmp_path / "t.txt", "native_id\tname\n101\tP (A)\n")
    ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=trace)
    assert [r[0] for r in ledger.query("SELECT job_id FROM jobs")] == ["101"]
    ours = write(tmp_path / "t2.txt", "native_id\tname\n909\tP (A)\n")
    with pytest.raises(slurm.FormatError, match=r"line 3: AllocCPUS is empty"):
        ingest.register_run(ledger, "R2", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct, trace=ours)
    with pytest.raises(slurm.FormatError, match=r"line 3: AllocCPUS is empty"):
        ingest.register_run(ledger, "R3", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct,
                            sacct_run_only=True)


def test_r6_a_job_map_job_missing_from_sacct_is_noted(tmp_path, ledger):
    _r3_one_sample(tmp_path, ledger)
    sacct = write(tmp_path / "s.txt", "JobID|JobIDRaw|State|Elapsed|AllocCPUS|ReqMem\n"
                  "101|101|COMPLETED|01:00:00|4|8G\n")
    job_map = write(tmp_path / "m.csv", "job_id,sample_id\n101,A\n102,A\n")
    reg = ingest.register_run(ledger, "R1", "p", tmp_path / "o", ["A"], commit=COMMIT, sacct=sacct,
                              job_map=job_map)
    assert "1 job map job(s) not found in sacct: 102" in reg.notes
