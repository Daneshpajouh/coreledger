"""Known-answer validation for coreledger's cost recovery and audit.

Part 1, cost recovery. Synthetic projects with random jobs are billed by coreledger. Each
project's bill is checked against an exact total computed separately with Decimal at 50
digits, and the sample lines are checked to add up to the bill. The baseline is the usual
spreadsheet: each sample's share rounded to cents on its own, then summed.

Part 2, audit. Synthetic pipeline runs are registered through the public API with real
files on disk, and one known defect is planted in some of them. The audit's findings are
compared with the planted list. The baseline is a SHA-256 manifest of each run's result
files, written when the run is registered and checked again after the defects are planted,
as ``sha256sum -c`` would do; that is what many cores keep. A planted defect counts as found
by the manifest when the check fails for that run.

Everything is synthetic and seeded; no real facility data is used. Run:

    python validation/known_answer.py --out validation/results.json
"""

import argparse
import json
import random
import shutil
import sys
import tempfile
from decimal import ROUND_HALF_UP, Decimal, getcontext
from fractions import Fraction
from pathlib import Path

from coreledger import audit, billing, db, ingest

getcontext().prec = 50
COMMIT = "0123456789abcdef0123456789abcdef01234567"
RATES = {"cpu_hour": "0.0125", "mem_gib_hour": "0.0021", "gpu_hour": "0.85"}


def decimal_cost(elapsed: int, cpus: int, mem: int, gpus: int) -> Decimal:
    hours = Decimal(elapsed) / Decimal(3600)
    return (
        hours * cpus * Decimal(RATES["cpu_hour"])
        + hours * (Decimal(mem) / Decimal(1024**3)) * Decimal(RATES["mem_gib_hour"])
        + hours * gpus * Decimal(RATES["gpu_hour"])
    )


def part1(n_projects: int, seed: int) -> dict:
    rng = random.Random(seed)
    card = billing.RateCard("CAD", {k: Fraction(v) for k, v in RATES.items()})
    ledger = db.connect(":memory:")
    db.init(ledger)
    ledger.execute("INSERT INTO seq_runs (run_id) VALUES (?)", ("RUN",))
    lines_off = naive_off = 0
    naive_abs = []
    decimal_mismatch = 0
    for p in range(n_projects):
        project = f"P{p:05d}"
        n = rng.randint(2, 96)
        samples = [f"{project}S{i:03d}" for i in range(n)]
        ledger.execute("INSERT INTO projects (project_id) VALUES (?)", (project,))
        ledger.executemany(
            "INSERT INTO samples (sample_id, project_id) VALUES (?, ?)", [(s, project) for s in samples]
        )
        reads = {s: rng.randint(5 * 10**6, 80 * 10**6) for s in samples}
        ledger.executemany(
            "INSERT INTO sample_runs (sample_id, run_id, lane, reads) VALUES (?, ?, ?, ?)",
            [(s, "RUN", "1", reads[s]) for s in samples],
        )
        prun = f"{project}-R"
        ledger.execute(
            "INSERT INTO pipeline_runs (prun_id, pipeline, outdir, registered) VALUES (?, ?, ?, ?)",
            (prun, "synthetic", "/nonexistent", "2026-10-28"),
        )
        ledger.executemany(
            "INSERT INTO prun_samples (prun_id, sample_id, reads) VALUES (?, ?, ?)",
            [(prun, s, reads[s]) for s in samples],
        )
        jobs = []
        for s in samples:
            for _ in range(rng.randint(3, 15)):
                jobs.append((s, rng.randint(30, 86400), rng.choice([1, 2, 4, 8, 16, 32]),
                             rng.randint(1, 256) * 1024**3, rng.choice([0] * 9 + [1])))
        for _ in range(rng.randint(1, 5)):
            jobs.append((None, rng.randint(30, 7200), rng.choice([1, 2, 4]), rng.randint(1, 32) * 1024**3, 0))
        ledger.executemany(
            "INSERT INTO jobs (job_id, prun_id, state, elapsed_s, alloc_cpus, req_mem_bytes, gpus, sample_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(f"{project}J{j}", prun, "COMPLETED", e, c, m, g, s) for j, (s, e, c, m, g) in enumerate(jobs)],
        )
        ledger.commit()
        bill = billing.build_bill(ledger, card, prun_ids=[prun])
        cents = bill.projects[project]
        exact = sum((decimal_cost(e, c, m, g) for _, e, c, m, g in jobs), Decimal(0))
        if int((exact * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP)) != cents:
            decimal_mismatch += 1
        if sum(sl.cents for sl in bill.samples) != cents:
            lines_off += 1
        naive = billing.naive_cents(bill)[project]
        if naive != cents:
            naive_off += 1
        naive_abs.append(abs(naive - cents))
    ledger.close()
    return {
        "projects": n_projects,
        "seed": seed,
        "rates": RATES,
        "bill_vs_decimal_mismatches": decimal_mismatch,
        "coreledger_projects_lines_not_adding_up": lines_off,
        "naive_projects_lines_not_adding_up": naive_off,
        "naive_max_abs_cents": max(naive_abs),
        "naive_mean_abs_cents": round(sum(naive_abs) / len(naive_abs), 3),
    }


DEFECTS = ["NO_RESULTS", "SAME_CONTENT", "NO_SEQUENCING", "NO_COMMIT", "NO_GATE",
           "GATE_NOT_PASS", "GATE_MOVED", "MISSING", "CHANGED", "UNBILLABLE"]


def manifest(outdir: Path) -> dict:
    """Relative path to SHA-256 for every file under a run's results folder."""
    return {p.relative_to(outdir).as_posix(): ingest.sha256_file(p) for p in sorted(outdir.rglob("*")) if p.is_file()}


def manifest_fails(outdir: Path, kept: dict) -> bool:
    """True when ``sha256sum -c`` on the kept manifest would report a missing or changed file."""
    for rel, digest in kept.items():
        p = outdir / rel
        if not p.is_file() or ingest.sha256_file(p) != digest:
            return True
    return False


def part2(n_runs: int, seed: int, work: Path) -> dict:
    rng = random.Random(seed)
    ledger = db.connect(str(work / "core.sqlite"))
    db.init(ledger)
    expected = set()
    planted = {d: 0 for d in DEFECTS}
    after = []
    manifests = {}
    for r in range(n_runs):
        prun = f"R{r:04d}"
        n = rng.randint(2, 12)
        samples = [f"{prun}S{i:02d}" for i in range(n)]
        defect = rng.choice(DEFECTS) if rng.random() < 0.5 else None
        sheet = work / f"{prun}_intake.csv"
        sheet.write_text("sample_id,project_id\n" + "".join(f"{s},P{r % 7}\n" for s in samples))
        ingest.intake(ledger, sheet)
        lanes = work / f"{prun}_lanes.csv"
        with_lanes = samples[1:] if defect == "NO_SEQUENCING" else samples
        lanes.write_text("sample_id,run_id,lane,reads\n" + "".join(f"{s},SEQ{r},1,{1000 + i}\n"
                                                                     for i, s in enumerate(with_lanes)))
        ingest.add_sequencing(ledger, lanes)
        out = work / prun
        for i, s in enumerate(samples):
            if defect == "NO_RESULTS" and i == 0:
                continue
            (out / "align" / s).mkdir(parents=True)
            (out / "align" / s / f"{s}.bam").write_bytes(f"bam {s} {rng.random()}".encode())
            (out / "counts").mkdir(exist_ok=True)
            (out / "counts" / f"{s}.counts.tsv").write_text(f"gene\tcount\ng1\t{rng.randint(0, 10**6)}\n")
        if defect == "SAME_CONTENT":
            a, b = samples[0], samples[1]
            shutil.copyfile(out / "counts" / f"{a}.counts.tsv", out / "counts" / f"{b}.counts.tsv")
        (out / "multiqc").mkdir(parents=True)
        (out / "multiqc" / "multiqc_report.html").write_text(f"report {prun}\n")
        gate = work / f"{prun}_gate.json"
        gate.write_text(json.dumps({"verdict": "FAIL" if defect == "GATE_NOT_PASS" else "PASS"}))
        sacct = work / f"{prun}_sacct.txt"
        sacct.write_text("JobIDRaw|Elapsed|AllocCPUS|ReqMem\n" + f"{prun}J1|01:00:00|2|4G\n")
        ingest.register_run(
            ledger, prun, "synthetic", out, samples,
            commit=None if defect == "NO_COMMIT" else COMMIT,
            gate=None if defect == "NO_GATE" else gate,
            sacct=sacct, sacct_run_only=True, today="2026-10-28",
        )
        ingest.release(ledger, prun, force=defect in ("NO_GATE", "GATE_NOT_PASS"), today="2026-10-28")
        manifests[prun] = (out, manifest(out))
        if defect is None:
            continue
        planted[defect] += 1
        s0 = samples[0]
        if defect in ("NO_RESULTS", "NO_SEQUENCING"):
            expected.add((defect, prun, s0))
        elif defect == "SAME_CONTENT":
            expected.add((defect, prun, ",".join(sorted(samples[:2]))))
        elif defect in ("NO_COMMIT", "NO_GATE", "GATE_NOT_PASS", "UNBILLABLE"):
            expected.add((defect, prun, prun))
        elif defect == "GATE_MOVED":
            expected.add((defect, prun, str(gate.resolve())))
        elif defect in ("MISSING", "CHANGED"):
            expected.add((defect, prun, f"align/{s0}/{s0}.bam"))
        after.append((defect, prun, out, gate, s0))

    for defect, prun, out, gate, s0 in after:
        target = out / "align" / s0 / f"{s0}.bam"
        if defect == "GATE_MOVED":
            gate.write_text(json.dumps({"verdict": "FAIL"}))
        elif defect == "MISSING":
            moved = work / "moved_away"
            moved.mkdir(exist_ok=True)
            target.rename(moved / f"{prun}_{s0}.bam")
        elif defect == "CHANGED":
            with open(target, "ab") as fh:
                fh.write(b"x")
        elif defect == "UNBILLABLE":
            ledger.execute("DELETE FROM prun_samples WHERE prun_id = ?", (prun,))
            ledger.commit()
            # a run with no samples has no NO_RESULTS or NO_SEQUENCING lines to expect

    found = {(f.code, f.prun_id, f.subject) for f in audit.audit(ledger, check_files=True)}
    ledger.close()
    manifest_runs = {prun for prun, (out, kept) in manifests.items() if manifest_fails(out, kept)}
    defect_runs = {e[1] for e in expected}
    per = {}
    for d in DEFECTS:
        exp = {e for e in expected if e[0] == d}
        hit = exp & found
        per[d] = {
            "planted": planted[d],
            "coreledger_found": len(hit),
            "manifest_found": sum(e[1] in manifest_runs for e in exp),
        }
    false_flags = sorted(found - expected)
    return {
        "runs": n_runs,
        "seed": seed,
        "runs_with_a_defect": sum(planted.values()),
        "per_defect": per,
        "coreledger_found": len(expected & found),
        "manifest_found": sum(v["manifest_found"] for v in per.values()),
        "planted": len(expected),
        "false_flags": len(false_flags),
        "manifest_false_flags": len(manifest_runs - defect_runs),
        "false_flag_examples": [list(x) for x in false_flags[:5]],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--projects", type=int, default=2000)
    ap.add_argument("--runs", type=int, default=300)
    ap.add_argument("--seed", type=int, default=20261028)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workdir", help="folder for part 2's files (default: a new temporary folder)")
    args = ap.parse_args()
    work = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix="coreledger-ka-"))
    work.mkdir(parents=True, exist_ok=True)
    result = {"part1_cost_recovery": part1(args.projects, args.seed), "part2_audit": part2(args.runs, args.seed, work)}
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    json.dump(result, sys.stdout, indent=2)
    print()
    p1, p2 = result["part1_cost_recovery"], result["part2_audit"]
    ok = (
        p1["bill_vs_decimal_mismatches"] == 0
        and p1["coreledger_projects_lines_not_adding_up"] == 0
        and p2["coreledger_found"] == p2["planted"]
        and p2["false_flags"] == 0
    )
    print("known answers reproduced" if ok else "KNOWN ANSWERS NOT REPRODUCED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
