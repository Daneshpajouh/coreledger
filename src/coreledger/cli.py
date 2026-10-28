"""Command line: coreledger init | intake | sequencing | register | release | audit | bill | sop."""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Optional

from . import __version__, audit, billing, db, ingest


def _open(target: str) -> db.Ledger:
    return db.open_existing(target)


def cmd_init(args: argparse.Namespace) -> int:
    ledger = db.connect(args.db)
    db.init(ledger)
    ledger.close()
    print(f"catalogue ready: {args.db}")
    return 0


def cmd_intake(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    n = ingest.intake(ledger, Path(args.sheet))
    print(f"{n} sample(s) added")
    return 0


def cmd_sequencing(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    n = ingest.add_sequencing(ledger, Path(args.sheet))
    print(f"{n} lane(s) added")
    return 0


def cmd_register(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    samples: List[str] = list(args.sample or [])
    if args.samplesheet:
        samples += [s for s in ingest.read_samplesheet(Path(args.samplesheet)) if s not in samples]
    reg = ingest.register_run(
        ledger,
        args.prun_id,
        args.pipeline,
        Path(args.outdir),
        samples,
        version=args.version,
        commit=args.commit,
        profile=args.profile,
        params=Path(args.params) if args.params else None,
        sacct=Path(args.sacct) if args.sacct else None,
        trace=Path(args.trace) if args.trace else None,
        job_map=Path(args.job_map) if args.job_map else None,
        gate=Path(args.gate) if args.gate else None,
        exclude=tuple(args.exclude) if args.exclude is not None else ("pipeline_info",),
    )
    print(
        f"{reg.prun_id}: {reg.results} result file(s), {reg.per_sample} named for a sample; "
        f"{reg.jobs} Slurm job(s), {reg.jobs_per_sample} tagged with a sample"
    )
    for note in reg.notes:
        print(f"note: {note}")
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    ingest.release(ledger, args.prun_id, force=args.force)
    print(f"{args.prun_id} released")
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    findings = audit.audit(ledger, check_files=args.files)
    if args.json:
        Path(args.json).write_text(
            json.dumps(
                [{"code": f.code, "level": f.level, "prun_id": f.prun_id, "subject": f.subject, "message": f.message}
                 for f in findings],
                indent=2,
            )
            + "\n"
        )
    for f in findings:
        print(f"{f.level}\t{f.code}\t{f.prun_id}\t{f.message}")
    errors = sum(f.level == audit.ERROR for f in findings)
    print(f"{len(findings)} finding(s), {errors} error(s)")
    return 1 if errors else 0


def cmd_bill(args: argparse.Namespace) -> int:
    ledger = _open(args.db)
    card = billing.RateCard.load(Path(args.rates))
    bill = billing.build_bill(ledger, card, split=args.split, prun_ids=args.prun or None)
    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "samples.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["project_id", "sample_id", "cpu_hours", "mem_gib_hours", "gpu_hours", "exact", "amount", "currency"]
        )
        for sl in bill.samples:
            w.writerow([
                sl.project_id,
                sl.sample_id,
                billing.decimal_text(sl.cpu_hours, 4),
                billing.decimal_text(sl.mem_gib_hours, 4),
                billing.decimal_text(sl.gpu_hours, 4),
                billing.decimal_text(sl.exact, 6),
                billing.decimal_text(billing.Fraction(sl.cents, 100), 2),
                bill.currency,
            ])
    with open(out / "projects.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["project_id", "samples", "exact", "amount", "currency"])
        for project, cents in bill.projects.items():
            n = sum(sl.project_id == project for sl in bill.samples)
            w.writerow([
                project, n, billing.decimal_text(bill.project_exact[project], 6),
                billing.decimal_text(billing.Fraction(cents, 100), 2), bill.currency,
            ])
    for project, cents in bill.projects.items():
        lines = sum(sl.cents for sl in bill.samples if sl.project_id == project)
        status = "sample lines add up" if lines == cents else "SAMPLE LINES DO NOT ADD UP"
        print(f"{project}: {billing.money(cents, bill.currency)} ({status})")
    for job_id, cost in bill.unallocated:
        print(f"unallocated: job {job_id}, {billing.decimal_text(cost, 6)} {bill.currency} (no samples to charge)")
    for note in bill.skipped:
        print(f"skipped: {note}")
    return 1 if bill.unallocated else 0


def cmd_sop(args: argparse.Namespace) -> int:
    from . import sop

    schema = sop.load_schema(Path(args.schema))
    params = sop.load_params(Path(args.params))
    old = sop.load_schema(Path(args.old_schema)) if args.old_schema else None
    text = sop.render_sop(
        schema, params, args.pipeline, args.version, profile=args.profile, old_schema=old, old_version=args.old_version
    )
    if args.out:
        Path(args.out).write_text(text)
    else:
        sys.stdout.write(text)
    errors = [i for i in sop.check_params(schema, params) if i.level == "error"]
    return 1 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="coreledger", description=__doc__)
    p.add_argument("--version", action="version", version=f"coreledger {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init", help="create a catalogue (SQLite file or postgresql:// URL)")
    s.add_argument("db")
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("intake", help="add samples from a CSV (sample_id, project_id, ...)")
    s.add_argument("db")
    s.add_argument("sheet")
    s.set_defaults(func=cmd_intake)

    s = sub.add_parser("sequencing", help="add sequencing lanes from a CSV (sample_id, run_id, lane, reads, ...)")
    s.add_argument("db")
    s.add_argument("sheet")
    s.set_defaults(func=cmd_sequencing)

    s = sub.add_parser("register", help="record a finished pipeline run")
    s.add_argument("db")
    s.add_argument("prun_id", help="your ID for this pipeline run")
    s.add_argument("--pipeline", required=True)
    s.add_argument("--outdir", required=True)
    s.add_argument("--samplesheet", help="nf-core style samplesheet with a 'sample' column")
    s.add_argument("--sample", action="append", help="a sample of this run (repeat)")
    s.add_argument("--version", dest="version")
    s.add_argument("--commit", help="full git commit of the pipeline")
    s.add_argument("--profile")
    s.add_argument("--params", help="the params file used (its hash is kept)")
    s.add_argument("--sacct", help="sacct --parsable2 output for the run's jobs")
    s.add_argument("--trace", help="Nextflow trace file (maps Slurm jobs to task tags)")
    s.add_argument("--job-map", help="CSV job_id,sample_id for jobs a trace cannot tag")
    s.add_argument("--gate", help="tolgate JSON report (or any JSON with verdict PASS, FAIL or ERROR)")
    s.add_argument(
        "--exclude", action="append", default=None,
        help="folder names to leave out (repeat for more; default pipeline_info)",
    )
    s.set_defaults(func=cmd_register)

    s = sub.add_parser("release", help="mark a pipeline run released (needs a passing gate)")
    s.add_argument("db")
    s.add_argument("prun_id")
    s.add_argument("--force", action="store_true", help="release without a passing gate (the audit will flag it)")
    s.set_defaults(func=cmd_release)

    s = sub.add_parser("audit", help="check provenance; exit 1 on any error")
    s.add_argument("db")
    s.add_argument("--files", action="store_true", help="also rehash every result file")
    s.add_argument("--json", help="write the findings as JSON")
    s.set_defaults(func=cmd_audit)

    s = sub.add_parser("bill", help="cost recovery from the recorded Slurm jobs")
    s.add_argument("db")
    s.add_argument("--rates", required=True, help="rate card JSON")
    s.add_argument("--split", choices=["reads", "equal"], default="reads", help="how shared jobs are split")
    s.add_argument("--prun", action="append", help="only these pipeline runs (repeat)")
    s.add_argument("--outdir", required=True)
    s.set_defaults(func=cmd_bill)

    s = sub.add_parser("sop", help="draft an SOP from a pipeline's parameter schema and your params file")
    s.add_argument("--schema", required=True, help="nextflow_schema.json or a JSON config schema")
    s.add_argument("--params", required=True, help="your params JSON")
    s.add_argument("--pipeline", required=True)
    s.add_argument("--version", dest="version", required=True)
    s.add_argument("--profile")
    s.add_argument("--old-schema", help="schema of the release you run now, to list what changes")
    s.add_argument("--old-version")
    s.add_argument("--out")
    s.set_defaults(func=cmd_sop)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ingest.InputError, ValueError, RuntimeError, OSError) as exc:
        print(f"coreledger: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
