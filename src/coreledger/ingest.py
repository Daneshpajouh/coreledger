"""Sample intake, sequencing runs, and pipeline runs with their results, jobs and gate report."""

import csv
import datetime as dt
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from . import slurm
from .db import Ledger

ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SEPARATORS = "._-"


class InputError(ValueError):
    """A sheet or file that cannot go into the catalogue as it is."""


def _rows(path: Path, required: Sequence[str]) -> List[Dict[str, str]]:
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in required if c not in (reader.fieldnames or [])]
        if missing:
            raise InputError(f"{path}: missing column(s) {', '.join(missing)}")
        rows = []
        for n, row in enumerate(reader, start=2):
            clean = {k: (v or "").strip() for k, v in row.items() if k is not None}
            for c in required:
                if not clean[c]:
                    raise InputError(f"{path} line {n}: {c} is empty")
            clean["_line"] = str(n)
            rows.append(clean)
    return rows


def _check_id(value: str, what: str, where: str) -> None:
    if not ID_RE.match(value):
        raise InputError(f"{where}: {what} {value!r} must start with a letter or digit and use only A-Z, 0-9, . _ -")


def intake(ledger: Ledger, path: Path) -> int:
    """Add samples from a CSV with columns sample_id and project_id (external_id, material, received optional).

    The whole sheet is refused if any line is wrong, including a sample ID that is already
    in the catalogue or repeated in the sheet.
    """
    rows = _rows(path, ("sample_id", "project_id"))
    seen = set()
    for row in rows:
        where = f"{path} line {row['_line']}"
        _check_id(row["sample_id"], "sample_id", where)
        _check_id(row["project_id"], "project_id", where)
        if row["sample_id"] in seen:
            raise InputError(f"{where}: sample {row['sample_id']} appears twice in the sheet")
        seen.add(row["sample_id"])
        if ledger.one("SELECT 1 FROM samples WHERE sample_id = ?", (row["sample_id"],)):
            raise InputError(f"{where}: sample {row['sample_id']} is already in the catalogue")
        received = row.get("received", "")
        if received:
            try:
                dt.date.fromisoformat(received)
            except ValueError as exc:
                raise InputError(f"{where}: received {received!r} is not a date (YYYY-MM-DD)") from exc
    try:
        for row in rows:
            ledger.execute(
                "INSERT INTO projects (project_id) VALUES (?) ON CONFLICT (project_id) DO NOTHING",
                (row["project_id"],),
            )
            ledger.execute(
                "INSERT INTO samples (sample_id, project_id, external_id, material, received) VALUES (?, ?, ?, ?, ?)",
                (
                    row["sample_id"],
                    row["project_id"],
                    row.get("external_id") or None,
                    row.get("material") or None,
                    row.get("received") or None,
                ),
            )
        ledger.commit()
    except Exception:
        ledger.rollback()
        raise
    return len(rows)


def add_sequencing(ledger: Ledger, path: Path) -> int:
    """Add sample lanes from a CSV with sample_id, run_id, lane, reads (instrument, flowcell, run_date optional)."""
    rows = _rows(path, ("sample_id", "run_id", "lane"))
    for row in rows:
        where = f"{path} line {row['_line']}"
        if not ledger.one("SELECT 1 FROM samples WHERE sample_id = ?", (row["sample_id"],)):
            raise InputError(f"{where}: sample {row['sample_id']} is not in the catalogue; add it by intake first")
        _check_id(row["run_id"], "run_id", where)
        if row.get("reads") and not row["reads"].isdigit():
            raise InputError(f"{where}: reads {row['reads']!r} is not a whole number")
    try:
        for row in rows:
            ledger.execute(
                "INSERT INTO seq_runs (run_id, instrument, flowcell, run_date) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (run_id) DO NOTHING",
                (
                    row["run_id"],
                    row.get("instrument") or None,
                    row.get("flowcell") or None,
                    row.get("run_date") or None,
                ),
            )
            ledger.execute(
                "INSERT INTO sample_runs (sample_id, run_id, lane, reads) VALUES (?, ?, ?, ?)",
                (row["sample_id"], row["run_id"], row["lane"], int(row["reads"]) if row.get("reads") else None),
            )
        ledger.commit()
    except Exception:
        ledger.rollback()
        raise
    return len(rows)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def match_sample(text: str, samples: Iterable[str]) -> Optional[str]:
    """The sample whose ID starts ``text`` followed by a separator or the end; the longest wins."""
    best = None
    for s in samples:
        if text == s or (text.startswith(s) and text[len(s)] in SEPARATORS):
            if best is None or len(s) > len(best):
                best = s
    return best


CONTRAST_RE = re.compile(r"(?:^|[._-])vs(?:[._-]|$)", re.I)


def other_sample_in(text: str, first: str, samples: Iterable[str]) -> bool:
    """True if a ``vs`` contrast names a second sample ID, between separators (``KO_1_vs_WT_1.tsv``).

    Without ``vs`` the rest is read as part of the first sample's name, so an Illumina file
    ``S1_S1_L001_R1_001.fastq.gz`` stays with S1 even when a sample is named ``L001`` or ``1``.
    """
    rest = text[len(first):]
    if not CONTRAST_RE.search(rest):
        return False
    for s in samples:
        if s == first:
            continue
        start = 0
        while True:
            i = rest.find(s, start)
            if i < 0:
                break
            end = i + len(s)
            if (i == 0 or rest[i - 1] in SEPARATORS) and (end == len(rest) or rest[end] in SEPARATORS):
                return True
            start = i + 1
    return False


def path_samples(rel: str, samples: Sequence[str]) -> Tuple[Set[str], bool]:
    """The samples a result path's folders and file name start with, and whether a piece is a contrast."""
    found = set()
    contrast = False
    for part in Path(rel).parts:
        hit = match_sample(part, samples)
        if hit:
            contrast = contrast or other_sample_in(part, hit, samples)
            found.add(hit)
    return found, contrast


def sample_for_path(rel: str, samples: Sequence[str]) -> Optional[str]:
    """The one sample a result path names, in a folder or the file name; None if none or more than one.

    A path piece that names two samples, such as a contrast file ``KO_1_vs_WT_1.tsv``, belongs to neither.
    """
    found, contrast = path_samples(rel, samples)
    return found.pop() if len(found) == 1 and not contrast else None


@dataclass
class Registered:
    prun_id: str
    results: int = 0
    per_sample: int = 0
    jobs: int = 0
    jobs_per_sample: int = 0
    notes: List[str] = field(default_factory=list)


def read_samplesheet(path: Path) -> List[str]:
    """Sample IDs from an nf-core style samplesheet (column ``sample``), in first-seen order."""
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        if "sample" not in (reader.fieldnames or []):
            raise InputError(f"{path}: a samplesheet needs a 'sample' column")
        out: List[str] = []
        for row in reader:
            s = (row["sample"] or "").strip()
            if s and s not in out:
                out.append(s)
    return out


def _left_out(folder: Path, inner: Sequence[Path]) -> List[str]:
    """Files under ``folder`` that are not under any of the folders in ``inner``, relative to ``folder``.

    A linked folder outside ``inner`` is listed by its own name with a trailing slash: ``os.walk`` does not follow it,
    and it is not a file, so without this the files behind it would not count.
    """
    rest: List[str] = []
    for root, dirs, names in os.walk(folder):
        kept = []
        for d in sorted(dirs):
            here = Path(root, d)
            if any(here.resolve().is_relative_to(t) for t in inner):
                continue
            if here.is_symlink():
                rest.append(here.relative_to(folder).as_posix() + "/")
                continue
            kept.append(d)
        dirs[:] = kept
        rest += [Path(root, n).relative_to(folder).as_posix() for n in sorted(names)]
    return rest


def _walk_results(outdir: Path, exclude: Sequence[str]) -> Tuple[List[Path], List[str], List[str], List[str]]:
    """The result files under ``outdir``, the linked folders entered, the links not entered and the file links
    left out.

    Linked folders are entered (nf-core publishDir mode symlink). A link is not entered when it
    points back to a folder already on the way down, so a loop ends; into the results folder,
    whose files are recorded under their own paths; at a folder that holds the results folder
    or the target of another link in the results folder itself, when it holds nothing else; or,
    inside a linked folder, out of that folder, since a task's staged inputs belong to other tasks.
    A link that holds another target and other files or linked folders too is refused, since entering it would
    record other tasks' files and skipping it would leave files out. A plain folder is always
    entered. Only links in the results folder itself count as targets, so the outcome does not
    depend on the order of names. Inside a linked folder, a link to a file outside that folder is
    left out too, for the same reason: it is a task's staged input (round 9, Reviewer B); a file
    link in a plain folder is recorded, as nf-core publishDir mode symlink needs.
    """
    top = outdir.resolve()
    targets: Set[Path] = set()
    for here in outdir.iterdir():
        if here.name not in exclude and here.is_symlink() and here.is_dir():
            if not here.resolve().is_relative_to(top):
                targets.add(here.resolve())
    files: List[Path] = []
    linked: List[str] = []
    loops: List[str] = []
    staged: List[str] = []
    above = {outdir: frozenset({top})}
    store: Dict[Path, Optional[Path]] = {outdir: None}
    for root, dirs, names in os.walk(outdir, followlinks=True):
        kept = []
        inside = store[Path(root)]
        for d in sorted(dirs):
            if d in exclude:
                continue
            here = Path(root, d)
            real = here.resolve()
            store[here] = inside
            if here.is_symlink():
                holds = real != top and top.is_relative_to(real)
                leaves = inside is not None and not real.is_relative_to(inside)
                if real in above[Path(root)] or real.is_relative_to(top) or holds or leaves:
                    loops.append(here.relative_to(outdir).as_posix())
                    continue
                inner = [t for t in targets if t != real and t.is_relative_to(real)]
                if inner:
                    rest = _left_out(real, inner)
                    if rest:
                        raise InputError(
                            f"{here.relative_to(outdir).as_posix()} holds another linked folder and {len(rest)} "
                            f"other file(s) or linked folder(s) ({', '.join(rest[:5])}); entering it would record "
                            f"other tasks' files and skipping it would leave these out: exclude it (--exclude {d}) "
                            "or link the folders apart"
                        )
                    loops.append(here.relative_to(outdir).as_posix())
                    continue
                linked.append(here.relative_to(outdir).as_posix())
                if inside is None:
                    store[here] = real
            above[here] = above[Path(root)] | {real}
            kept.append(d)
        dirs[:] = kept
        for name in sorted(names):
            p = Path(root) / name
            if inside is not None and p.is_symlink() and p.is_file() and not p.resolve().is_relative_to(inside):
                staged.append(p.relative_to(outdir).as_posix())
                continue
            if p.is_file():
                files.append(p)
            elif p.is_symlink():
                raise InputError(f"{p} is a link that does not point at a file; the result it stood for is gone")
    return files, linked, loops, staged

def register_run(
    ledger: Ledger,
    prun_id: str,
    pipeline: str,
    outdir: Path,
    samples: Sequence[str],
    version: Optional[str] = None,
    commit: Optional[str] = None,
    profile: Optional[str] = None,
    params: Optional[Path] = None,
    sacct: Optional[Path] = None,
    trace: Optional[Path] = None,
    job_map: Optional[Path] = None,
    gate: Optional[Path] = None,
    exclude: Sequence[str] = ("pipeline_info",),
    today: Optional[str] = None,
    sacct_run_only: bool = False,
) -> Registered:
    """Record one pipeline run: its samples, every result file with its hash, its Slurm jobs and its gate.

    Without a trace, an sacct file is taken whole only when ``sacct_run_only`` says it holds this
    run's jobs alone; otherwise only the jobs in ``job_map`` are kept, and with no job map either
    the run is refused, since a wide sacct export would bill other people's jobs to this run.
    """
    _check_id(prun_id, "pipeline run ID", "register")
    if ledger.one("SELECT 1 FROM pipeline_runs WHERE prun_id = ?", (prun_id,)):
        raise InputError(f"pipeline run {prun_id} is already registered")
    outdir = Path(outdir)
    if not outdir.is_dir():
        raise InputError(f"{outdir} is not a folder")
    if not samples:
        raise InputError("a pipeline run needs at least one sample")
    unknown = [s for s in samples if not ledger.one("SELECT 1 FROM samples WHERE sample_id = ?", (s,))]
    if unknown:
        raise InputError(f"not in the catalogue: {', '.join(unknown)}; add them by intake first")
    if commit is not None and not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise InputError(f"commit {commit!r} is not a full 40-character git commit hash")
    if sacct is not None and trace is None and job_map is None and not sacct_run_only:
        raise InputError(
            "sacct without --trace or --job-map would bill every job in the file to this run; "
            "give one of them, or --sacct-run-only if the file holds this run's jobs alone"
        )

    reg = Registered(prun_id)
    gate_verdict = gate_cfg = gate_path = None
    if gate is not None:
        try:
            report = json.loads(Path(gate).read_text())
        except json.JSONDecodeError as exc:
            raise InputError(f"{gate}: the gate report is not valid JSON ({exc})") from exc
        if not isinstance(report, dict):
            raise InputError(f"{gate}: the gate report is not a JSON object")
        gate_verdict = report.get("verdict")
        if gate_verdict not in ("PASS", "FAIL", "ERROR"):
            raise InputError(f"{gate}: no verdict PASS, FAIL or ERROR in the gate report")
        gate_cfg = report.get("config_sha256")
        gate_path = str(Path(gate).resolve())

    files, linked_dirs, loops, staged = _walk_results(outdir, exclude)
    if linked_dirs:
        reg.notes.append(f"{len(linked_dirs)} linked folder(s) entered: {', '.join(linked_dirs[:5])}")
    if loops:
        reg.notes.append(f"{len(loops)} linked folder(s) not entered (a loop, a folder inside the results, "
                         "a folder that holds the results or another linked folder, or a link out of a linked "
                         f"folder): {', '.join(loops[:5])}")
    if staged:
        reg.notes.append(f"{len(staged)} link(s) to a file outside its linked folder not recorded (a task's staged "
                         f"input): {', '.join(staged[:5])}")

    jobs: List[slurm.Job] = []
    tags: Dict[str, str] = {}
    held: List[Tuple[str, str]] = []
    blank: List[Tuple[str, str, str]] = []
    zero: List[Tuple[str, str]] = []
    cached: Set[str] = set()
    mapped: Dict[str, str] = {}
    if job_map is not None:
        for row in _rows(job_map, ("job_id", "sample_id")):
            if row["sample_id"] not in samples:
                raise InputError(f"{job_map} line {row['_line']}: sample {row['sample_id']} is not in this run")
            mapped[row["job_id"]] = row["sample_id"]
    if sacct is not None:
        jobs, notes = slurm.read_sacct(sacct, held, blank, zero)
        reg.notes.extend(notes)
    if trace is not None:
        tags = slurm.read_nextflow_trace(trace, cached)
    ours = set(tags) | set(mapped)
    # A blank size stops the run only on a job it would bill, not on another user's line in a wide export.
    for shown_id, raw_id, problem in blank:
        if sacct_run_only or (trace is None and job_map is None) or shown_id in ours or raw_id in ours:
            raise slurm.FormatError(problem)
    if not sacct_run_only and (trace is not None or job_map is not None):
        # An array task's trace ID may be its JobIDRaw (301) while the range shows the parent (300_[3-10]),
        # and a task not yet started is listed by either ID (300_1 or 301).
        bases = {t.split("_")[0] for t in ours}
        for j in jobs:
            if j.job_id in ours or j.alt_id in ours:
                bases |= {i.split("_")[0] for i in (j.job_id, j.alt_id) if i}
        for ids in held:
            if any(i in ours for i in ids if i):
                bases |= {i.split("_")[0] for i in ids if i}
        # A zero-time task of the run ties its array too (round 9, Reviewer B: 300_1 cancelled at no time, with
        # 300_[2-10] pending, registered with the pending range never billed).
        for ids in zero:
            if any(i in ours for i in ids if i):
                bases |= {i.split("_")[0] for i in ids if i}
        held = [(s, r) for s, r in held if (s or r).split("_")[0] in bases]
    if held:
        shown = [s or r for s, r in held]
        raise InputError(
            f"{len(held)} Slurm job(s) of this run have not finished ({', '.join(shown[:5])}); "
            "register the run after they end, so their time is billed"
        )
    listed = {j.job_id for j in jobs} | {j.alt_id for j in jobs}
    # A zero-time line with a blank size is in the export and bills nothing; it has its own note, not "not found".
    listed |= {i for ids in zero for i in ids if i}
    if trace is not None:
        missing = sorted(set(tags) - listed)
        if sacct is not None and missing:
            reg.notes.append(f"{len(missing)} trace job(s) not found in sacct: {', '.join(missing[:5])}")
    if job_map is not None:
        missing = sorted(set(mapped) - listed)
        if sacct is not None and missing:
            reg.notes.append(f"{len(missing)} job map job(s) not found in sacct: {', '.join(missing[:5])}")
    if (trace is not None or job_map is not None) and not sacct_run_only:
        # A job in the trace or in the job map is this run's; with both given, either list counts.
        kept = [j for j in jobs if j.job_id in ours or j.alt_id in ours]
        if sacct is not None and len(kept) != len(jobs):
            which = "the trace or job map" if trace is not None and job_map is not None else (
                "the trace" if trace is not None else "the job map")
            reg.notes.append(f"{len(jobs) - len(kept)} sacct job(s) not in {which} were left out "
                             "(--sacct-run-only keeps them, shared across the run's samples)")
        jobs = kept
    if jobs and trace is None and job_map is None:
        reg.notes.append("no --trace or --job-map: every Slurm job is shared across the run's samples")
    if sacct_run_only and (trace is not None or job_map is not None):
        extra = [j for j in jobs if j.job_id not in ours and j.alt_id not in ours]
        if extra:
            reg.notes.append(f"{len(extra)} sacct job(s) not in the trace or job map were kept, "
                             "shared across the run's samples (--sacct-run-only)")

    two = []
    for p in files:
        rel = p.relative_to(outdir).as_posix()
        named, contrast = path_samples(rel, samples)
        if len(named) > 1 and not contrast:
            two.append(rel)
    if two:
        reg.notes.append(f"{len(two)} result(s) in folders that name two samples were recorded for the run: "
                         f"{', '.join(two[:5])}")
    weights = {s: reads_on_record(ledger, s) for s in samples}
    blank = [s for s in samples if weights[s] is None]
    if blank and jobs:
        reg.notes.append(f"no read total for {', '.join(blank[:5])} (no lanes, or a lane without reads); "
                         "this run's shared jobs stay unallocated in a bill split by reads")

    try:
        ledger.execute(
            "INSERT INTO pipeline_runs (prun_id, pipeline, version, commit_sha, profile, params_sha256, outdir, "
            "registered, gate_report, gate_verdict, gate_config_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                prun_id,
                pipeline,
                version,
                commit,
                profile,
                sha256_file(params) if params else None,
                str(outdir.resolve()),
                today or dt.date.today().isoformat(),
                gate_path,
                gate_verdict,
                gate_cfg,
            ),
        )
        ledger.executemany(
            "INSERT INTO prun_samples (prun_id, sample_id, reads) VALUES (?, ?, ?)",
            [(prun_id, s, weights[s]) for s in samples],
        )
        for p in files:
            rel = p.relative_to(outdir).as_posix()
            sample = sample_for_path(rel, samples)
            ledger.execute(
                "INSERT INTO results (prun_id, path, sample_id, sha256, bytes) VALUES (?, ?, ?, ?, ?)",
                (prun_id, rel, sample, sha256_file(p), p.stat().st_size),
            )
            reg.results += 1
            reg.per_sample += sample is not None
        for j in jobs:
            sample = mapped.get(j.job_id) or mapped.get(j.alt_id)
            if sample is None and tags:
                tag = tags.get(j.job_id) or tags.get(j.alt_id) or ""
                sample = match_sample(tag, samples) if tag else None
                if sample is not None and other_sample_in(tag, sample, samples):
                    sample = None
            existing = ledger.one("SELECT prun_id FROM jobs WHERE job_id = ?", (j.key,))
            if existing and (j.job_id in cached or j.alt_id in cached):
                reg.notes.append(f"Slurm job {j.key} is a cached task already recorded for {existing[0]}; left out")
                continue
            if existing:
                raise InputError(f"Slurm job {j.key} is already recorded for pipeline run {existing[0]}")
            ledger.execute(
                "INSERT INTO jobs (job_id, prun_id, name, tag, state, elapsed_s, alloc_cpus, req_mem_bytes, gpus, "
                "sample_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    j.key,
                    prun_id,
                    j.name or None,
                    tags.get(j.job_id) or tags.get(j.alt_id) or None,
                    j.state or None,
                    j.elapsed_s,
                    j.alloc_cpus,
                    j.req_mem_bytes,
                    j.gpus,
                    sample,
                ),
            )
            reg.jobs += 1
            reg.jobs_per_sample += sample is not None
        ledger.commit()
    except Exception:
        ledger.rollback()
        raise
    return reg


def reads_on_record(ledger: Ledger, sample: str) -> Optional[int]:
    """The sample's total reads over its lanes now, or None if it has no lanes or a lane has no read count.

    A partial total is never returned: it would bill the sample less than its share of a run's shared jobs.
    """
    row = ledger.one("SELECT COUNT(*), COUNT(reads), SUM(reads) FROM sample_runs WHERE sample_id = ?", (sample,))
    if row is None or row[0] == 0 or row[0] != row[1]:
        return None
    return int(row[2])


def release(ledger: Ledger, prun_id: str, force: bool = False, today: Optional[str] = None) -> None:
    """Mark a pipeline run's results as released; refused unless its gate passed, or ``force``."""
    row = ledger.one("SELECT gate_verdict, released FROM pipeline_runs WHERE prun_id = ?", (prun_id,))
    if row is None:
        raise InputError(f"no pipeline run {prun_id}")
    if row[1]:
        raise InputError(f"pipeline run {prun_id} was released on {row[1]}")
    if row[0] != "PASS" and not force:
        raise InputError(f"pipeline run {prun_id} has gate verdict {row[0] or 'none'}; release refused (see --force)")
    ledger.execute(
        "UPDATE pipeline_runs SET released = ? WHERE prun_id = ?", (today or dt.date.today().isoformat(), prun_id)
    )
    ledger.commit()
