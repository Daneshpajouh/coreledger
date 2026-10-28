"""Read Slurm accounting and Nextflow trace files into job records.

sacct input is ``sacct --parsable2`` output with a header line (fields separated by ``|``,
no trailing ``|``), as described in the sacct manual (https://slurm.schedmd.com/sacct.html).
Only allocation lines are kept; step lines such as ``1001.batch`` are skipped, since the
allocation is what the scheduler holds and what a core pays for. Job array lines that still
cover a range of tasks (``1001_[3-10]``) are skipped with a note, since each task that ran has
its own line; a range that was cancelled before it started costs nothing, and any other range
is tasks still waiting to run. A ``REQUEUED`` line is an attempt that held the allocation, so it counts like a
``FAILED`` one.

Columns used: ``JobIDRaw`` or ``JobID`` (one is required), ``Elapsed`` or ``ElapsedRaw``,
``AllocCPUS``, ``ReqMem`` (required); ``JobID``, ``JobName``, ``State``, ``NNodes`` and
``AllocTRES`` when present. A suitable call:

    sacct --parsable2 --allocations --starttime 2026-10-01 \\
        --format=JobID,JobIDRaw,JobName,State,Elapsed,AllocCPUS,NNodes,ReqMem,AllocTRES
"""

import csv
import re
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Dict, List, Optional, Tuple

UNITS = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
NOT_FINISHED = {"PENDING", "RUNNING", "RESIZING", "SUSPENDED"}


class FormatError(ValueError):
    """A line or field that does not follow the expected format."""


@dataclass(frozen=True)
class Job:
    job_id: str
    alt_id: str
    name: str
    state: str
    elapsed_s: int
    alloc_cpus: int
    req_mem_bytes: int
    gpus: int


def parse_elapsed(text: str) -> int:
    """Slurm durations: ``[D-]HH:MM:SS``, ``MM:SS`` or ``MM:SS.mmm``, to whole seconds."""
    text = text.strip()
    m = re.fullmatch(r"(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)(?:\.\d+)?", text)
    if not m:
        raise FormatError(f"not a Slurm duration: {text!r}")
    days, hours, minutes, seconds = (int(g) if g else 0 for g in m.groups())
    if m.group(1) and m.group(2) is None:
        raise FormatError(f"not a Slurm duration: {text!r}")
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


def parse_reqmem(text: str, cpus: int, nodes: int) -> int:
    """ReqMem to bytes for the whole job.

    Slurm 21.08 and newer print whole-job memory with a unit (``32G``). Slurm 20.11 and
    older add ``c`` (per CPU) or ``n`` (per node); a number without a unit is megabytes.
    """
    text = text.strip()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGTP]?)([cn]?)", text)
    if not m:
        raise FormatError(f"not a ReqMem value: {text!r}")
    number, unit, per = m.groups()
    scale = UNITS[unit] if unit else UNITS["M"]
    value = round(Fraction(number) * scale)
    if per == "c":
        value *= cpus
    elif per == "n":
        value *= nodes
    return value


def parse_gpus(tres: str) -> int:
    """GPU count from AllocTRES (``cpu=8,gres/gpu=2,gres/gpu:a100=2,mem=32G``)."""
    plain, typed = None, 0
    for part in filter(None, tres.split(",")):
        key, _, value = part.partition("=")
        if key == "gres/gpu":
            plain = int(value)
        elif key.startswith("gres/gpu:"):
            typed += int(value)
    return plain if plain is not None else typed


def read_sacct(path: Path, held: Optional[List[str]] = None) -> Tuple[List[Job], List[str]]:
    """Jobs that have finished, and notes on the lines skipped.

    ``held``, if given, collects the IDs of lines skipped because they have not finished: a job
    still pending or running, or an array range whose tasks have not started (``1001_[3-10]``).
    """
    notes: List[str] = []
    with open(path, newline="") as fh:
        lines = [line.rstrip("\r\n") for line in fh if line.strip()]
    if not lines:
        raise FormatError(f"{path}: empty sacct file")
    header = lines[0].split("|")
    col = {name: i for i, name in enumerate(header)}
    id_col = "JobIDRaw" if "JobIDRaw" in col else "JobID" if "JobID" in col else None
    if id_col is None:
        raise FormatError(f"{path}: needs a JobIDRaw or JobID column")
    for need in ("AllocCPUS", "ReqMem"):
        if need not in col:
            raise FormatError(f"{path}: needs a {need} column")
    if "ElapsedRaw" not in col and "Elapsed" not in col:
        raise FormatError(f"{path}: needs an Elapsed or ElapsedRaw column")

    jobs: List[Job] = []
    for n, line in enumerate(lines[1:], start=2):
        fields = line.split("|")
        if len(fields) != len(header):
            raise FormatError(f"{path} line {n}: {len(fields)} fields, header has {len(header)}")

        def get(name: str, row: List[str] = fields) -> str:
            return row[col[name]] if name in col else ""

        job_id = get(id_col)
        shown = get("JobID")
        if "." in job_id or "." in shown:
            continue
        state = get("State").split(" ")[0]
        if "[" in job_id or "[" in shown:
            notes.append(f"job {shown or job_id} skipped: an array line that covers a range of tasks")
            if state != "CANCELLED" and held is not None:
                held.append(shown or job_id)
            continue
        if state in NOT_FINISHED:
            notes.append(f"job {job_id} skipped: state {state}")
            if held is not None:
                held.append(shown or job_id)
            continue
        try:
            cpus = int(get("AllocCPUS") or 0)
            nodes = int(get("NNodes") or 1)
            if get("ReqMem").strip().endswith("n") and "NNodes" not in col:
                raise FormatError(f"ReqMem {get('ReqMem')!r} is per node and there is no NNodes column")
            elapsed = int(get("ElapsedRaw")) if get("ElapsedRaw") else parse_elapsed(get("Elapsed"))
            mem = parse_reqmem(get("ReqMem"), cpus, nodes) if get("ReqMem") else 0
            gpus = parse_gpus(get("AllocTRES"))
        except (ValueError, FormatError) as exc:
            raise FormatError(f"{path} line {n}: {exc}") from exc
        alt = get("JobID") if id_col == "JobIDRaw" else ""
        jobs.append(Job(job_id, alt if alt != job_id else "", get("JobName"), state, elapsed, cpus, mem, gpus))
    return jobs, notes


def read_nextflow_trace(path: Path) -> Dict[str, str]:
    """Slurm job ID (``native_id``) to the task tag, from a Nextflow trace file.

    The tag is the ``tag`` column when the trace has one, otherwise the text in the last
    pair of parentheses of ``name`` (``NFCORE_RNASEQ:RNASEQ:FASTQC (WT_REP1)``). Tasks
    without a tag map to an empty string. Cached and failed attempts keep their own IDs.
    """
    out: Dict[str, str] = {}
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if not reader.fieldnames or "native_id" not in reader.fieldnames:
            raise FormatError(f"{path}: a Nextflow trace needs a native_id column")
        for row in reader:
            native = (row.get("native_id") or "").strip()
            if not native or native == "-":
                continue
            tag = (row.get("tag") or "").strip()
            if not tag or tag == "-":
                tag = task_tag(row.get("name") or "") or ""
            out[native] = tag
    return out


def task_tag(name: str) -> Optional[str]:
    """The text in the bracketed tag that ends a task name (``PROCESS (tag)``), if any.

    Brackets are matched from the end, so a tag that holds brackets itself stays whole.
    """
    text = name.rstrip()
    if not text.endswith(")"):
        return None
    depth = 0
    for i in range(len(text) - 1, -1, -1):
        if text[i] == ")":
            depth += 1
        elif text[i] == "(":
            depth -= 1
            if depth == 0:
                return text[i + 1 : -1].strip()
    return None
