"""Checks that every result can be traced, and that nothing went out that should not have.

Each finding has a code, so a core can decide which ones block a release:

- ``NO_RESULTS``: a sample of a pipeline run has no result file named for it.
- ``SAME_CONTENT``: two samples have a result file with the same name pattern and the same
  bytes, which is how a copied file or a sample sheet error often shows.
- ``NO_SEQUENCING``: a sample of a pipeline run has no sequencing lane recorded.
- ``NO_COMMIT``: a pipeline run has no git commit, so it cannot be rerun as it was.
- ``NO_GATE``: a released pipeline run has no gate report.
- ``GATE_NOT_PASS``: a released pipeline run's gate verdict is FAIL or ERROR.
- ``GATE_MOVED``: the gate report file is missing or no longer gives the recorded verdict.
- ``MISSING``: a recorded result file is gone (with ``--files``).
- ``CHANGED``: a recorded result file's bytes changed (with ``--files``).
- ``UNBILLABLE``: a pipeline run has Slurm jobs but no samples to charge them to.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from .db import Ledger
from .ingest import SEPARATORS, sha256_file

ERROR = "error"
WARNING = "warning"
LEVELS = {
    "NO_RESULTS": ERROR,
    "SAME_CONTENT": ERROR,
    "NO_SEQUENCING": WARNING,
    "NO_COMMIT": WARNING,
    "NO_GATE": ERROR,
    "GATE_NOT_PASS": ERROR,
    "GATE_MOVED": ERROR,
    "MISSING": ERROR,
    "CHANGED": ERROR,
    "UNBILLABLE": WARNING,
}


@dataclass(frozen=True)
class Finding:
    code: str
    prun_id: str
    subject: str
    message: str

    @property
    def level(self) -> str:
        return LEVELS[self.code]


def name_pattern(path: str, sample: str) -> str:
    """The result path with its sample ID replaced, so the same output of two samples compares equal.

    Only a path piece that is the ID, or starts with it and a separator, is rewritten, so sample ``qc``
    leaves the folder ``fastqc`` alone.
    """
    parts = []
    for part in path.split("/"):
        if part == sample or (part.startswith(sample) and part[len(sample)] in SEPARATORS):
            part = "{sample}" + part[len(sample):]
        parts.append(part)
    return "/".join(parts)


def audit(ledger: Ledger, check_files: bool = False) -> List[Finding]:
    out: List[Finding] = []
    pruns = ledger.query(
        "SELECT prun_id, commit_sha, gate_report, gate_verdict, released, outdir FROM pipeline_runs ORDER BY prun_id"
    )
    for prun_id, commit, gate_report, verdict, released, outdir in pruns:
        samples = [r[0] for r in ledger.query(
            "SELECT sample_id FROM prun_samples WHERE prun_id = ? ORDER BY sample_id", (prun_id,)
        )]
        counts = dict(ledger.query(
            "SELECT sample_id, COUNT(*) FROM results WHERE prun_id = ? AND sample_id IS NOT NULL GROUP BY sample_id",
            (prun_id,),
        ))
        for s in samples:
            if not counts.get(s):
                out.append(Finding("NO_RESULTS", prun_id, s, f"no result file under {outdir} names sample {s}"))
            if not ledger.one("SELECT 1 FROM sample_runs WHERE sample_id = ?", (s,)):
                out.append(Finding("NO_SEQUENCING", prun_id, s, f"sample {s} has no sequencing lane recorded"))

        groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for path, sample, digest, size in ledger.query(
            "SELECT path, sample_id, sha256, bytes FROM results WHERE prun_id = ? AND sample_id IS NOT NULL "
            "ORDER BY path",
            (prun_id,),
        ):
            if int(size) == 0:
                continue
            groups.setdefault((name_pattern(path, sample), digest), []).append((sample, path))
        for (pattern, _), members in sorted(groups.items()):
            who = sorted({s for s, _ in members})
            if len(who) > 1:
                paths = ", ".join(p for _, p in members)
                out.append(Finding(
                    "SAME_CONTENT", prun_id, ",".join(who), f"{pattern}: identical bytes for {', '.join(who)} ({paths})"
                ))

        if not commit:
            out.append(Finding("NO_COMMIT", prun_id, prun_id, "no git commit recorded for this pipeline run"))
        if released:
            if not gate_report:
                out.append(Finding("NO_GATE", prun_id, prun_id, f"released on {released} with no gate report"))
            elif verdict != "PASS":
                msg = f"released on {released}, gate verdict {verdict}"
                out.append(Finding("GATE_NOT_PASS", prun_id, prun_id, msg))
        if gate_report:
            try:
                now = json.loads(Path(gate_report).read_text()).get("verdict")
            except (OSError, ValueError):
                now = None
            if now != verdict:
                out.append(Finding(
                    "GATE_MOVED", prun_id, gate_report,
                    f"gate report gives {now or 'nothing readable'}, catalogue recorded {verdict}",
                ))

        jobs = ledger.one("SELECT COUNT(*) FROM jobs WHERE prun_id = ?", (prun_id,))
        if jobs and jobs[0] and not samples:
            out.append(Finding("UNBILLABLE", prun_id, prun_id, f"{jobs[0]} Slurm job(s) and no samples to charge"))

        if check_files:
            for path, digest in ledger.query(
                "SELECT path, sha256 FROM results WHERE prun_id = ? ORDER BY path", (prun_id,)
            ):
                p = Path(outdir) / path
                if not p.is_file():
                    out.append(Finding("MISSING", prun_id, path, f"{p} is gone"))
                elif sha256_file(p) != digest:
                    out.append(Finding("CHANGED", prun_id, path, f"{p} changed since it was registered"))
    return out
