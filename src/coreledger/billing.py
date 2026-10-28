"""Cost recovery: Slurm jobs priced by a rate card and shared out to samples.

Every amount is an exact fraction until the last step. A project's bill is its exact total
rounded once to cents (half up). That bill is then split into per-sample amounts in cents by
the largest remainder method, so the sample lines always add up to the project bill. Rounding
each sample's share on its own, as a spreadsheet would, can leave the lines a few cents away
from the total.

A job tagged with one sample is charged to that sample. A job with no sample tag (an index
build, MultiQC) is shared by the samples of its pipeline run, in proportion to their reads
or equally.
"""

import json
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from .db import Ledger

RESOURCES = ("cpu_hour", "mem_gib_hour", "gpu_hour")


@dataclass(frozen=True)
class RateCard:
    currency: str
    rates: Dict[str, Fraction]
    bill_states: Optional[Tuple[str, ...]] = None

    @classmethod
    def load(cls, path: Path) -> "RateCard":
        data = json.loads(Path(path).read_text())
        currency = data.get("currency")
        if not isinstance(currency, str) or not currency:
            raise ValueError(f"{path}: the rate card needs a currency code, for example \"CAD\"")
        rates = {}
        for key, value in (data.get("rates") or {}).items():
            if key not in RESOURCES:
                raise ValueError(f"{path}: unknown resource {key!r}; known: {', '.join(RESOURCES)}")
            if not isinstance(value, str):
                raise ValueError(f"{path}: write rate {key} as a string, for example \"0.05\", to keep it exact")
            rate = Fraction(value)
            if rate < 0:
                raise ValueError(f"{path}: rate {key} is negative")
            rates[key] = rate
        if not rates:
            raise ValueError(f"{path}: no rates given")
        states = data.get("bill_states")
        return cls(currency, rates, tuple(states) if states is not None else None)


@dataclass(frozen=True)
class JobUse:
    job_id: str
    prun_id: str
    state: str
    sample_id: Optional[str]
    cpu_hours: Fraction
    mem_gib_hours: Fraction
    gpu_hours: Fraction

    def cost(self, card: RateCard) -> Fraction:
        return (
            self.cpu_hours * card.rates.get("cpu_hour", Fraction(0))
            + self.mem_gib_hours * card.rates.get("mem_gib_hour", Fraction(0))
            + self.gpu_hours * card.rates.get("gpu_hour", Fraction(0))
        )


@dataclass
class SampleLine:
    project_id: str
    sample_id: str
    cpu_hours: Fraction = Fraction(0)
    mem_gib_hours: Fraction = Fraction(0)
    gpu_hours: Fraction = Fraction(0)
    exact: Fraction = Fraction(0)
    cents: int = 0


@dataclass
class Bill:
    currency: str
    samples: List[SampleLine]
    projects: Dict[str, int]
    project_exact: Dict[str, Fraction]
    unallocated: List[Tuple[str, Fraction]] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


def round_half_up(value: Fraction) -> int:
    """Nearest integer, halves away from zero (amounts here are never negative)."""
    floor = value.numerator // value.denominator
    return floor + 1 if value - floor >= Fraction(1, 2) else floor


def split_cents(total: int, shares: Sequence[Tuple[str, Fraction]]) -> Dict[str, int]:
    """Split ``total`` cents in proportion to ``shares`` by the largest remainder method.

    Ties in the remainder go to the key that sorts first, so the split is deterministic.
    """
    weight = sum((s for _, s in shares), Fraction(0))
    if not shares:
        return {}
    if weight == 0:
        quotas = [(k, Fraction(total, len(shares))) for k, _ in shares]
    else:
        quotas = [(k, total * s / weight) for k, s in shares]
    out = {k: q.numerator // q.denominator for k, q in quotas}
    left = total - sum(out.values())
    order = sorted(quotas, key=lambda kq: (-(kq[1] - kq[1].numerator // kq[1].denominator), kq[0]))
    for k, _ in order[:left]:
        out[k] += 1
    return out


def job_uses(ledger: Ledger, prun_ids: Optional[Sequence[str]] = None) -> List[JobUse]:
    sql = "SELECT job_id, prun_id, state, sample_id, elapsed_s, alloc_cpus, req_mem_bytes, gpus FROM jobs"
    args: List[str] = []
    if prun_ids:
        sql += " WHERE prun_id IN (" + ", ".join("?" for _ in prun_ids) + ")"
        args = list(prun_ids)
    out = []
    for job_id, prun_id, state, sample_id, elapsed, cpus, mem, gpus in ledger.query(sql + " ORDER BY job_id", args):
        hours = Fraction(int(elapsed), 3600)
        out.append(
            JobUse(
                job_id,
                prun_id,
                state or "",
                sample_id,
                hours * int(cpus),
                hours * Fraction(int(mem), 1024**3),
                hours * int(gpus),
            )
        )
    return out


def sample_weights(ledger: Ledger, prun_id: str, by: str) -> List[Tuple[str, str, Fraction]]:
    """(project, sample, weight) for every sample of a pipeline run.

    Read weights are the totals recorded when the run was registered, so a lane added later does not
    change how an earlier run's shared jobs were split.
    """
    rows = ledger.query(
        "SELECT s.project_id, s.sample_id, p.reads FROM prun_samples p JOIN samples s ON s.sample_id = p.sample_id "
        "WHERE p.prun_id = ? ORDER BY s.sample_id",
        (prun_id,),
    )
    if by == "equal":
        return [(p, s, Fraction(1)) for p, s, _ in rows]
    out = []
    for project, sample, reads in rows:
        if reads is None:
            raise ValueError(
                f"sample {sample} had no lanes, or a lane without a read count, when {prun_id} was registered, "
                "so its shared jobs cannot be split by reads; use --split equal"
            )
        out.append((project, sample, Fraction(int(reads))))
    return out


def build_bill(
    ledger: Ledger, card: RateCard, split: str = "reads", prun_ids: Optional[Sequence[str]] = None
) -> Bill:
    if split not in ("reads", "equal"):
        raise ValueError("split must be 'reads' or 'equal'")
    lines: Dict[str, SampleLine] = {}
    unallocated: List[Tuple[str, Fraction]] = []
    skipped: List[str] = []
    notes: List[str] = []
    weights_cache: Dict[str, List[Tuple[str, str, Fraction]]] = {}
    members_cache: Dict[str, Set[str]] = {}
    project_of = dict(ledger.query("SELECT sample_id, project_id FROM samples"))

    def line(project: str, sample: str) -> SampleLine:
        if sample not in lines:
            lines[sample] = SampleLine(project, sample)
        return lines[sample]

    for use in job_uses(ledger, prun_ids):
        if card.bill_states is not None and use.state not in card.bill_states:
            skipped.append(f"job {use.job_id}: state {use.state} not billed")
            continue
        cost = use.cost(card)
        if use.sample_id is not None:
            if use.prun_id not in members_cache:
                members_cache[use.prun_id] = {r[0] for r in ledger.query(
                    "SELECT sample_id FROM prun_samples WHERE prun_id = ?", (use.prun_id,)
                )}
            if use.sample_id not in members_cache[use.prun_id]:
                unallocated.append((use.job_id, cost))
                continue
            parts = [(project_of[use.sample_id], use.sample_id, Fraction(1))]
        else:
            if use.prun_id not in weights_cache:
                try:
                    weights_cache[use.prun_id] = sample_weights(ledger, use.prun_id, split)
                except ValueError as exc:
                    weights_cache[use.prun_id] = []
                    notes.append(f"{use.prun_id}: shared jobs unallocated: {exc}")
            parts = weights_cache[use.prun_id]
        total_w = sum((w for _, _, w in parts), Fraction(0))
        if not parts or total_w == 0:
            unallocated.append((use.job_id, cost))
            continue
        for project, sample, w in parts:
            frac = w / total_w
            sl = line(project, sample)
            sl.cpu_hours += use.cpu_hours * frac
            sl.mem_gib_hours += use.mem_gib_hours * frac
            sl.gpu_hours += use.gpu_hours * frac
            sl.exact += cost * frac

    project_exact: Dict[str, Fraction] = {}
    for sl in lines.values():
        project_exact[sl.project_id] = project_exact.get(sl.project_id, Fraction(0)) + sl.exact
    projects = {p: round_half_up(v * 100) for p, v in project_exact.items()}
    for project, cents in projects.items():
        members = sorted((sl for sl in lines.values() if sl.project_id == project), key=lambda s: s.sample_id)
        for sample, c in split_cents(cents, [(s.sample_id, s.exact) for s in members]).items():
            lines[sample].cents = c
    samples = sorted(lines.values(), key=lambda s: (s.project_id, s.sample_id))
    return Bill(card.currency, samples, dict(sorted(projects.items())), project_exact, unallocated, skipped, notes)


def naive_cents(bill: Bill) -> Dict[str, int]:
    """Per-project totals if each sample's share were rounded to cents on its own (the usual spreadsheet)."""
    out: Dict[str, int] = {}
    for sl in bill.samples:
        out[sl.project_id] = out.get(sl.project_id, 0) + round_half_up(sl.exact * 100)
    return out


def money(cents: int, currency: str) -> str:
    return f"{currency} {cents // 100:,}.{cents % 100:02d}"


def decimal_text(value: Fraction, places: int = 6) -> str:
    scaled = round_half_up(value * 10**places)
    whole, frac = divmod(scaled, 10**places)
    return f"{whole}.{frac:0{places}d}"
