# coreledger

coreledger keeps the records a sequencing or analysis core needs behind each delivered result: which sample a file belongs to, which sequencing lanes fed it, which pipeline release and commit made it, whether the change gate passed before release, and what the compute cost, down to the sample. It is a command-line tool over one SQLite file or a PostgreSQL database.

It does four things:

- `intake`, `sequencing` and `register` record samples, lanes, and finished pipeline runs with a SHA-256 for every result file, the Slurm jobs from `sacct`, and the gate report from [tolgate](https://github.com/Daneshpajouh/tolgate) or any JSON with a PASS, FAIL or ERROR verdict.
- `audit` checks that every result can be traced and that nothing was released that should not have been, and exits 1 on any error, so it can gate a delivery script.
- `bill` prices the recorded jobs with a rate card and shares the cost out to samples, with sample lines that always add up to the project total.
- `sop` drafts a standard operating procedure from a pipeline's parameter schema and the core's own params file, and lists what a release upgrade changes.

LIMS such as [MISO](https://github.com/miso-lims/miso-lims) and [iSkyLIMS](https://github.com/BU-ISCIII/iskylims) track samples and sequencing runs; [Open XDMoD](https://github.com/ubccr/xdmod) reports cluster use and [ColdFront](https://github.com/coldfront/coldfront) manages allocations. coreledger sits between them and the pipelines: it starts where the sample sheet goes into Nextflow and ends at the delivered files and the bill.

## Install

```
pip install git+https://github.com/Daneshpajouh/coreledger
```

Python 3.9 or later, standard library only. For PostgreSQL, add `psycopg` (`pip install "coreledger[postgres] @ git+https://github.com/Daneshpajouh/coreledger"`) and pass a `postgresql://` URL wherever a catalogue path goes. Not yet on PyPI or Bioconda; a conda recipe is in `recipe/`.

## Use

The files below are in `examples/demo`: three RNA-seq samples, one nf-core/rnaseq run on Slurm, the run's `sacct` output and Nextflow trace, and a passing gate report. `KO_REP1`'s `quant.sf` was copied over from `WT_REP1` on purpose.

```
$ coreledger init core.sqlite
catalogue ready: core.sqlite
$ coreledger intake core.sqlite intake.csv
3 sample(s) added
$ coreledger sequencing core.sqlite lanes.csv
3 lane(s) added
$ coreledger register core.sqlite rnaseq-2026-10-12 --pipeline nf-core/rnaseq --version 3.27.0 \
    --commit 0123456789abcdef0123456789abcdef01234567 --outdir results --samplesheet samplesheet.csv \
    --sacct sacct.txt --trace trace.txt --gate gate.json
rnaseq-2026-10-12: 7 result file(s), 6 named for a sample; 9 Slurm job(s), 7 tagged with a sample
note: 1 sacct job(s) not in the trace were left out
$ coreledger release core.sqlite rnaseq-2026-10-12
rnaseq-2026-10-12 released
$ coreledger audit core.sqlite --files
error	SAME_CONTENT	rnaseq-2026-10-12	star_salmon/{sample}/quant.sf: identical bytes for KO_REP1, WT_REP1 (star_salmon/KO_REP1/quant.sf, star_salmon/WT_REP1/quant.sf)
1 finding(s), 1 error(s)
$ coreledger bill core.sqlite --rates rates.json --outdir bill
PRJ0042: CAD 2.51 (sample lines add up)
$ cat bill/samples.csv
project_id,sample_id,cpu_hours,mem_gib_hours,gpu_hours,exact,amount,currency
PRJ0042,KO_REP1,40.9301,327.7972,0.0000,1.200000,1.20,CAD
PRJ0042,WT_REP1,25.6865,175.1856,0.0000,0.688971,0.69,CAD
PRJ0042,WT_REP2,23.1028,157.6494,0.0000,0.619849,0.62,CAD
```

A result belongs to a sample when a folder or file name in its path starts with the sample ID followed by `.`, `_`, `-` or nothing; the longest matching ID wins, and a path that names two samples belongs to neither, so a contrast file such as `KO_REP1_vs_WT_REP1.tsv` is recorded for the run. `pipeline_info` is left out unless `--exclude` is given, which then replaces it. Files that name no sample, such as the MultiQC report, are recorded for the run. A linked file (Nextflow's `publishDir` mode `symlink`) is recorded with the hash of the file it points to; a linked folder is not entered, and `register` says so.

With `--trace`, only the Slurm jobs listed in the Nextflow trace are taken from `sacct`, so a `sacct` query that caught someone else's job (8811299 above) does no harm. A job's tag, the bracketed part of the task name (`STAR_ALIGN (WT_REP1)`), names its sample. Jobs without a sample tag, such as the genome index build and MultiQC, are shared by the run's samples. For pipelines without a Nextflow trace, `--job-map` takes a CSV of `job_id,sample_id`. With neither, every job is shared, and `register` says so.

`release` refuses a run whose gate did not pass. `--force` releases it anyway, and the audit then reports it.

## Audit findings

| Code | Level | Meaning |
|---|---|---|
| `NO_RESULTS` | error | a sample of a run has no result file named for it |
| `SAME_CONTENT` | error | two samples have the same output (same path with the sample ID replaced) with identical bytes, as a copied file or a sample sheet error leaves it; empty files are skipped |
| `NO_GATE` | error | a released run has no gate report |
| `GATE_NOT_PASS` | error | a released run's gate said FAIL or ERROR |
| `GATE_MOVED` | error | the gate report file is gone or no longer gives the recorded verdict; checked before release too, since `release` goes by the recorded verdict |
| `MISSING` | error | a recorded result file is gone (with `--files`) |
| `CHANGED` | error | a recorded result file's bytes changed (with `--files`) |
| `NO_SEQUENCING` | warning | a sample of a run has no sequencing lane recorded |
| `NO_COMMIT` | warning | a run has no git commit, so it cannot be rerun as it was |
| `UNBILLABLE` | warning | a run has Slurm jobs but no samples to charge them to |

## Cost recovery

The rate card is JSON. Rates are strings so that they stay exact:

```json
{
  "currency": "CAD",
  "rates": {"cpu_hour": "0.0125", "mem_gib_hour": "0.0021", "gpu_hour": "0.85"},
  "bill_states": ["COMPLETED", "FAILED", "OUT_OF_MEMORY", "TIMEOUT"]
}
```

Jobs are billed on what they held, not what they used: allocated CPUs times elapsed time, requested memory (GiB) times elapsed time, and GPUs from `AllocTRES`. That is what the scheduler kept from other users. `ReqMem` is read in both the Slurm 21.08 and later form (`72G`, whole job) and the older per-CPU and per-node forms (`4000Mc`, `16000Mn`). Without `bill_states`, every finished job is billed, failed attempts included: in the example, `KO_REP1`'s first STAR run ran out of memory after 41 minutes and is part of its CAD 1.20. That default also bills `CANCELLED`, `NODE_FAIL`, `PREEMPTED` and `REQUEUED` attempts for the time they ran; list the states you charge for in `bill_states` if your core's policy differs. Running and pending jobs are never billed, and a job array line that still covers a range of tasks (`1001_[3-10]`) is skipped with a note.

Shared jobs are split in proportion to each sample's reads (`--split reads`, the default) or equally (`--split equal`). The reads are the totals on record when the run was registered, so a lane added later does not change an earlier run's split; record the lanes, with read counts, before `register`. A job tagged with a sample that is not in its run, or a run with no samples, is reported as unallocated, and `bill` then exits 1 so that a delivery script does not send a bill with money left out.

All arithmetic is in exact fractions, `ReqMem` values included. Each project's total is rounded once, to the cent, half up. That total is then divided into sample amounts in cents by the largest remainder method, with ties going to the sample ID that sorts first, so the sample lines always add up to the project line. Rounding each sample's share on its own does not: in the two-sample test case, shares of CAD 0.315 and CAD 0.175 round to CAD 0.32 and CAD 0.18, which is CAD 0.50 for a CAD 0.49 project.

## SOPs from a parameter schema

A pipeline's parameter schema documents every option it has. A core needs something narrower: the values it runs with, what each one does, which defaults it leaves to the pipeline, and what changes when it moves to a new release.

```
$ coreledger sop --schema rnaseq_3.27.0_nextflow_schema.json --params params_3.14.0.json \
    --old-schema rnaseq_3.14.0_nextflow_schema.json --old-version 3.14.0 \
    --pipeline nf-core/rnaseq --version 3.27.0 --out SOP_3.27.0.md
```

The output, `examples/upgrade/SOP_3.27.0.md`, comes from the published schemas of nf-core/rnaseq 3.14.0 and 3.27.0 (MIT licence, copies in `tests/fixtures`) and a params file written for 3.14.0. Its checks section:

```
- warning: `aligner`: value is set to the schema default
- error: `max_cpus`: parameter is not defined in the schema
- error: `max_memory`: parameter is not defined in the schema
```

The same params file passes against the 3.14.0 schema (one warning). Between the two releases, 35 parameters were added and 8 removed, `max_cpus`, `max_memory` and `max_time` among them, and `aligner` gained `bowtie2_salmon`. Six schema defaults differ. Five of those are defaults that 3.14.0's schema did not state and 3.27.0's does (`skip_qualimap` and four others, now `false`); the sixth is a trailing slash on `igenomes_base`. The SOP marks each one, because a schema starting to state a default says nothing about the value a run gets: that comes from the pipeline's `nextflow.config`, which `sop` does not read. The command exits 1 when the params file has an error.

Schemas are read from JSON: nf-core's `$defs` or `definitions` groups in `allOf` order, or the top-level `properties` of a plain JSON Schema, such as a Snakemake config schema saved as JSON. Properties inside nested objects are not read.

## Validation

`validation/known_answer.py` checks the two parts where a wrong answer would cost a core money or a client. The data are synthetic and seeded, with no facility's records. Results from the run in `validation/results.json` (seed 20261028):

Cost recovery, 2,000 projects of 2 to 96 samples, 3 to 15 jobs per sample and 1 to 5 shared jobs per project:

| Check | coreledger | Rounding each sample line |
|---|---|---|
| Project total equals an independent 50-digit `Decimal` computation | 2,000 of 2,000 | (same total) |
| Sample lines add up to the project total | 2,000 of 2,000 | 492 of 2,000 |
| Largest gap between lines and total | 0 cents | 7 cents |
| Mean gap | 0 cents | 1.538 cents |

Audit, 300 pipeline runs registered through the API with real files on disk, 145 of which got one planted defect from the ten types above:

| | Planted | Found by `coreledger audit --files` | Found by a SHA-256 manifest (`sha256sum -c`) |
|---|---|---|---|
| All types | 145 | 145 | 29 |
| False findings on the other runs | | 0 | 0 |

The manifest is written for each run's results folder at registration and checked again after the defects are planted; a defect counts as found when that check fails for its run. It finds only the moved and edited files, which is what it is built for. This test shows that each check fires on the defect it was written for and stays quiet otherwise. It has limits: the expected findings follow the same rules as the code, each run gets at most one defect, and the planted files are plain. So it cannot show that the rules are right for a given core or that the ten types cover what goes wrong there. The harder cases the reviews raised (array lines, linked files, contrast files, a sample ID inside a folder name, lanes added later) are unit tests in `tests/test_core.py`. The per-type counts are in `validation/results.json`.

Run it with `python validation/known_answer.py --out results.json`. CI runs a smaller size on every push.

## PostgreSQL

The tables use the SQL that SQLite and PostgreSQL share. `tests/test_postgres.py` repeats the end-to-end test and the rollback test on PostgreSQL when `COREL_PG_URL` is set. The CI workflow runs it against PostgreSQL 16, and it has been run locally against PostgreSQL 15.15. Each test makes and drops its own schema.

## Limits

- Results are matched to samples by name. A pipeline that writes per-sample files without the sample ID in the path leaves them as run-level files.
- Jobs are tagged from Nextflow traces, or from a job map for anything else. Snakemake's Slurm job names are not parsed.
- Jobs are billed on allocation. A core that bills on use (`TotalCPU`, `MaxRSS`) needs a different price model.
- There is no web front end. The catalogue is plain tables, readable from any SQL client.

## Licence

Apache-2.0.
