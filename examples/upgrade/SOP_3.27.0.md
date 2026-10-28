# nf-core/rnaseq 3.27.0: standard operating procedure (draft)

This draft was generated from the pipeline's parameter schema and this service's params file. Review it before use.

## Run command

```
nextflow run nf-core/rnaseq -r 3.27.0 -params-file params.json
```

## Parameters this service sets

| Parameter | Value | Pipeline default | What it does |
| --- | --- | --- | --- |
| input | "samplesheet.csv" | none | Path to the sample sheet (CSV) containing metadata about the experimental samples. |
| outdir | "results" | none | The output directory where the results will be saved. |
| fasta | "GRCh38.primary_assembly.genome.fa" | none | Path to FASTA genome file. |
| gtf | "gencode.v46.primary_assembly.annotation.gtf" | none | Path to GTF annotation file. |
| aligner | "star_salmon" | "star_salmon" | Specifies the alignment algorithm to use - available options are 'star_salmon', 'star_rsem', 'hisat2', and 'bowtie2_salmon'. |
| skip_dupradar | true | false | Skip dupRadar. |

## Pipeline defaults this service relies on

| Parameter | Default | What it does |
| --- | --- | --- |
| hisat2_build_memory | "200.GB" | Minimum memory required to use splice sites and exons in the HiSAT2 index build process. |
| gtf_extra_attributes | "gene_name" | By default, the pipeline uses the `gene_name` field to obtain additional gene identifiers from the input GTF file when running Salmon. |
| gtf_group_features | "gene_id" | Define the attribute type used to group features in the GTF file when running Salmon. |
| featurecounts_group_type | "gene_biotype" | The attribute type used to group feature types in the GTF file when generating the biotype plot with featureCounts. |
| featurecounts_feature_type | "exon" | By default, the pipeline assigns reads based on the 'exon' attribute within the GTF file. |
| trimmer | "trimgalore" | Specifies the trimming tool to use - available options are 'trimgalore' and 'fastp'. |
| min_trimmed_reads | 10000 | Minimum number of trimmed reads below which samples are removed from further processing. |
| ribo_removal_tool | "sortmerna" | Tool to use for rRNA removal. |
| ribo_database_manifest | "${projectDir}/assets/rrna-db-defaults.txt" | Text file containing paths to fasta files (one per line) that will be used to create the database for SortMeRNA. |
| umi_dedup_tool | "umitools" | Specifies the tool to use for UMI deduplication - available options are 'umitools' and 'umicollapse'. |
| umitools_extract_method | "string" | UMI pattern to use. |
| umitools_grouping_method | "directional" | Method to use to determine read groups by subsuming those with similar UMIs. |
| pseudo_aligner_kmer_size | 31 | Kmer length passed to indexing step of pseudoaligners |
| min_mapped_reads | 5 | Minimum percentage of uniquely mapped reads below which samples are removed from further processing. |
| kallisto_quant_fraglen | 200 | In single-end mode Kallisto requires an estimated fragment length (in base pairs). |
| kallisto_quant_fraglen_sd | 200 | In single-end mode, Kallisto requires an estimated standard error for fragment length (in base pairs). |
| stranded_threshold | 0.8 | The fraction of stranded reads that must be assigned to a strandedness for confident assignment. |
| unstranded_threshold | 0.1 | The difference in fraction of stranded reads assigned to 'forward' and 'reverse' below which a sample is classified as 'unstranded'. |
| extra_fqlint_args | "--disable-validator P001" | Extra arguments to pass to the fq lint command. |
| deseq2_vst | true | Use vst transformation instead of rlog with DESeq2. |
| rseqc_modules | "bam_stat,inner_distance,infer_experiment,junction_annotation,junction_saturation,read_distribution,read_duplication" | Comma-separated list of RSeQC modules to run. |
| contaminant_screening_input | "unmapped" | Read set to screen for contaminants: aligner-unmapped reads (default), trimmed reads (post-BBSplit/rRNA), post-trim pre-BBSplit reads, or raw reads. |
| bracken_precision | "S" | Taxonomic level for Bracken abundance estimations. |
| skip_bbsplit | true | Skip BBSplit for removal of non-reference genome reads. |
| use_rustqc | false | Experimental. |
| skip_preseq | true | Skip Preseq. |
| skip_qualimap | false | Skip Qualimap. |
| skip_rseqc | false | Skip RSeQC. |
| skip_biotype_qc | false | Skip additional featureCounts process for biotype QC. |
| skip_deseq2_qc | false | Skip DESeq2 PCA and heatmap plotting. |

## Required inputs

- `input`: Path to the sample sheet (CSV) containing metadata about the experimental samples.
- `outdir`: The output directory where the results will be saved.

## Checks

- warning: `aligner`: value is set to the schema default
- error: `max_cpus`: parameter is not defined in the schema
- error: `max_memory`: parameter is not defined in the schema

## Changes since 3.14.0

### Added

- `arm`
- `bowtie2_index`
- `bowtie2_rrna_index`
- `bracken_precision`
- `contaminant_screening`
- `contaminant_screening_input`
- `extra_bowtie2_align_args`
- `extra_fqlint_args`
- `extra_hisat2_align_args`
- `extra_salmon_index_args`
- `gffread_transcript_fasta`
- `gpu_container_options`
- `help_full`
- `kraken_db`
- `pipelines_testdata_base_path`
- `prokaryotic`
- `ribo_removal_tool`
- `save_kraken_assignments`
- `save_kraken_unassigned`
- `seq_platform`
- `show_hidden`
- `skip_linting`
- `skip_quantification_merge`
- `sortmerna_index`
- `stranded_threshold`
- `sylph_db`
- `sylph_taxonomy`
- `trace_report_suffix`
- `umi_dedup_tool`
- `umitools_dedup_primary_only`
- `unstranded_threshold`
- `use_gpu_ribodetector`
- `use_parabricks_star`
- `use_rustqc`
- `use_sentieon_star`

### Removed

- `hook_url`
- `max_cpus`
- `max_memory`
- `max_time`
- `test_data_base`
- `validationFailUnrecognisedParams`
- `validationLenientMode`
- `validationShowHiddenParams`

### Default changes

- `igenomes_base`: "s3://ngi-igenomes/igenomes" to "s3://ngi-igenomes/igenomes/" (not pinned by this service: the default changes on upgrade)
- `skip_biotype_qc`: none to false (not stated in the old schema; compare the old release's nextflow.config)
- `skip_deseq2_qc`: none to false (not stated in the old schema; compare the old release's nextflow.config)
- `skip_dupradar`: none to false (pinned)
- `skip_qualimap`: none to false (not stated in the old schema; compare the old release's nextflow.config)
- `skip_rseqc`: none to false (not stated in the old schema; compare the old release's nextflow.config)

### Type changes

- `help`: boolean to boolean|string

### Enum changes

- `aligner`: ['star_salmon', 'star_rsem', 'hisat2'] to ['star_salmon', 'star_rsem', 'hisat2', 'bowtie2_salmon']
