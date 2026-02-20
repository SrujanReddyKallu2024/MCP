# audience-engine — repo contents + input file paths

## Repo contents (as in TFS repo `audience-engine`)

**Root**
- `/README.md`
- `/Counties_Lookupv2.xlsx`

**Folders**
- `/bash_scripts/`
  - `/bash_scripts/conf.sh`
  - `/bash_scripts/run_pipeline.sh`
- `/Scripts/`
  - `/Scripts/config.py`
  - `/Scripts/data_ingestion_m1.py`
  - `/Scripts/excel_builder.py`
  - `/Scripts/functions.py`
  - `/Scripts/minmax_comparison.py`
  - `/Scripts/minmax_processor.py`

## Input file locations / patterns (from `/bash_scripts/conf.sh` + `/bash_scripts/run_pipeline.sh`)

The pipeline dynamically detects “latest” files and then passes the resolved paths into Spark.

### Base directories
- HDFS data landing (copies some local files here): `/user/unity/audience_engine/data`
- HDFS outputs: `/user/unity/audience_engine/output/{YYYY_MM}/{TIMESTAMP}/`

### Inputs (7 required)
1) **A16 CBAF utility (HDFS)**
- Directory: `match2/digitaltaxonomy/input`
- Pattern: `A16_*_cbaf_utility*.csv`
- Detection: `hdfs dfs -ls -t match2/digitaltaxonomy/input/A16_*_cbaf_utility*.csv | head -1`

2) **F45 CBAF utility (HDFS)**
- Directory: `match2/digitaltaxonomy/input`
- Pattern: `F45.*.cbaf_utility*.csv`
- Detection: `hdfs dfs -ls -t match2/digitaltaxonomy/input/F45.*.cbaf_utility*.csv | head -1`

3) **Digital Taxonomy (HDFS)**
- Directory base: `match2/taxonomy`
- Pattern: `*_Digital_taxonomy*.txt`
- Detection:
  - Finds latest folder under `match2/taxonomy` (sorted desc)
  - Then picks first match under `{latest}/*_Digital_taxonomy*.txt`

4) **Experian matched IDs (LOCAL → copied into HDFS)**
- Local source directory: `/data/data_from_sts/internal/misc`
- Pattern: `Experian_matched_IDs_*.csv`
- Copy target: `/user/unity/audience_engine/data/{filename}`

5) **Match output flags (LOCAL → copied into HDFS)**
- Local source directory: `/data/data_from_sts/internal/misc`
- Pattern: `MatchOutputFile*.csv`
- Copy target: `/user/unity/audience_engine/data/{filename}`

6) **PAM Directory (HDFS if present, else LOCAL → copied into HDFS)**
- Pattern: `F03_*_PAMDirectory.csv`
- First tries in HDFS: `/user/unity/audience_engine/data/F03_*_PAMDirectory.csv`
- Else local source dir: `/data_office`
- Copy target: `/user/unity/audience_engine/data/{filename}`

7) **Counties Lookup Excel (HDFS if present, else LOCAL repo file → copied into HDFS)**
- Pattern: `Counties_Lookup*.xlsx`
- First tries in HDFS: `/user/unity/audience_engine/data/Counties_Lookup*.xlsx`
- Else local source dir: repo root directory (same folder as `Counties_Lookupv2.xlsx`)
- Copy target: `/user/unity/audience_engine/data/{filename}`

### Where these resolved inputs are used
`run_pipeline.sh` passes these as args into the Module 1 Spark job (`data_ingestion_m1.py`):
- `--digital_taxonomy_file` (HDFS)
- `--experian_match_file` (HDFS copy)
- `--pam_directory_file` (HDFS or HDFS copy)
- `--cbaf_utility_file` (HDFS)
- `--utility_file` (HDFS)
- `--counties_lookup_file` (HDFS or HDFS copy)
- `--match_output_file` (HDFS copy)

## Output delivery (FYI)
- Local STS pickup directory: `/data/data_to_sts/audience_engine`
- Local backup: `/tmp/audience_engine_backup/{TIMESTAMP}`
