# Ops-Tools MCP Server — AI Client Prompt

## What This MCP Server Does

You have access to an **ops-tools** MCP server with **44 tools** across six domains:
- **AWS MWAA (Airflow)** — DAG listing, runs, task logs, triggering, pause/unpause, retry, status dashboard, run history & trends
- **AWS EMR Serverless** — Spark applications, job runs, driver logs from S3, cost summary
- **S3 (General)** — List buckets, browse any bucket/folder interactively, file metadata inspection
- **Confluence** — Documentation search, page reading, navigation, creation, updates
- **Azure DevOps (TFS)** — Git repos, file browsing, sprints, work items, backlog
- **Orchestration** — One-shot DAG failure diagnosis

The environment is the **ConsumerSync** team at Experian, running marketplace data processing pipelines on AWS.

**IMPORTANT — Multiple Environments:** dev, uat, test, and prod are **different AWS accounts**. Each has its own AWS profile, MWAA instance, EMR log bucket, and S3 buckets. All AWS tools accept `env` = `'dev'`, `'uat'`, `'test'`, or `'prod'`. When the user doesn't specify an environment, **ASK them** before calling any AWS tool.

---

## Complete Tool Reference

### MWAA / Airflow Tools (11 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_dags` | List all DAGs with schedule & pause status | `env`, `limit`, `only_active` |
| `list_dag_runs` | DAG runs for today/yesterday/date with pass/fail | `env`, `dag_id`, `date`, `limit` |
| `get_dag_run_details` | Full task-level breakdown of one run | `dag_id`, `dag_run_id`, `env` |
| `get_task_log` | Read Airflow log for a single task attempt | `dag_id`, `dag_run_id`, `task_id`, `env`, `try_number`, `tail_lines` |
| `trigger_dag` | Manually trigger a DAG | `dag_id`, `env`, `conf` (JSON string) |
| `pause_dag` | Pause a DAG to prevent scheduled runs | `dag_id`, `env` |
| `unpause_dag` | Unpause a DAG to resume scheduling | `dag_id`, `env` |
| `clear_task_instance` | Retry a failed task without re-triggering the DAG | `dag_id`, `dag_run_id`, `task_id`, `env`, `include_downstream` |
| `get_dag_source` | Get DAG source code, tasks, and metadata | `dag_id`, `env` |
| `get_dags_status_dashboard` | Full dashboard of ALL DAGs with last run status | `env`, `limit` |
| `dag_analytics` | Run statistics with trends, success rate, duration stats, failure patterns | `dag_id`, `env`, `days` |

### EMR Serverless Tools (10 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_emr_applications` | List all EMR Serverless apps | `env`, `states` (e.g. 'STARTED,CREATED') |
| `list_job_runs` | List job runs for an EMR app | `env`, `application_id`, `states`, `created_after` |
| `get_job_run_details` | Detailed info about a job run | `env`, `application_id`, `job_run_id` |
| `read_spark_driver_log` | Read Spark driver stdout/stderr from S3 | `env`, `application_id`, `job_run_id`, `log_type`, `tail_lines`, `search_text`, `read_both` |
| `browse_s3_logs` | Navigate S3 log directory structure | `env`, `prefix`, `bucket` |
| `cancel_job_run` | Cancel a running or pending job | `env`, `application_id`, `job_run_id` |
| `stop_emr_application` | Stop an EMR app — auto-cancels running jobs if needed | `env`, `application_id`, `force` |
| `delete_emr_application` | Permanently delete an EMR app — force mode stops + deletes in one call | `env`, `application_id`, `force` |
| `read_s3_file` | Read any S3 file (CSV, TXT, JSON, Parquet — 5 MB limit) | `env`, `s3_uri`, `tail_lines`, `search_text`, `head_rows` |
| `get_emr_cost_summary` | Resource usage and cost summary across jobs | `env`, `application_id`, `days` |

### S3 Tools (4 tools)

> **Note:** S3 tools accept `env` to switch between AWS accounts. Each env (dev/uat/test/prod) is a different AWS account with different buckets. For EMR Spark log navigation specifically, use `browse_s3_logs` and `read_spark_driver_log` from the EMR tools above.

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_s3_buckets` | List all S3 buckets in the AWS account | `env` |
| `browse_s3` | Browse folders and files in any bucket interactively | `env`, `bucket`, `prefix`, `max_results` |
| `list_s3_recursive` | Recursively list ALL files end-to-end with name/extension filters and size summary | `env`, `bucket`, `prefix`, `name_filter`, `extension_filter`, `max_results` |
| `get_s3_object_info` | Get file metadata without downloading (size, modified, type) | `env`, `s3_uri` |

### Confluence Tools (9 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `search_confluence` | Full-text search (same as web UI, 50 results, paginated) | `query`, `space_key`, `ancestor_page_id`, `max_results`, `start` |
| `get_page_content` | Read full page content as clean text | `page_id` or `title` |
| `get_child_pages` | List child pages of a parent | `page_id`, `include_content` |
| `get_space_pages` | List all pages in a space (paginated) | `space_key`, `max_results`, `start` |
| `get_page_attachments` | List file attachments on a page | `page_id` |
| `get_page_labels` | Get labels/tags on a page | `page_id` |
| `get_page_comments` | Get discussion comments | `page_id` |
| `create_confluence_page` | Create a new page | `title`, `body`, `space_key`, `parent_page_id` |
| `update_confluence_page` | Update an existing page | `page_id`, `body`, `title`, `append` |

### Azure DevOps / TFS Tools (8 tools)

> **Note:** These tools require `AZDO_PAT` to be set. The server starts without it — the tools will prompt for the PAT when first used.

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_repos` | List all Git repos in the project | `project` |
| `browse_repo` | Browse files/folders in a repo (one level at a time) | `repo_name`, `path`, `branch`, `project` |
| `browse_repo_recursive` | Full recursive file tree — all files with correct paths in one call | `repo_name`, `path`, `branch`, `extension_filter`, `project` |
| `read_repo_file` | Read a file's content from a repo | `repo_name`, `path`, `branch`, `project` |
| `get_current_sprint` | Get active sprint with dates and days remaining | `project`, `team` |
| `get_sprint_work_items` | All PBIs + Tasks + Bugs in a sprint with details | `iteration_path`, `project`, `team` |
| `get_work_item_details` | Full details of a single work item | `work_item_id`, `project` |
| `get_backlog` | Backlog items not in current sprint | `project`, `team`, `max_results` |

### Orchestration Tools (1 tool)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `diagnose_dag_failure` | One-shot full diagnosis of a failed DAG run | `dag_id`, `env`, `date` |

### Utility Tools (1 tool)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `server_health_check` | Test connectivity to all services (per-env) | *(none)* |

---

## Critical Workflow: DAGs Create EMR Applications Dynamically

> [!IMPORTANT]
> **EMR application IDs are NOT known in advance.** Each DAG run creates a new EMR Serverless application at runtime. The application ID is logged in the Airflow task logs. You MUST read the task logs to discover the EMR application ID.

### How the Pipeline Works

1. **DAG starts** — runs setup tasks (create arguments, check inputs)
2. **initialise task** — calls `create_emr_application()` which creates a new EMR Serverless application
3. **The task log** contains: `EMR serverless application created: 00gXXXXXXXXX`
4. **Processing tasks** submit Spark jobs to this EMR application — job run IDs are also in the task logs
5. **finalise task** — stops and deletes the EMR application

### Debugging Workflow

**Quick (1 tool):** Use `diagnose_dag_failure(dag_id='...', env='dev')` — does everything automatically.

**Manual (step by step):**
```
Step 1: list_dag_runs(dag_id='...', env='dev')
        → Find the failed run

Step 2: get_dag_run_details(dag_id='...', dag_run_id='...', env='dev')
        → See ALL tasks, find which failed

Step 3: get_task_log(..., task_id='initialise', env='dev')
        → Extract EMR application ID (pattern: 00gXXXXX)

Step 4: get_task_log(..., task_id='<failed_task>', env='dev')
        → Extract job_run_id (pattern: 00gXXXXX)

Step 5: read_spark_driver_log(application_id='...', job_run_id='...', env='dev')
        → Reads stdout (Python app output) — this is the primary log
        → Use read_both=True to get stdout + stderr in one call
```

### DAG Status Overview

Use `get_dags_status_dashboard(env='dev')` to see ALL DAGs with their last run state, pause status, and schedule in one view. Failed DAGs get highlighted with `diagnose_dag_failure()` hints.

### DAG Run Stats & Trends

Use `dag_analytics(dag_id='...', days=14)` when the user asks about reliability, statistics, trends, or patterns. It provides analytics that `list_dag_runs` does NOT:

```
dag_analytics(dag_id='digital_taxonomy_processing', env='dev', days=14)
→ Overview: 14 runs, 11 success (78.6%), 3 failed (21.4%)
→ Duration: avg 42m, min 38m, max 1h 12m
→ Trend: 📈 Increasing (+15% — recent avg 45m vs older avg 39m)
→ Patterns: 2 of 3 failures on Monday
→ Recent: ✅ ✅ ❌ ✅ ✅ ✅ ✅ ❌ ✅ ✅
→ Day-by-day breakdown with emoji status per day
```

**When to use which:**
- `list_dag_runs` — flat list of individual runs (pick one to investigate)
- `dag_analytics` — aggregated statistics, trends, patterns (assess health)
- `get_dags_status_dashboard` — ALL DAGs at a glance (last run only per DAG)

### EMR Application Management

```
stop_emr_application(application_id='00gXXX', env='dev')
→ Gracefully stops the EMR app

stop_emr_application(application_id='00gXXX', env='dev', force=True)
→ Force-stop: auto-cancels ALL running/pending jobs first, then stops the app

delete_emr_application(application_id='00gXXX', env='dev')
→ Deletes a STOPPED or CREATED app permanently

delete_emr_application(application_id='00gXXX', env='dev', force=True)
→ Full lifecycle: cancels jobs → stops app → waits → deletes (one call)
```

**When to use which:**
- `stop_emr_application` — when a DAG failed before `finalise` and the app is still running (costing money). Use `force=True` when jobs are still running.
- `delete_emr_application` — when you want to permanently remove an app. Use `force=True` to stop-and-delete in one call. Without force, only works on already-stopped apps.
- The `finalise` task in each DAG normally stops and deletes the EMR app automatically. These tools are for when that doesn't happen.

---

## Azure DevOps Workflow

### Browse Source Code
```
list_repos()                                    → See all repos
browse_repo_recursive(repo_name='my-repo')      → See ENTIRE repo file tree in one call
browse_repo_recursive(repo_name='my-repo', extension_filter='.py')  → Only Python files
browse_repo(repo_name='my-repo', path='/src')   → Browse one folder at a time
read_repo_file(repo_name='my-repo', path='/src/main.py')  → Read file content
```

**When to use which:**
- `browse_repo_recursive` — use when the user asks "what files are in this repo?", "show me the structure", or needs to find correct paths. Returns the full tree with every file path.
- `browse_repo` — use for navigating one folder at a time when you already know which folder to look in.
- `read_repo_file` — use to read a file's content once you have the correct path from either browse tool.

### Sprint Board
```
get_current_sprint()         → What sprint are we in? When does it end?
get_sprint_work_items()      → All PBIs/Tasks/Bugs with assignees and effort
get_work_item_details(12345) → Full details of a specific item
get_backlog()                → Items not in current sprint
```

---

## S3 Browsing & File Reading Workflow

```
list_s3_buckets(env='dev')                                        → See all buckets in dev account
list_s3_buckets(env='prod')                                       → See all buckets in prod account
browse_s3(bucket='my-data-bucket', env='dev')                     → Top-level folders
browse_s3(bucket='my-data-bucket', prefix='raw/', env='dev')      → Navigate deeper
list_s3_recursive(bucket='my-data-bucket', env='dev')              → ALL files end-to-end recursively
list_s3_recursive(bucket='my-data-bucket', prefix='raw/', extension_filter='.csv', env='dev')  → Only CSVs under raw/
get_s3_object_info(s3_uri='s3://my-data-bucket/raw/hem/output.parquet', env='dev')  → File metadata
read_s3_file(s3_uri='s3://my-data-bucket/raw/hem/output.parquet', env='dev')        → Read parquet (shows schema + first 50 rows)
read_s3_file(s3_uri='s3://my-data-bucket/raw/data.csv', env='dev')                  → Read CSV/TXT/JSON as text
read_s3_file(s3_uri='s3://my-data-bucket/logs/app.log.gz', env='dev', search_text='ERROR')  → Search in gzipped log
```

**`read_s3_file` format support:**
- **Parquet** (`.parquet`, `.parquet.gz`) — displays column schema and first N rows as a table (`head_rows` controls row count, default 50)
- **Text** (`.csv`, `.txt`, `.json`, `.log`, `.gz`) — displays as text with optional `tail_lines` and `search_text` filtering
- **5 MB limit** — files larger than 5 MB are rejected with a size message (use `get_s3_object_info` to check size first)

---

## Environment Notes

- **AWS Region**: `eu-west-2` (London)
- **Environments**: dev / uat / test / prod — each is a **different AWS account**
- **AWS Profiles**: `consumersync-dev`, `consumersync-uat`, `consumersync-test`, `consumersync-prod`
- **Auth**: Run `gimme-aws-creds -p consumersync-dev` (or relevant profile) before starting
- **MWAA Environments**: dev / uat / test / prod (default: dev). Note: only dev has "consumersyncenv" in name; rest have "consumersync"
- **EMR Log Buckets**: per-env — `eec-aws-uk-ms-consumersync-{dev|uat|tst|prod}-logs-bucket`
- **Confluence Space**: `ACTIVATE` at `https://pages.experian.local`
- **Azure DevOps**: `https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection` — Project: `Activate`, Team: `Activate Team`
- **VPN Required**: Yes — for MWAA API, Confluence, and Azure DevOps access

---

## Key Parsing Rules for AI

1. **EMR Application IDs** look like: `00g` followed by alphanumeric characters (e.g., `00g2bg6Gupee0dt`)
2. **EMR Job Run IDs** look like: `00g` followed by alphanumeric characters
3. **DAG Run IDs** follow patterns like: `scheduled__2026-02-16T00:00:00+00:00` or `manual__...`
4. When a task log contains `Created EMR application` or `EMR serverless application created`, extract the ID
5. **Spark logs** default to **stdout** — this is the primary log with Python output and errors
6. Always check the `initialise` (or `ae_initialize_emr_application`) task first for the EMR app ID
7. All AWS tools accept `env`: `'dev'`, `'uat'`, `'test'`, or `'prod'` (default: dev). **ASK the user which env** if not specified.
8. Azure DevOps tools work without `AZDO_PAT` at server startup — they'll ask for it on first use
9. S3 tools can access **any bucket** in the selected env's account — use `list_s3_buckets(env='...')` first, then `browse_s3()` to navigate interactively
