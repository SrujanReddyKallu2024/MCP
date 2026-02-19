# Ops-Tools MCP Server — AI Client Prompt

## What This MCP Server Does

You have access to an **ops-tools** MCP server with **36 tools** across five domains:
- **AWS MWAA (Airflow)** — DAG listing, runs, task logs, triggering, pause/unpause, retry, status dashboard
- **AWS EMR Serverless** — Spark applications, job runs, driver logs from S3, cost summary
- **Confluence** — Documentation search, page reading, navigation, creation, updates
- **Azure DevOps (TFS)** — Git repos, file browsing, sprints, work items, backlog
- **Orchestration** — One-shot DAG failure diagnosis

The environment is the **ConsumerSync** team at Experian, running marketplace data processing pipelines on AWS.

---

## Complete Tool Reference

### MWAA / Airflow Tools (10 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_dags` | List all DAGs with schedule & pause status | `env`, `limit`, `only_active` |
| `get_dag_runs` | DAG runs for today/yesterday/date with pass/fail | `env`, `dag_id`, `date`, `limit` |
| `get_dag_run_details` | Full task-level breakdown of one run | `dag_id`, `dag_run_id`, `env` |
| `get_task_log` | Read Airflow log for a single task attempt | `dag_id`, `dag_run_id`, `task_id`, `env`, `try_number`, `tail_lines` |
| `trigger_dag` | Manually trigger a DAG | `dag_id`, `env`, `conf` (JSON string) |
| `pause_dag` | Pause a DAG to prevent scheduled runs | `dag_id`, `env` |
| `unpause_dag` | Unpause a DAG to resume scheduling | `dag_id`, `env` |
| `clear_task_instance` | Retry a failed task without re-triggering the DAG | `dag_id`, `dag_run_id`, `task_id`, `env`, `include_downstream` |
| `get_dag_source` | Get DAG source code, tasks, and metadata | `dag_id`, `env` |
| `get_dag_status_report` | Full dashboard of ALL DAGs with last run status | `env`, `limit` |

### EMR Serverless Tools (8 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_emr_applications` | List all EMR Serverless apps | `states` (e.g. 'STARTED,CREATED') |
| `list_job_runs` | List job runs for an EMR app | `application_id`, `states`, `created_after` |
| `get_job_run_details` | Detailed info about a job run | `application_id`, `job_run_id` |
| `read_spark_driver_log` | Read Spark driver stdout/stderr from S3 | `application_id`, `job_run_id`, `log_type`, `tail_lines`, `search_text`, `read_both` |
| `browse_s3_logs` | Navigate S3 log directory structure | `prefix`, `bucket` |
| `cancel_job_run` | Cancel a running or pending job | `application_id`, `job_run_id` |
| `read_s3_file` | Read any file from S3 by URI | `s3_uri`, `tail_lines`, `search_text` |
| `get_emr_cost_summary` | Resource usage and cost summary across jobs | `application_id`, `days` |

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

### Azure DevOps / TFS Tools (7 tools)

> **Note:** These tools require `AZDO_PAT` to be set. The server starts without it — the tools will prompt for the PAT when first used.

| Tool | Purpose | Key Args |
|------|---------|----------|
| `list_repos` | List all Git repos in the project | `project` |
| `browse_repo` | Browse files/folders in a repo (directory listing) | `repo_name`, `path`, `branch`, `project` |
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
| `server_health_check` | Test connectivity to all services | *(none)* |

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

**Quick (1 tool):** Use `diagnose_dag_failure(dag_id='...')` — does everything automatically.

**Manual (step by step):**
```
Step 1: get_dag_runs(dag_id='...', env='dev')
        → Find the failed run

Step 2: get_dag_run_details(dag_id='...', dag_run_id='...')
        → See ALL tasks, find which failed

Step 3: get_task_log(..., task_id='initialise')
        → Extract EMR application ID (pattern: 00gXXXXX)

Step 4: get_task_log(..., task_id='<failed_task>')
        → Extract job_run_id (pattern: 00gXXXXX)

Step 5: read_spark_driver_log(application_id='...', job_run_id='...')
        → Reads stdout (Python app output) — this is the primary log
        → Use read_both=True to get stdout + stderr in one call
```

### DAG Status Overview

Use `get_dag_status_report(env='dev')` to see ALL DAGs with their last run state, pause status, and schedule in one view. Failed DAGs get highlighted with `diagnose_dag_failure()` hints.

---

## Azure DevOps Workflow

### Browse Source Code
```
list_repos()                                    → See all repos
browse_repo(repo_name='my-repo')                → See root directory
browse_repo(repo_name='my-repo', path='/src')   → Navigate deeper
read_repo_file(repo_name='my-repo', path='/src/main.py')  → Read file
```

### Sprint Board
```
get_current_sprint()         → What sprint are we in? When does it end?
get_sprint_work_items()      → All PBIs/Tasks/Bugs with assignees and effort
get_work_item_details(12345) → Full details of a specific item
get_backlog()                → Items not in current sprint
```

---

## Environment Notes

- **AWS Region**: `eu-west-2` (London)
- **AWS Profile**: `consumersync` (requires `gimme-aws-creds -p consumersync` for auth)
- **MWAA Environments**: dev / test / prod (default: dev)
- **EMR Log Bucket**: `eec-aws-uk-ms-consumersync-dev-logs-bucket`
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
7. All MWAA tools accept `env`: `'dev'`, `'test'`, or `'prod'` (default: dev)
8. Azure DevOps tools work without `AZDO_PAT` at server startup — they'll ask for it on first use
