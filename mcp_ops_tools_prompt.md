# Ops-Tools MCP Server — AI Client Prompt

## What This MCP Server Does

You have access to an **ops-tools** MCP server with **18 tools** across three domains:
- **AWS MWAA (Airflow)** — DAG runs, task logs, triggering
- **AWS EMR Serverless** — Spark applications, job runs, driver logs from S3
- **Confluence** — Documentation search, page reading, navigation

The environment is the **ConsumerSync** team at Experian, running marketplace data processing pipelines on AWS.

---

## Complete Tool Reference

### MWAA / Airflow Tools (5 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| [list_dags](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/mwaa_tools.py#235-277) | List all DAGs | [env](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/.env) (dev/test/prod), `limit`, `only_active` |
| [get_dag_runs_today](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/mwaa_tools.py#279-346) | All DAG runs triggered today | [env](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/.env), `dag_id` (optional filter) |
| [get_dag_run_details](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/mwaa_tools.py#348-425) | Full task-level breakdown of one run | `dag_id`, `dag_run_id`, [env](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/.env) |
| [get_task_log](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/mwaa_tools.py#343-390) | Read Airflow log for a single task attempt | `dag_id`, `dag_run_id`, `task_id`, [env](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/.env), `try_number`, `tail_lines` |
| [trigger_dag](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/mwaa_tools.py#392-421) | Manually trigger a DAG | `dag_id`, [env](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/.env), [conf](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#235-333) (JSON string) |

### EMR Serverless Tools (5 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| [list_emr_applications](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/emr_tools.py#144-180) | List all EMR Serverless apps | `states` (e.g. 'STARTED,CREATED') |
| [list_job_runs](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/emr_tools.py#182-233) | List job runs for an EMR app | `application_id`, `states`, `created_after` |
| [get_job_run_details](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/emr_tools.py#235-310) | Detailed info about a job run | `application_id`, `job_run_id` |
| [read_spark_driver_log](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/emr_tools.py#312-389) | Read Spark driver stderr/stdout from S3 | `application_id`, `job_run_id`, `log_type`, `tail_lines`, `search_text` |
| [browse_s3_logs](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/emr_tools.py#470-538) | Navigate S3 log directory structure | `prefix`, `bucket` |

### Confluence Tools (8 tools)

| Tool | Purpose | Key Args |
|------|---------|----------|
| [search_confluence](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#235-333) | Full-text search (same as web UI) | `query`, `space_key`, `ancestor_page_id` |
| [get_page_content](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#335-430) | Read full page content | `page_id` or [title](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#543-572) |
| [get_child_pages](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#432-486) | List child pages of a parent | `page_id`, `include_content` |
| [get_space_pages](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#488-541) | List all pages in a space | `space_key` |
| [get_page_by_title](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#543-572) | Find page by exact title | [title](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#543-572), `space_key` |
| [get_page_attachments](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#574-620) | List file attachments on a page | `page_id` |
| [get_page_labels](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#633-653) | Get labels/tags on a page | `page_id` |
| [get_page_comments](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#655-695) | Get discussion comments | `page_id` |

---

## Critical Workflow: DAGs Create EMR Applications Dynamically

> [!IMPORTANT]
> **EMR application IDs are NOT known in advance.** Each DAG run creates a new EMR Serverless application at runtime. The application ID is logged in the Airflow task logs and pushed to XCom. You MUST read the task logs to discover the EMR application ID.

### How the Pipeline Works

1. **DAG starts** → runs setup tasks (create arguments, check inputs)
2. **[initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264) task** → calls `create_emr_application()` which creates a new EMR Serverless application with a timestamped name (e.g., `stackadapt-20260216143000`, `audience-engine-20260216093500`)
3. **The task log** contains lines like:
   ```
   INFO - Created EMR application 00g2bg6Gupee0dt
   INFO - EMR serverless application created: 00g2bg6Gupee0dt
   INFO - Starting application 00g2bg6Gupee0dt
   INFO - Serverless Application status is: STARTING
   ```
4. **Processing tasks** submit Spark jobs to this EMR application → job run IDs are also in the task logs:
   ```
   INFO - Created EMR application ttdcustom-processing-20260212095606 identifier 00g3cnc21d5f9t
   INFO - S3 logs available at: s3://eec-aws-uk-ms-consumersync-dev-logs-bucket/...
   ```
5. **[finalise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#546-554) task** → stops and deletes the EMR application

### Debugging Workflow (Step by Step)

When debugging a failed or running DAG:

```
Step 1: get_dag_runs_today(env='dev')
        → Find the DAG run (look for ❌ failed or ⏳ running)
        → Note the dag_id and dag_run_id

Step 2: get_dag_run_details(dag_id='...', dag_run_id='...', env='dev')
        → See ALL tasks with status, duration, tries
        → Find which task(s) failed

Step 3: get_task_log(dag_id='...', dag_run_id='...', task_id='initialise', env='dev')
        → Read the initialise/ae_initialize_emr_application task log
        → EXTRACT the EMR application ID (looks like: 00gXXXXXXXXXXX)

Step 4: list_job_runs(application_id='00gXXXXXXXXXXX')
        → See all Spark jobs submitted to this EMR app
        → Note any FAILED job_run_id

Step 5: read_spark_driver_log(application_id='00gXXXXXXXXXXX', job_run_id='...')
        → Read the actual Spark stderr log to find the root cause
        → Use search_text='ERROR' or search_text='Exception' to filter

Step 6: If needed, browse_s3_logs() to navigate the S3 log directory
```

---

## Known DAGs and Their Structure

### `audience_engine_processing`
- **Purpose**: Process Audience Engine data end-to-end
- **Schedule**: `0 10 13 * *`
- **Key tasks**: `ae_setup_script_arguments` → `ae_initialize_emr_application` → `ae_validate_input_files` → stats tasks → `ae_data_ingestion_module1` → `ae_minmax_processor` → `ae_excel_builder` → delivery → `ae_cleanup_finalise`
- **EMR init task**: `ae_initialize_emr_application`

### `ttdcustom_processing`
- **Purpose**: Process TTD Custom metadata and segment data ingestion
- **Schedule**: `0 11 * * *`
- **Key tasks**: [start](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#105-166) → `check_for_new_ttdcustom_files` → [create_script_arguments](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#154-210) → [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264) → [check_input](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#211-253) → `process_metadata_files` → `process_data_files` → delivery → [finalise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#546-554)
- **EMR init task**: [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264)

### [stackadapt_processing](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#415-468)
- **Purpose**: Generate StackAdapt marketplace data
- **Schedule**: `0 12 * * 1` (weekly Monday)
- **Key tasks**: [start](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/MCP/mcp_server/tools/confluence_tools.py#105-166) → [create_script_arguments](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#154-210) → [check_inputs](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#211-253) → [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264) → stats collection → [run_stackadapt_processing](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#415-468) → [rename_output_files](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#469-499) → [finalise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#546-554)
- **EMR init task**: [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264)

### Common DAG Pattern
All marketplace DAGs follow a similar pattern:
1. **Setup** — create arguments, validate inputs
2. **Initialise** — create EMR Serverless application (the task name varies: [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264) or `ae_initialize_emr_application`)
3. **Processing** — submit Spark jobs to the EMR app
4. **Delivery** — move output files, send notifications
5. **Finalise** — cleanup EMR application (always runs)

---

## Example Prompts and Expected AI Behavior

### 1. "What failed today?"
```
AI chains:
  get_dag_runs_today(env='dev')
  → Finds failed run: s3-housekeeper-tagging
  get_dag_run_details(dag_id='s3-housekeeper-tagging', dag_run_id='manual__2026-02-16T08:32:35...')
  → Shows which task failed
  get_task_log(dag_id=..., task_id='<failed_task>')
  → Reads the error log
  → Summarizes the root cause
```

### 2. "Why did audience_engine_processing fail?"
```
AI chains:
  get_dag_runs_today(env='dev', dag_id='audience_engine_processing')
  → Finds the failed run
  get_dag_run_details(...)
  → Finds ae_data_ingestion_module1 FAILED
  get_task_log(..., task_id='ae_data_ingestion_module1')
  → Sees Spark job submitted, but needs Spark logs
  get_task_log(..., task_id='ae_initialize_emr_application')
  → EXTRACTS EMR app ID: 00g2bg6Gupee0dt
  list_job_runs(application_id='00g2bg6Gupee0dt', states='FAILED')
  → Finds the failed Spark job
  read_spark_driver_log(application_id='00g2bg6Gupee0dt', job_run_id='...')
  → Reads Spark stderr → finds the actual Python/Spark exception
```

### 3. "Tell me about the Audience Engine process"
```
AI chains:
  search_confluence('Audience Engine')
  → Finds "Audience Engine Process Walkthrough" (ID: 1877449965)
  get_page_content(page_id='1877449965')
  → Reads the full page with code blocks, links, images
  get_child_pages(page_id='1877449965')
  → Discovers sub-pages for deeper reading
  → Comprehensive summary with code snippets and links
```

### 4. "Check the Spark logs for the latest ttdcustom run"
```
AI chains:
  get_dag_runs_today(dag_id='ttdcustom_processing')
  → Finds latest run
  get_dag_run_details(...)
  → Gets all tasks
  get_task_log(..., task_id='initialise')
  → Extracts EMR app ID: 00g3cnc21d5f9t
  list_job_runs(application_id='00g3cnc21d5f9t')
  → Lists all Spark jobs
  read_spark_driver_log(application_id='00g3cnc21d5f9t', job_run_id='...')
  → Shows Spark driver log
```

### 5. "Trigger a manual run of stackadapt and monitor it"
```
AI chains:
  trigger_dag(dag_id='stackadapt_processing', env='dev')
  → Triggers the DAG, gets run ID
  get_dag_run_details(dag_id='stackadapt_processing', dag_run_id='...')
  → Monitors progress, checks task states
```

---

## Environment Notes

- **AWS Region**: `eu-west-2` (London)
- **AWS Profile**: `consumersync` (requires `gimme-aws-creds -p consumersync` for auth)
- **MWAA Dev Environment**: `eec-aws-uk-ms-dev-consumersyncenv-mwaa`
- **EMR Log Bucket**: `eec-aws-uk-ms-consumersync-dev-logs-bucket`
- **Confluence Space**: `ACTIVATE` (EMS Activate) at `https://pages.experian.local`
- **VPN Required**: Yes — for MWAA API access
- **Auth Method**: Token-based (create_web_login_token → session cookie, with CLI token fallback)

---

## Key Parsing Rules for AI

1. **EMR Application IDs** look like: `00g` followed by alphanumeric characters (e.g., `00g2bg6Gupee0dt`, `00g3cnc21d5f9t`)
2. **EMR Job Run IDs** look like: `00g` followed by alphanumeric characters
3. **DAG Run IDs** follow patterns like: `scheduled__2026-02-16T00:00:00+00:00` or `manual__2026-02-16T08:55:02+00:00`
4. When a task log contains `Created EMR application` or `EMR serverless application created`, extract the ID that follows
5. When a task log contains `S3 logs available at: s3://...`, that's the S3 path to the Spark logs
6. Always check the [initialise](file:///c:/Users/c13505e/OneDrive%20-%20EXPERIAN%20SERVICES%20CORP/ems-code-marketplaces-stackadapt/src/dags/stackadapt_dag.py#254-264) (or `ae_initialize_emr_application`) task first to get the EMR app ID before trying to read Spark logs
