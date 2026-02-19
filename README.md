# Ops MCP Server

A local MCP server that gives AI assistants (GitHub Copilot, Gemini CLI, Claude) access to the systems we use daily:

- **MWAA (Airflow)** — check DAG runs, read task logs, trigger/pause/unpause DAGs, retry tasks, view history
- **EMR Serverless** — read Spark application logs from S3, cancel jobs, browse any S3 file, cost summary
- **Confluence** — search and read documentation pages, create and update pages
- **Orchestration** — one-shot DAG failure diagnosis (chains 5+ tools automatically)
- **Utilities** — server health check to verify all connections

Runs on your machine via stdio transport.

---

## Setup

### 1. Install

```bash
pip install -e .
```

### 2. Configure

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

You'll need:
- **AWS profile** — run `gimme-aws-creds -p consumersync` before starting
- **MWAA environment name** — at least one (dev, test, or prod)
- **Confluence PAT** — generate one from your Confluence profile
- **EMR log bucket** — the S3 bucket where Spark logs are stored

### 3. Connect to your AI client

**VS Code (GitHub Copilot)** — add to `.vscode/mcp.json`:
```json
{
  "servers": {
    "ops-tools": {
      "command": "python",
      "args": ["-m", "mcp_server.main"],
      "cwd": "/path/to/this/repo"
    }
  }
}
```

**Gemini CLI** — add to `.gemini/settings.json`:
```json
{
  "mcpServers": {
    "ops-tools": {
      "command": "python",
      "args": ["-m", "mcp_server.main"],
      "cwd": "/path/to/this/repo"
    }
  }
}
```

---

## Tools

### Utilities (1)

| Tool | What it does |
|------|-------------|
| `server_health_check` | Verify connectivity to AWS, MWAA, EMR, S3, Confluence |

### Airflow / MWAA (10)

| Tool | What it does |
|------|-------------|
| `list_dags` | List all DAGs with schedule and status |
| `get_dag_runs_today` | Show today's runs — which passed, which failed |
| `get_dag_runs_by_date` | Historical runs — check any date or 'yesterday'/'last_week' |
| `get_dag_run_details` | Full task breakdown for a specific run |
| `get_task_log` | Read the log of a single task attempt |
| `trigger_dag` | Manually trigger a DAG |
| `pause_dag` | Pause a DAG — stop scheduled runs |
| `unpause_dag` | Unpause a DAG — resume scheduled runs |
| `clear_task_instance` | Retry a failed task without re-triggering the DAG |
| `get_dag_source` | View DAG source code, tasks, and dependencies |

### EMR Serverless (8)

| Tool | What it does |
|------|-------------|
| `list_emr_applications` | List active EMR Serverless applications |
| `list_job_runs` | List Spark jobs for an EMR application |
| `get_job_run_details` | Job config, arguments, resource usage |
| `read_spark_driver_log` | Read Spark stdout/stderr from S3 |
| `browse_s3_logs` | Navigate the S3 log folder structure |
| `cancel_job_run` | Cancel a stuck or failing Spark job |
| `read_s3_file` | Read any S3 file (input data, configs, outputs) |
| `get_emr_cost_summary` | Aggregate resource usage across recent jobs |

### Confluence (10)

| Tool | What it does |
|------|-------------|
| `search_confluence` | Full-text search (same as Confluence search bar) |
| `get_page_content` | Read a page as clean text |
| `get_child_pages` | List child pages under a parent |
| `get_space_pages` | List all pages in a space |
| `get_page_by_title` | Find a page by exact title |
| `get_page_attachments` | List files attached to a page |
| `get_page_labels` | Get tags on a page |
| `get_page_comments` | Read comments and discussions |
| `create_confluence_page` | Create a new page (incident reports, runbooks) |
| `update_confluence_page` | Update or append to an existing page |

### Orchestration (1)

| Tool | What it does |
|------|-------------|
| `diagnose_dag_failure` | One-shot failure diagnosis: finds failed run, reads task logs, extracts EMR IDs, reads Spark logs — all in one call |

---

## How the AI debugs a failed DAG

### Quick way (one tool call)

```
You: "Why did ttdcustom_processing fail?"

AI does:
1. diagnose_dag_failure(dag_id='ttdcustom_processing')
   → Automatically finds the failed run
   → Reads all failed task logs
   → Extracts EMR application ID from 'initialise' task
   → Reads Spark driver stdout
   → Returns complete diagnosis
```

### Manual way (step by step, more control)

```
You: "Why did ttdcustom_processing fail?"

AI does:
1. get_dag_runs_today → finds the failed run
2. get_dag_run_details → sees 'process_data_files' task FAILED
3. get_task_log('initialise') → finds EMR app ID: 00g3cmcm2id6f50t
4. get_task_log('process_data_files') → finds Spark job ID: 00g1dt0gdqt8g80v
5. read_spark_driver_log(log_type='stdout') → reads actual Python output
6. Tells you: "The job processed 2 files but found 0 matches after joining
   with match_id lookup. The data file date 2025-08-05 doesn't match the
   execution date 2025-08-03."
```

---

## Project structure

```
├── .env.example          # Template — copy to .env
├── .gitignore
├── pyproject.toml         # Package config
├── server.json            # MCP Registry entry (Experian format)
├── README.md
└── mcp_server/
    ├── __init__.py
    ├── config.py               # Loads .env, validates config
    ├── main.py                 # FastMCP server, registers all tools
    └── tools/
        ├── utility_tools.py        # 1 utility tool
        ├── mwaa_tools.py           # 10 Airflow tools
        ├── emr_tools.py            # 8 EMR Serverless tools
        ├── confluence_tools.py     # 10 Confluence tools
        └── orchestration_tools.py  # 1 orchestration tool
```

---

## Author

Kallu Srujan Reddy — EMS ConsumerSync, Experian
"# MCP" 
"# MCP" 
