# Ops Tools MCP Server

One chat window to talk to all your infrastructure — Airflow, EMR, S3, Confluence, and Azure DevOps.

No more jumping between 5 different UIs. Just ask what you want in plain English.

---

## What Is This?

It's an MCP (Model Context Protocol) server that gives AI assistants (like Gemini CLI) access to 43 tools across your entire ops stack. You talk to it in natural language, and it calls the right APIs for you.

**Example:**
> You: "Which DAGs failed today in prod?"
> AI: *calls the Airflow API, gets all runs, filters failures, shows you a summary with diagnosis commands*

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Your `.env` File

Copy the example and fill in your values:

```bash
cp .env.example .env
```

### 3. Run the Server

```bash
python -m mcp_server.main
```

The server runs on stdio — connect it to Gemini CLI, VS Code, or any MCP client.

### 4. Connect from Gemini CLI

Add this to your MCP config (`server.json`):

```json
{
  "mcpServers": {
    "ops-tools": {
      "command": "python",
      "args": ["-m", "mcp_server.main"],
      "cwd": "D:\\MCP"
    }
  }
}
```

---

## Environment Setup

This server works across **4 AWS accounts** (dev, uat, test, prod). Each is a separate AWS account with its own credentials.

### AWS Profiles (via gimme-aws-creds)

```env
AWS_REGION=eu-west-2

AWS_PROFILE_DEV=consumersync-dev
AWS_PROFILE_UAT=consumersync-uat
AWS_PROFILE_TEST=consumersync-test
AWS_PROFILE_PROD=consumersync-prod
```

### MWAA Environments (Airflow)

```env
MWAA_ENV_DEV=eec-aws-uk-ms-dev-consumersyncenv-mwaa
MWAA_ENV_UAT=eec-aws-uk-ms-uat-consumersync-mwaa
MWAA_ENV_TEST=eec-aws-uk-ms-tst-consumersync-mwaa
MWAA_ENV_PROD=eec-aws-uk-ms-prod-consumersync-mwaa
```

### EMR Log Buckets

```env
EMR_LOG_BUCKET_DEV=eec-aws-uk-ms-consumersync-dev-logs-bucket
EMR_LOG_BUCKET_UAT=eec-aws-uk-ms-consumersync-uat-logs-bucket
EMR_LOG_BUCKET_TEST=eec-aws-uk-ms-consumersync-tst-logs-bucket
EMR_LOG_BUCKET_PROD=eec-aws-uk-ms-consumersync-prod-logs-bucket
EMR_LOG_PREFIX=spark-logs
```

### Confluence

```env
CONFLUENCE_BASE_URL=https://pages.experian.local
CONFLUENCE_PAT=your-personal-access-token
CONFLUENCE_SPACE_KEY=ACTIVATE
```

### Azure DevOps (TFS)

```env
AZDO_BASE_URL=https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection
AZDO_PAT=your-personal-access-token
AZDO_PROJECT=Activate
AZDO_TEAM=Activate Team
```

> **Important:** The AI will always ask you "Which environment?" before calling any AWS tool. It never defaults silently — this prevents accidental cross-account mistakes.

---

## All 43 Tools

### Airflow / MWAA (11 tools)

Everything you need to monitor, debug, and manage your DAGs.

| Tool | What It Does |
|------|-------------|
| `list_dags` | Lists all DAGs with their schedule and pause status |
| `list_dag_runs` | Shows runs for today/yesterday/any date — numbered list so you can pick one |
| `get_dag_run_details` | Full task-level breakdown for a specific run — which tasks passed, which failed |
| `get_task_log` | Reads the Airflow log for a specific task attempt — the raw log output |
| `trigger_dag` | Manually kicks off a DAG run (with optional config) |
| `pause_dag` | Pauses a DAG so it won't run on schedule (already-running jobs finish) |
| `unpause_dag` | Unpauses a DAG so scheduled runs resume |
| `clear_task_instance` | Retries a failed task without re-running the entire DAG |
| `get_dag_source` | Shows the DAG's Python source code, tasks, operators, and dependencies |
| `get_dags_status_dashboard` | Full dashboard of ALL DAGs — states, schedules, failures, everything at a glance |
| `dag_analytics` | Analytics: success rate, duration trends, failure patterns, visual streaks |

**Common things you'd say:**
- "Show me all DAGs in dev"
- "Which DAGs failed today in prod?"
- "How has hem_processing been running lately?"
- "Trigger ttdcustom_processing in uat"
- "Pause digital_taxonomy in prod"
- "Retry the initialise task on yesterday's failed run"

---

### EMR Serverless (9 tools)

Manage Spark jobs, read driver logs, browse S3 log files, track costs.

| Tool | What It Does |
|------|-------------|
| `list_emr_applications` | Lists all EMR Serverless apps (note: DAGs create temporary apps that get cleaned up) |
| `list_job_runs` | Shows job runs for an application — with state and duration |
| `get_job_run_details` | Deep dive into a job: Spark config, resource usage, S3 log paths |
| `read_spark_driver_log` | Reads stdout/stderr from the Spark driver — the actual Python output and errors |
| `browse_s3_logs` | Navigates the S3 log directory structure folder by folder |
| `cancel_job_run` | Cancels a running or stuck Spark job |
| `stop_emr_application` | Stops an EMR app — auto-cancels running jobs if needed |
| `read_s3_file` | Reads any file from S3 (CSV, TXT, JSON, Parquet) — 5 MB limit, auto-detects format |
| `get_emr_cost_summary` | Shows vCPU hours, memory, storage usage — broken down per app |

**Common things you'd say:**
- "Show me the Spark driver log for this job"
- "What failed in the stdout log?"
- "Cancel that stuck job"
- "Stop that EMR application"
- "Force-stop the app and cancel all running jobs"
- "How much has EMR cost us this week?"
- "Read this S3 file: s3://bucket/path/to/file.csv"

---

### S3 — General (4 tools)

Browse any S3 bucket in the account — not just EMR logs.

| Tool | What It Does |
|------|-------------|
| `list_s3_buckets` | Lists all S3 buckets in the AWS account |
| `browse_s3` | Interactive folder/file browsing — like a file explorer for S3 |
| `list_s3_recursive` | Recursively lists ALL files end-to-end with filters and size summary |
| `get_s3_object_info` | Shows file metadata (size, modified date, content type, encryption) without downloading |

**Common things you'd say:**
- "What S3 buckets do we have in dev?"
- "Show me what's in the raw data bucket"
- "List all CSV files in the raw bucket"
- "How much data is in this S3 folder?"
- "Read this parquet file from S3"
- "How big is this file?"

---

### Confluence (9 tools)

Search, read, and write documentation — without opening a browser.

| Tool | What It Does |
|------|-------------|
| `search_confluence` | Full-text search across pages — ranked by relevance (same as the web UI) |
| `get_page_content` | Reads a page's full content — converted from HTML to clean text |
| `get_child_pages` | Lists all child pages under a parent page |
| `get_space_pages` | Lists all pages in a space (paginated) |
| `get_page_attachments` | Lists file attachments on a page (name, size, download URL) |
| `get_page_labels` | Shows tags/labels on a page |
| `get_page_comments` | Reads comments and discussions on a page |
| `create_confluence_page` | Creates a new page (plain text or HTML content) |
| `update_confluence_page` | Updates an existing page — replace or append content |

**Common things you'd say:**
- "Find documentation about Audience Engine"
- "Read that runbook page"
- "Create a new troubleshooting guide under the runbooks section"
- "What are the child pages under the HEM documentation?"

> **Pro tip:** When you say "docs", "documentation", "wiki", or "runbook", the AI knows to search Confluence automatically.

---

### Azure DevOps / TFS (8 tools)

Sprint tracking, work items, source code — all from chat.

| Tool | What It Does |
|------|-------------|
| `list_repos` | Lists all Git repositories in the project |
| `browse_repo` | Browse files and folders in a repo — one folder at a time |
| `browse_repo_recursive` | Full recursive file tree of a repo in one call — shows every file with correct paths |
| `read_repo_file` | Read the content of any file (with syntax highlighting) |
| `get_current_sprint` | Shows active sprint name, dates, and days remaining |
| `get_sprint_work_items` | All PBIs, Tasks, and Bugs in the sprint — who's doing what |
| `get_work_item_details` | Full details for a PBI/Task/Bug: description, acceptance criteria, links |
| `get_backlog` | Items not in the current sprint — what's coming next |

**Common things you'd say:**
- "What sprint are we in?"
- "What's everyone working on?"
- "Show me PBI 12345"
- "What's in the backlog?"
- "Show me all the files in the hem_processing repo"
- "List all Python files in this repo"
- "What's the folder structure of this repo?"

---

### Orchestration (1 tool)

The power tool — chains multiple tools together for one-shot answers.

| Tool | What It Does |
|------|-------------|
| `diagnose_dag_failure` | Complete failure diagnosis in one call — finds the failed run, reads task logs, extracts EMR IDs, reads Spark driver logs, returns root cause analysis |

**What you'd say:**
- "Diagnose the failure for hem_processing in prod"
- "What went wrong with ttdcustom_processing yesterday?"

This one tool replaces 5-6 manual steps that used to take 20 minutes.

---

### Utility (1 tool)

| Tool | What It Does |
|------|-------------|
| `server_health_check` | Confirms the server is running and connected |

---

## How It Works

```
You (plain English) → AI (Gemini/Claude) → MCP Server → APIs (Airflow, EMR, S3, Confluence, TFS)
```

1. You type a question in natural language
2. The AI figures out which tool(s) to call
3. The MCP server calls the actual APIs (MWAA, boto3, Confluence REST, Azure DevOps REST)
4. Results come back formatted and readable
5. The AI can chain tools together — e.g. find a failed run → read its logs → show root cause

---

## Architecture

```
D:\MCP\
├── mcp_server/
│   ├── main.py              # Server entry point + tool registration
│   ├── config.py             # Environment config (4 AWS accounts)
│   └── tools/
│       ├── _aws_helpers.py   # Shared AWS helpers (S3 client, formatting)
│       ├── mwaa_tools.py     # 11 Airflow tools
│       ├── emr_tools.py      # 9 EMR Serverless tools
│       ├── s3_tools.py       # 4 general S3 tools
│       ├── confluence_tools.py # 9 Confluence tools
│       ├── azdo_tools.py     # 8 Azure DevOps tools
│       ├── orchestration_tools.py # 1 orchestration tool
│       └── utility_tools.py  # 1 utility tool
├── .env                      # Your local config (not committed)
├── .env.example              # Template for .env
├── server.json               # MCP client config
├── requirements.txt          # Python dependencies
├── DEMO_SCRIPT.md            # 15-minute demo walkthrough
└── README.md                 # This file
```

---

## Key Design Decisions

- **Fresh credentials every call** — No client caching for S3 or EMR. Every API call gets a fresh boto3 session so expired credentials never cause silent failures.
- **Environment-aware** — All AWS tools require you to specify dev/uat/test/prod. The AI asks if you forget. Each env points to a different AWS account.
- **MWAA session cache** — The Airflow login token is cached (it needs auth cookies), but the cache clears automatically on 401/403 errors and retries.
- **Clean log output** — Spark driver logs are auto-decompressed from .gz, Confluence HTML is converted to clean markdown text.
- **Interactive responses** — DAG runs are numbered so you can say "tell me about run #3". Work items show ready-to-use follow-up commands.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "Cannot connect to MWAA webserver" | Connect to VPN first |
| "Access denied" on S3 | Run `gimme-aws-creds` to refresh your AWS credentials |
| "CONFLUENCE_PAT not set" | Add your Confluence Personal Access Token to `.env` |
| "AZDO_PAT not set" | Generate a PAT in Azure DevOps → User Settings → Personal Access Tokens |
| AI calls all environments at once | The server instructions should prevent this — if it happens, say "just dev" |
| Stale Airflow session | The server auto-retries on 401/403 — if it persists, restart the server |

---

## Requirements

- Python 3.10+
- VPN access (for MWAA, Confluence, Azure DevOps)
- `gimme-aws-creds` configured for all 4 AWS accounts
- Confluence PAT
- Azure DevOps PAT
- MCP-compatible client (Gemini CLI, VS Code, Claude Code, etc.)
