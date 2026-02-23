# Ops Tools MCP Server — Setup & Usage Guide

**Version:** 1.6.0
**Team:** ConsumerSync, Experian UK
**What it does:** Talk to Airflow, EMR, S3, Confluence, and Azure DevOps in plain English from VS Code. 44 tools, one chat window.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Install uv (First Time Only)](#2-install-uv-first-time-only)
3. [Install the MCP Server from Registry](#3-install-the-mcp-server-from-registry)
4. [Fix the Configuration](#4-fix-the-configuration)
5. [Set Up Your Tokens](#5-set-up-your-tokens)
6. [Refresh AWS Credentials](#6-refresh-aws-credentials)
7. [Start the Server](#7-start-the-server)
8. [Switch to Agent Mode](#8-switch-to-agent-mode)
9. [All 44 Tools — What They Do](#9-all-44-tools--what-they-do)
10. [How to Use It — Real Examples](#10-how-to-use-it--real-examples)
11. [Troubleshooting](#11-troubleshooting)
12. [Tips for Getting the Best Results](#12-tips-for-getting-the-best-results)

---

## 1. Prerequisites

Before you start, make sure you have:

- **VS Code** with the **GitHub Copilot** extension (or any MCP-compatible extension)
- **VPN connected** — required for Airflow, Confluence, and Azure DevOps
- **gimme-aws-creds** set up for all 4 AWS accounts
- A **Confluence Personal Access Token**
- An **Azure DevOps Personal Access Token** (optional — only needed for TFS tools)

---

## 2. Install uv (First Time Only)

`uv` is the tool that runs the MCP server. If you don't have it yet, open **PowerShell** and run:

```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then add it to your PATH:

```powershell
[Environment]::SetEnvironmentVariable(
    "Path",
    $env:Path + ";$env:USERPROFILE\.local\bin",
    [EnvironmentVariableTarget]::User
)
```

Close and reopen your terminal (or restart VS Code) for the PATH change to take effect.

To verify it worked:

```powershell
uvx --version
```

You should see a version number. If you get "command not found", restart VS Code and try again.

---

## 3. Install the MCP Server from Registry

1. Go to the MCP Registry: **https://mcp-registry.mn-na-sit.preprod-ascend-na.io/**
2. Find **ops-mcp-server-consumersync** in the list
3. Click **Install**

<!-- [IMAGE: Registry page showing the Install button] -->

You'll get an error — that's normal. Don't worry about it.

4. Click **Open Configuration** when prompted

<!-- [IMAGE: Error message with Open Configuration button] -->

This opens the MCP configuration file in VS Code. You need to replace its contents with the correct config (next step).

---

## 4. Fix the Configuration

Replace whatever is in the config file with this:

```json
{
  "servers": {
    "local.experian/ops-mcp-server-consumersync": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--native-tls",
        "--index-url",
        "https://artifacts.experian.local/artifactory/api/pypi/pypi/simple",
        "ops-mcp-server@1.6.0"
      ],
      "env": {
        "AWS_REGION": "eu-west-2",
        "DEFAULT_ENV": "dev",
        "AWS_PROFILE_DEV": "consumersync",
        "AWS_PROFILE_UAT": "consumersync-uat",
        "AWS_PROFILE_TEST": "consumersync-test",
        "AWS_PROFILE_PROD": "consumersync-prod",
        "MWAA_ENV_DEV": "eec-aws-uk-ms-dev-consumersyncenv-mwaa",
        "MWAA_ENV_UAT": "eec-aws-uk-ms-uat-consumersync-mwaa",
        "MWAA_ENV_TEST": "eec-aws-uk-ms-tst-consumersync-mwaa",
        "MWAA_ENV_PROD": "eec-aws-uk-ms-prod-consumersync-mwaa",
        "EMR_LOG_BUCKET_DEV": "eec-aws-uk-ms-consumersync-dev-logs-bucket",
        "EMR_LOG_BUCKET_UAT": "eec-aws-uk-ms-consumersync-uat-logs-bucket",
        "EMR_LOG_BUCKET_TEST": "eec-aws-uk-ms-consumersync-tst-logs-bucket",
        "EMR_LOG_BUCKET_PROD": "eec-aws-uk-ms-consumersync-prod-logs-bucket",
        "EMR_LOG_PREFIX": "spark-logs",
        "CONFLUENCE_BASE_URL": "https://pages.experian.local",
        "CONFLUENCE_PAT": "",
        "CONFLUENCE_SPACE_KEY": "ACTIVATE",
        "AZDO_BASE_URL": "https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection",
        "AZDO_PAT": "",
        "AZDO_PROJECT": "Activate",
        "AZDO_TEAM": "Activate Team"
      }
    }
  }
}
```

Three things to pay attention to:

- **`--native-tls`** in the args — required for Experian's internal certificates. Won't be there by default.
- **`--index-url`** pointing to Artifactory — so it downloads from our internal PyPI, not the public one. Won't be there by default.
- **`AWS_PROFILE_*` names must match your CLI profiles exactly.** When you run `gimme-aws-creds -p consumersync`, it creates a profile called `consumersync` in your `~/.aws/credentials` file. The `AWS_PROFILE_DEV` value in the config above must be that exact same name. If your profile is called something different (check `~/.aws/credentials` or run `aws configure list-profiles`), update the config to match. A mismatch here means "Access denied" on every AWS call.

Save the file.

---

## 5. Set Up Your Tokens

You need to fill in two tokens in the config above. Both go in the `"env"` section.

### Confluence PAT

1. Go to **https://pages.experian.local**
2. Click your profile icon (top right) > **Settings**
3. Go to **Personal Access Tokens**
4. Click **Create token**, give it a name, click **Create**
5. Copy the token and paste it into `"CONFLUENCE_PAT": "your-token-here"`

<!-- [IMAGE: Confluence PAT creation page] -->

### Azure DevOps PAT

1. Go to **https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection**
2. Click your profile icon (top right) > **Security**
3. Go to **Personal Access Tokens**
4. Click **New Token**, set scope to **Full access**, click **Create**
5. Copy the token and paste it into `"AZDO_PAT": "your-token-here"`

<!-- [IMAGE: Azure DevOps PAT creation page] -->

Save the config file after adding both tokens.

---

## 6. Refresh AWS Credentials

Before using any AWS tools (Airflow, EMR, S3), you need active credentials. Run this in your terminal:

```powershell
gimme-aws-creds -p consumersync
```

This gives you credentials for the dev account. For other environments:

```powershell
gimme-aws-creds -p consumersync-uat
gimme-aws-creds -p consumersync-test
gimme-aws-creds -p consumersync-prod
```

Credentials expire after a few hours. If you get "Access denied" or "Expired token" errors later, just run the command again.

**The profile name you use here must match the `AWS_PROFILE_*` values in your config.** For example, if you run `gimme-aws-creds -p consumersync`, the config must have `"AWS_PROFILE_DEV": "consumersync"`. If your team uses a different profile name (like `consumersync-dev` instead of `consumersync`), update the config to match. You can check your profile names by looking at `~/.aws/credentials` or running `aws configure list-profiles`.

---

## 7. Start the Server

1. Open VS Code
2. Open the **Copilot Chat** panel (or your MCP client panel)
3. You should see the MCP server listed with a small icon next to it

<!-- [IMAGE: VS Code showing the MCP server icon in the chat panel] -->

4. Click **Start** to launch the server

<!-- [IMAGE: Server started, tools visible] -->

Once started, you'll see all 44 tools available in the tools list. The server is ready.

---

## 8. Switch to Agent Mode

This is important. The MCP server works best in **Agent mode**, not the default "Ask" or "Edit" mode.

In the Copilot Chat panel, look at the mode dropdown at the top. Change it to **Agent**.

<!-- [IMAGE: Mode dropdown showing Agent selected] -->

In Agent mode, the AI can chain multiple tools together automatically. For example, if you ask "diagnose the failure for hem_processing", it will call 5-6 tools in sequence without you having to guide each step.

---

## 9. All 44 Tools — What They Do

### Airflow / MWAA (11 tools)

Monitor, debug, and manage your DAGs.

| Tool | What It Does |
|------|-------------|
| `list_dags` | Lists all DAGs with schedule and pause status |
| `list_dag_runs` | Shows runs for a DAG — pick one by number to drill in |
| `get_dag_run_details` | Task-level breakdown — which tasks passed, which failed |
| `get_task_log` | Reads the raw Airflow log for a task |
| `trigger_dag` | Manually kicks off a DAG run |
| `pause_dag` | Pauses a DAG so it won't run on schedule |
| `unpause_dag` | Unpauses a DAG |
| `clear_task_instance` | Retries a failed task without re-running the whole DAG |
| `get_dag_source` | Shows the DAG's Python source code and task dependencies |
| `get_dags_status_dashboard` | Full dashboard of ALL DAGs at a glance |
| `dag_analytics` | Success rate, duration trends, failure patterns over time |

### EMR Serverless (10 tools)

Manage Spark jobs, read logs, track costs.

| Tool | What It Does |
|------|-------------|
| `list_emr_applications` | Lists all EMR Serverless apps |
| `list_job_runs` | Shows job runs for an app with state and duration |
| `get_job_run_details` | Spark config, resource usage, S3 log paths |
| `read_spark_driver_log` | Reads the actual Python output and errors from Spark |
| `browse_s3_logs` | Navigates the S3 log folders |
| `cancel_job_run` | Cancels a running or stuck job |
| `stop_emr_application` | Stops an app — cancels running jobs if needed |
| `delete_emr_application` | Permanently deletes an app — force mode stops and deletes in one go |
| `read_s3_file` | Reads any S3 file (CSV, TXT, JSON, Parquet) up to 5 MB |
| `get_emr_cost_summary` | vCPU hours, memory, storage usage per app |

### S3 (4 tools)

Browse any S3 bucket in any environment.

| Tool | What It Does |
|------|-------------|
| `list_s3_buckets` | Lists all buckets in the AWS account |
| `browse_s3` | Browse folders and files like a file explorer |
| `list_s3_recursive` | Lists ALL files recursively with filters and size summary |
| `get_s3_object_info` | File metadata (size, modified date, type) without downloading |

### Confluence (9 tools)

Search, read, and write documentation without opening a browser.

| Tool | What It Does |
|------|-------------|
| `search_confluence` | Full-text search across all pages |
| `get_page_content` | Reads a page's full content as clean text |
| `get_child_pages` | Lists child pages under a parent |
| `get_space_pages` | Lists all pages in a space |
| `get_page_attachments` | Lists file attachments on a page |
| `get_page_labels` | Shows tags on a page |
| `get_page_comments` | Reads comments and discussions |
| `create_confluence_page` | Creates a new page |
| `update_confluence_page` | Updates an existing page |

### Azure DevOps / TFS (8 tools)

Sprint tracking, work items, and source code.

| Tool | What It Does |
|------|-------------|
| `list_repos` | Lists all Git repos in the project |
| `browse_repo` | Browse files and folders one level at a time |
| `browse_repo_recursive` | Full file tree of a repo in one call with correct paths |
| `read_repo_file` | Reads any file with syntax highlighting |
| `get_current_sprint` | Active sprint name, dates, days remaining |
| `get_sprint_work_items` | All PBIs, Tasks, Bugs — who's doing what |
| `get_work_item_details` | Full details for any work item |
| `get_backlog` | Backlog items not in the current sprint |

### Orchestration (1 tool)

| Tool | What It Does |
|------|-------------|
| `diagnose_dag_failure` | Complete failure diagnosis in one call — finds the failed run, reads logs, extracts EMR IDs, reads Spark output, gives you the root cause |

### Utility (1 tool)

| Tool | What It Does |
|------|-------------|
| `server_health_check` | Confirms the server is running |

---

## 10. How to Use It — Real Examples

Just type in plain English. Here are things you can ask:

### Check what's happening

```
Which DAGs failed today in prod?
Show me the status of all DAGs in dev
How has hem_processing been running lately?
What sprint are we in?
What's everyone working on?
```

### Debug a failure

```
Diagnose the failure for hem_processing in prod
What went wrong with ttdcustom_processing yesterday?
Show me the Spark driver log for this job
```

The `diagnose_dag_failure` tool is the big one. It does everything in one shot — finds the failed run, reads the task logs, pulls the EMR application ID, reads the Spark logs, and tells you what went wrong. This replaces 20 minutes of clicking through Airflow, the EMR console, and S3.

### Manage DAGs

```
Pause digital_taxonomy in uat
Trigger ttdcustom_processing in dev
Retry the initialise task on yesterday's failed run
```

### Browse S3

```
What S3 buckets do we have in dev?
Show me what's in the raw data bucket
List all CSV files in this bucket
Read this file: s3://bucket/path/to/file.csv
```

It handles CSV, TXT, JSON, gzipped logs, and Parquet files. Parquet files show the column schema and first 50 rows as a table.

### Manage EMR applications

```
List all EMR applications in dev
Stop that EMR application
Delete that EMR application
Force-stop and delete the app in one go
How much has EMR cost us this week?
```

### Find documentation

```
Find the documentation for Audience Engine
Read that runbook page
Create a new troubleshooting guide under runbooks
```

When you say "docs", "documentation", or "runbook", it searches Confluence automatically.

### Browse source code

```
Show me all files in the hem_processing repo
List all Python files in this repo
Read the main.py file
```

The `browse_repo_recursive` tool shows the entire repo tree in one call so the AI knows every file path and can read any file you ask about.

### Sprint and work items

```
What sprint are we in?
Show me PBI 12345
What's in the backlog?
```

### Environment handling

Every AWS tool (Airflow, EMR, S3) needs an environment: dev, uat, test, or prod. Each one is a different AWS account.

If you don't say which environment, the AI will ask you. It never guesses. This prevents accidental cross-environment mistakes.

```
You: "Which DAGs failed today?"
AI:  "Which environment? dev, uat, test, or prod?"
You: "prod"
AI:  [shows results from prod]
```

---

## 11. Troubleshooting

| Problem | What to Do |
|---------|------------|
| "Cannot connect to MWAA" | Connect to VPN |
| "Access denied" or "Expired token" on S3/EMR | Run `gimme-aws-creds -p consumersync` (or the relevant profile) |
| "CONFLUENCE_PAT not set" | Add your token to the config (see Step 5) |
| "AZDO_PAT not set" | Add your token to the config (see Step 5) |
| Server won't start | Make sure `uvx` is installed (see Step 2) |
| "command not found: uvx" | Restart VS Code after installing uv |
| Tools not showing up | Make sure the server is started (green icon). Try restarting it |
| AI calls all environments at once | Say "just dev" to keep it focused on one |
| Stale Airflow session | The server auto-retries. If it persists, restart the server |
| Slow Spark log reading | Pass the S3 log path directly if you have it from the task log |

---

## 12. Tips for Getting the Best Results

### Give it context first, then ask

The AI works with real data — it doesn't make things up. But it only knows what it can look up through the tools. If you ask about something it hasn't seen before, guide it to the right place first.

**Bad:**
```
What input files do I need for ID Graphs deployment to prod?
```
The AI has no idea what "ID Graphs" needs. It will either guess wrong or give you a vague answer.

**Good:**
```
Search Confluence for ID Graphs deployment documentation
```
Then after it finds and reads the page:
```
Based on that doc, what input files do I need to transfer for prod?
```

The same applies to source code. Don't ask "what does the hem_processing pipeline do?" out of nowhere. Instead:
```
Show me all the files in the hem_processing repo
```
Then:
```
Read the main.py file
```
Then:
```
Now explain what this pipeline does
```

Build context step by step. The AI is working with real data from your tools — not from memory.

### No hallucination — everything comes from your actual systems

This is not ChatGPT making up answers. Every response is backed by real API calls to your actual Airflow, EMR, S3, Confluence, and Azure DevOps. If a DAG run failed, it reads the real logs. If you ask for file contents, it reads the real file. The AI cannot invent data that doesn't exist in your systems.

If it says "I couldn't find that" — it genuinely couldn't find it. Check the spelling, the environment, or whether the thing actually exists.

### Credits and model selection

All usage is deducted from your **GitHub Copilot** subscription credits. Every tool call and every AI response costs credits.

If your credits run out or you're running low:
- Switch to one of the free models (look for models marked with `0x` or free tier in the model selector)
- Free models may be slower, but they still work accurately with the tools
- The tools themselves are not affected by which model you pick — they return the same data regardless

To change the model, use the model dropdown in the Copilot Chat panel.

### Keep it focused

- Ask about one environment at a time. Don't say "check all environments" unless you actually need all four.
- If the AI goes off track, just say "stop" and rephrase your question.
- For complex investigations, work step by step rather than asking everything at once.

---

That's it. Start the server, switch to Agent mode, and ask it anything.
