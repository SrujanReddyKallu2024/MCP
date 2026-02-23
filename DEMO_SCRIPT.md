# Ops Tools MCP Server — Demo Script

> **Duration:** ~15 minutes
> **Audience:** Your team
> **Tone:** Casual, conversational. You're showing your teammates something cool you built.
> **Setup:** Have Gemini CLI (or any MCP client) open and connected to the server.

---

## Before You Start

Say something like:

> "Hey everyone, I want to show you something I've been working on. It's an MCP server that lets you talk to all our infrastructure in plain English — Airflow, EMR, S3, Confluence, and Azure DevOps — all from one chat window. No more jumping between 5 different UIs. Let me walk you through it."

---

## Use Case 1: Health Check + Environment Awareness (~1 min)

**What you say:**
> "Is the server running?"

**What happens:** The AI calls `server_health_check()` and confirms everything is connected.

**Then say:**
> "List all S3 buckets"

**What happens:** The AI doesn't just go ahead — it asks you: *"Which environment? dev, uat, test, or prod?"*

**Why it's impressive:** It knows we have 4 separate AWS accounts and it won't accidentally mix them up. Every tool is environment-aware. No more "oops, I was looking at prod."

**Say:** "dev"

**What happens:** Shows all S3 buckets in the dev account with creation dates.

---

## Use Case 2: DAG Dashboard — Full Status Report (~2 min)

**What you say:**
> "Show me the status of all DAGs in prod"

**What happens:** The AI calls `get_dags_status_dashboard(env='prod')` and gives you a full dashboard:
- How many DAGs are active vs paused
- Which ones failed today
- Which ones are currently running
- Last run state for every single DAG
- At the bottom, it highlights the failures with ready-to-use diagnosis commands

**Why it's impressive:** This is like opening the Airflow UI, clicking through every DAG, and writing a summary — but it happens in 3 seconds. One question, one answer.

**Follow up with:**
> "How has ttdcustom_processing been running lately?"

**What happens:** The AI calls `dag_analytics(dag_id='ttdcustom_processing', env='prod', days=14)` and shows you:
- Success rate (e.g. 92.3%)
- Average/min/max run duration
- Whether it's getting faster or slower (trend)
- Failure patterns (e.g. "3 of 4 failures happened on Monday")
- A visual streak: ✅✅✅❌✅✅✅✅✅✅

---

## Use Case 3: One-Shot Failure Diagnosis (~3 min)

> This is the showstopper. This is the one that saves you 20 minutes every morning.

**What you say:**
> "Diagnose the failure for hem_processing in prod"

**What happens:** The AI calls `diagnose_dag_failure(dag_id='hem_processing', env='prod')` and does ALL of this automatically:
1. Finds today's failed run
2. Identifies which task(s) failed
3. Reads the Airflow task logs
4. Extracts the EMR Application ID from the initialise task
5. Extracts the Job Run ID from the processing task
6. Reads the Spark driver stdout (Python app errors)
7. Reads the Spark driver stderr (framework errors)

**What you get:** A complete failure report — root cause, stack traces, which file failed, which row caused it — all in one response.

**Why it's impressive:** This used to take 6-8 manual steps across 3 different UIs (Airflow → EMR console → S3 bucket). Now it's one sentence.

---

## Use Case 4: DAG Operations — Pause, Trigger, Retry (~2 min)

**What you say:**
> "Pause the digital_taxonomy_processing DAG in uat"

**What happens:** The AI calls `pause_dag(dag_id='digital_taxonomy_processing', env='uat')` and confirms: "DAG is now PAUSED — scheduled runs will NOT trigger."

**Then say:**
> "Actually, unpause it and trigger a manual run"

**What happens:** Two calls — `unpause_dag()` then `trigger_dag()`. You get a new run ID and confirmation it's queued.

**Then say:**
> "The initialise task failed on yesterday's run of ttdgeo_metadata. Can you retry just that task?"

**What happens:** The AI calls `clear_task_instance(dag_id='ttdgeo_metadata', dag_run_id='...', task_id='initialise', env='uat')`. The task gets cleared and retries automatically — no need to re-run the entire DAG.

**Why it's impressive:** Full DAG lifecycle management from chat. No Airflow UI needed.

---

## Use Case 5: S3 Browsing + EMR Cost Tracking (~2 min)

**What you say:**
> "What's in the logs bucket in dev? Show me the spark-logs folder"

**What happens:** The AI calls `browse_s3(bucket='eec-aws-uk-ms-consumersync-dev-logs-bucket', prefix='spark-logs/', env='dev')` and shows folders for each process — like a file explorer.

**Drill in:**
> "Go into the ttdcustom folder and show me the latest files"

**What happens:** Interactive browsing — folder by folder, file by file, with sizes and timestamps.

**Then say:**
> "How much has EMR cost us this week in prod?"

**What happens:** The AI calls `get_emr_cost_summary(days=7, env='prod')` and shows total vCPU hours, memory GB-hours, storage GB-hours, broken down per application. Also shows how many jobs failed.

**Why it's impressive:** You get S3 browsing and cost visibility without opening the AWS console.

---

## Use Case 6: Confluence — Find and Read Docs Instantly (~2 min)

**What you say:**
> "Find the documentation for Audience Engine"

**What happens:** The AI calls `search_confluence(query='Audience Engine')` and returns matching pages ranked by relevance — with titles, page IDs, paths, last modified dates, and direct URLs.

**Then say:**
> "Read the first one"

**What happens:** The AI calls `get_page_content(page_id='...')` and gives you the full page content — converted from HTML to clean, readable text. Code blocks, links, images — all preserved.

**Then say:**
> "Create a new page under the runbooks section called 'HEM Processing Troubleshooting Guide' with some initial content"

**What happens:** The AI calls `create_confluence_page()` and creates the page. You get back the page ID and a direct URL.

**Why it's impressive:** Search, read, and create Confluence pages without opening a browser. When someone says "check the docs" — this is how you do it.

---

## Use Case 7: Azure DevOps — Sprint & Code (~3 min)

**What you say:**
> "What sprint are we in?"

**What happens:** The AI calls `get_current_sprint()` and shows the sprint name, start/end dates, and days remaining.

**Then say:**
> "What's everyone working on?"

**What happens:** The AI calls `get_sprint_work_items()` and shows every PBI, Task, and Bug in the sprint — grouped by type, with assignees, states, priority, and story points. Plus a "Team Members" summary showing who has how many items.

**Then say:**
> "Show me PBI 12345"

**What happens:** The AI calls `get_work_item_details(work_item_id=12345)` and shows the full details — description, acceptance criteria, parent/child links, tags, iteration path, everything.

**Then say:**
> "Show me the source code for the hem_processing repo"

**What happens:** The AI calls `browse_repo(repo_name='hem_processing')` and shows the folder structure. You can drill into any file and read the code right there.

**Why it's impressive:** Sprint tracking, work items, AND source code — all from one chat. No switching between boards, backlogs, and repos.

---

## Wrap Up (~1 min)

> "So that's it — 43 tools across 6 different platforms, all accessible through natural language. The key things to remember:
>
> 1. **It always asks which environment** — dev, uat, test, or prod. No accidental production changes.
> 2. **One-shot diagnosis** saves 20 minutes every morning when something fails.
> 3. **Everything is interactive** — it gives you numbered lists so you can drill deeper.
> 4. **No UI hopping** — Airflow, EMR, S3, Confluence, and TFS all in one place.
>
> Questions?"

---

## Tips for the Demo

- **If something fails:** That's okay! Show the error message — the tool gives clear, actionable errors (e.g. "Is VPN connected?", "Check IAM permissions").
- **Use real DAG names:** Makes it feel real. Pick a DAG that recently failed for the diagnosis demo.
- **Let people ask questions live:** "Want me to look up your PBI?" or "Which DAG are you curious about?" — let them drive.
- **Don't rush:** The AI responses are fast. Pause and let people read the output.
