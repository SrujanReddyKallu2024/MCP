"""
MCP Server — Ops Tools for MWAA, EMR Serverless, Confluence & Azure DevOps.

Single server exposing tools across five domains:
  • Airflow (via MWAA)   — DAG listing, run details, task logs, triggering,
                           pause/unpause, task retry, historical runs, DAG source
  • EMR Serverless       — app listing, job runs, Spark driver S3 logs, S3 browsing,
                           job cancellation, general S3 file reading, cost summary
  • Confluence           — search, page content, child pages, attachments, labels,
                           comments, page creation, page updates
  • Azure DevOps (TFS)   — Git repos, file browsing, sprints, work items, backlog
  • Orchestration        — one-shot DAG failure diagnosis
  • Utilities            — server health check

Runs via stdio transport — designed for Gemini CLI or any MCP client.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

# ── Create server ────────────────────────────────────────────────────────────

mcp = FastMCP(
    "ops-tools",
    instructions=(
        "This server provides operations tools for AWS MWAA (Airflow), "
        "EMR Serverless, Confluence, Azure DevOps (TFS), and utility functions."
        "\n\n"
        "DOCUMENTATION: When the user says 'documentation', 'docs', 'runbook', "
        "or 'wiki', ALWAYS use Confluence tools. search_confluence to find, "
        "get_page_content to read. Do NOT ask 'where should I look?' — Confluence is the default."
        "\n\n"
        "INTERACTIVE OUTPUT: When the user asks about runs, logs, or processing "
        "for a specific DAG/process but doesn't specify which run — call get_dag_runs "
        "to get a numbered list and present it so the user can pick one. "
        "Do NOT ask multiple follow-up questions. Give the list in ONE shot. "
        "Example: 'I found 10 runs for HEM processing. Which one? 1. ✅ Feb 19 SUCCESS, "
        "2. ❌ Feb 18 FAILED, ...' — let them pick by number."
        "\n\n"
        "LOG PRIORITY: read_spark_driver_log defaults to stdout (the primary log "
        "with Python print statements, row counts, file paths, and errors). "
        "Use read_both=True to get stdout + stderr in one call. "
        "Only use log_type='stderr' when specifically investigating Spark framework issues."
        "\n\n"
        "DAG STATUS: When the user asks for a status report, dashboard, overview, "
        "'which DAGs failed', or 'what's running' — use get_dag_status_report(). "
        "It shows EVERY DAG with last run state, pause status, and schedule in one view. "
        "Use list_dags() only when the user specifically wants just DAG names/schedules."
        "\n\n"
        "DEBUGGING WORKFLOW: For quick diagnosis, use diagnose_dag_failure(dag_id='...') "
        "which does everything automatically. For manual step-by-step: "
        "(1) get_dag_runs → find the failed run, "
        "(2) get_dag_run_details → find which task failed, "
        "(3) get_task_log on 'initialise' task → get EMR application ID (pattern: '00g...'), "
        "(4) get_task_log on failed processing task → get job_run_id, "
        "(5) read_spark_driver_log with log_type='stdout' for Python output, "
        "then log_type='stderr' for Spark framework logs."
        "\n\n"
        "DAG LIFECYCLE: Use pause_dag/unpause_dag to control scheduling. "
        "Use clear_task_instance to retry a failed task without re-triggering the whole DAG."
        "\n\n"
        "S3 FILES: Use read_s3_file(s3_uri='s3://bucket/key') to read any S3 file."
        "\n\n"
        "All MWAA tools accept 'env': 'dev', 'test', or 'prod' (default: dev)."
        "\n\n"
        "AZURE DEVOPS: When the user asks about repos, source code, sprints, "
        "work items, PBIs, tasks, bugs, backlogs, or iterations — use Azure DevOps tools. "
        "list_repos to see repos, browse_repo to explore, read_repo_file to read code. "
        "get_current_sprint for active sprint, get_sprint_work_items for sprint content, "
        "get_work_item_details for full PBI/Task/Bug details, get_backlog for backlog."
    ),
)


# ── Import tool functions ────────────────────────────────────────────────────

from mcp_server.tools.utility_tools import (          # noqa: E402
    server_health_check,
)
from mcp_server.tools.mwaa_tools import (             # noqa: E402
    list_dags,
    get_dag_runs,
    get_dag_run_details,
    get_task_log,
    trigger_dag,
    pause_dag,
    unpause_dag,
    clear_task_instance,
    get_dag_source,
    get_dag_status_report,
)
from mcp_server.tools.emr_tools import (              # noqa: E402
    list_emr_applications,
    list_job_runs,
    get_job_run_details,
    read_spark_driver_log,
    browse_s3_logs,
    cancel_job_run,
    read_s3_file,
    get_emr_cost_summary,
)
from mcp_server.tools.confluence_tools import (       # noqa: E402
    search_confluence,
    get_page_content,
    get_child_pages,
    get_page_attachments,
    get_page_labels,
    get_page_comments,
    create_confluence_page,
    update_confluence_page,
)
from mcp_server.tools.azdo_tools import (              # noqa: E402
    list_repos,
    browse_repo,
    read_repo_file,
    get_current_sprint,
    get_sprint_work_items,
    get_work_item_details,
    get_backlog,
)
from mcp_server.tools.orchestration_tools import (    # noqa: E402
    diagnose_dag_failure,
)

# ── Register Utility tools ──────────────────────────────────────────────────

mcp.tool()(server_health_check)

# ── Register MWAA tools (10) ────────────────────────────────────────────────

mcp.tool()(list_dags)
mcp.tool()(get_dag_runs)
mcp.tool()(get_dag_run_details)
mcp.tool()(get_task_log)
mcp.tool()(trigger_dag)
mcp.tool()(pause_dag)
mcp.tool()(unpause_dag)
mcp.tool()(clear_task_instance)
mcp.tool()(get_dag_source)
mcp.tool()(get_dag_status_report)

# ── Register EMR Serverless tools (8) ────────────────────────────────────────

mcp.tool()(list_emr_applications)
mcp.tool()(list_job_runs)
mcp.tool()(get_job_run_details)
mcp.tool()(read_spark_driver_log)
mcp.tool()(browse_s3_logs)
mcp.tool()(cancel_job_run)
mcp.tool()(read_s3_file)
mcp.tool()(get_emr_cost_summary)

# ── Register Confluence tools (8) ────────────────────────────────────────────

mcp.tool()(search_confluence)
mcp.tool()(get_page_content)
mcp.tool()(get_child_pages)
mcp.tool()(get_page_attachments)
mcp.tool()(get_page_labels)
mcp.tool()(get_page_comments)
mcp.tool()(create_confluence_page)
mcp.tool()(update_confluence_page)

# ── Register Azure DevOps tools (7) ─────────────────────────────────────────

mcp.tool()(list_repos)
mcp.tool()(browse_repo)
mcp.tool()(read_repo_file)
mcp.tool()(get_current_sprint)
mcp.tool()(get_sprint_work_items)
mcp.tool()(get_work_item_details)
mcp.tool()(get_backlog)

# ── Register Orchestration tools (1) ────────────────────────────────────────

mcp.tool()(diagnose_dag_failure)


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    """Run the MCP server via stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
