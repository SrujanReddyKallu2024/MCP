"""
MCP Server — Ops Tools for MWAA, EMR Serverless & Confluence.

Single server exposing tools across four domains:
  • Airflow (via MWAA)   — DAG listing, run details, task logs, triggering,
                           pause/unpause, task retry, historical runs, DAG source
  • EMR Serverless       — app listing, job runs, Spark driver S3 logs, S3 browsing,
                           job cancellation, general S3 file reading, cost summary
  • Confluence           — search, page content, child pages, attachments, labels,
                           comments, page creation, page updates
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
        "EMR Serverless, Confluence, and utility functions."
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
        "LOG PRIORITY: When reading Spark logs, ALWAYS read stdout FIRST "
        "(Python app output from stdout.gz), then stderr (Spark framework). "
        "The stdout.gz log is the most important — it has print statements, row counts, "
        "file paths, and actual application errors."
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
from mcp_server.tools.orchestration_tools import (    # noqa: E402
    diagnose_dag_failure,
)

# ── Register Utility tools ──────────────────────────────────────────────────

mcp.tool()(server_health_check)

# ── Register MWAA tools (9) ─────────────────────────────────────────────────

mcp.tool()(list_dags)
mcp.tool()(get_dag_runs)
mcp.tool()(get_dag_run_details)
mcp.tool()(get_task_log)
mcp.tool()(trigger_dag)
mcp.tool()(pause_dag)
mcp.tool()(unpause_dag)
mcp.tool()(clear_task_instance)
mcp.tool()(get_dag_source)

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

# ── Register Orchestration tools (1) ────────────────────────────────────────

mcp.tool()(diagnose_dag_failure)


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    """Run the MCP server via stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
