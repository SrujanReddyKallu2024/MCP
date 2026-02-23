"""
Azure DevOps (TFS) tools.

Connects to Azure DevOps Server (on-prem TFS or cloud) using the REST API v7.1
with a Personal Access Token (Basic auth).  Provides repository browsing,
sprint tracking, work item queries, and backlog management.

Tools:
  * list_repos              -- list all Git repos in the project
  * browse_repo             -- browse files/folders in a repo (one level)
  * browse_repo_recursive   -- full recursive file tree of a repo
  * read_repo_file          -- read a file's content from a repo
  * get_current_sprint      -- get the active sprint with dates and summary
  * get_sprint_work_items   -- get all PBIs + Tasks in a sprint
  * get_work_item_details   -- get full details of a specific work item
  * get_backlog             -- get backlog items not in current sprint
"""

from __future__ import annotations

import base64
import re
from datetime import date, datetime
from typing import Any

import requests
import urllib3

from mcp_server.config import (
    AZDO_BASE_URL,
    AZDO_PAT,
    AZDO_PROJECT,
    AZDO_TEAM,
    validate_azuredevops,
)

# Suppress SSL warnings for internal/self-signed certs (on-prem TFS)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── HTTP session with auth ───────────────────────────────────────────────────

_session: requests.Session | None = None

API_VERSION = "7.0"


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        token = base64.b64encode(f":{AZDO_PAT}".encode()).decode()
        _session = requests.Session()
        _session.headers.update({
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        _session.verify = False  # internal cert (on-prem TFS)
    return _session


def _project_url(project: str | None = None) -> str:
    """Build base URL for a project: {base}/{project}"""
    proj = project or AZDO_PROJECT
    return f"{AZDO_BASE_URL.rstrip('/')}/{proj}"


def _team_url(project: str | None = None, team: str | None = None) -> str:
    """Build base URL for a team: {base}/{project}/{team}"""
    proj = project or AZDO_PROJECT
    t = team or AZDO_TEAM
    return f"{AZDO_BASE_URL.rstrip('/')}/{proj}/{t}"


def _api_get(url: str, params: dict | None = None) -> dict[str, Any] | list:
    """GET request to Azure DevOps REST API. api-version is injected automatically."""
    err = validate_azuredevops()
    if err:
        return {"error": err}

    session = _get_session()
    params = dict(params or {})
    params.setdefault("api-version", API_VERSION)

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code >= 400:
            return {"error": f"Azure DevOps API returned HTTP {resp.status_code}: {resp.text[:500]}"}
        return resp.json()
    except requests.exceptions.ConnectionError as exc:
        return {"error": f"Cannot connect to Azure DevOps at {AZDO_BASE_URL}. Is VPN connected? Error: {exc}"}
    except Exception as exc:
        return {"error": f"Azure DevOps API error: {exc}"}


def _api_post(url: str, body: dict | None = None, params: dict | None = None) -> dict[str, Any] | list:
    """POST request to Azure DevOps REST API. api-version is injected automatically."""
    err = validate_azuredevops()
    if err:
        return {"error": err}

    session = _get_session()
    params = dict(params or {})
    params.setdefault("api-version", API_VERSION)

    try:
        resp = session.post(url, json=body, params=params, timeout=30)
        if resp.status_code >= 400:
            return {"error": f"Azure DevOps API returned HTTP {resp.status_code}: {resp.text[:500]}"}
        return resp.json()
    except requests.exceptions.ConnectionError as exc:
        return {"error": f"Cannot connect to Azure DevOps at {AZDO_BASE_URL}. Is VPN connected? Error: {exc}"}
    except Exception as exc:
        return {"error": f"Azure DevOps API error: {exc}"}


# ── Internal helpers ─────────────────────────────────────────────────────────


def _strip_html(html: str) -> str:
    """Strip HTML tags from Azure DevOps description fields."""
    if not html:
        return "—"
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or "—"


def _fetch_work_items(ids: list[int], project: str | None = None) -> list[dict]:
    """
    Batch-fetch work items by ID. Returns list of work item dicts.
    Azure DevOps limits batch gets to 200 IDs at a time.
    """
    if not ids:
        return []

    results = []
    for i in range(0, len(ids), 200):
        chunk = ids[i : i + 200]
        id_str = ",".join(str(x) for x in chunk)
        url = f"{_project_url(project)}/_apis/wit/workitems"
        params = {"ids": id_str, "$expand": "All"}
        data = _api_get(url, params=params)
        if isinstance(data, dict) and "error" in data:
            break
        items = data.get("value", []) if isinstance(data, dict) else []
        results.extend(items)
    return results


_TYPE_EMOJI = {
    "Product Backlog Item": "\U0001f4cb",  # clipboard
    "User Story": "\U0001f4cb",
    "Task": "\U0001f527",      # wrench
    "Bug": "\U0001f41b",       # bug
    "Feature": "\U0001f31f",   # star
    "Epic": "\U0001f3d4\ufe0f",  # mountain
}

_STATE_EMOJI = {
    "New": "\U0001f195",       # new
    "Active": "\U0001f535",    # blue circle
    "Committed": "\U0001f535",
    "In Progress": "\U0001f7e1",  # yellow circle
    "Resolved": "\U0001f7e2",    # green circle
    "Closed": "\u2705",       # check
    "Done": "\u2705",
    "Removed": "\U0001f5d1\ufe0f",
}


def _format_work_item_row(wi: dict) -> str:
    """Format a single work item into a compact multi-line block."""
    fields = wi.get("fields", {})
    wi_id = wi.get("id", "?")
    wi_type = fields.get("System.WorkItemType", "?")
    title = fields.get("System.Title", "?")
    state = fields.get("System.State", "?")
    assigned = fields.get("System.AssignedTo", {})
    assigned_name = assigned.get("displayName", "Unassigned") if isinstance(assigned, dict) else str(assigned or "Unassigned")
    effort = fields.get("Microsoft.VSTS.Scheduling.Effort", "—")
    priority = fields.get("Microsoft.VSTS.Common.Priority", "—")

    type_em = _TYPE_EMOJI.get(wi_type, "\U0001f4c4")
    state_em = _STATE_EMOJI.get(state, "\u26aa")

    lines = [
        f"  {type_em} **{wi_type} #{wi_id}: {title}**",
        f"     State    : {state_em} {state}",
        f"     Assigned : {assigned_name}",
        f"     Priority : {priority}  |  Effort: {effort}",
    ]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════════


def list_repos(
    project: str | None = None,
) -> str:
    """
    List all Git repositories in the Azure DevOps project.

    USE THIS TOOL when the user asks about repos, code repositories,
    or wants to browse source code in Azure DevOps / TFS.

    Args:
        project: Project name (default: Activate from config).

    Returns a list of repositories with name, default branch, size, and URL.
    Use browse_repo(repo_name='...') to explore a repo's files.
    """
    url = f"{_project_url(project)}/_apis/git/repositories"
    data = _api_get(url)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    repos = data.get("value", []) if isinstance(data, dict) else []
    if not repos:
        return f"No Git repositories found in project '{project or AZDO_PROJECT}'."

    lines = [f"\U0001f4e6 **{len(repos)} Repository(ies) in '{project or AZDO_PROJECT}'**\n"]

    for repo in sorted(repos, key=lambda r: r.get("name", "").lower()):
        name = repo.get("name", "?")
        repo_id = repo.get("id", "?")
        default_branch = repo.get("defaultBranch", "—").replace("refs/heads/", "")
        size_kb = repo.get("size", 0)
        size_str = f"{size_kb / 1024:.1f} MB" if size_kb > 1024 else f"{size_kb} KB"
        web_url = repo.get("webUrl", "—")

        lines.append(f"\U0001f4c1 **{name}**")
        lines.append(f"   ID      : {repo_id}")
        lines.append(f"   Branch  : {default_branch}")
        lines.append(f"   Size    : {size_str}")
        lines.append(f"   URL     : {web_url}")
        lines.append(f"   \U0001f4a1 `browse_repo(repo_name='{name}')`")
        lines.append("")

    return "\n".join(lines)


def browse_repo(
    repo_name: str,
    path: str = "/",
    branch: str | None = None,
    project: str | None = None,
) -> str:
    """
    Browse files and folders in a Git repository — like a directory listing.

    USE THIS TOOL when the user wants to see what files are in a repo,
    explore folder structure, or find a specific file path.

    Args:
        repo_name: Repository name (from list_repos).
        path: Folder path to browse (default '/' for root). Use forward slashes.
        branch: Branch name (default: repo's default branch).
        project: Project name (default from config).

    Returns a directory listing with file/folder names and paths.
    Use read_repo_file(repo_name='...', path='...') to read a file's content.
    """
    url = f"{_project_url(project)}/_apis/git/repositories/{repo_name}/items"
    params: dict[str, str] = {
        "scopePath": path,
        "recursionLevel": "oneLevel",
    }
    if branch:
        params["versionDescriptor.version"] = branch
        params["versionDescriptor.versionType"] = "branch"

    data = _api_get(url, params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    items = data.get("value", []) if isinstance(data, dict) else []
    if not items:
        return f"No items found at path '{path}' in repo '{repo_name}'."

    # Separate folders and files, skip the root entry itself
    folders = []
    files = []
    for item in items:
        item_path = item.get("path", "?")
        if item_path == path and item.get("isFolder", False):
            continue  # skip the directory entry itself
        if item.get("isFolder", False):
            folders.append(item)
        else:
            files.append(item)

    lines = [f"\U0001f4c2 **{repo_name}** \u2014 `{path}`\n"]

    if folders:
        lines.append(f"**Folders ({len(folders)}):**")
        for f in sorted(folders, key=lambda x: x.get("path", "")):
            folder_path = f.get("path", "?")
            folder_name = folder_path.rsplit("/", 1)[-1]
            lines.append(f"  \U0001f4c1 {folder_name}/")
            lines.append(f"     \u2192 `browse_repo(repo_name='{repo_name}', path='{folder_path}')`")
        lines.append("")

    if files:
        lines.append(f"**Files ({len(files)}):**")
        for f in sorted(files, key=lambda x: x.get("path", "")):
            file_path = f.get("path", "?")
            file_name = file_path.rsplit("/", 1)[-1]
            lines.append(f"  \U0001f4c4 {file_name}")
            lines.append(f"     \u2192 `read_repo_file(repo_name='{repo_name}', path='{file_path}')`")
        lines.append("")

    return "\n".join(lines)


def browse_repo_recursive(
    repo_name: str,
    path: str = "/",
    branch: str | None = None,
    extension_filter: str | None = None,
    project: str | None = None,
) -> str:
    """
    List ALL files in a Git repository recursively — the full file tree.

    USE THIS TOOL when the user asks 'what files are in this repo?',
    'show me the whole repo structure', 'list all Python files', or needs
    to find correct file paths before reading. Much faster than calling
    browse_repo folder-by-folder.

    Args:
        repo_name: Repository name (from list_repos).
        path: Starting folder path (default '/' for entire repo).
        branch: Branch name (default: repo's default branch).
        extension_filter: Filter by file extension, e.g. '.py', '.json', '.yaml'.
                          Case-insensitive. Only files matching this extension are shown.
        project: Project name (default from config).

    Returns a tree of ALL files and folders with full paths.
    Use read_repo_file(repo_name='...', path='...') to read any file.
    """
    url = f"{_project_url(project)}/_apis/git/repositories/{repo_name}/items"
    params: dict[str, str] = {
        "scopePath": path,
        "recursionLevel": "full",
    }
    if branch:
        params["versionDescriptor.version"] = branch
        params["versionDescriptor.versionType"] = "branch"

    data = _api_get(url, params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    items = data.get("value", []) if isinstance(data, dict) else []
    if not items:
        return f"No items found at path '{path}' in repo '{repo_name}'."

    # Separate folders and files
    folders = []
    files = []
    for item in items:
        item_path = item.get("path", "?")
        if item.get("isFolder", False):
            folders.append(item_path)
        else:
            files.append(item_path)

    # Apply extension filter
    if extension_filter:
        ext = extension_filter.lower() if extension_filter.startswith(".") else f".{extension_filter.lower()}"
        files = [f for f in files if f.lower().endswith(ext)]

    # Build tree output
    lines = [
        f"\U0001f333 **{repo_name}** \u2014 full file tree from `{path}`",
        f"   {len(folders)} folder(s), {len(files)} file(s)",
        "",
    ]

    if not files and not folders:
        lines.append("(empty)")
        return "\n".join(lines)

    # Group files by top-level folder for readability
    tree: dict[str, list[str]] = {}
    for f in sorted(files):
        # Get the directory part
        parts = f.rsplit("/", 1)
        folder = parts[0] if len(parts) > 1 and parts[0] else "/"
        tree.setdefault(folder, []).append(f)

    for folder in sorted(tree.keys()):
        lines.append(f"\U0001f4c1 **{folder}/**")
        for file_path in tree[folder]:
            file_name = file_path.rsplit("/", 1)[-1]
            lines.append(f"   \U0001f4c4 {file_name}  \u2192  `{file_path}`")
        lines.append("")

    lines.append(f"\U0001f4a1 Use `read_repo_file(repo_name='{repo_name}', path='<path>')` to read any file.")

    return "\n".join(lines)


def read_repo_file(
    repo_name: str,
    path: str,
    branch: str | None = None,
    project: str | None = None,
) -> str:
    """
    Read the content of a single file from a Git repository.

    USE THIS TOOL when the user wants to see source code, config,
    or content of a specific file. Get the path from browse_repo first.

    Args:
        repo_name: Repository name.
        path: Full file path (e.g. '/src/main.py'). Use forward slashes.
        branch: Branch name (default: repo's default branch).
        project: Project name (default from config).

    Returns the raw file content with metadata header.
    """
    url = f"{_project_url(project)}/_apis/git/repositories/{repo_name}/items"
    params: dict[str, str] = {
        "path": path,
        "includeContent": "true",
    }
    if branch:
        params["versionDescriptor.version"] = branch
        params["versionDescriptor.versionType"] = "branch"

    data = _api_get(url, params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    content = data.get("content", "") if isinstance(data, dict) else ""
    item_path = data.get("path", path) if isinstance(data, dict) else path
    commit_id = data.get("commitId", "?") if isinstance(data, dict) else "?"

    if not content and isinstance(data, dict) and data.get("isFolder"):
        return f"\u274c '{path}' is a folder, not a file. Use browse_repo to list its contents."

    if not content:
        return f"\u274c No content returned for '{path}'. The file may be binary or empty."

    file_name = item_path.rsplit("/", 1)[-1]
    # Detect language for code fence
    ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    lang_map = {
        "py": "python", "js": "javascript", "ts": "typescript",
        "json": "json", "yaml": "yaml", "yml": "yaml",
        "xml": "xml", "sql": "sql", "sh": "bash",
        "md": "markdown", "tf": "hcl", "cfg": "ini",
        "csv": "csv", "java": "java", "scala": "scala",
    }
    lang = lang_map.get(ext, ext)
    line_count = content.count("\n") + 1

    lines = [
        f"\U0001f4c4 **{file_name}** \u2014 `{item_path}`",
        f"   Repo   : {repo_name}",
        f"   Lines  : {line_count}",
        f"   Commit : {commit_id[:8]}",
        "",
        f"```{lang}",
        content,
        "```",
    ]

    return "\n".join(lines)


def get_current_sprint(
    project: str | None = None,
    team: str | None = None,
) -> str:
    """
    Get the current (active) sprint/iteration with dates and summary.

    USE THIS TOOL when the user asks 'what sprint are we in?', 'current iteration',
    'sprint dates', 'when does the sprint end?', or any question about the active sprint.

    Args:
        project: Project name (default from config).
        team: Team name (default from config).

    Returns current sprint name, date range, and time remaining.
    Use get_sprint_work_items() to see what's in the sprint.
    """
    t = team or AZDO_TEAM
    if not t:
        return "\u26a0\ufe0f AZDO_TEAM is not set. Set it in .env to use sprint tools."

    url = f"{_team_url(project, team)}/_apis/work/teamsettings/iterations"
    params = {"$timeframe": "current"}
    data = _api_get(url, params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    iterations = data.get("value", []) if isinstance(data, dict) else []
    if not iterations:
        return "\u26a0\ufe0f No current sprint found. The team may not have an active iteration configured."

    sprint = iterations[0]
    name = sprint.get("name", "?")
    sprint_path = sprint.get("path", "?")
    sprint_id = sprint.get("id", "?")
    attrs = sprint.get("attributes", {})
    start_date = attrs.get("startDate", "?")[:10] if attrs.get("startDate") else "?"
    end_date = attrs.get("finishDate", "?")[:10] if attrs.get("finishDate") else "?"
    time_frame = attrs.get("timeFrame", "?")

    # Calculate days remaining
    days_info = ""
    if end_date != "?":
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            today = date.today()
            remaining = (end_dt - today).days
            if remaining > 0:
                days_info = f"({remaining} day(s) remaining)"
            elif remaining == 0:
                days_info = "(ends today!)"
            else:
                days_info = f"(ended {abs(remaining)} day(s) ago)"
        except ValueError:
            pass

    lines = [
        f"\U0001f3c3 **Current Sprint: {name}** {days_info}",
        f"   Path       : {sprint_path}",
        f"   Start      : {start_date}",
        f"   End        : {end_date}",
        f"   Time Frame : {time_frame}",
        f"   Sprint ID  : {sprint_id}",
        "",
        f"\U0001f4a1 Use `get_sprint_work_items()` to see all work items in this sprint.",
    ]

    return "\n".join(lines)


def get_sprint_work_items(
    iteration_path: str | None = None,
    project: str | None = None,
    team: str | None = None,
) -> str:
    """
    Get all work items (PBIs, Tasks, Bugs) in a sprint with full details.

    USE THIS TOOL when the user asks 'what's in the sprint?', 'sprint board',
    'who's working on what?', 'sprint details', or wants to see sprint content.

    Shows each work item with type, title, state, assigned person, effort/story points.
    Groups items by type (PBI, Task, Bug) with totals.

    Args:
        iteration_path: Full iteration path (e.g. 'Activate\\Sprint 23 Q4 FY26').
                        If omitted, uses the current sprint automatically.
        project: Project name (default from config).
        team: Team name (default from config).

    Returns all work items grouped by type with assignee, state, effort, and priority.
    Use get_work_item_details(work_item_id=...) for full details of any item.
    """
    t = team or AZDO_TEAM
    if not t:
        return "\u26a0\ufe0f AZDO_TEAM is not set. Set it in .env to use sprint tools."

    proj = project or AZDO_PROJECT

    # If no iteration_path, resolve the current sprint
    if not iteration_path:
        url = f"{_team_url(project, team)}/_apis/work/teamsettings/iterations"
        params = {"$timeframe": "current"}
        iter_data = _api_get(url, params=params)

        if isinstance(iter_data, dict) and "error" in iter_data:
            return iter_data["error"]

        iterations = iter_data.get("value", []) if isinstance(iter_data, dict) else []
        if not iterations:
            return "\u26a0\ufe0f No current sprint found. Provide iteration_path explicitly."
        iteration_path = iterations[0].get("path", "")
        sprint_name = iterations[0].get("name", "?")
    else:
        sprint_name = iteration_path.rsplit("\\", 1)[-1]

    # WIQL query for sprint items
    wiql = (
        "SELECT [System.Id], [System.Title], [System.State], "
        "[System.AssignedTo], [System.WorkItemType], "
        "[Microsoft.VSTS.Scheduling.Effort], [Microsoft.VSTS.Common.Priority] "
        "FROM WorkItems "
        f"WHERE [System.IterationPath] UNDER '{iteration_path}' "
        "AND [System.WorkItemType] IN ('Product Backlog Item', 'User Story', 'Task', 'Bug') "
        "ORDER BY [System.WorkItemType], [Microsoft.VSTS.Common.Priority]"
    )

    url = f"{_team_url(project, team)}/_apis/wit/wiql"
    data = _api_post(url, body={"query": wiql})

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    work_item_refs = data.get("workItems", []) if isinstance(data, dict) else []
    if not work_item_refs:
        return f"No work items found in sprint '{sprint_name}'."

    # Batch-fetch full details
    ids = [ref["id"] for ref in work_item_refs]
    items = _fetch_work_items(ids, project=project)

    if not items:
        return f"\u26a0\ufe0f Found {len(ids)} work item IDs but could not fetch details."

    # Group by type
    grouped: dict[str, list[dict]] = {}
    for wi in items:
        wi_type = wi.get("fields", {}).get("System.WorkItemType", "Other")
        grouped.setdefault(wi_type, []).append(wi)

    # Tally states
    total = len(items)
    state_counts: dict[str, int] = {}
    for wi in items:
        s = wi.get("fields", {}).get("System.State", "?")
        state_counts[s] = state_counts.get(s, 0) + 1

    state_summary = ", ".join(f"{v} {k}" for k, v in sorted(state_counts.items()))
    total_effort = sum(
        wi.get("fields", {}).get("Microsoft.VSTS.Scheduling.Effort", 0) or 0
        for wi in items
    )

    lines = [
        f"\U0001f3c3 **Sprint: {sprint_name}** \u2014 {total} work item(s)",
        f"   States : {state_summary}",
        f"   Effort : {total_effort} total story points",
        "",
    ]

    # Group by assignee for a quick who's-doing-what summary
    by_person: dict[str, int] = {}
    for wi in items:
        a = wi.get("fields", {}).get("System.AssignedTo", {})
        name = a.get("displayName", "Unassigned") if isinstance(a, dict) else "Unassigned"
        by_person[name] = by_person.get(name, 0) + 1

    lines.append("**Team Members:**")
    for person, count in sorted(by_person.items()):
        lines.append(f"   \U0001f464 {person}: {count} item(s)")
    lines.append("")

    for wi_type, type_items in grouped.items():
        type_effort = sum(
            wi.get("fields", {}).get("Microsoft.VSTS.Scheduling.Effort", 0) or 0
            for wi in type_items
        )
        type_em = _TYPE_EMOJI.get(wi_type, "\U0001f4c4")
        lines.append(f"\u2500\u2500 {type_em} {wi_type} ({len(type_items)}) \u2014 {type_effort} pts \u2500\u2500")
        for wi in type_items:
            lines.append(_format_work_item_row(wi))
            lines.append("")
        lines.append("")

    return "\n".join(lines)


def get_work_item_details(
    work_item_id: int,
    project: str | None = None,
) -> str:
    """
    Get full details of a single work item (PBI, Task, Bug, Feature, Epic).

    USE THIS TOOL when the user asks about a specific work item by ID,
    wants the description, acceptance criteria, history, or full details.

    Args:
        work_item_id: The numeric work item ID (e.g. 12345).
        project: Project name (default from config).

    Returns complete work item details including description, acceptance criteria,
    parent/child links, and all metadata fields.
    """
    url = f"{_project_url(project)}/_apis/wit/workitems/{work_item_id}"
    params = {"$expand": "All"}
    data = _api_get(url, params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    if not isinstance(data, dict):
        return "\u274c Unexpected response format."

    fields = data.get("fields", {})
    wi_id = data.get("id", "?")
    wi_type = fields.get("System.WorkItemType", "?")
    title = fields.get("System.Title", "?")
    state = fields.get("System.State", "?")
    reason = fields.get("System.Reason", "\u2014")
    assigned = fields.get("System.AssignedTo", {})
    assigned_name = assigned.get("displayName", "Unassigned") if isinstance(assigned, dict) else str(assigned or "Unassigned")
    created_by = fields.get("System.CreatedBy", {})
    created_name = created_by.get("displayName", "?") if isinstance(created_by, dict) else str(created_by or "?")
    created_date = str(fields.get("System.CreatedDate", "?"))[:10]
    changed_date = str(fields.get("System.ChangedDate", "?"))[:10]
    iteration = fields.get("System.IterationPath", "\u2014")
    area = fields.get("System.AreaPath", "\u2014")
    priority = fields.get("Microsoft.VSTS.Common.Priority", "\u2014")
    effort = fields.get("Microsoft.VSTS.Scheduling.Effort", "\u2014")
    description = fields.get("System.Description", "")
    acceptance = fields.get("Microsoft.VSTS.Common.AcceptanceCriteria", "")
    repro_steps = fields.get("Microsoft.VSTS.TCM.ReproSteps", "")
    tags = fields.get("System.Tags", "\u2014")

    # Relations (parent, children, related)
    relations = data.get("relations", []) or []
    parent_links = []
    child_links = []
    related_links = []
    for rel in relations:
        rel_type = rel.get("attributes", {}).get("name", "")
        rel_url = rel.get("url", "")
        rel_id_match = re.search(r"/workItems/(\d+)", rel_url)
        rel_id = rel_id_match.group(1) if rel_id_match else "?"
        if "Parent" in rel_type:
            parent_links.append(rel_id)
        elif "Child" in rel_type:
            child_links.append(rel_id)
        else:
            related_links.append(f"#{rel_id} ({rel_type})")

    type_em = _TYPE_EMOJI.get(wi_type, "\U0001f4c4")
    state_em = _STATE_EMOJI.get(state, "\u26aa")

    lines = [
        f"{type_em} **{wi_type} #{wi_id}: {title}**",
        "",
        f"   State       : {state_em} {state} ({reason})",
        f"   Assigned To : {assigned_name}",
        f"   Priority    : {priority}  |  Effort: {effort}",
        f"   Iteration   : {iteration}",
        f"   Area        : {area}",
        f"   Tags        : {tags}",
        f"   Created     : {created_date} by {created_name}",
        f"   Updated     : {changed_date}",
    ]

    if parent_links:
        lines.append(f"   Parent      : #{', #'.join(parent_links)}")
    if child_links:
        lines.append(f"   Children    : #{', #'.join(child_links)}")
        lines.append(f"   \U0001f4a1 Use `get_work_item_details(work_item_id=<id>)` to see child details")
    if related_links:
        lines.append(f"   Related     : {', '.join(related_links)}")

    lines.append("")

    if description:
        lines.append("\u2500\u2500 Description \u2500\u2500")
        lines.append(_strip_html(description))
        lines.append("")

    if acceptance:
        lines.append("\u2500\u2500 Acceptance Criteria \u2500\u2500")
        lines.append(_strip_html(acceptance))
        lines.append("")

    if repro_steps:
        lines.append("\u2500\u2500 Repro Steps \u2500\u2500")
        lines.append(_strip_html(repro_steps))
        lines.append("")

    return "\n".join(lines)


def get_backlog(
    project: str | None = None,
    team: str | None = None,
    max_results: int = 50,
) -> str:
    """
    Get backlog items (PBIs/User Stories not in the current sprint).

    USE THIS TOOL when the user asks about the backlog, upcoming work,
    'what's next?', 'what's not in the sprint?', or items waiting to be picked up.

    Args:
        project: Project name (default from config).
        team: Team name (default from config).
        max_results: Maximum items to return (default 50).

    Returns backlog items ordered by priority with state and effort.
    """
    t = team or AZDO_TEAM
    if not t:
        return "\u26a0\ufe0f AZDO_TEAM is not set. Set it in .env to use backlog tools."

    # WIQL: items NOT under current iteration, not Done/Closed
    wiql = (
        "SELECT [System.Id], [System.Title], [System.State], "
        "[System.AssignedTo], [System.WorkItemType], "
        "[Microsoft.VSTS.Scheduling.Effort], [Microsoft.VSTS.Common.Priority] "
        "FROM WorkItems "
        "WHERE [System.WorkItemType] IN ('Product Backlog Item', 'User Story') "
        "AND [System.State] <> 'Done' "
        "AND [System.State] <> 'Closed' "
        "AND [System.State] <> 'Removed' "
        "AND [System.IterationPath] NOT UNDER @CurrentIteration "
        "ORDER BY [Microsoft.VSTS.Common.Priority], [System.CreatedDate] DESC"
    )

    url = f"{_team_url(project, team)}/_apis/wit/wiql"
    data = _api_post(url, body={"query": wiql, "$top": max_results})

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    work_item_refs = data.get("workItems", []) if isinstance(data, dict) else []
    if not work_item_refs:
        return "\U0001f389 Backlog is empty \u2014 no items outside the current sprint."

    # Limit and fetch
    refs = work_item_refs[:max_results]
    ids = [ref["id"] for ref in refs]
    items = _fetch_work_items(ids, project=project)

    if not items:
        return f"\u26a0\ufe0f Found {len(ids)} backlog item IDs but could not fetch details."

    total_effort = sum(
        wi.get("fields", {}).get("Microsoft.VSTS.Scheduling.Effort", 0) or 0
        for wi in items
    )

    lines = [
        f"\U0001f4cb **Backlog** \u2014 {len(items)} item(s), {total_effort} total story points",
        "",
    ]

    for wi in items:
        lines.append(_format_work_item_row(wi))
        lines.append("")

    if len(work_item_refs) > max_results:
        lines.append(f"\U0001f4a1 Showing {max_results} of {len(work_item_refs)} total backlog items.")
        lines.append(f"   Use `max_results={max_results + 50}` to see more.")

    return "\n".join(lines)
