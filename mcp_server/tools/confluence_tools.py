"""
Confluence Server tools.

Connects to on-prem Confluence (pages.experian.local) using the REST API v1
with a Personal Access Token (Bearer auth).  Handles HTML→text conversion
so the AI gets clean readable content.

Tools:
  • search_confluence    — full-text search across a space (default 50 results, paginated)
  • get_page_content     — read a page's full content as clean text
  • get_child_pages      — list children of a parent page
  • get_space_pages      — list all pages in a space (paginated)
  • get_page_attachments — list file attachments on a page
  • get_page_labels      — get page tags/labels
  • get_page_comments    — read page comments
  • create_confluence_page — create a new page
  • update_confluence_page — update an existing page
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any

import requests
import urllib3

from mcp_server.config import CONFLUENCE_BASE_URL, CONFLUENCE_PAT, CONFLUENCE_SPACE_KEY, CONFLUENCE_ROOT_PAGE_ID, validate_confluence

# Suppress SSL warnings for internal/self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── HTTP session with auth ───────────────────────────────────────────────────

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Authorization": f"Bearer {CONFLUENCE_PAT}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        _session.verify = False  # internal cert
    return _session


def _api_get(path: str, params: dict | None = None) -> dict[str, Any] | list:
    """
    GET request to Confluence REST API v1.
    path should start with / e.g. '/rest/api/content'
    """
    err = validate_confluence()
    if err:
        return {"error": err}

    url = f"{CONFLUENCE_BASE_URL.rstrip('/')}{path}"
    session = _get_session()

    try:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body = ""
        try:
            body = exc.response.text[:500] if exc.response is not None else ""
        except Exception:
            pass
        return {"error": f"Confluence API returned HTTP {status}: {body}"}
    except requests.exceptions.ConnectionError as exc:
        return {"error": f"Cannot connect to Confluence at {CONFLUENCE_BASE_URL}: {exc}"}
    except Exception as exc:
        return {"error": f"Confluence API error: {exc}"}


# ── HTML→Text conversion ────────────────────────────────────────────────────

class _HTMLTextExtractor(HTMLParser):
    """Strips HTML tags and extracts readable text with rich formatting.

    Preserves:
      • Code blocks (<pre>/<code>) → triple-backtick fences
      • Bold (<strong>/<b>) → **text**
      • Italic (<em>/<i>) → *text*
      • Images (<img>) → [Image: alt/filename]
      • Internal links (<a>) → [text](url)
      • Headings, lists, tables, blockquotes
    """

    def __init__(self, base_url: str = ""):
        super().__init__()
        self._result: list[str] = []
        self._skip = False
        self._list_depth = 0
        self._in_code = False
        self._in_pre = False
        self._in_link = False
        self._link_href = ""
        self._link_text: list[str] = []
        self._base_url = base_url

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        tag = tag.lower()
        attr_dict = dict(attrs)

        if tag in ("script", "style", "head"):
            self._skip = True
        elif tag == "br":
            self._result.append("\n")

        # ── Code blocks ──
        elif tag == "pre":
            self._in_pre = True
            self._result.append("\n```\n")
        elif tag == "code":
            if not self._in_pre:
                self._in_code = True
                self._result.append("`")

        # ── Headings / structure ──
        elif tag in ("p", "div", "tr", "blockquote"):
            self._result.append("\n")
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._result.append("\n")
            level = int(tag[1])
            self._result.append("#" * level + " ")

        # ── Lists ──
        elif tag in ("ul", "ol"):
            self._list_depth += 1
            self._result.append("\n")
        elif tag == "li":
            indent = "  " * max(0, self._list_depth - 1)
            self._result.append(f"\n{indent}• ")

        # ── Tables ──
        elif tag in ("td", "th"):
            self._result.append(" | ")

        # ── Bold / Italic ──
        elif tag in ("strong", "b"):
            self._result.append("**")
        elif tag in ("em", "i"):
            self._result.append("*")

        # ── Images ──
        elif tag == "img":
            alt = attr_dict.get("alt", "")
            src = attr_dict.get("data-filename", "") or attr_dict.get("src", "")
            label = alt or src.split("/")[-1] if src else "image"
            self._result.append(f"[Image: {label}]")

        # ── Links ──
        elif tag == "a":
            href = attr_dict.get("href", "")
            if href and not href.startswith("#"):
                self._in_link = True
                # Make relative Confluence links absolute
                if href.startswith("/"):
                    href = f"{self._base_url}{href}"
                self._link_href = href
                self._link_text = []

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if tag in ("script", "style", "head"):
            self._skip = False

        # ── Code blocks ──
        elif tag == "pre":
            self._in_pre = False
            self._result.append("\n```\n")
        elif tag == "code":
            if not self._in_pre:
                self._in_code = False
                self._result.append("`")

        # ── Headings / structure ──
        elif tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._result.append("\n")
        elif tag == "tr":
            self._result.append("\n")

        # ── Lists ──
        elif tag in ("ul", "ol"):
            self._list_depth = max(0, self._list_depth - 1)

        # ── Bold / Italic ──
        elif tag in ("strong", "b"):
            self._result.append("**")
        elif tag in ("em", "i"):
            self._result.append("*")

        # ── Links ──
        elif tag == "a":
            if self._in_link:
                text = "".join(self._link_text).strip()
                if text:
                    self._result.append(f"[{text}]({self._link_href})")
                self._in_link = False
                self._link_href = ""
                self._link_text = []

    def handle_data(self, data: str):
        if self._skip:
            return
        if self._in_link:
            self._link_text.append(data)
        else:
            self._result.append(data)

    def get_text(self) -> str:
        raw = "".join(self._result)
        # Collapse multiple blank lines
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def _html_to_text(html: str, base_url: str = "") -> str:
    """Convert Confluence HTML body to clean readable text."""
    extractor = _HTMLTextExtractor(base_url=base_url)
    extractor.feed(html)
    return extractor.get_text()



# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════════


def search_confluence(
    query: str,
    space_key: str | None = None,
    ancestor_page_id: str | None = None,
    max_results: int = 50,
    start: int = 0,
) -> str:
    """
    Search Confluence pages — the primary tool for finding documentation.

    USE THIS TOOL when the user says 'docs', 'documentation', 'wiki', 'runbook',
    'find page about X', or any documentation-related question.

    Searches both page titles and content, ranked by relevance (same as the web UI).

    Args:
        query: The search text (e.g. 'Audience Engine'). Also supports raw CQL
               queries (e.g. "type=page AND title~'Walkthrough'").
        space_key: Space to search in (default: ACTIVATE from config).
        ancestor_page_id: Optional parent page ID to scope search within a page tree.
        max_results: Max results to return (default 50).
        start: Pagination offset — skip this many results (default 0). Use for page 2, 3, etc.

    Returns a list of matching pages with titles, space, last modified, and URLs.
    Use get_page_content(page_id='...') to read any result.
    """
    space = space_key or CONFLUENCE_SPACE_KEY
    root_page = ancestor_page_id or CONFLUENCE_ROOT_PAGE_ID

    # Build CQL: if user gives raw CQL (contains '='), use as-is
    if "=" in query:
        cql = query
        if space and "space" not in query.lower():
            cql = f'space="{space}" AND ({cql})'
    else:
        # Use siteSearch for proper relevance ranking (like the web UI)
        cql = f'type=page AND space="{space}"'

    # Optionally scope to a page tree
    if root_page:
        cql = f'ancestor={root_page} AND ({cql})'

    # Use /rest/api/search — this is the same endpoint the Confluence web UI
    # uses. It supports full-text relevance-ranked search via the 'cql'
    # parameter combined with 'cqlcontext' or by passing the query as
    # a siteSearch term.
    err = validate_confluence()
    if err:
        return err

    # For raw CQL queries, use CQL search directly
    if "=" in query:
        params = {
            "cql": cql,
            "limit": str(max_results),
            "start": str(start),
            "expand": "content.version,content.space,content.ancestors",
        }
        data = _api_get("/rest/api/search", params=params)
    else:
        # For natural language queries, use siteSearch for relevance ranking
        escaped = query.replace('"', '\\"')
        search_cql = f'{cql} AND siteSearch ~ "{escaped}"'
        params = {
            "cql": search_cql,
            "limit": str(max_results),
            "start": str(start),
            "expand": "content.version,content.space,content.ancestors",
        }
        data = _api_get("/rest/api/search", params=params)

    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return f"No pages found matching '{query}' in space {space}."

    lines = [f"🔍 **{len(results)} result(s) for '{query}'**\n"]

    for item in results:
        # /rest/api/search wraps results under 'content' key
        page = item.get("content", item)
        pid = page.get("id", "?")
        title = page.get("title", "?")
        space_name = page.get("space", {}).get("name", space)
        version = page.get("version", {})
        modified_by = version.get("by", {}).get("displayName", "?")
        modified_when = version.get("when", "?")[:10] if version.get("when") else "?"
        page_url = f"{CONFLUENCE_BASE_URL}{page.get('_links', {}).get('webui', '')}"

        # Ancestor breadcrumb
        ancestors = page.get("ancestors", [])
        breadcrumb = " > ".join(a.get("title", "?") for a in ancestors) if ancestors else ""

        lines.append(f"📄 **{title}**")
        lines.append(f"   ID       : {pid}")
        if breadcrumb:
            lines.append(f"   Path     : {breadcrumb} > {title}")
        lines.append(f"   Space    : {space_name}")
        lines.append(f"   Modified : {modified_when} by {modified_by}")
        lines.append(f"   URL      : {page_url}")
        lines.append("")

    # Pagination info
    total = data.get("totalSize", len(results)) if isinstance(data, dict) else len(results)
    if start + len(results) < total:
        lines.append(f"\n📄 Showing {start + 1}–{start + len(results)} of {total} total results.")
        lines.append(f"   💡 Use `start={start + len(results)}` to see more.")
    elif total > len(results):
        lines.append(f"\n📄 Showing {len(results)} results (total: {total}).")

    return "\n".join(lines)


def get_page_content(
    page_id: str | None = None,
    title: str | None = None,
    space_key: str | None = None,
    include_metadata: bool = True,
) -> str:
    """
    Read the full content of a Confluence page.

    Args:
        page_id: The page ID (numeric). Provide this OR title.
        title: Page title to look up (slower, requires space_key).
        space_key: Space key (default from config). Required if using title.
        include_metadata: Include page metadata header (default True).

    Returns the page content converted to clean readable text.
    """
    space = space_key or CONFLUENCE_SPACE_KEY

    # Resolve title → page_id
    if not page_id and title:
        lookup = _api_get("/rest/api/content", params={
            "spaceKey": space,
            "title": title,
            "type": "page",
            "limit": "1",
        })
        if isinstance(lookup, dict) and "error" in lookup:
            return lookup["error"]
        results = lookup.get("results", []) if isinstance(lookup, dict) else []
        if not results:
            return f"No page found with title '{title}' in space {space}."
        page_id = results[0]["id"]

    if not page_id:
        return "❌ Please provide either page_id or title."

    # Fetch page with body
    raw = _api_get(f"/rest/api/content/{page_id}", params={
        "expand": "body.view,version,space,ancestors,children.page",
    })
    if isinstance(raw, dict) and "error" in raw:
        return raw["error"]
    if not isinstance(raw, dict):
        return "❌ Unexpected response format from Confluence."

    page_data: dict[str, Any] = raw
    title = page_data.get("title", "?")
    space_name = page_data.get("space", {}).get("name", "?")
    version = page_data.get("version", {})
    modified_by = version.get("by", {}).get("displayName", "?")
    when_str = version.get("when", "")
    modified_when = when_str[:10] if when_str else "?"
    ver_num = version.get("number", "?")
    page_url = f"{CONFLUENCE_BASE_URL}{page_data.get('_links', {}).get('webui', '')}"

    # Extract body
    body_html = page_data.get("body", {}).get("view", {}).get("value", "")
    content = _html_to_text(body_html, base_url=CONFLUENCE_BASE_URL)

    # Ancestor breadcrumb
    ancestors = page_data.get("ancestors", [])
    breadcrumb = " > ".join(a.get("title", "?") for a in ancestors)

    # Child pages
    children = page_data.get("children", {}).get("page", {}).get("results", [])

    lines = []
    if include_metadata:
        lines.extend([
            f"📄 **{title}**",
            f"   Space    : {space_name}",
            f"   Path     : {breadcrumb + ' > ' + str(title) if breadcrumb else title}",
            f"   Version  : {ver_num} — Modified {modified_when} by {modified_by}",
            f"   URL      : {page_url}",
            f"   Page ID  : {page_id}",
        ])
        if children:
            lines.append(f"   Children : {len(children)} child page(s)")
        lines.append("")
        lines.append("─" * 80)
        lines.append("")

    lines.append(content)

    if children:
        lines.append("")
        lines.append("─" * 80)
        lines.append(f"\n📂 **Child Pages ({len(children)}):**")
        for child in children:
            cid = child.get("id", "?")
            ctitle = child.get("title", "?")
            lines.append(f"  • [{ctitle}] (ID: {cid})")

    return "\n".join(lines)


def get_child_pages(
    page_id: str,
    include_content: bool = False,
    max_results: int = 50,
) -> str:
    """
    List all child pages of a given parent page.

    Args:
        page_id: The parent page ID.
        include_content: If True, include a short content preview for each child.
        max_results: Max children to return (default 50).

    Returns a list of child pages with titles and IDs.
    """
    expand = "version"
    if include_content:
        expand += ",body.view"

    data = _api_get(f"/rest/api/content/{page_id}/child/page", params={
        "expand": expand,
        "limit": str(max_results),
    })
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return f"No child pages found for page ID {page_id}."

    lines = [f"📂 **{len(results)} Child Page(s) of page {page_id}**\n"]

    for page in results:
        pid = page.get("id", "?")
        title = page.get("title", "?")
        version = page.get("version", {})
        when_val = version.get("when", "")
        modified = when_val[:10] if when_val else "?"
        page_url = f"{CONFLUENCE_BASE_URL}{page.get('_links', {}).get('webui', '')}"

        lines.append(f"📄 **{title}**")
        lines.append(f"   ID       : {pid}")
        lines.append(f"   Modified : {modified}")
        lines.append(f"   URL      : {page_url}")

        if include_content:
            body_html = page.get("body", {}).get("view", {}).get("value", "")
            preview = _html_to_text(body_html, base_url=CONFLUENCE_BASE_URL)[:200]
            if preview:
                lines.append(f"   Preview  : {preview}...")

        lines.append("")

    return "\n".join(lines)


def get_space_pages(
    space_key: str | None = None,
    max_results: int = 50,
    page_type: str = "page",
    start: int = 0,
) -> str:
    """
    List all pages in a Confluence space.

    Args:
        space_key: Space key (default: ACTIVATE from config).
        max_results: Max pages per request (default 50, max 100).
        page_type: 'page' or 'blogpost' (default 'page').
        start: Pagination offset (default 0).

    Returns a list of pages with titles, IDs, and URLs (paginated).
    """
    space = space_key or CONFLUENCE_SPACE_KEY

    data = _api_get("/rest/api/content", params={
        "spaceKey": space,
        "type": page_type,
        "limit": str(min(max_results, 100)),
        "start": str(start),
        "expand": "version,ancestors",
        "orderby": "title",
    })
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    total_size = data.get("size", len(results)) if isinstance(data, dict) else len(results)

    if not results:
        return f"No pages found in space {space}."

    total_avail = data.get("totalSize", "?") if isinstance(data, dict) else "?"
    lines = [f"📚 **Pages in space '{space}'** (showing {start+1}–{start+len(results)} of {total_avail})\n"]

    for page in results:
        pid = page.get("id", "?")
        title = page.get("title", "?")
        ancestors = page.get("ancestors", [])
        depth = len(ancestors)
        indent = "  " * depth
        page_url = f"{CONFLUENCE_BASE_URL}{page.get('_links', {}).get('webui', '')}"

        lines.append(f"{indent}📄 {title}  (ID: {pid})")

    if isinstance(data, dict) and data.get("_links", {}).get("next"):
        lines.append(f"\n💡 More pages available. Use `start={start + len(results)}` to see the next batch.")

    return "\n".join(lines)


def get_page_attachments(
    page_id: str,
    max_results: int = 50,
) -> str:
    """
    List all file attachments on a Confluence page.

    Args:
        page_id: The page ID.
        max_results: Max attachments to return (default 50).

    Returns a list of attachments with name, size, type, and download URL.
    """
    data = _api_get(f"/rest/api/content/{page_id}/child/attachment", params={
        "limit": str(max_results),
        "expand": "version",
    })
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return f"No attachments found on page {page_id}."

    lines = [f"📎 **{len(results)} Attachment(s) on page {page_id}**\n"]

    for att in results:
        title = att.get("title", "?")
        media_type = att.get("metadata", {}).get("mediaType", "?")
        size_bytes = att.get("extensions", {}).get("fileSize", 0)
        size_str = _format_size(size_bytes)
        version = att.get("version", {}).get("number", "?")
        download = att.get("_links", {}).get("download", "")
        download_url = f"{CONFLUENCE_BASE_URL}{download}" if download else "?"
        comment = att.get("metadata", {}).get("comment", "")

        lines.append(f"📄 **{title}**")
        lines.append(f"   Type     : {media_type}")
        lines.append(f"   Size     : {size_str}")
        lines.append(f"   Version  : {version}")
        lines.append(f"   Download : {download_url}")
        if comment:
            lines.append(f"   Comment  : {comment}")
        lines.append("")

    return "\n".join(lines)


def _format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def get_page_labels(page_id: str) -> str:
    """
    Get all labels (tags) on a Confluence page.

    Args:
        page_id: The page ID.

    Returns a list of labels. Useful for finding related pages or understanding
    page categorization.
    """
    data = _api_get(f"/rest/api/content/{page_id}/label")
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return f"No labels found on page {page_id}."

    labels = [lab.get("name", "?") for lab in results]
    return f"🏷️ **Labels on page {page_id}:** {', '.join(labels)}"


def get_page_comments(
    page_id: str,
    max_results: int = 25,
) -> str:
    """
    Get comments on a Confluence page.

    Args:
        page_id: The page ID.
        max_results: Max comments to return (default 25).

    Returns comments with author, date, and content.
    Useful for understanding discussions and context around a page.
    """
    data = _api_get(f"/rest/api/content/{page_id}/child/comment", params={
        "expand": "body.view,version",
        "limit": str(max_results),
    })
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return f"No comments found on page {page_id}."

    lines = [f"💬 **{len(results)} Comment(s) on page {page_id}**\n"]

    for comment in results:
        version = comment.get("version", {})
        author = version.get("by", {}).get("displayName", "?")
        when_str = version.get("when", "")
        when = when_str[:16].replace("T", " ") if when_str else "?"
        body_html = comment.get("body", {}).get("view", {}).get("value", "")
        body_text = _html_to_text(body_html, base_url=CONFLUENCE_BASE_URL)

        lines.append(f"👤 **{author}** — {when}")
        lines.append(f"   {body_text}")
        lines.append("")

    return "\n".join(lines)


# ── HTTP POST/PUT for write operations ───────────────────────────────────────

def _api_post(path: str, body: dict | None = None) -> dict[str, Any]:
    """POST request to Confluence REST API v1."""
    err = validate_confluence()
    if err:
        return {"error": err}

    url = f"{CONFLUENCE_BASE_URL.rstrip('/')}{path}"
    session = _get_session()

    try:
        resp = session.post(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body_text = ""
        try:
            body_text = exc.response.text[:500] if exc.response is not None else ""
        except Exception:
            pass
        return {"error": f"Confluence API returned HTTP {status}: {body_text}"}
    except Exception as exc:
        return {"error": f"Confluence API error: {exc}"}


def _api_put(path: str, body: dict | None = None) -> dict[str, Any]:
    """PUT request to Confluence REST API v1."""
    err = validate_confluence()
    if err:
        return {"error": err}

    url = f"{CONFLUENCE_BASE_URL.rstrip('/')}{path}"
    session = _get_session()

    try:
        resp = session.put(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body_text = ""
        try:
            body_text = exc.response.text[:500] if exc.response is not None else ""
        except Exception:
            pass
        return {"error": f"Confluence API returned HTTP {status}: {body_text}"}
    except Exception as exc:
        return {"error": f"Confluence API error: {exc}"}


def create_confluence_page(
    title: str,
    body: str,
    space_key: str | None = None,
    parent_page_id: str | None = None,
) -> str:
    """
    Create a new Confluence page.

    Use this to create incident reports, runbook entries, meeting notes,
    or any documentation. Content should be in simple HTML or plain text
    (which will be wrapped in HTML paragraphs).

    Args:
        title: The page title (must be unique within the space).
        body: The page content. Can be HTML or plain text.
              For plain text, paragraphs are separated by blank lines.
        space_key: Space key (default from config).
        parent_page_id: Optional parent page ID to nest under.

    Returns confirmation with the new page ID and URL.
    """
    space = space_key or CONFLUENCE_SPACE_KEY

    # If body looks like plain text (no HTML tags), wrap in paragraphs
    if "<" not in body:
        paragraphs = body.split("\n\n")
        body_html = "".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())
    else:
        body_html = body

    payload: dict[str, Any] = {
        "type": "page",
        "title": title,
        "space": {"key": space},
        "body": {
            "storage": {
                "value": body_html,
                "representation": "storage",
            }
        },
    }

    if parent_page_id:
        payload["ancestors"] = [{"id": parent_page_id}]

    data = _api_post("/rest/api/content", body=payload)
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    page_id = data.get("id", "?")
    page_url = f"{CONFLUENCE_BASE_URL}{data.get('_links', {}).get('webui', '')}"

    return (
        f"✅ **Created page: {title}**\n"
        f"   Page ID : {page_id}\n"
        f"   Space   : {space}\n"
        f"   URL     : {page_url}\n"
        f"   💡 Use `get_page_content(page_id='{page_id}')` to verify the content."
    )


def update_confluence_page(
    page_id: str,
    body: str,
    title: str | None = None,
    append: bool = False,
) -> str:
    """
    Update an existing Confluence page's content.

    Args:
        page_id: The page ID to update.
        body: New page content (HTML or plain text).
        title: Optional new title. If omitted, keeps the existing title.
        append: If True, append to existing content instead of replacing.

    Returns confirmation of the update.
    """
    # Get current page info (need version number and current title)
    current = _api_get(f"/rest/api/content/{page_id}", params={
        "expand": "version,body.storage",
    })
    if isinstance(current, dict) and "error" in current:
        return current["error"]
    if not isinstance(current, dict):
        return "❌ Unexpected response format."

    current_version = current.get("version", {}).get("number", 0)
    current_title = current.get("title", "?")
    new_title = title or current_title

    # If body looks like plain text, wrap in paragraphs
    if "<" not in body:
        paragraphs = body.split("\n\n")
        body_html = "".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())
    else:
        body_html = body

    # If appending, prepend existing content
    if append:
        existing_html = current.get("body", {}).get("storage", {}).get("value", "")
        body_html = existing_html + body_html

    payload: dict[str, Any] = {
        "type": "page",
        "title": new_title,
        "version": {"number": current_version + 1},
        "body": {
            "storage": {
                "value": body_html,
                "representation": "storage",
            }
        },
    }

    data = _api_put(f"/rest/api/content/{page_id}", body=payload)
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    page_url = f"{CONFLUENCE_BASE_URL}{data.get('_links', {}).get('webui', '')}"
    action = "Appended to" if append else "Updated"

    return (
        f"✅ **{action} page: {new_title}**\n"
        f"   Page ID  : {page_id}\n"
        f"   Version  : {current_version + 1}\n"
        f"   URL      : {page_url}"
    )
