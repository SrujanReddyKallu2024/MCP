"""
General-purpose S3 tools.

Provides account-wide Amazon S3 access: list buckets, browse any
bucket/prefix interactively, and inspect object metadata.

These are GENERAL S3 tools for any bucket.  For EMR-specific Spark log
navigation use browse_s3_logs and read_s3_file from emr_tools instead.

Tools:
  * list_s3_buckets    -- list all S3 buckets in the AWS account
  * browse_s3          -- interactively browse folders and files in any bucket
  * get_s3_object_info -- get full metadata for a single object (head_object)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from mcp_server.config import AWS_REGION, validate_env, aws_env_header
from mcp_server.tools._aws_helpers import get_s3_client, fmt_size


# Use shared S3 client from _aws_helpers
_get_s3 = get_s3_client
_fmt_size = fmt_size


def _fmt_time(dt: Any) -> str:
    """Format a datetime to a clean readable string."""
    if dt is None:
        return "—"
    try:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(dt)[:19]
    except Exception:
        return str(dt)


# ═════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═════════════════════════════════════════════════════════════════════════════


def list_s3_buckets(env: str | None = None) -> str:
    """
    List all S3 buckets in the AWS account.

    USE THIS TOOL when the user asks about S3 buckets, storage,
    "what buckets do we have", or anything about S3 at account level.

    Args:
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns every bucket with its creation date, sorted alphabetically.
    """
    err = validate_env(env)
    if err:
        return err

    s3 = _get_s3(env)

    try:
        resp = s3.list_buckets()
    except Exception as exc:
        return f"❌ Failed to list S3 buckets: {exc}"

    buckets = resp.get("Buckets", [])
    if not buckets:
        return aws_env_header(env) + "📦 No S3 buckets found in this account."

    buckets_sorted = sorted(buckets, key=lambda b: b.get("Name", ""))

    lines = [
        aws_env_header(env),
        f"🪣 **{len(buckets_sorted)} S3 Bucket(s)**",
        f"   Region: {AWS_REGION}",
        "",
    ]

    for b in buckets_sorted:
        name = b.get("Name", "?")
        created = _fmt_time(b.get("CreationDate"))
        lines.append(f"  🪣 **{name}**")
        lines.append(f"     Created: {created}")

    lines.append("")
    lines.append("💡 Use `browse_s3(bucket='bucket-name')` to browse a bucket.")
    lines.append("💡 Use `read_s3_file(s3_uri='s3://bucket/key')` to read a file.")

    return "\n".join(lines)


def browse_s3(
    bucket: str,
    prefix: str = "",
    max_results: int = 200,
    env: str | None = None,
) -> str:
    """
    Browse folders and files in any S3 bucket interactively.

    USE THIS TOOL when the user asks to see what's in an S3 bucket,
    list files in a folder, navigate S3, or explore any S3 path.

    Start with no prefix to see top-level folders, then drill into
    subfolders using the hints in the output.

    Args:
        bucket: S3 bucket name (required).
        prefix: S3 prefix/path to browse (default: root of bucket).
                Example: 'raw/hem_processing/' to see that folder.
        max_results: Max items to show (default 200).
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a directory listing showing folders and files with sizes
    and last modified times.
    """
    err = validate_env(env)
    if err:
        return err

    s3 = _get_s3(env)

    # Normalise prefix — ensure trailing slash for directory listing
    browse_prefix = prefix.strip()
    if browse_prefix and not browse_prefix.endswith("/"):
        browse_prefix += "/"

    try:
        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=browse_prefix,
            Delimiter="/",
            MaxKeys=max_results,
        )
    except s3.exceptions.NoSuchBucket if hasattr(s3, "exceptions") else Exception as exc:
        return f"❌ Failed to browse `s3://{bucket}/{browse_prefix}`: {exc}"
    except Exception as exc:
        return f"❌ Failed to browse `s3://{bucket}/{browse_prefix}`: {exc}"

    folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    files = resp.get("Contents", [])

    if not folders and not files:
        return (
            f"📂 **Empty:** `s3://{bucket}/{browse_prefix}`\n"
            f"   No files or folders found at this path."
        )

    lines = [f"📂 **s3://{bucket}/{browse_prefix or ''}**\n"]

    # ── Folders ──
    if folders:
        lines.append(f"**Folders ({len(folders)}):**")
        for folder in sorted(folders):
            name = folder[len(browse_prefix):].rstrip("/")
            lines.append(f"  📁 **{name}/**")
            lines.append(
                f"     → `browse_s3(bucket='{bucket}', prefix='{folder}')`"
            )
        lines.append("")

    # ── Files ──
    real_files = [f for f in files if f["Key"] != browse_prefix]
    if real_files:
        lines.append(f"**Files ({len(real_files)}):**")

        # Column-aligned output
        for f in sorted(real_files, key=lambda x: x["Key"]):
            name = f["Key"][len(browse_prefix):]
            size = _fmt_size(f.get("Size", 0))
            modified = _fmt_time(f.get("LastModified"))
            storage = f.get("StorageClass", "STANDARD")

            lines.append(f"  📄 {name}")
            lines.append(
                f"     {size}  |  {modified}  |  {storage}"
            )

        lines.append("")
        # Hint for reading files — use the FIRST file as example
        first_key = real_files[0]["Key"]
        lines.append(
            f"💡 Use `read_s3_file(s3_uri='s3://{bucket}/{first_key}')` to read a file."
        )
        lines.append(
            f"💡 Use `get_s3_object_info(s3_uri='s3://{bucket}/{first_key}')` for file details."
        )

    # ── Truncation warning ──
    if resp.get("IsTruncated"):
        lines.append(
            f"\n⚠️ More items exist — showing first {max_results}. "
            f"Use a more specific prefix to see deeper."
        )

    return "\n".join(lines)


def list_s3_recursive(
    bucket: str,
    prefix: str = "",
    name_filter: str | None = None,
    extension_filter: str | None = None,
    max_results: int = 500,
    env: str | None = None,
) -> str:
    """
    Recursively list ALL files under an S3 bucket/prefix in a single call.

    USE THIS TOOL when the user asks to see everything in a bucket or folder
    end-to-end, wants a full file listing, or needs to find files by name
    or extension across nested folders.

    Args:
        bucket: S3 bucket name (required).
        prefix: Starting prefix/folder (default: root of bucket).
                Example: 'raw/hem_processing/' to list that subtree.
        name_filter: Optional — case-insensitive substring match on filename.
                     Example: 'taxonomy' shows only files with 'taxonomy' in the name.
        extension_filter: Optional — file extension to filter by (with or without dot).
                          Example: '.csv' or 'csv' or '.parquet' or '.gz'
        max_results: Max files to return (default 500, max 2000).
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a full recursive file listing with sizes, plus a summary with
    total count, total size, and file type breakdown.
    """
    err = validate_env(env)
    if err:
        return err

    max_results = max(1, min(max_results, 2000))

    # Normalise extension filter
    ext = None
    if extension_filter:
        ext = extension_filter.strip().lower()
        if not ext.startswith("."):
            ext = "." + ext

    name_lower = name_filter.strip().lower() if name_filter else None

    s3 = _get_s3(env)

    # Normalise prefix
    scan_prefix = prefix.strip()
    if scan_prefix and not scan_prefix.endswith("/"):
        scan_prefix += "/"

    files: list[dict] = []
    total_size = 0
    total_count = 0
    ext_counts: dict[str, int] = {}
    ext_sizes: dict[str, int] = {}
    continuation_token = None
    truncated = False

    try:
        while True:
            kwargs: dict = {
                "Bucket": bucket,
                "Prefix": scan_prefix,
                "MaxKeys": 1000,
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = s3.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):  # Skip folder markers
                    continue

                filename = key.rsplit("/", 1)[-1]
                file_ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""

                # Apply filters
                if ext and file_ext != ext:
                    continue
                if name_lower and name_lower not in filename.lower():
                    continue

                size = obj.get("Size", 0)
                total_count += 1
                total_size += size
                ext_counts[file_ext or "(no ext)"] = ext_counts.get(file_ext or "(no ext)", 0) + 1
                ext_sizes[file_ext or "(no ext)"] = ext_sizes.get(file_ext or "(no ext)", 0) + size

                if len(files) < max_results:
                    files.append({
                        "key": key,
                        "size": size,
                        "modified": _fmt_time(obj.get("LastModified")),
                    })

            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")
            # Safety: if we've already got enough counted files, stop paginating
            if total_count > max_results * 5:
                truncated = True
                break

    except Exception as exc:
        return f"❌ Failed to list `s3://{bucket}/{scan_prefix}`: {exc}"

    if total_count == 0:
        filter_msg = ""
        if ext:
            filter_msg += f" matching `*{ext}`"
        if name_lower:
            filter_msg += f" containing '{name_filter}'"
        return f"📂 No files found in `s3://{bucket}/{scan_prefix}`{filter_msg}."

    # ── Build output ──
    lines = [f"📂 **s3://{bucket}/{scan_prefix or ''}** — {total_count:,} file(s), {_fmt_size(total_size)} total\n"]

    if ext or name_lower:
        filters = []
        if ext:
            filters.append(f"extension=`{ext}`")
        if name_lower:
            filters.append(f"name contains '{name_filter}'")
        lines.append(f"   🔍 Filters: {', '.join(filters)}\n")

    # File listing
    for f in files:
        rel_path = f["key"][len(scan_prefix):] if scan_prefix else f["key"]
        lines.append(f"  📄 {rel_path}")
        lines.append(f"     {_fmt_size(f['size'])}  |  {f['modified']}")

    if total_count > len(files):
        lines.append(f"\n   ⚠️ Showing {len(files):,} of {total_count:,} files (use filters to narrow down).")

    if truncated:
        lines.append(f"   ⚠️ Scan stopped early — more files exist. Use a more specific prefix or filter.")

    # ── Summary footer ──
    lines.append("")
    lines.append("**Summary:**")
    lines.append(f"   Total files : {total_count:,}")
    lines.append(f"   Total size  : {_fmt_size(total_size)}")
    if len(ext_counts) > 1 or (len(ext_counts) == 1 and not ext):
        lines.append("   **By type:**")
        for e in sorted(ext_counts, key=lambda x: -ext_sizes.get(x, 0)):
            lines.append(f"     {e:12s}  {ext_counts[e]:>5,} file(s)  {_fmt_size(ext_sizes[e])}")

    lines.append("")
    first_key = files[0]["key"] if files else f"{scan_prefix}example.csv"
    lines.append(f"💡 `read_s3_file(s3_uri='s3://{bucket}/{first_key}')` to read a file.")
    lines.append(f"💡 `get_s3_object_info(s3_uri='s3://{bucket}/{first_key}')` for file details.")

    return "\n".join(lines)


def get_s3_object_info(s3_uri: str, env: str | None = None) -> str:
    """
    Get detailed metadata for a single S3 object without downloading it.

    USE THIS TOOL when the user asks about a specific file's size,
    modification time, type, or other details. This is fast because
    it only reads the metadata header, not the file content.

    Args:
        s3_uri: Full S3 URI (e.g. 's3://bucket-name/path/to/file.csv').
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns file metadata: size, last modified, content type, storage
    class, encryption, and ETag.
    """
    err = validate_env(env)
    if err:
        return err

    if not s3_uri.startswith("s3://"):
        return "❌ Invalid S3 URI: '{s3_uri}'. Must start with 's3://' (e.g. 's3://bucket-name/path/to/file.csv')."

    # Parse URI
    path = s3_uri.replace("s3://", "")
    bucket, _, key = path.partition("/")
    if not key:
        return f"❌ No file path specified in URI: '{s3_uri}'."

    s3 = _get_s3(env)

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        error_str = str(exc)
        if "404" in error_str or "NoSuchKey" in error_str or "Not Found" in error_str:
            return f"❌ Object not found: `{s3_uri}`"
        if "403" in error_str or "Forbidden" in error_str or "AccessDenied" in error_str:
            return f"❌ Access denied: `{s3_uri}` — check IAM permissions."
        return f"❌ Failed to get info for `{s3_uri}`: {exc}"

    size_raw = head.get("ContentLength", 0)
    modified = head.get("LastModified")
    content_type = head.get("ContentType", "—")
    etag = head.get("ETag", "—").strip('"')
    storage_class = head.get("StorageClass", "STANDARD")
    encryption = head.get("ServerSideEncryption", "—")
    version_id = head.get("VersionId", "—")
    encoding = head.get("ContentEncoding", "—")

    # User-defined metadata
    metadata = head.get("Metadata", {})

    lines = [
        f"📄 **Object Info:** `{s3_uri}`",
        "",
        f"   Size           : {_fmt_size(size_raw)} ({size_raw:,} bytes)",
        f"   Last Modified  : {_fmt_time(modified)}",
        f"   Content Type   : {content_type}",
        f"   Storage Class  : {storage_class}",
        f"   ETag           : {etag}",
        f"   Encryption     : {encryption}",
    ]

    if version_id and version_id != "—":
        lines.append(f"   Version ID     : {version_id}")

    if encoding and encoding != "—":
        lines.append(f"   Encoding       : {encoding}")

    if metadata:
        lines.append("")
        lines.append("   **User Metadata:**")
        for k, v in metadata.items():
            lines.append(f"     {k}: {v}")

    lines.append("")
    lines.append(f"💡 Use `read_s3_file(s3_uri='{s3_uri}')` to read the file contents.")

    return "\n".join(lines)
