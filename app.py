import asyncio
import json
import logging
import mimetypes
import os
import re
import secrets
import subprocess
import tempfile
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urlparse
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple
import inspect

import asyncpg
import requests
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ClientAuthenticationError
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential, DefaultAzureCredential, ManagedIdentityCredential
from openai import AzureOpenAI
from quart import Quart, Response, abort, g, jsonify, redirect, render_template, request, session, url_for


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOTENV_PATHS = [
    REPO_ROOT / "r7jibungotoka" / "src" / ".env",
    REPO_ROOT / "r7jibungotoka" / ".env",
]

for dotenv_path in DEFAULT_DOTENV_PATHS:
    if dotenv_path.exists():
        load_dotenv(dotenv_path)

load_dotenv()  # allow environment overrides


logging.basicConfig(level=os.getenv("ADMINPORTAL_LOG_LEVEL", "INFO"))
logger = logging.getLogger("adminportal")


CHUNK_CHAR_LENGTH = int(os.getenv("CHUNK_CHAR_LENGTH", "1000"))
CHUNK_CHAR_OVERLAP = int(os.getenv("CHUNK_CHAR_OVERLAP", "200"))
MAX_LIST_BLOBS = int(os.getenv("ADMINPORTAL_MAX_LIST_BLOBS", "500"))
ENABLE_LIBREOFFICE_CONVERSION = os.getenv("ENABLE_LIBREOFFICE_CONVERSION", "0") == "1"
CHUNK_PAGE_SIZE = int(os.getenv("ADMINPORTAL_CHUNK_PAGE_SIZE", "50"))
COLLECTION_ID = os.getenv("COLLECTION_ID") or os.getenv("ADMINPORTAL_COLLECTION_ID") or "kumozu"
MANIFEST_CONTAINER = os.getenv("MANIFEST_CONTAINER", "manifests")
MANIFEST_BLOB_NAME = os.getenv("MANIFEST_BLOB_NAME", f"{COLLECTION_ID}/manifest.json")


UPSERT_SQL = """
INSERT INTO documents
(id, doc_id, chunk_index, title, page_start, page_end, content, metadata, embedding)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (doc_id, chunk_index) DO UPDATE SET
  title = EXCLUDED.title,
  page_start = EXCLUDED.page_start,
  page_end = EXCLUDED.page_end,
  content = EXCLUDED.content,
  metadata = EXCLUDED.metadata,
  embedding = EXCLUDED.embedding;
"""


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _secure_filename(filename: str) -> str:
    name = (filename or "").strip().replace("\\", "/").split("/")[-1]
    name = unicodedata.normalize("NFKC", name).replace("\x00", "")

    # Keep Unicode filenames intact; remove path-ish and control characters.
    cleaned: List[str] = []
    for ch in name:
        if ch in {"/", "\\"}:
            cleaned.append("_")
            continue
        if unicodedata.category(ch).startswith("C"):
            continue
        cleaned.append(ch)

    name = "".join(cleaned)
    name = re.sub(r"[<>:\"|?*]+", "_", name)  # Windows-invalid filename chars
    name = re.sub(r"\s+", " ", name).strip()
    name = name.strip(" ._")
    name = re.sub(r"_+", "_", name)

    if not name:
        return f"upload_{secrets.token_hex(8)}"

    max_len = 180
    if len(name) > max_len:
        suffix = Path(name).suffix
        stem = Path(name).stem
        keep = max(1, max_len - len(suffix))
        name = stem[:keep] + suffix

    return name


_SITE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def _normalize_site_id(site_id: str) -> str:
    value = (site_id or "").strip()
    if not _SITE_ID_RE.match(value):
        raise ValueError("site_id must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    return value


def _normalize_site_type(site_type: str) -> str:
    value = (site_type or "").strip()
    if value not in {"html_pdf_links", "direct_pdf"}:
        raise ValueError("type must be html_pdf_links or direct_pdf")
    return value


def _normalize_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise ValueError("url is required")
    p = urlparse(value)
    if p.scheme not in {"http", "https"} or not p.netloc:
        raise ValueError("url must be http(s)")
    return value


def _normalize_doc_id(doc_id: str) -> str:
    value = (doc_id or "").strip().replace("\\", "/").lstrip("/")
    if not value:
        raise ValueError("doc_id is empty")
    parts = PurePosixPath(value).parts
    if any(p in {".", ".."} for p in parts):
        raise ValueError("doc_id contains invalid path segments")
    return value


def to_pgvector(vec: List[float]) -> str:
    return "[" + ", ".join(f"{x:.8f}" for x in vec) + "]"


async def get_pg() -> asyncpg.Connection:
    sslmode = os.getenv("PGSSLMODE", "require").lower()
    ssl = "require" if sslmode not in {"disable", "allow", "prefer"} else None
    return await asyncpg.connect(
        host=_require_env("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        user=_require_env("PGUSER"),
        password=_require_env("PGPASSWORD"),
        database=_require_env("PGDATABASE"),
        ssl=ssl,
    )


async def list_sites() -> List[Dict[str, Any]]:
    conn = await get_pg()
    try:
        rows = await conn.fetch(
            """
            SELECT site_id, enabled, type, url, created_at, updated_at
            FROM sites
            ORDER BY site_id
            """
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def upsert_site(site_id: str, enabled: bool, site_type: str, url: str) -> None:
    conn = await get_pg()
    try:
        await conn.execute(
            """
            INSERT INTO sites (site_id, enabled, type, url, updated_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (site_id) DO UPDATE SET
              enabled = EXCLUDED.enabled,
              type = EXCLUDED.type,
              url = EXCLUDED.url,
              updated_at = now()
            """,
            site_id,
            enabled,
            site_type,
            url,
        )
    finally:
        await conn.close()


async def toggle_site(site_id: str) -> bool:
    conn = await get_pg()
    try:
        row = await conn.fetchrow(
            """
            UPDATE sites
            SET enabled = NOT enabled, updated_at = now()
            WHERE site_id = $1
            RETURNING enabled
            """,
            site_id,
        )
        if not row:
            raise ValueError("site not found")
        return bool(row["enabled"])
    finally:
        await conn.close()


async def delete_site(site_id: str) -> None:
    conn = await get_pg()
    try:
        await conn.execute("DELETE FROM sites WHERE site_id = $1", site_id)
    finally:
        await conn.close()


def _aca_configured() -> bool:
    return bool(os.getenv("ACA_SUBSCRIPTION_ID") and os.getenv("ACA_RESOURCE_GROUP") and os.getenv("ACA_JOB_NAME"))


def _get_arm_credential():
    """
    Credential used for Azure Resource Manager calls (ACA job status/start).

    Supports:
    - Service principal (recommended when Managed Identity isn't available):
        ACA_ARM_TENANT_ID / ACA_ARM_CLIENT_ID / ACA_ARM_CLIENT_SECRET
    - User-assigned Managed Identity:
        ACA_MANAGED_IDENTITY_CLIENT_ID
    - Fallback:
        DefaultAzureCredential (system-assigned MI on App Service, Azure CLI locally, etc.)
    """

    tenant_id = os.getenv("ACA_ARM_TENANT_ID")
    client_id = os.getenv("ACA_ARM_CLIENT_ID")
    client_secret = os.getenv("ACA_ARM_CLIENT_SECRET")
    if tenant_id and client_id and client_secret:
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    mi_client_id = os.getenv("ACA_MANAGED_IDENTITY_CLIENT_ID")
    if mi_client_id:
        return ManagedIdentityCredential(client_id=mi_client_id)

    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


def _get_arm_token() -> str:
    try:
        credential = _get_arm_credential()
        return credential.get_token("https://management.azure.com/.default").token
    except ClientAuthenticationError as e:
        hint = (
            "ARM auth failed. On Azure App Service, enable a Managed Identity (system- or user-assigned) "
            "and grant it access, or set ACA_ARM_TENANT_ID/ACA_ARM_CLIENT_ID/ACA_ARM_CLIENT_SECRET. "
            "If you use only a user-assigned identity, set ACA_MANAGED_IDENTITY_CLIENT_ID."
        )
        raise RuntimeError(f"{hint} ({e})") from e


def _arm_http_error_hint(resp: requests.Response, operation: str) -> Optional[str]:
    if resp.status_code != 403:
        return None

    scope_hint = (
        "Grant the Web App's Managed Identity an RBAC role that can read/start the ACA Job "
        f"(operation: {operation}). Easiest: assign 'Azure Container Apps Contributor' (or 'Contributor') "
        "on the ACA Job resource or the resource group."
    )

    mi_client_id = os.getenv("ACA_MANAGED_IDENTITY_CLIENT_ID")
    if mi_client_id:
        scope_hint += f" Managed identity client_id in use: {mi_client_id}."
    else:
        scope_hint += " (Using default credential; on App Service this is typically system-assigned MI.)"

    return scope_hint


def _raise_for_status_with_arm_hint(resp: requests.Response, operation: str) -> None:
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        hint = _arm_http_error_hint(resp, operation)
        if hint:
            raise RuntimeError(f"{e}. {hint}") from e
        raise


def _start_aca_job() -> Dict[str, Any]:
    subscription_id = _require_env("ACA_SUBSCRIPTION_ID")
    resource_group = _require_env("ACA_RESOURCE_GROUP")
    job_name = _require_env("ACA_JOB_NAME")
    api_version = os.getenv("ACA_JOBS_API_VERSION", "2023-05-01")

    token = _get_arm_token()
    url = (
        "https://management.azure.com/subscriptions/"
        f"{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.App/jobs/{job_name}/start"
        f"?api-version={api_version}"
    )
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={})
    _raise_for_status_with_arm_hint(resp, operation="Microsoft.App/jobs/start/action")
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # ARM often returns RFC3339 with Z
        cleaned = value.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def _fmt_dt_utc(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    try:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return None


def _aca_status_from_execution(execution: Optional[Dict[str, Any]], fetch_error: Optional[str] = None) -> Dict[str, Any]:
    if fetch_error:
        return {
            "label": "Unknown",
            "chip_class": "chip--warn",
            "title": f"ACA status unavailable: {fetch_error}",
            "is_active": False,
            "execution_name": None,
            "status_raw": None,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
        }

    if not execution:
        return {
            "label": "No runs",
            "chip_class": "chip",
            "title": "No executions found for this ACA job.",
            "is_active": False,
            "execution_name": None,
            "status_raw": None,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
        }

    props = execution.get("properties") or {}
    status_raw = props.get("status") or props.get("provisioningState") or "unknown"
    status_norm = str(status_raw).strip().lower()

    is_active = any(k in status_norm for k in ["running", "inprogress", "in_progress", "processing", "pending", "queued"])

    if "succeed" in status_norm:
        label = "成功"
        chip_class = "chip--ok"
    elif any(k in status_norm for k in ["fail", "error", "cancel"]):
        label = "失敗" if "fail" in status_norm else status_raw
        chip_class = "chip--danger"
    elif any(k in status_norm for k in ["pending", "queued"]):
        label = "待機中"
        chip_class = "chip--warn"
    elif any(k in status_norm for k in ["running", "inprogress", "in_progress", "processing"]):
        label = "実行中"
        chip_class = "chip--warn"
    else:
        label = str(status_raw).strip().title() or "不明"
        chip_class = "chip"

    start_dt = _parse_iso_datetime(props.get("startTime") or props.get("startTimeUtc"))
    end_dt = _parse_iso_datetime(props.get("endTime") or props.get("endTimeUtc"))
    duration_seconds: Optional[int] = None
    if start_dt and end_dt:
        try:
            duration_seconds = max(0, int((end_dt - start_dt).total_seconds()))
        except Exception:
            duration_seconds = None

    exec_name = execution.get("name") or props.get("name") or None
    title = f"Execution: {exec_name or 'unknown'} · Status: {status_raw}"
    if start_dt:
        title += f" · Start: {_fmt_dt_utc(start_dt) or props.get('startTime')}"
    if end_dt:
        title += f" · End: {_fmt_dt_utc(end_dt) or props.get('endTime')}"

    return {
        "label": label,
        "chip_class": chip_class,
        "title": title[:400],
        "is_active": is_active,
        "execution_name": exec_name,
        "status_raw": status_raw,
        "start_time": _fmt_dt_utc(start_dt),
        "end_time": _fmt_dt_utc(end_dt),
        "duration_seconds": duration_seconds,
    }


def _get_aca_latest_execution() -> Optional[Dict[str, Any]]:
    subscription_id = _require_env("ACA_SUBSCRIPTION_ID")
    resource_group = _require_env("ACA_RESOURCE_GROUP")
    job_name = _require_env("ACA_JOB_NAME")
    api_version = os.getenv("ACA_JOBS_API_VERSION", "2023-05-01")

    token = _get_arm_token()
    url = (
        "https://management.azure.com/subscriptions/"
        f"{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.App/jobs/{job_name}/executions"
        f"?api-version={api_version}"
    )
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    _raise_for_status_with_arm_hint(resp, operation="Microsoft.App/jobs/executions/read")
    data = resp.json() if resp.content else {}
    executions = data.get("value") if isinstance(data, dict) else None
    if not isinstance(executions, list) or not executions:
        return None

    def sort_key(ex: Dict[str, Any]) -> float:
        props = ex.get("properties") or {}
        start_dt = _parse_iso_datetime(props.get("startTime") or props.get("startTimeUtc"))
        if start_dt:
            try:
                return start_dt.timestamp()
            except Exception:
                return 0.0
        return 0.0

    return sorted(executions, key=sort_key, reverse=True)[0]


def _get_aca_execution_name(execution: Optional[Dict[str, Any]]) -> Optional[str]:
    if not execution or not isinstance(execution, dict):
        return None
    return execution.get("name") or ((execution.get("properties") or {}).get("name"))


def get_di_client() -> DocumentAnalysisClient:
    return DocumentAnalysisClient(
        endpoint=_require_env("AZURE_DI_ENDPOINT"),
        credential=AzureKeyCredential(_require_env("AZURE_DI_KEY")),
    )


def get_embedding_client() -> AzureOpenAI:
    return AzureOpenAI(
        api_version=_require_env("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=_require_env("AZURE_OPENAI_ENDPOINT"),
        api_key=_require_env("AZURE_OPENAI_KEY"),
    )


def get_blob_container_client():
    container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME") or os.getenv("AZURE_BLOB_CONTAINER") or "docs"

    conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")
    if conn_str:
        service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = service_client.get_container_client(container_name)
        try:
            container_client.create_container()
        except Exception:
            pass
        return container_client

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    if not account_name or not account_key:
        raise RuntimeError(
            "Missing Blob settings. Set either AZURE_BLOB_CONNECTION_STRING "
            "or AZURE_STORAGE_ACCOUNT_NAME/AZURE_STORAGE_ACCOUNT_KEY."
        )

    service_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    container_client = service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass
    return container_client


def upload_bytes_to_blob(container_client, data: bytes, blob_name: str) -> str:
    blob_client = container_client.get_blob_client(blob_name)
    content_type, _ = mimetypes.guess_type(blob_name)
    content_settings = ContentSettings(content_type=content_type or "application/octet-stream")
    blob_client.upload_blob(BytesIO(data), overwrite=True, content_settings=content_settings)

    # Optional: sanity-check existence (kept lightweight; if the SDK raises, we'll already fail)
    try:
        if hasattr(blob_client, "exists") and not blob_client.exists():
            raise RuntimeError("Blob upload completed but blob does not exist (exists() returned False).")
    except Exception:
        # If exists() fails (permissions/network), we don't want false negatives after upload_blob succeeded.
        pass

    return blob_client.url


def delete_blob_if_exists(container_client, blob_name: str) -> bool:
    """
    Best-effort delete. Returns True if delete succeeded, False if it failed.
    (We don't swallow silently anymore.)
    """
    try:
        container_client.delete_blob(blob_name)
        return True
    except Exception:
        return False


def delete_blobs_by_prefix(container_client, prefix: str) -> int:
    """
    Best-effort delete by prefix. Returns count of successfully deleted blobs.
    Errors are ignored but should be handled by callers via *_report functions
    if they need diagnostics.
    """
    deleted = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        try:
            container_client.delete_blob(blob.name)
            deleted += 1
        except Exception:
            continue
    return deleted


def delete_blob_report(container_client, blob_name: str) -> Dict[str, Any]:
    try:
        container_client.delete_blob(blob_name)
        return {"name": blob_name, "deleted": True}
    except Exception as e:
        return {"name": blob_name, "deleted": False, "error": str(e)}


def delete_blobs_by_prefix_report(container_client, prefix: str, sample_limit: int = 25) -> Dict[str, Any]:
    deleted = 0
    sample: List[str] = []
    errors: List[str] = []
    for blob in container_client.list_blobs(name_starts_with=prefix):
        try:
            container_client.delete_blob(blob.name)
            deleted += 1
            if len(sample) < sample_limit:
                sample.append(blob.name)
        except Exception as e:
            if len(errors) < sample_limit:
                errors.append(f"{blob.name}: {e}")
            continue
    return {"prefix": prefix, "deleted": deleted, "sample": sample, "errors_sample": errors}


def _table_to_markdown(table) -> str:
    rows: List[List[str]] = [[""] * table.column_count for _ in range(table.row_count)]
    for cell in table.cells:
        r = cell.row_index
        c = cell.column_index
        rows[r][c] = (cell.content or "").replace("\n", " ").strip()
    if not rows:
        return ""
    header = rows[0]
    body = rows[1:] if len(rows) > 1 else []
    md_lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    for row in body:
        md_lines.append("| " + " | ".join(row) + " |")
    return "\n".join(md_lines)


def extract_pages_with_layout(di_client: DocumentAnalysisClient, path: str) -> List[Tuple[int, str]]:
    with open(path, "rb") as f:
        poller = di_client.begin_analyze_document("prebuilt-layout", document=f)
        result = poller.result()

    tables_by_page: Dict[int, List[Any]] = {}
    
    tables = getattr(result, "tables", None) or []
    
    for t in tables:
        brs = getattr(t, "bounding_regions", None) or []
        if not brs:
            continue
        pg = getattr(brs[0], "page_number", None)
        if not pg:
            continue
        tables_by_page.setdefault(int(pg), []).append(t)

    pages: List[Tuple[int, str]] = []
    for page in result.pages:
        page_num = page.page_number
        parts: List[str] = []
        if getattr(page, "lines", None):
            for line in page.lines:
                text = (line.content or "").strip()
                if text:
                    parts.append(text)
        for table in tables_by_page.get(page_num, []):
            md = _table_to_markdown(table)
            if md:
                parts.extend(["", md, ""])
        page_text = "\n".join(parts).strip()
        if page_text:
            pages.append((page_num, page_text))
    return pages


def simple_chunk_text(text: str) -> List[str]:
    chunks: List[str] = []
    start = 0
    length = len(text)
    while start < length:
        end = min(start + CHUNK_CHAR_LENGTH, length)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == length:
            break
        start = end - CHUNK_CHAR_OVERLAP
    return chunks


def convert_office_to_pdf_via_libreoffice(input_path: str) -> Optional[str]:
    input_path = os.path.abspath(input_path)
    out_dir = tempfile.mkdtemp(prefix="lo-office-")
    soffice_path = os.getenv("LIBREOFFICE_PATH", "soffice")
    cmd = [
        soffice_path,
        "--headless",
        "--convert-to",
        "pdf",
        "--outdir",
        out_dir,
        input_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=180,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    pdfs = [str(Path(out_dir) / n) for n in os.listdir(out_dir) if n.lower().endswith(".pdf")]
    return pdfs[0] if pdfs else None


def extract_images_by_page(pdf_path: str, blob_container, prefix: str) -> Dict[int, List[str]]:
    try:
        import fitz  # PyMuPDF
    except Exception:
        return {}

    images_by_page: Dict[int, List[str]] = {}
    doc = fitz.open(pdf_path)
    try:
        for page_index in range(len(doc)):
            page = doc[page_index]
            page_num = page_index + 1
            urls: List[str] = []
            for img_index, img in enumerate(page.get_images(full=True)):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                try:
                    if pix.colorspace is None:
                        continue
                    if pix.alpha:
                        pix = fitz.Pixmap(pix, 0)
                    cs_name = pix.colorspace.name if pix.colorspace else None
                    if cs_name not in ("DeviceRGB", "DeviceGray"):
                        pix = fitz.Pixmap(fitz.csRGB, pix)
                    img_bytes = pix.tobytes("png")
                    blob_name = f"{prefix}/page_{page_num}_img_{img_index}.png"
                    blob_url = upload_bytes_to_blob(blob_container, img_bytes, blob_name)
                    urls.append(blob_url)
                finally:
                    pix = None
            if urls:
                images_by_page[page_num] = urls
    finally:
        doc.close()
    return images_by_page


async def embed_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not chunks:
        return []

    def _embed_sync() -> List[List[float]]:
        client = get_embedding_client()
        contents = [c["content"] for c in chunks]
        emb = client.embeddings.create(
            model=_require_env("AZURE_OPENAI_EMBEDDING_MODEL"),
            input=contents,
        )
        return [e.embedding for e in emb.data]

    vectors = await asyncio.to_thread(_embed_sync)
    for c, v in zip(chunks, vectors):
        c["embedding"] = v
    return chunks


@dataclass(frozen=True)
class IngestResult:
    doc_id: str
    blob_url: Optional[str]
    chunks: int


async def ingest_file_to_pgvector(local_path: str, doc_id: str, progress=None) -> IngestResult:
    filename = Path(doc_id).name
    ext = Path(local_path).suffix.lower()

    def _emit(stage: str, file_percent: int, message: Optional[str] = None, **extra: Any) -> None:
        if not progress:
            return
        ev: Dict[str, Any] = {"type": "progress", "stage": stage, "file_percent": int(file_percent)}
        if message:
            ev["message"] = message
        ev.update(extra)
        try:
            progress(ev)
        except Exception:
            pass

    _emit("start", 0, "Starting ingest")

    blob_container = get_blob_container_client()
    blob_name = doc_id

    # Best-effort cleanup of old extracted images
    _emit("cleanup_images", 5, "Removing previous extracted images (if any)")
    cleanup_report = await asyncio.to_thread(delete_blobs_by_prefix_report, blob_container, f"images/{blob_name}/")
    if cleanup_report.get("errors_sample"):
        logger.warning("pre_upload_image_cleanup_errors %s", json.dumps(cleanup_report, ensure_ascii=False))

    raw_bytes = Path(local_path).read_bytes()
    _emit("upload_blob", 12, "Uploading file to Blob storage")
    blob_url = await asyncio.to_thread(upload_bytes_to_blob, blob_container, raw_bytes, blob_name)

    pages: List[Tuple[int, str]] = []
    images_by_page: Dict[int, List[str]] = {}

    if ext in {".txt", ".md"}:
        _emit("extract_text", 20, "Reading text content")
        text = raw_bytes.decode("utf-8", errors="ignore")
        if text.strip():
            pages = [(1, text)]
    else:
        di_client = get_di_client()
        adi_input_path = local_path
        if ENABLE_LIBREOFFICE_CONVERSION and ext in {".docx", ".pptx"}:
            _emit("convert_office", 18, "Converting Office file to PDF")
            converted_pdf = await asyncio.to_thread(convert_office_to_pdf_via_libreoffice, local_path)
            if converted_pdf:
                adi_input_path = converted_pdf

        _emit("extract_layout", 28, "Extracting text with Document Intelligence")
        pages = await asyncio.to_thread(extract_pages_with_layout, di_client, adi_input_path)

        if adi_input_path.lower().endswith(".pdf"):
            _emit("extract_images", 40, "Extracting images from PDF")
            images_by_page = await asyncio.to_thread(
                extract_images_by_page,
                adi_input_path,
                blob_container,
                f"images/{blob_name}",
            )
        elif ext in {".png", ".jpg", ".jpeg", ".tif", ".tiff"}:
            images_by_page = {1: [blob_url]} if blob_url else {}

    if not pages:
        raise RuntimeError("No readable content extracted (pages empty).")

    _emit("chunk", 55, "Chunking extracted text")
    chunks: List[Dict[str, Any]] = []
    chunk_index = 0
    for page_num, page_text in pages:
        for chunk_text in simple_chunk_text(page_text):
            chunks.append(
                {
                    "doc_id": doc_id,
                    "chunk_index": chunk_index,
                    "title": filename,
                    "page_start": page_num,
                    "page_end": page_num,
                    "content": chunk_text,
                    "metadata": {
                        "source": blob_name,
                        "page": page_num,
                        "blob_url": blob_url,
                        "images": images_by_page.get(page_num, []),
                    },
                }
            )
            chunk_index += 1

    _emit("embed", 70, f"Embedding {len(chunks)} chunk(s)")
    chunks = await embed_chunks(chunks)

    _emit("insert", 90, "Writing chunks to Postgres")
    conn = await get_pg()
    try:
        async with conn.transaction():
            await conn.execute("DELETE FROM documents WHERE doc_id = $1", doc_id)
            for c in chunks:
                await conn.execute(
                    UPSERT_SQL,
                    c["doc_id"],
                    c["chunk_index"],
                    c["title"],
                    c["page_start"],
                    c["page_end"],
                    c["content"],
                    json.dumps(c["metadata"], ensure_ascii=False),
                    to_pgvector(c["embedding"]),
                )
    finally:
        await conn.close()

    _emit("done", 100, "Completed")
    return IngestResult(doc_id=doc_id, blob_url=blob_url, chunks=len(chunks))


async def list_docs() -> List[Dict[str, Any]]:
    return await list_docs_by_prefix(prefix=None)


async def list_docs_by_prefix(prefix: Optional[str]) -> List[Dict[str, Any]]:
    conn = await get_pg()
    try:
        if prefix:
            like = prefix.replace("\\", "/") + "%"
            rows = await conn.fetch(
                """
                SELECT
                  doc_id,
                  max(title) AS title,
                  count(*)::int AS chunks,
                  min(page_start)::int AS page_min,
                  max(page_end)::int AS page_max
                FROM documents
                WHERE doc_id LIKE $1
                GROUP BY doc_id
                ORDER BY doc_id
                """,
                like,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                  doc_id,
                  max(title) AS title,
                  count(*)::int AS chunks,
                  min(page_start)::int AS page_min,
                  max(page_end)::int AS page_max
                FROM documents
                GROUP BY doc_id
                ORDER BY doc_id
                """
            )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def get_doc_chunks(doc_id: str, limit: int, offset: int) -> Dict[str, Any]:
    conn = await get_pg()
    try:
        total = await conn.fetchval("SELECT count(*)::int FROM documents WHERE doc_id = $1", doc_id)
        rows = await conn.fetch(
            """
            SELECT chunk_index, page_start, page_end, content, metadata
            FROM documents
            WHERE doc_id = $1
            ORDER BY chunk_index
            LIMIT $2 OFFSET $3
            """,
            doc_id,
            limit,
            offset,
        )
        return {"total": int(total or 0), "rows": [dict(r) for r in rows]}
    finally:
        await conn.close()


async def get_doc_sources(doc_id: str) -> List[str]:
    conn = await get_pg()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT (metadata->>'source') AS source
            FROM documents
            WHERE doc_id = $1
              AND metadata IS NOT NULL
              AND (metadata->>'source') IS NOT NULL
              AND (metadata->>'source') <> ''
            ORDER BY source
            """,
            doc_id,
        )
        return [r["source"] for r in rows if r["source"]]
    finally:
        await conn.close()


def _parse_scraped_doc_id(doc_id: str) -> Optional[Dict[str, str]]:
    """
    sitefiles/ingest.py sets doc_id like:
      <COLLECTION_ID>/<site_id>/<doc_key>/<filename>
    watch_and_ingest_versioned_sites.py stores raw PDFs like:
      collections/<COLLECTION_ID>/sites/<site_id>/raw/<doc_key>/...
    """
    parts = doc_id.split("/")
    if len(parts) < 4:
        return None
    if parts[0] != COLLECTION_ID:
        return None
    collection_id, site_id, doc_key = parts[0], parts[1], parts[2]
    filename = "/".join(parts[3:])
    if not site_id or not doc_key or not filename:
        return None
    return {"collection_id": collection_id, "site_id": site_id, "doc_key": doc_key, "filename": filename}


def _display_filename(doc_id: str) -> str:
    try:
        return Path(doc_id).name or doc_id
    except Exception:
        return doc_id


def _manifest_client():
    conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_BLOB_CONNECTION_STRING is required for manifest operations")
    svc = BlobServiceClient.from_connection_string(conn_str)
    return svc.get_container_client(MANIFEST_CONTAINER).get_blob_client(MANIFEST_BLOB_NAME)


def _try_remove_from_manifest(site_id: str, doc_key: str) -> bool:
    try:
        blob = _manifest_client()
        data = blob.download_blob().readall()
        manifest = json.loads(data.decode("utf-8"))
        docs = manifest.get("docs", {})
        manifest_key = f"{site_id}/{doc_key}"
        if manifest_key not in docs:
            return False
        del docs[manifest_key]
        manifest["docs"] = docs
        blob.upload_blob(
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        return True
    except Exception:
        return False


async def delete_doc_everywhere(doc_id: str) -> Dict[str, Any]:
    container = get_blob_container_client()
    sources = await get_doc_sources(doc_id)
    if not sources:
        sources = [doc_id]

    per_source: List[Dict[str, Any]] = []
    for source in sources:
        images_prefix = f"images/{source}/"
        images_report = await asyncio.to_thread(delete_blobs_by_prefix_report, container, images_prefix)
        blob_report = await asyncio.to_thread(delete_blob_report, container, source)
        per_source.append({"source": source, "blob": blob_report, "images": images_report})

    scraped = _parse_scraped_doc_id(doc_id)
    raw_report: Optional[Dict[str, Any]] = None
    manifest_updated = False
    if scraped:
        raw_prefix = f"collections/{scraped['collection_id']}/sites/{scraped['site_id']}/raw/{scraped['doc_key']}/"
        raw_report = await asyncio.to_thread(delete_blobs_by_prefix_report, container, raw_prefix)
        manifest_updated = await asyncio.to_thread(_try_remove_from_manifest, scraped["site_id"], scraped["doc_key"])

    conn = await get_pg()
    try:
        result = await conn.execute("DELETE FROM documents WHERE doc_id = $1", doc_id)
    finally:
        await conn.close()

    try:
        pg_deleted = int(str(result).split()[-1])
    except Exception:
        pg_deleted = None

    report = {
        "pg": result,
        "pg_deleted": pg_deleted,
        "doc_id": doc_id,
        "display_name": _display_filename(doc_id),
        "sources": sources,
        "per_source": per_source,
        "raw_report": raw_report,
        "manifest_updated": manifest_updated,
    }
    logger.info("delete_report %s", json.dumps(report, ensure_ascii=False))
    return report


def _collect_delete_warnings(info: Dict[str, Any], limit: int = 6) -> List[str]:
    """
    Summarize deletion failures/partial failures into short strings suitable for UI messages.
    """
    warnings: List[str] = []

    # Per-source blob delete errors
    for s in info.get("per_source", []) or []:
        blob = (s or {}).get("blob") or {}
        if blob.get("deleted") is False and blob.get("error"):
            warnings.append(f"blob_delete_failed[{blob.get('name')}]: {blob.get('error')}")

        images = (s or {}).get("images") or {}
        for err in (images.get("errors_sample") or [])[: max(0, limit - len(warnings))]:
            warnings.append(f"image_delete_failed: {err}")
        if len(warnings) >= limit:
            break

    # Raw deletions
    if len(warnings) < limit:
        raw = info.get("raw_report") or {}
        for err in (raw.get("errors_sample") or [])[: max(0, limit - len(warnings))]:
            warnings.append(f"raw_delete_failed: {err}")

    # Manifest update (if scraped doc)
    if len(warnings) < limit:
        if info.get("raw_report") is not None and info.get("manifest_updated") is False:
            warnings.append("manifest_not_updated (check AZURE_BLOB_CONNECTION_STRING / manifest permissions)")

    return warnings[:limit]


async def list_blobs(prefix: str = "") -> List[Dict[str, Any]]:
    container = get_blob_container_client()

    def _list() -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for idx, b in enumerate(container.list_blobs(name_starts_with=prefix)):
            if idx >= MAX_LIST_BLOBS:
                break
            out.append(
                {
                    "name": b.name,
                    "size": getattr(b, "size", None),
                    "last_modified": getattr(b, "last_modified", None),
                    "content_type": getattr(b, "content_settings", None).content_type
                    if getattr(b, "content_settings", None)
                    else None,
                }
            )
        return out

    return await asyncio.to_thread(_list)


async def list_files(prefix: str = "") -> Dict[str, Any]:
    docs: List[Dict[str, Any]] = []
    docs_error: Optional[str] = None
    try:
        docs = await list_docs_by_prefix(prefix=prefix or None)
    except Exception as e:
        docs_error = str(e)

    blobs_error: Optional[str] = None
    doc_map = {d["doc_id"]: d for d in docs}
    all_ids = sorted(doc_map.keys())
    files: List[Dict[str, Any]] = []
    for doc_id in all_ids:
        d = doc_map.get(doc_id, {})
        scraped = _parse_scraped_doc_id(doc_id)
        files.append(
            {
                "doc_id": doc_id,
                "display_name": (scraped["filename"] if scraped else _display_filename(doc_id)),
                "title": d.get("title"),
                "chunks": d.get("chunks", 0),
                "page_min": d.get("page_min"),
                "page_max": d.get("page_max"),
                "scraped_site_id": scraped["site_id"] if scraped else None,
                "scraped_doc_key": scraped["doc_key"] if scraped else None,
            }
        )

    return {"errors": {"docs": docs_error, "blobs": blobs_error}, "files": files}


def _guess_content_type(blob_name: str) -> str:
    ctype, _ = mimetypes.guess_type(blob_name)
    return ctype or "application/octet-stream"


async def download_blob_bytes(blob_name: str) -> bytes:
    container = get_blob_container_client()

    def _download() -> bytes:
        blob_client = container.get_blob_client(blob_name)
        downloader = blob_client.download_blob()
        return downloader.readall()

    return await asyncio.to_thread(_download)


def _auth_enabled() -> bool:
    if os.getenv("ADMINPORTAL_DISABLE_AUTH", "0") == "1":
        return False
    return bool(_admin_userid() and _admin_password())

def _admin_userid() -> str:
    return (os.getenv("ADMIN_USERID") or os.getenv("ADMIN_USERNAME") or "").strip()


def _admin_password() -> str:
    return os.getenv("ADMIN_PASSWORD") or ""


def _is_logged_in() -> bool:
    if not _auth_enabled():
        return True
    expected_userid = _admin_userid()
    authed_userid = session.get("adminportal_userid") or ""
    return bool(expected_userid and secrets.compare_digest(str(authed_userid), expected_userid))


def _wants_html() -> bool:
    if request.path.endswith(".json"):
        return False
    accept = (request.headers.get("Accept") or "").lower()
    if "application/json" in accept:
        return False
    # Browsers commonly send text/html or */*
    return ("text/html" in accept) or ("*/*" in accept) or (not accept)


def _safe_next_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return "/"
    if value.startswith("//"):
        return "/"
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        return "/"
    if not value.startswith("/"):
        return "/"
    return value


@dataclass
class JobState:
    job_id: str
    queue: "asyncio.Queue[Dict[str, Any]]"
    created_at: float
    done: bool = False
    ok: bool = False
    error: Optional[str] = None
    results: Optional[Dict[str, Any]] = None


JOBS: Dict[str, JobState] = {}
JOB_TTL_SECONDS = int(os.getenv("ADMINPORTAL_JOB_TTL_SECONDS", "3600"))


def _new_job_id() -> str:
    return secrets.token_urlsafe(12)


def _job_cleanup() -> None:
    now = time.time()
    expired: List[str] = []
    for job_id, job in JOBS.items():
        if (now - job.created_at) > JOB_TTL_SECONDS:
            expired.append(job_id)
    for job_id in expired:
        JOBS.pop(job_id, None)


async def _job_emit(job: JobState, event: Dict[str, Any]) -> None:
    event = dict(event)
    event.setdefault("ts", int(time.time()))
    await job.queue.put(event)


async def _run_upload_job(job: JobState, files: List[Dict[str, Any]]) -> None:
    """
    files: [{display_name, doc_id, tmp_path}]
    """
    try:
        await _job_emit(job, {"type": "status", "status": "starting", "total_files": len(files), "overall_percent": 0})
        job_results: List[Dict[str, Any]] = []

        for idx, f in enumerate(files, start=1):
            await _job_emit(
                job,
                {
                    "type": "file",
                    "status": "start",
                    "index": idx,
                    "total": len(files),
                    "display_name": f["display_name"],
                    "overall_percent": int(((idx - 1) / max(1, len(files))) * 100),
                },
            )

            def _progress(
                ev: Dict[str, Any],
                *,
                file_index: int = idx,
                total_files: int = len(files),
                display_name: str = f["display_name"],
            ) -> None:
                file_percent = int(ev.get("file_percent", 0) or 0)
                overall = int(((file_index - 1) + (file_percent / 100.0)) / max(1, total_files) * 100)
                ev = dict(ev)
                ev["index"] = file_index
                ev["total"] = total_files
                ev["display_name"] = display_name
                ev["overall_percent"] = overall
                asyncio.create_task(_job_emit(job, ev))

            try:
                res = await ingest_file_to_pgvector(f["tmp_path"], doc_id=f["doc_id"], progress=_progress)
                job_results.append({"doc_id": res.doc_id, "display_name": f["display_name"], "chunks": res.chunks})
                await _job_emit(
                    job,
                    {
                        "type": "file",
                        "status": "done",
                        "index": idx,
                        "total": len(files),
                        "display_name": f["display_name"],
                        "chunks": res.chunks,
                        "overall_percent": int((idx / max(1, len(files))) * 100),
                    },
                )
            except Exception as e:
                await _job_emit(
                    job,
                    {
                        "type": "file",
                        "status": "error",
                        "index": idx,
                        "total": len(files),
                        "display_name": f["display_name"],
                        "error": str(e),
                        "overall_percent": int((idx / max(1, len(files))) * 100),
                    },
                )
                job_results.append({"doc_id": f["doc_id"], "display_name": f["display_name"], "error": str(e)})
            finally:
                try:
                    os.unlink(f["tmp_path"])
                except Exception:
                    pass

        job.ok = True
        job.results = {"files": job_results}
        await _job_emit(job, {"type": "done", "ok": True, "results": job.results, "overall_percent": 100})
    except Exception as e:
        job.ok = False
        job.error = str(e)
        await _job_emit(job, {"type": "done", "ok": False, "error": job.error, "overall_percent": 100})
    finally:
        job.done = True


app = Quart(__name__, template_folder=str(Path(__file__).parent / "templates"))
app.secret_key = os.getenv("ADMINPORTAL_SECRET_KEY", secrets.token_hex(16))

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = os.getenv("ADMINPORTAL_SESSION_COOKIE_SAMESITE", "Lax")
app.config["SESSION_COOKIE_SECURE"] = os.getenv("ADMINPORTAL_SESSION_COOKIE_SECURE", "0") == "1"


def _get_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return str(token)


async def _csrf_provided_token() -> Optional[str]:
    header_token = request.headers.get("X-CSRF-Token")
    if header_token:
        return header_token

    if request.is_json:
        data = await request.get_json(silent=True)
        if isinstance(data, dict) and data.get("csrf_token"):
            return str(data.get("csrf_token"))

    # Covers application/x-www-form-urlencoded and multipart/form-data
    try:
        form = await request.form
        if form and form.get("csrf_token"):
            return str(form.get("csrf_token"))
    except Exception:
        return None

    return None


async def _enforce_csrf() -> Optional[Response]:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None

    # CSRF is a browser concern; allow non-browser clients to opt in with the header.
    expected = session.get("csrf_token")
    provided = await _csrf_provided_token()
    if not expected or not provided or not secrets.compare_digest(str(provided), str(expected)):
        wants_json = request.is_json or ("application/json" in (request.headers.get("Accept") or ""))
        if wants_json:
            return jsonify({"ok": False, "error": "CSRF validation failed"}), 400
        resp = await app.make_response("CSRF validation failed")
        resp.status_code = 400
        return resp

    return None


@app.before_request
async def _enforce_auth():
    g.csp_nonce = secrets.token_urlsafe(18)

    if not _auth_enabled():
        return

    if request.path.startswith("/static/"):
        return

    if request.path == "/login":
        return

    if request.path == "/logout":
        return

    if _is_logged_in():
        return

    if request.method in {"GET", "HEAD"} and _wants_html():
        next_url = request.full_path
        if next_url.endswith("?"):
            next_url = request.path
        return redirect(url_for("login_get", next=next_url))

    return jsonify({"ok": False, "error": "Unauthorized"}), 401


@app.before_request
async def _enforce_csrf_before_request():
    if request.path.startswith("/static/"):
        return
    # Login POST should be CSRF-protected too (token is set on GET /login).
    return await _enforce_csrf()


@app.after_request
async def _set_security_headers(resp: Response):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    resp.headers.setdefault("X-Frame-Options", "DENY")

    nonce = getattr(g, "csp_nonce", "")
    if nonce:
        csp = (
            "default-src 'self'; "
            f"script-src 'self' 'nonce-{nonce}'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "base-uri 'self'; "
            "form-action 'self'; "
            "frame-ancestors 'none'; "
            "object-src 'none'"
        )
        resp.headers.setdefault("Content-Security-Policy", csp)

    if os.getenv("ADMINPORTAL_ENABLE_HSTS", "0") == "1":
        resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

    return resp


@app.errorhandler(401)
async def _unauthorized(_err):
    wants_json = request.is_json or ("application/json" in (request.headers.get("Accept") or ""))
    if wants_json:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    resp = await app.make_response("Unauthorized")
    resp.status_code = 401
    return resp


@app.context_processor
async def _inject_template_globals():
    return {
        "auth_enabled": _auth_enabled(),
        "auth_userid": session.get("adminportal_userid"),
        "csrf_token": _get_csrf_token(),
        "csp_nonce": getattr(g, "csp_nonce", ""),
    }


@app.get("/login")
async def login_get():
    if not _auth_enabled():
        return redirect(url_for("index"))
    if _is_logged_in():
        return redirect(url_for("index"))

    message = request.args.get("message")
    next_raw = request.args.get("next", "/")
    return await render_template("login.html", message=message, next=_safe_next_url(next_raw))


@app.post("/login")
async def login_post():
    if not _auth_enabled():
        return redirect(url_for("index"))

    form = await request.form
    userid = (form.get("userid") or "").strip()
    password = form.get("password") or ""
    next_url = _safe_next_url(form.get("next", "/"))

    expected_userid = _admin_userid()
    expected_password = _admin_password()

    userid_ok = secrets.compare_digest(userid, expected_userid)
    password_ok = secrets.compare_digest(password, expected_password)
    if not (userid_ok and password_ok):
        return redirect(url_for("login_get", message="ユーザーIDまたはパスワードが正しくありません。", next=next_url))

    session["adminportal_userid"] = expected_userid
    return redirect(next_url)


@app.post("/logout")
async def logout_post():
    session.pop("adminportal_userid", None)
    return redirect(url_for("login_get"))

@app.get("/sites")
async def sites_page():
    message = request.args.get("message")
    try:
        sites = await list_sites()
    except Exception as e:
        sites = []
        message = message or f"List sites error: {e}"
    aca_status = None
    if _aca_configured():
        try:
            timeout_s = float(os.getenv("ACA_STATUS_TIMEOUT_SECONDS", "3.5"))
            latest = await asyncio.wait_for(asyncio.to_thread(_get_aca_latest_execution), timeout=timeout_s)
            aca_status = _aca_status_from_execution(latest)
        except Exception as e:
            aca_status = _aca_status_from_execution(None, fetch_error=str(e))
    return await render_template(
        "sites.html",
        sites=sites,
        message=message,
        aca_configured=_aca_configured(),
        aca_status=aca_status,
    )

@app.get("/sites/aca-status.json")
async def sites_aca_status_json():
    if not _aca_configured():
        return {"ok": True, "configured": False, "aca_status": None}
    try:
        latest = await asyncio.to_thread(_get_aca_latest_execution)
        return {"ok": True, "configured": True, "aca_status": _aca_status_from_execution(latest)}
    except Exception as e:
        return {"ok": False, "configured": True, "error": str(e)}, 500


@app.post("/sites/upsert")
async def sites_upsert():
    form = await request.form
    try:
        site_id = _normalize_site_id(form.get("site_id", ""))
        site_type = _normalize_site_type(form.get("type", ""))
        url = _normalize_url(form.get("url", ""))
        enabled = form.get("enabled") == "on"
        await upsert_site(site_id=site_id, enabled=enabled, site_type=site_type, url=url)
        return redirect(url_for("sites_page", message=f"Saved site {site_id}."))
    except Exception as e:
        return redirect(url_for("sites_page", message=f"Save failed: {e}"))


@app.post("/sites/toggle")
async def sites_toggle():
    form = await request.form
    try:
        site_id = _normalize_site_id(form.get("site_id", ""))
        enabled = await toggle_site(site_id)
        return redirect(url_for("sites_page", message=f"{'Enabled' if enabled else 'Disabled'} {site_id}."))
    except Exception as e:
        return redirect(url_for("sites_page", message=f"Toggle failed: {e}"))


@app.post("/sites/toggle-json")
async def sites_toggle_json():
    try:
        if request.is_json:
            payload = await request.get_json()
            site_id_raw = (payload or {}).get("site_id", "")
        else:
            form = await request.form
            site_id_raw = form.get("site_id", "")
        site_id = _normalize_site_id(site_id_raw)
        enabled = await toggle_site(site_id)
        return {"ok": True, "site_id": site_id, "enabled": enabled}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.post("/sites/delete")
async def sites_delete():
    form = await request.form
    try:
        site_id = _normalize_site_id(form.get("site_id", ""))
        await delete_site(site_id)
        return redirect(url_for("sites_page", message=f"Deleted site {site_id}."))
    except Exception as e:
        return redirect(url_for("sites_page", message=f"Delete failed: {e}"))


@app.post("/sites/run")
async def sites_run():
    if not _aca_configured():
        return redirect(url_for("sites_page", message="ACA job not configured (set ACA_SUBSCRIPTION_ID/ACA_RESOURCE_GROUP/ACA_JOB_NAME)."))
    try:
        result = await asyncio.to_thread(_start_aca_job)
        name = result.get("name") or result.get("id") or "started"
        return redirect(url_for("sites_page", message=f"Started ACA job: {name}"))
    except Exception as e:
        return redirect(url_for("sites_page", message=f"Start ACA job failed: {e}"))


@app.post("/sites/run-json")
async def sites_run_json():
    if not _aca_configured():
        return {"ok": False, "configured": False, "error": "ACA job not configured"}, 400

    try:
        before = await asyncio.to_thread(_get_aca_latest_execution)
        before_name = _get_aca_execution_name(before)

        await asyncio.to_thread(_start_aca_job)

        execution: Optional[Dict[str, Any]] = None
        deadline = time.time() + float(os.getenv("ACA_START_DISCOVERY_TIMEOUT_SECONDS", "8"))
        poll_s = float(os.getenv("ACA_START_DISCOVERY_POLL_SECONDS", "0.8"))
        while time.time() < deadline:
            latest = await asyncio.to_thread(_get_aca_latest_execution)
            latest_name = _get_aca_execution_name(latest)
            if latest and latest_name and latest_name != before_name:
                execution = latest
                break
            await asyncio.sleep(poll_s)

        if not execution:
            execution = await asyncio.to_thread(_get_aca_latest_execution)

        return {"ok": True, "configured": True, "aca_status": _aca_status_from_execution(execution)}
    except Exception as e:
        return {"ok": False, "configured": True, "error": str(e)}, 500


@app.get("/")
async def index():
    prefix = request.args.get("prefix", "")
    message = request.args.get("message")
    wrapped = await list_files(prefix=prefix)
    errors = wrapped["errors"]
    files = wrapped["files"]
    if not message:
        errs = [v for v in [errors.get("docs"), errors.get("blobs")] if v]
        if errs:
            message = " | ".join([f"List error: {e}" for e in errs])[:900]
    return await render_template(
        "index.html",
        files=files,
        prefix=prefix,
        message=message,
    )


@app.get("/job/<job_id>")
async def job_page(job_id: str):
    _job_cleanup()
    job = JOBS.get(job_id)
    if not job:
        return redirect(url_for("index", message="Upload job not found (expired)."))
    return await render_template("job.html", job_id=job_id)


@app.get("/events/<job_id>")
async def job_events(job_id: str):
    _job_cleanup()
    job = JOBS.get(job_id)
    if not job:
        abort(404)

    async def stream():
        yield "retry: 1000\n\n"
        while True:
            if job.done and job.queue.empty():
                break
            try:
                ev = await asyncio.wait_for(job.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

    return Response(
        stream(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/upload")
async def upload():
    files = (await request.files).getlist("files")
    if not files:
        wants_json = "application/json" in (request.headers.get("Accept") or "")
        if wants_json:
            return jsonify({"ok": False, "error": "ファイルが選択されていません。"}), 400
        return redirect(url_for("index", message="ファイルが選択されていません。"))

    _job_cleanup()
    job_id = _new_job_id()
    job = JobState(job_id=job_id, queue=asyncio.Queue(), created_at=time.time())
    JOBS[job_id] = job

    job_files: List[Dict[str, Any]] = []
    for storage in files:
        display_name = _secure_filename(storage.filename)
        doc_id = display_name
        maybe_bytes = storage.read()
        data = await maybe_bytes if inspect.isawaitable(maybe_bytes) else maybe_bytes
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(display_name).suffix) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        job_files.append({"display_name": display_name, "doc_id": doc_id, "tmp_path": tmp_path})

    asyncio.create_task(_run_upload_job(job, job_files))
    wants_json = "application/json" in (request.headers.get("Accept") or "")
    if wants_json:
        return jsonify(
            {
                "ok": True,
                "job_id": job_id,
                "events_url": url_for("job_events", job_id=job_id),
            }
        )
    return redirect(url_for("job_page", job_id=job_id))


@app.get("/file/<path:doc_id>")
async def file_detail(doc_id: str):
    try:
        doc_id = _normalize_doc_id(doc_id)
    except Exception as e:
        return redirect(url_for("index", message=f"Invalid doc_id: {e}"))

    message = request.args.get("message")
    sources = await get_doc_sources(doc_id)
    if not sources:
        sources = [doc_id]

    images_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for source in sources:
        images_prefix = f"images/{source}/"
        try:
            images_by_source[source] = await list_blobs(prefix=images_prefix)
        except Exception as e:
            images_by_source[source] = []
            message = message or f"Image list error: {e}"
    images_total = sum(len(v) for v in images_by_source.values())

    scraped = _parse_scraped_doc_id(doc_id)
    raw_blobs: List[Dict[str, Any]] = []
    raw_prefix: Optional[str] = None
    if scraped:
        raw_prefix = f"collections/{scraped['collection_id']}/sites/{scraped['site_id']}/raw/{scraped['doc_key']}/"
        try:
            raw_blobs = await list_blobs(prefix=raw_prefix)
        except Exception as e:
            raw_blobs = []
            message = message or f"Raw blob list error: {e}"

    try:
        chunks_page = max(1, int(request.args.get("page", "1")))
    except Exception:
        chunks_page = 1
    offset = (chunks_page - 1) * CHUNK_PAGE_SIZE
    try:
        chunk_data = await get_doc_chunks(doc_id, limit=CHUNK_PAGE_SIZE, offset=offset)
    except Exception as e:
        chunk_data = {"total": 0, "rows": []}
        message = message or f"Chunk list error: {e}"

    total_chunks = int(chunk_data.get("total", 0))
    total_pages = max(1, (total_chunks + CHUNK_PAGE_SIZE - 1) // CHUNK_PAGE_SIZE) if total_chunks else 1
    chunks_page = min(chunks_page, total_pages)

    return await render_template(
        "file.html",
        doc_id=doc_id,
        display_name=(scraped["filename"] if scraped else _display_filename(doc_id)),
        scraped=scraped,
        message=message,
        sources=sources,
        chunks=chunk_data["rows"],
        chunks_total=total_chunks,
        chunks_page=chunks_page,
        chunks_page_size=CHUNK_PAGE_SIZE,
        chunks_total_pages=total_pages,
        images_by_source=images_by_source,
        images_total=images_total,
        raw_prefix=raw_prefix,
        raw_blobs=raw_blobs,
    )


@app.get("/blob/<path:blob_name>")
async def blob_proxy(blob_name: str):
    try:
        blob_name = _normalize_doc_id(blob_name)
    except Exception:
        return abort(404)

    try:
        data = await download_blob_bytes(blob_name)
    except Exception:
        return abort(404)

    return Response(data, content_type=_guess_content_type(blob_name))


@app.post("/delete")
async def delete():
    form = await request.form
    raw_doc_id = form.get("doc_id", "")
    try:
        doc_id = _normalize_doc_id(raw_doc_id)
    except Exception as e:
        return redirect(url_for("index", message=f"Invalid doc_id: {e}"))

    try:
        info = await delete_doc_everywhere(doc_id)

        deleted_images = sum(int(s["images"]["deleted"]) for s in info.get("per_source", []) if s.get("images"))
        deleted_source_blobs = sum(1 for s in info.get("per_source", []) if s.get("blob", {}).get("deleted"))
        deleted_raw_blobs = int(info["raw_report"]["deleted"]) if info.get("raw_report") else 0

        warnings = _collect_delete_warnings(info, limit=6)
        warning_text = (" | Warnings: " + " | ".join(warnings)) if warnings else ""

        status_prefix = "Deleted"
        if warnings:
            status_prefix = "Deleted (partial / warnings)"

        return redirect(
            url_for(
                "index",
                message=(
                    f"{status_prefix} {info.get('display_name', doc_id)} "
                    f"(pg_rows={info.get('pg_deleted')}, blobs_deleted={deleted_source_blobs}, "
                    f"images_deleted={deleted_images}, raw_blobs_deleted={deleted_raw_blobs}, "
                    f"manifest_updated={info.get('manifest_updated')})"
                    f"{warning_text}"
                )[:900],
            )
        )
    except Exception as e:
        return redirect(url_for("index", message=f"Delete failed for {doc_id}: {e}"))


if __name__ == "__main__":
    app.run(port=int(os.getenv("PORT", "5055")))
