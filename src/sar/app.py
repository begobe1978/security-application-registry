# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sar.auth.session import COOKIE_NAME, sign_session
from sar.auth.users import authenticate
from sar.permissions import cookie_settings, current_user_optional, require_role, require_user

from sar.core.utils import canon, df_to_csv_stream, first_existing_col, safe_count
from sar.infra.registry_repo import read_sheet, lookup_options_by_level
from sar.infra.registry_repo import (
    read_meta_dict,
    write_meta_kv,
    get_schema_map,
    schema_hash,
    add_missing_columns,
)
from sar.services.compute_service import regenerate_views
from sar.services.crud_service import update_record_existing_fields, add_new_field, create_record
from sar.core.mapping import meta_for_level
from sar.infra.registry_repo import generate_next_human_id

from sar.services.record_service import (
    detect_level_meta,
    get_row_by_human_id,
    issues_for,
    list_children,
    list_descendants_counts,
)

from sar.services.diagram_service import build_record_diagram
from sar.services.report_service import generate_c4_chain_report_docx, generate_c4_chain_report_html

app = FastAPI()


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    request.state.user = current_user_optional(request)
    return await call_next(request)

BASE_DIR = Path(__file__).resolve().parent

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

DATA_DIR = Path(os.getenv("SAR_DATA_DIR", "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

TEMPLATE_PATH = Path(os.getenv("SAR_TEMPLATE_PATH", str(DATA_DIR / "registry_template.xlsx"))).resolve()

# Word report template (docxtpl)
REPORT_TEMPLATE_DOCX = Path(
    os.getenv("SAR_REPORT_TEMPLATE_DOCX", str(BASE_DIR / "report_templates" / "c4_chain_report.docx"))
).resolve()

# HTML report template
REPORT_TEMPLATE_HTML = Path(
    os.getenv("SAR_REPORT_TEMPLATE_HTML", str(BASE_DIR / "report_templates" / "c4_chain_report.html.j2"))
).resolve()
REPORTS_DIR = Path(os.getenv("SAR_REPORTS_DIR", str(DATA_DIR / "reports"))).resolve()
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SCHEMA_SHEETS = [
    "META",
    "LOOKUPS",
    "RULES",
    "C1_Proyectos",
    "C2_Aplicaciones",
    "C3_Componentes",
    "C4_Runtime",
]

STATE = {
    "path": "",
    "view_full": pd.DataFrame(),
    "issues": pd.DataFrame(),
    "views_by_level": {},
    "last_error": "",
    "last_regen": "",
}

# Parent chain (child level -> parent level)
PARENT_LEVEL = {"C2": "C1", "C3": "C2", "C4": "C3"}


def _ensure_registry_loaded() -> bool:
    return bool(STATE.get("path")) and os.path.exists(STATE["path"])


def _bump_semver(v: str) -> str:
    """Bump patch of a semver-like string. Falls back to appending '.1'."""
    s = str(v or "").strip()
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", s)
    if m:
        a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{a}.{b}.{c+1}"
    m2 = re.match(r"^(\d+)\.(\d+)$", s)
    if m2:
        a, b = int(m2.group(1)), int(m2.group(2))
        return f"{a}.{b}.1"
    if s.isdigit():
        return str(int(s) + 1)
    return (s + ".1") if s else "0.1.0"


def _schema_state() -> dict:
    """Compute schema/template compatibility state for UI banners."""
    out = {
        "template_exists": TEMPLATE_PATH.exists(),
        "template_path": str(TEMPLATE_PATH),
        "registry_path": STATE.get("path", ""),
        "status": "no_template",
        "template_schema_version": "",
        "registry_schema_version": "",
        "added": {},
        "missing": {},
        "dirty": False,
    }

    if not TEMPLATE_PATH.exists():
        return out

    tmeta = read_meta_dict(str(TEMPLATE_PATH))
    out["template_schema_version"] = tmeta.get("schema_version") or tmeta.get("template_version", "")

    if not _ensure_registry_loaded():
        out["status"] = "template_only"
        return out

    rmeta = read_meta_dict(STATE["path"])
    out["registry_schema_version"] = rmeta.get("schema_version") or rmeta.get("template_version", "")

    # Schema maps
    tmap = get_schema_map(str(TEMPLATE_PATH), SCHEMA_SHEETS)
    rmap = get_schema_map(STATE["path"], SCHEMA_SHEETS)

    # Diff (normalised headers)
    added = {}
    missing = {}
    dirty = False
    for sh in SCHEMA_SHEETS:
        tcols = set(tmap.get(sh, []))
        rcols = set(rmap.get(sh, []))
        a = sorted(list(rcols - tcols))
        m = sorted(list(tcols - rcols))
        if a:
            added[sh] = a
            dirty = True
        if m:
            missing[sh] = m
            dirty = True

    out["added"] = added
    out["missing"] = missing
    out["dirty"] = dirty

    if not dirty and (out["template_schema_version"] == out["registry_schema_version"] or not out["template_schema_version"] or not out["registry_schema_version"]):
        out["status"] = "ok"
    else:
        out["status"] = "dirty"

    return out


def _render(request: Request, template_name: str, ctx: dict):
    """TemplateResponse wrapper injecting global UI state."""
    base_ctx = {
        "request": request,
        "schema": _schema_state(),
        "current_user": getattr(request.state, "user", None),
    }
    merged = {**base_ctx, **(ctx or {})}
    return templates.TemplateResponse(template_name, merged)



def _latest_xlsx_in_data_dir() -> str:
    """Return absolute path of the newest .xlsx in /data (by filename timestamp if present, else mtime)."""
    if not DATA_DIR.exists():
        return ""
    candidates = [p for p in DATA_DIR.glob("*.xlsx") if p.is_file()]
    if not candidates:
        return ""
    # Prefer SAR-style timestamp prefix: YYYYMMDD_HHMMSS__name.xlsx
    def sort_key(p: Path):
        m = re.match(r"^(\d{8}_\d{6})__.+\.xlsx$", p.name)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
                return (1, dt.timestamp())
            except Exception:
                pass
        return (0, p.stat().st_mtime)

    newest = max(candidates, key=sort_key)
    return str(newest.resolve())


def _search_parent_candidates(*, child_level: str, q: str, limit: int = 20):
    """Search existing parents for a given child level.

    Results are returned from in-memory views when available; falls back to reading the
    corresponding sheet if needed.
    """
    cl = str(child_level or "").strip().upper()
    parent_level = PARENT_LEVEL.get(cl, "")
    if not parent_level:
        return []

    df = (STATE.get("views_by_level", {}) or {}).get(parent_level)
    if df is None or df.empty or "human_id" not in df.columns:
        pm = meta_for_level(parent_level)
        if not pm:
            return []
        df = read_sheet(STATE["path"], pm["sheet"])

    # Ensure columns exist
    if "name" not in df.columns:
        df = df.copy()
        df["name"] = ""

    q_raw = (q or "").strip()
    if not q_raw:
        return []

    ql = q_raw.lower()
    qcanon = canon(q_raw)

    tmp = df[["human_id", "name"]].copy()
    tmp["__hid"] = tmp["human_id"].astype(str).map(canon)
    tmp["__name"] = tmp["name"].astype(str).str.lower()
    hit = tmp[(tmp["__hid"].str.contains(qcanon)) | (tmp["__name"].str.contains(ql))]

    # Prefer exact-ish matches first
    hit["__rank"] = 2
    hit.loc[hit["__hid"] == qcanon, "__rank"] = 0
    hit.loc[hit["__hid"].str.startswith(qcanon), "__rank"] = 1
    hit = hit.sort_values(by=["__rank", "__hid"]).head(max(1, min(int(limit or 20), 50)))

    out = []
    for _, r in hit.iterrows():
        hid = str(r.get("human_id", "") or "").strip()
        name = str(r.get("name", "") or "").strip()
        label = hid if not name else f"{hid} — {name}"
        out.append({"human_id": hid, "label": label})
    return out


# ------------------ Routes ------------------


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request, next: str = "/"):
    if getattr(request.state, "user", None):
        return RedirectResponse(url=next or "/", status_code=303)
    return _render(request, "login.html", {"path": STATE.get("path", ""), "next": next, "error": ""})


@app.post("/login")
def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/"),
):
    u = authenticate(username=username, password=password)
    if not u:
        return _render(request, "login.html", {"path": STATE.get("path", ""), "next": next, "error": "Credenciales inválidas"})
    token = sign_session(u.username)
    resp = RedirectResponse(url=(next or "/"), status_code=303)
    resp.set_cookie(
        COOKIE_NAME,
        token,
        max_age=int(os.getenv("SAR_SESSION_MAX_AGE", "28800")),
        **cookie_settings(),
    )
    return resp


@app.post("/logout")
def logout_post(request: Request):
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie(COOKIE_NAME)
    return resp


@app.get("/", response_class=HTMLResponse)
def home(request: Request, user=Depends(require_user)):
    view_rows = int(len(STATE["view_full"])) if STATE["view_full"] is not None else 0
    issues_errors = safe_count(STATE["issues"], "severity", "error")
    issues_warnings = safe_count(STATE["issues"], "severity", "warning")

    has_data = (STATE["view_full"] is not None and not STATE["view_full"].empty) or (
        STATE["issues"] is not None and not STATE["issues"].empty
    )

    return _render(
        request,
        "index.html",
        {
            "request": request,
            "path": STATE["path"],
            "has_data": has_data,
            "view_rows": view_rows,
            "issues_errors": issues_errors,
            "issues_warnings": issues_warnings,
            "last_error": STATE.get("last_error", ""),
            "last_regen": STATE.get("last_regen", ""),
            "last_data_registry": _latest_xlsx_in_data_dir(),
        },
    )



@app.get("/open-last")
def open_last(user=Depends(require_user)):
    """Load the newest registry found in /data and regenerate views."""
    STATE["last_error"] = ""
    try:
        p = _latest_xlsx_in_data_dir()
        if not p:
            raise ValueError("No se ha encontrado ningún .xlsx en /data.")
        STATE["path"] = p
        view_full, issues, views_by_level = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return RedirectResponse(url="/view-full", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)


@app.post("/registry/reset-from-template")
def reset_registry_from_template(user=Depends(require_role("admin"))):
    """Create a fresh registry in /data from the base template and open it."""
    STATE["last_error"] = ""
    try:
        if not TEMPLATE_PATH.exists():
            raise ValueError("No existe la plantilla base (registry_template.xlsx).")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = DATA_DIR / f"{ts}__registry.xlsx"
        shutil.copy2(TEMPLATE_PATH, out_path)

        # Stamp meta on the new registry
        tmeta = read_meta_dict(str(TEMPLATE_PATH))
        base_ver = tmeta.get("schema_version") or tmeta.get("template_version", "")
        tmap = get_schema_map(str(TEMPLATE_PATH), SCHEMA_SHEETS)
        th = schema_hash(tmap)
        write_meta_kv(
            str(out_path),
            {
                "schema_version": base_ver,
                "schema_hash": th,
                "schema_base_version": base_ver,
                "schema_dirty": "no",
                "last_modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        STATE["path"] = str(out_path.resolve())
        view_full, issues, views_by_level = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)


@app.post("/template/promote")
def promote_registry_schema_to_template(user=Depends(require_role("admin"))):
    """Promote columns present in the active registry but missing in the template."""
    STATE["last_error"] = ""
    try:
        if not TEMPLATE_PATH.exists():
            raise ValueError("No existe la plantilla base (registry_template.xlsx).")
        if not _ensure_registry_loaded():
            raise ValueError("No hay un registry cargado.")

        st = _schema_state()
        if not st.get("added"):
            return RedirectResponse(url="/", status_code=303)

        # Apply missing columns to template
        for sh, cols in st["added"].items():
            # We write normalised headers as-is (stable and predictable)
            add_missing_columns(str(TEMPLATE_PATH), sh, cols)

        # Update schema meta in template
        tmeta = read_meta_dict(str(TEMPLATE_PATH))
        cur = tmeta.get("schema_version") or tmeta.get("template_version") or "0.1.0"
        new_ver = _bump_semver(cur)
        tmap = get_schema_map(str(TEMPLATE_PATH), SCHEMA_SHEETS)
        th = schema_hash(tmap)
        write_meta_kv(
            str(TEMPLATE_PATH),
            {
                "schema_version": new_ver,
                "schema_hash": th,
                "last_modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)


@app.post("/template/migrate-registry")
def migrate_registry_to_template_schema(user=Depends(require_role("admin"))):
    """Add to the active registry any columns that exist in the template but are missing in the registry."""
    STATE["last_error"] = ""
    try:
        if not TEMPLATE_PATH.exists():
            raise ValueError("No existe la plantilla base (registry_template.xlsx).")
        if not _ensure_registry_loaded():
            raise ValueError("No hay un registry cargado.")

        st = _schema_state()
        if st.get("missing"):
            for sh, cols in st["missing"].items():
                add_missing_columns(STATE["path"], sh, cols)

        # Sync schema meta into registry
        tmeta = read_meta_dict(str(TEMPLATE_PATH))
        base_ver = tmeta.get("schema_version") or tmeta.get("template_version", "")
        rmap = get_schema_map(STATE["path"], SCHEMA_SHEETS)
        rh = schema_hash(rmap)
        write_meta_kv(
            STATE["path"],
            {
                "schema_version": base_ver,
                "schema_hash": rh,
                "schema_dirty": "no" if not st.get("added") else "yes",
                "last_modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        # Regenerate views after structural change
        view_full, issues, views_by_level = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)


@app.post("/regenerate")
async def regenerate(
    path: str = Form(""),
    file: UploadFile | None = File(None),
    user=Depends(require_role("editor")),
):
    STATE["last_error"] = ""
    try:
        chosen_path = ""

        if file is not None and file.filename:
            if not file.filename.lower().endswith(".xlsx"):
                raise ValueError("El fichero debe ser .xlsx")

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = Path(file.filename).name
            out_path = DATA_DIR / f"{ts}__{safe_name}"

            content = await file.read()
            out_path.write_bytes(content)
            chosen_path = str(out_path.resolve())
        else:
            p = (path or "").strip()
            if not p:
                raise ValueError("Selecciona un fichero .xlsx o introduce una ruta válida.")
            if not os.path.exists(p):
                raise ValueError("La ruta indicada no existe.")
            chosen_path = p

        STATE["path"] = chosen_path
        view_full, issues, views_by_level = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return RedirectResponse(url="/view-full", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)

@app.get("/view-full", response_class=HTMLResponse)
def view_full(
    request: Request,
    user=Depends(require_user),
    q: str = "",
    exposure: str = "",
    internet_exposure: str = "",
    status: str = "",
    vuln_c1: str = "",
    vuln_c2: str = "",
    vuln_c3: str = "",
    vuln_c4: str = "",
):
    # Base DF (prefijado, dinámico)
    df = STATE["view_full"].copy() if STATE["view_full"] is not None else pd.DataFrame()

    if not df.empty:
        # Para no depender de cambios en templates:
        # - mantenemos parámetros q/exposure/internet_exposure/status
        # - filtramos sobre columnas prefijadas si existen
        exposure_col = first_existing_col(df, "c3__exposure", "exposure")
        internet_col = first_existing_col(df, "c4__internet_exposure", "internet_exposure")
        # runtime status: en excel suele ser "status" en C4 -> "c4__status"
        status_col = first_existing_col(df, "c4__status", "runtime_status", "status")

        if q:
            ql = q.lower()
            mask = df.apply(lambda r: ql in " ".join([str(x).lower() for x in r.values]), axis=1)
            df = df[mask]

        if exposure and exposure_col:
            df = df[df[exposure_col] == exposure]

        if internet_exposure and internet_col:
            df = df[df[internet_col] == internet_exposure]

        if status and status_col:
            df = df[df[status_col] == status]

        # vulnerabilities_detected filters (by level)
        def _apply_vuln_filter(param: str, colname: str):
            nonlocal df
            if param and colname in df.columns:
                df = df[df[colname] == param]

        _apply_vuln_filter(vuln_c1, "c1__vulnerabilities_detected")
        _apply_vuln_filter(vuln_c2, "c2__vulnerabilities_detected")
        _apply_vuln_filter(vuln_c3, "c3__vulnerabilities_detected")
        _apply_vuln_filter(vuln_c4, "c4__vulnerabilities_detected")

        # Nota: anteriormente se añadían aliases sin prefijo (exposure/internet_exposure/runtime_status)
        # para compatibilidad con templates antiguos. Ya no es necesario y generaba columnas redundantes
        # al final de la tabla en VIEW_Full.

    return _render(
        request,
        "view_full.html",
        {
            "request": request,
            "path": STATE["path"],
            "rows": df.to_dict(orient="records"),
            "q": q,
            "exposure": exposure,
            "internet_exposure": internet_exposure,
            "status": status,
            "vuln_c1": vuln_c1,
            "vuln_c2": vuln_c2,
            "vuln_c3": vuln_c3,
            "vuln_c4": vuln_c4,
        },
    )


@app.get("/api/parents/search")
def api_search_parents(child_level: str = "", q: str = "", limit: int = 20, user=Depends(require_user)):
    """Autocomplete endpoint for parent ids.

    - child_level: level being edited/created (C2/C3/C4)
    - q: user query (partial human_id or name)
    """
    if not _ensure_registry_loaded():
        return JSONResponse({"items": []})

    try:
        items = _search_parent_candidates(child_level=child_level, q=q, limit=limit)
        return JSONResponse({"items": items})
    except Exception:
        # Never break the form because of autocomplete.
        return JSONResponse({"items": []})


@app.get("/issues", response_class=HTMLResponse)
def issues(
    request: Request,
    user=Depends(require_user),
    severity: str = "",
    level: str = "",
    issue_type: str = "",
):
    df = STATE["issues"].copy() if STATE["issues"] is not None else pd.DataFrame()

    if not df.empty:
        if severity and "severity" in df.columns:
            df = df[df["severity"] == severity]
        if level and "level" in df.columns:
            df = df[df["level"] == level]
        if issue_type and "issue_type" in df.columns:
            df = df[df["issue_type"] == issue_type]

    return _render(
        request,
        "issues.html",
        {
            "request": request,
            "path": STATE["path"],
            "rows": df.to_dict(orient="records"),
            "severity": severity,
            "level": level,
            "issue_type": issue_type,
        },
    )


# ------------------ Level list (C1..C4) ------------------


@app.get("/level/{level}", response_class=HTMLResponse)
def list_level(
    request: Request,
    level: str,
    q: str = "",
    status: str = "",
    parent: str = "",  # filter by parent ref when applicable
    orphan: str = "",  # "1" => only orphans
    view: str = "compact",  # compact|full
    vulnerabilities_detected: str = "",
    user=Depends(require_user),
):
    """List all records for a given level (C1..C4).

    This view is *sheet-centric* (unlike VIEW_Full which is runtime-centric) and
    therefore includes orphan records and records without descendants.
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = meta_for_level(level)
    if not meta:
        STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
        return RedirectResponse(url="/", status_code=303)

    path = STATE["path"]
    sheet = meta["sheet"]
    parent_col = meta.get("parent_col")  # None for C1
    level_code = meta["level"]

    # Use derived view from engine (never raw Excel)
    df = (STATE.get("views_by_level", {}) or {}).get(level_code)
    if df is None:
        df = read_sheet(path, sheet)

    # Stable/common columns (if present)
    status_col = first_existing_col(df, "status")
    name_col = first_existing_col(df, "name")

    # --- Orphan detection (only when a parent is expected)
    parent_ids = set()
    if parent_col:
        parent_level = {"C2": "C1", "C3": "C2", "C4": "C3"}.get(level_code, "")
        pm = meta_for_level(parent_level) if parent_level else None
        if pm:
            pdf = read_sheet(path, pm["sheet"])
            if pdf is not None and not pdf.empty and "human_id" in pdf.columns:
                parent_ids = set(pdf["human_id"].astype(str).map(canon).tolist())

    # --- Immediate children counts (fast summaries)
    counts_c2_by_c1 = {}
    counts_c3_by_c2 = {}
    counts_c4_by_c3 = {}

    if level_code in ("C1",):
        c2 = read_sheet(path, "C2_Aplicaciones")
        if c2 is not None and not c2.empty and "c1_human_id" in c2.columns:
            counts_c2_by_c1 = (
                c2.assign(__k=c2["c1_human_id"].astype(str).map(canon)).groupby("__k").size().to_dict()
            )

    if level_code in ("C2",):
        c3 = read_sheet(path, "C3_Componentes")
        if c3 is not None and not c3.empty and "c2_human_id" in c3.columns:
            counts_c3_by_c2 = (
                c3.assign(__k=c3["c2_human_id"].astype(str).map(canon)).groupby("__k").size().to_dict()
            )

    if level_code in ("C3",):
        c4 = read_sheet(path, "C4_Runtime")
        if c4 is not None and not c4.empty and "c3_human_id" in c4.columns:
            counts_c4_by_c3 = (
                c4.assign(__k=c4["c3_human_id"].astype(str).map(canon)).groupby("__k").size().to_dict()
            )

    # --- Filtering
    out = df.copy() if df is not None else pd.DataFrame()
    if not out.empty:
        if q:
            ql = q.lower()
            mask = out.apply(lambda r: ql in " ".join([str(x).lower() for x in r.values]), axis=1)
            out = out[mask]

        if status and status_col:
            out = out[out[status_col].astype(str) == status]

        if vulnerabilities_detected and "vulnerabilities_detected" in out.columns:
            out = out[out["vulnerabilities_detected"].astype(str) == vulnerabilities_detected]

        # Optional: filter by explicit parent reference (contextual navigation)
        if parent and parent_col and parent_col in out.columns:
            p = canon(parent)
            out = out[out[parent_col].astype(str).map(canon) == p]

        if parent_col and parent_col in out.columns:
            out["__orphan"] = ~out[parent_col].astype(str).map(canon).isin(parent_ids)
            if orphan == "1":
                out = out[out["__orphan"] == True]
        else:
            out["__orphan"] = False

    # --- Columns to show
    base_cols = [c for c in ["human_id", name_col, status_col, parent_col] if c]
    base_cols = [c for c in base_cols if c in out.columns]
    # Avoid duplicates if name_col/status_col are already literal "name"/"status"
    base_cols = list(dict.fromkeys(base_cols))

    # In compact mode, show common fields + everything else hidden behind toggle.
    view = (view or "compact").strip().lower()
    show_all = view == "full"

    all_cols = out.columns.tolist() if not out.empty else []
    # never render internal col in table columns list
    if "__orphan" in all_cols:
        all_cols.remove("__orphan")

    if show_all:
        columns = all_cols
    else:
        # Compact: show base columns + a few extras if they exist
        preferred = []
        # try to show a couple of high-signal fields if present
        for cand in [
            "description",
            "owner",
            "business_owner",
            "business_criticality",
            "environments",
            "component_type",
            "exposure",
            "runtime_type",
            "internet_exposure",
        ]:
            if cand in all_cols and cand not in base_cols:
                preferred.append(cand)
            if len(preferred) >= 4:
                break
        columns = base_cols + preferred

    # --- Build rows payload
    rows = []
    if out is not None and not out.empty:
        for _, r in out.iterrows():
            hid = canon(str(r.get("human_id", "")))
            cnt_c2 = int(counts_c2_by_c1.get(hid, 0)) if level_code == "C1" else 0
            cnt_c3 = int(counts_c3_by_c2.get(hid, 0)) if level_code == "C2" else 0
            cnt_c4 = int(counts_c4_by_c3.get(hid, 0)) if level_code == "C3" else 0
            rows.append(
                {
                    "__human_id": hid,
                    "__orphan": bool(r.get("__orphan", False)),
                    "__cnt_c2": cnt_c2,
                    "__cnt_c3": cnt_c3,
                    "__cnt_c4": cnt_c4,
                    **{c: r.get(c, "") for c in columns},
                }
            )

    return _render(
        request,
        "level_list.html",
        {
            "request": request,
            "path": path,
            "level": level_code,
            "sheet": sheet,
            "rows": rows,
            "columns": columns,
            "q": q,
            "status": status,
            "parent": parent,
            "orphan": orphan,
            "has_parent": bool(parent_col),
            "view": view,
            "show_all": show_all,
            "vulnerabilities_detected": vulnerabilities_detected,
        },
    )


@app.get("/record/{human_id}", response_class=HTMLResponse)
def record(request: Request, human_id: str, user=Depends(require_user)):
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = detect_level_meta(human_id)
    if not meta:
        STATE["last_error"] = f"human_id '{human_id}' no reconocido (prefijo no soportado)."
        return RedirectResponse(url="/", status_code=303)

    path = STATE["path"]
    sheet = meta["sheet"]
    level = meta["level"]
    df = read_sheet(path, sheet)
    row = get_row_by_human_id(df, human_id)

    if not row:
        STATE["last_error"] = f"No se encontró '{human_id}' en la pestaña '{sheet}'."
        return RedirectResponse(url="/", status_code=303)

    # Parent
    parent_ref = ""
    parent_level = ""
    parent_col = meta.get("parent_col")
    if parent_col and parent_col in df.columns:
        parent_ref = str(row.get(parent_col, "")).strip()
        if parent_ref:
            pm = detect_level_meta(parent_ref)
            parent_level = pm["level"] if pm else ""

    # Children (immediate)
    children = list_children(path, level, human_id)
    # Descendants counts (summary)
    descendant_counts = list_descendants_counts(path, level, human_id)

    # Issues for this id
    issues_rows = issues_for(STATE["issues"], human_id)

    # Overlay derived fields for display (e.g., inherited vulnerabilities_detected)
    display_record = dict(row)
    ddf = (STATE.get("views_by_level", {}) or {}).get(level)
    if ddf is not None and not ddf.empty and "human_id" in ddf.columns:
        match = ddf[ddf["human_id"].astype(str).map(canon) == canon(human_id)]
        if not match.empty and "vulnerabilities_detected" in match.columns:
            display_record["vulnerabilities_detected"] = str(match.iloc[0].get("vulnerabilities_detected", ""))

    # Editable fields: allow editing of existing sheet columns, but never the identifier.
    non_editable = {"human_id"}
    # vulnerabilities_detected is only editable in C3/C4
    if level in ("C1", "C2"):
        non_editable.add("vulnerabilities_detected")
    editable_fields = [k for k in display_record.keys() if k not in non_editable]

    lookups = lookup_options_by_level(path, level)

    # Mermaid diagram for relationships (rendered client-side)
    mermaid_code = ""
    mermaid_error = ""
    diagram_meta = {"truncated": False, "node_count": 0, "max_nodes": 200}

    # Optional safety limit (protect browser). Can be overridden via ?max_nodes=500
    try:
        max_nodes = int(request.query_params.get("max_nodes", "200"))
    except Exception:
        max_nodes = 200

    try:
        res = build_record_diagram(path, human_id, max_nodes=max_nodes)
        if isinstance(res, tuple) and len(res) == 2:
            mermaid_code, diagram_meta = res
        else:
            mermaid_code = res or ""
            diagram_meta = {"truncated": False, "node_count": 0, "max_nodes": max_nodes}

        if not mermaid_code:
            mermaid_error = "No se pudo generar diagrama para este registro (sin relaciones o datos incompletos)."
    except Exception as e:
        # Diagram is a UI enhancement; never block the page if it fails.
        mermaid_error = f"Error generando diagrama: {type(e).__name__}"
        mermaid_code = ""
        diagram_meta = {"truncated": False, "node_count": 0, "max_nodes": max_nodes}

    return _render(
        request,
        "record.html",
        {
            "request": request,
            "path": path,
            "level": level,
            "sheet": sheet,
            "human_id": canon(human_id),
            "record": display_record,
            "parent_ref": parent_ref,
            "parent_level": parent_level,
            "parent_col": parent_col or "",
            "children": children,
            "desc_counts": descendant_counts,
            "issues": issues_rows,
            "editable_fields": editable_fields,
            "lookups": lookups,
            "mermaid_code": mermaid_code,
            "diagram_meta": diagram_meta,
            "mermaid_error": mermaid_error,
            "last_error": STATE.get("last_error",""),
        },
    )


@app.post("/record/{human_id}/edit")
async def edit_record_existing_fields(request: Request, human_id: str, user=Depends(require_role("editor"))):
    """Update multiple existing fields for a record.

    This endpoint only updates columns that already exist in the corresponding sheet.
    Field names come from the UI (not typed by the user).
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    try:
        form = await request.form()
        # FastAPI's FormData behaves like a multi-dict.
        fields = {str(k): ("" if v is None else str(v)) for k, v in form.items()}

        # Never allow editing the identifier from the UI.
        fields.pop("human_id", None)

        # If nothing to update, just go back.
        if not fields:
            return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

        view_full, issues, views_by_level = update_record_existing_fields(
            path=STATE["path"],
            human_id=human_id,
            fields=fields,
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.post("/record/{human_id}/add-field/preview")
async def preview_add_field(request: Request, human_id: str, user=Depends(require_role("editor"))):
    """Preview creation of a new field (column) before writing to Excel.

    This is a separate flow from updates to avoid creating columns by typo.
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    try:
        form = await request.form()
        field_name = str(form.get("field_name", "") or "").strip()
        value = "" if form.get("value") is None else str(form.get("value"))

        if not field_name:
            STATE["last_error"] = "El nombre del campo no puede estar vacío."
            return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

        # Render a confirmation page (two-step confirmation)
        STATE["last_error"] = ""
        return _render(
            request,
            "add_field_confirm.html",
            {
                "request": request,
                "human_id": canon(human_id),
                "field_name": field_name,
                "value": value,
            },
        )
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.post("/record/{human_id}/add-field")
async def confirm_add_field(request: Request, human_id: str, user=Depends(require_role("editor"))):
    """Confirm and create a new field (column) in the Excel registry."""
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    try:
        form = await request.form()
        field_name = str(form.get("field_name", "") or "").strip()
        value = "" if form.get("value") is None else str(form.get("value"))

        if not field_name:
            STATE["last_error"] = "El nombre del campo no puede estar vacío."
            return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

        view_full, issues, views_by_level = add_new_field(
            path=STATE["path"],
            human_id=human_id,
            field_name=field_name,
            value=value,
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.get("/report/c4/{human_id}.docx")
async def report_c4_docx(request: Request, human_id: str, user=Depends(require_user)):
    """Generate a Word report for a C4 record (RUN-xxxx) and return it as a download."""
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    run_id = canon(human_id)
    if not run_id.startswith("RUN-"):
        return JSONResponse({"error": "Solo disponible para registros C4 (RUN-xxxx)."}, status_code=400)

    # Ensure we have issues computed (report relies on them). If not available, regenerate.
    if STATE.get("issues") is None or getattr(STATE.get("issues"), "empty", True):
        try:
            regenerate_views(STATE["path"], STATE)
        except Exception:
            pass

    try:
        max_nodes = int(request.query_params.get("max_nodes", "200"))
    except Exception:
        max_nodes = 200

    try:
        out_docx = generate_c4_chain_report_docx(
            registry_path=STATE["path"],
            run_human_id=run_id,
            issues_df=STATE.get("issues"),
            template_docx_path=str(REPORT_TEMPLATE_DOCX),
            out_dir=str(REPORTS_DIR),
            max_nodes=max_nodes,
        )
    except FileNotFoundError:
        return JSONResponse(
            {"error": f"No se encontró la plantilla de informe Word: {REPORT_TEMPLATE_DOCX}"},
            status_code=500,
        )
    except Exception as e:
        return JSONResponse({"error": f"No se pudo generar el informe: {type(e).__name__}: {e}"}, status_code=500)

    filename = f"{run_id}__informe.docx"
    return FileResponse(
        path=str(out_docx),
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.get("/report/c4/{human_id}.html")
async def report_c4_html(request: Request, human_id: str, user=Depends(require_user)):
    """Generate an HTML report for a C4 record (RUN-xxxx).

    If `raw=1` query param is provided, returns the HTML as plain text for easy copy/paste.
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    run_id = canon(human_id)
    if not run_id.startswith("RUN-"):
        return JSONResponse({"error": "Solo disponible para registros C4 (RUN-xxxx)."}, status_code=400)

    # Ensure we have issues computed (report relies on them). If not available, regenerate.
    if STATE.get("issues") is None or getattr(STATE.get("issues"), "empty", True):
        try:
            regenerate_views(STATE["path"], STATE)
        except Exception:
            pass

    try:
        max_nodes = int(request.query_params.get("max_nodes", "200"))
    except Exception:
        max_nodes = 200

    try:
        out_html = generate_c4_chain_report_html(
            registry_path=STATE["path"],
            run_human_id=run_id,
            issues_df=STATE.get("issues"),
            template_html_path=str(REPORT_TEMPLATE_HTML),
            out_dir=str(REPORTS_DIR),
            max_nodes=max_nodes,
        )
    except FileNotFoundError:
        return JSONResponse(
            {"error": f"No se encontró la plantilla de informe HTML: {REPORT_TEMPLATE_HTML}"},
            status_code=500,
        )
    except Exception as e:
        return JSONResponse({"error": f"No se pudo generar el informe: {type(e).__name__}: {e}"}, status_code=500)

    html_text = Path(out_html).read_text(encoding="utf-8")
    if str(request.query_params.get("raw", "")).strip() in ("1", "true", "yes"):
        return PlainTextResponse(content=html_text, media_type="text/plain; charset=utf-8")
    return HTMLResponse(content=html_text)





@app.get("/create/{level}", response_class=HTMLResponse)
def create_form(request: Request, level: str, parent: str = "", user=Depends(require_role("editor"))):
    """Render creation form for a given level (C1-C4)."""
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = meta_for_level(level)
    if not meta:
        STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
        return RedirectResponse(url="/", status_code=303)

    sheet = meta["sheet"]
    df = read_sheet(STATE["path"], sheet)
    cols = [c for c in df.columns.tolist() if c != "human_id"]

    # Prefill parent reference if provided and applicable
    parent_col = meta.get("parent_col")
    prefill = {}
    if parent_col:
        prefill[parent_col] = (parent or "").strip()

    # Defaults
    prefill.setdefault("status", "draft")

    lookups = lookup_options_by_level(STATE["path"], meta["level"])

    return _render(
        request,
        "create_form.html",
        {
            "request": request,
            "level": meta["level"],
            "sheet": sheet,
            "columns": cols,
            "parent_col": parent_col or "",
            "prefill": prefill,
            "lookups": lookups,
            "last_error": STATE.get("last_error", ""),
        },
    )


@app.post("/create/{level}/preview", response_class=HTMLResponse)
async def create_preview(request: Request, level: str, user=Depends(require_role("editor"))):
    """Preview creation (two-step confirmation) without writing to Excel."""
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = meta_for_level(level)
    if not meta:
        STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
        return RedirectResponse(url="/", status_code=303)

    try:
        form = await request.form()
        fields = {str(k): ("" if v is None else str(v)).strip() for k, v in form.items()}

        # Minimal validations (same as create)
        name = fields.get("name", "").strip()
        if not name:
            raise ValueError("El campo 'name' es obligatorio.")

        parent_col = meta.get("parent_col")
        if parent_col:
            if not fields.get(parent_col, "").strip():
                raise ValueError(f"El campo '{parent_col}' es obligatorio para {meta['level']}.")

        # Predict next id for display (not a reservation)
        next_id = generate_next_human_id(STATE["path"], meta["sheet"], meta["prefix"])

        STATE["last_error"] = ""
        return _render(
            request,
            "create_confirm.html",
            {
                "request": request,
                "level": meta["level"],
                "sheet": meta["sheet"],
                "next_id": next_id,
                "fields": fields,
            },
        )
    except Exception as e:
        STATE["last_error"] = str(e)
        # redirect back to form (keep parent if present)
        parent = ""
        pc = meta.get("parent_col")
        if pc:
            try:
                parent = (await request.form()).get(pc, "") or ""
            except Exception:
                parent = ""
        url = f"/create/{meta['level']}"
        if parent:
            url += f"?parent={parent}"
        return RedirectResponse(url=url, status_code=303)


@app.post("/create/{level}")
async def create_confirm(request: Request, level: str, user=Depends(require_role("editor"))):
    """Confirm and create the record in Excel."""
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = meta_for_level(level)
    if not meta:
        STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
        return RedirectResponse(url="/", status_code=303)

    try:
        form = await request.form()
        fields = {str(k): ("" if v is None else str(v)).strip() for k, v in form.items()}

        new_id, view_full, issues, views_by_level = create_record(path=STATE["path"], level=meta["level"], fields=fields)
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""

        return RedirectResponse(url=f"/record/{canon(new_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        # back to form
        parent = ""
        pc = meta.get("parent_col")
        if pc:
            parent = fields.get(pc, "")
        url = f"/create/{meta['level']}"
        if parent:
            url += f"?parent={parent}"
        return RedirectResponse(url=url, status_code=303)



@app.post("/deprecate/{human_id}")
def deprecate(human_id: str, user=Depends(require_role("editor"))):
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = detect_level_meta(human_id)
    if not meta:
        STATE["last_error"] = f"human_id '{human_id}' no reconocido."
        return RedirectResponse(url="/", status_code=303)

    try:
        view_full, issues, views_by_level = update_record_existing_fields(
            path=STATE["path"], human_id=human_id, fields={"status": "deprecated"}
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""

        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.post("/update/{human_id}")
def update_existing_fields(
    human_id: str,
    field: str = Form(""),
    value: str = Form(""),
    user=Depends(require_role("editor")),
):
    """Update a single existing field (column) for a record.

    This is the first CRUD step: only existing columns can be updated.
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    try:
        if not field.strip():
            raise ValueError("El campo (field) no puede estar vacío.")

        view_full, issues, views_by_level = update_record_existing_fields(
            path=STATE["path"],
            human_id=human_id,
            fields={field: value},
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["views_by_level"] = views_by_level
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.get("/export/view-full.csv")
def export_view_full(user=Depends(require_user)):
    df = STATE["view_full"]
    if df is None or df.empty:
        return RedirectResponse(url="/", status_code=303)
    # Exporta la vista canónica (prefijada) SIN aliases de UI
    return df_to_csv_stream(df)


@app.get("/export/issues.csv")
def export_issues(user=Depends(require_user)):
    df = STATE["issues"]
    if df is None or df.empty:
        return RedirectResponse(url="/", status_code=303)
    return df_to_csv_stream(df)