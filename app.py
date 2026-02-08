# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sar.core.utils import canon, df_to_csv_stream, first_existing_col, safe_count
from sar.infra.registry_repo import read_sheet
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

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

STATE = {
    "path": "",
    "view_full": pd.DataFrame(),
    "issues": pd.DataFrame(),
    "last_error": "",
    "last_regen": "",
}


def _ensure_registry_loaded() -> bool:
    return bool(STATE.get("path")) and os.path.exists(STATE["path"])



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


# ------------------ Routes ------------------


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    view_rows = int(len(STATE["view_full"])) if STATE["view_full"] is not None else 0
    issues_errors = safe_count(STATE["issues"], "severity", "error")
    issues_warnings = safe_count(STATE["issues"], "severity", "warning")

    has_data = (STATE["view_full"] is not None and not STATE["view_full"].empty) or (
        STATE["issues"] is not None and not STATE["issues"].empty
    )

    return templates.TemplateResponse(
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
def open_last():
    """Load the newest registry found in /data and regenerate views."""
    STATE["last_error"] = ""
    try:
        p = _latest_xlsx_in_data_dir()
        if not p:
            raise ValueError("No se ha encontrado ningún .xlsx en /data.")
        STATE["path"] = p
        view_full, issues = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return RedirectResponse(url="/view-full", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)


@app.post("/regenerate")
async def regenerate(
    path: str = Form(""),
    file: UploadFile | None = File(None),
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
        view_full, issues = regenerate_views(STATE["path"])
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return RedirectResponse(url="/view-full", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url="/", status_code=303)

@app.get("/view-full", response_class=HTMLResponse)
def view_full(
    request: Request,
    q: str = "",
    exposure: str = "",
    internet_exposure: str = "",
    status: str = "",
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

        # Nota: anteriormente se añadían aliases sin prefijo (exposure/internet_exposure/runtime_status)
        # para compatibilidad con templates antiguos. Ya no es necesario y generaba columnas redundantes
        # al final de la tabla en VIEW_Full.

    return templates.TemplateResponse(
        "view_full.html",
        {
            "request": request,
            "path": STATE["path"],
            "rows": df.to_dict(orient="records"),
            "q": q,
            "exposure": exposure,
            "internet_exposure": internet_exposure,
            "status": status,
        },
    )


@app.get("/issues", response_class=HTMLResponse)
def issues(
    request: Request,
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

    return templates.TemplateResponse(
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

    return templates.TemplateResponse(
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
        },
    )


@app.get("/record/{human_id}", response_class=HTMLResponse)
def record(request: Request, human_id: str):
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

    # Editable fields (MVP): allow editing of existing sheet columns, but never the identifier.
    non_editable = {"human_id"}
    editable_fields = [k for k in row.keys() if k not in non_editable]

    return templates.TemplateResponse(
        "record.html",
        {
            "request": request,
            "path": path,
            "level": level,
            "sheet": sheet,
            "human_id": canon(human_id),
            "record": row,
            "parent_ref": parent_ref,
            "parent_level": parent_level,
            "children": children,
            "desc_counts": descendant_counts,
            "issues": issues_rows,
            "editable_fields": editable_fields,
            "last_error": STATE.get("last_error",""),
        },
    )


@app.post("/record/{human_id}/edit")
async def edit_record_existing_fields(request: Request, human_id: str):
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

        view_full, issues = update_record_existing_fields(
            path=STATE["path"],
            human_id=human_id,
            fields=fields,
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.post("/record/{human_id}/add-field/preview")
async def preview_add_field(request: Request, human_id: str):
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
        return templates.TemplateResponse(
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
async def confirm_add_field(request: Request, human_id: str):
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

        view_full, issues = add_new_field(
            path=STATE["path"],
            human_id=human_id,
            field_name=field_name,
            value=value,
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)

    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)





@app.get("/create/{level}", response_class=HTMLResponse)
def create_form(request: Request, level: str, parent: str = ""):
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

    return templates.TemplateResponse(
        "create_form.html",
        {
            "request": request,
            "level": meta["level"],
            "sheet": sheet,
            "columns": cols,
            "parent_col": parent_col or "",
            "prefill": prefill,
            "last_error": STATE.get("last_error", ""),
        },
    )


@app.post("/create/{level}/preview", response_class=HTMLResponse)
async def create_preview(request: Request, level: str):
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
        return templates.TemplateResponse(
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
async def create_confirm(request: Request, level: str):
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

        new_id, view_full, issues = create_record(path=STATE["path"], level=meta["level"], fields=fields)
        STATE["view_full"] = view_full
        STATE["issues"] = issues
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
def deprecate(human_id: str):
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    meta = detect_level_meta(human_id)
    if not meta:
        STATE["last_error"] = f"human_id '{human_id}' no reconocido."
        return RedirectResponse(url="/", status_code=303)

    try:
        view_full, issues = update_record_existing_fields(
            path=STATE["path"], human_id=human_id, fields={"status": "deprecated"}
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
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
):
    """Update a single existing field (column) for a record.

    This is the first CRUD step: only existing columns can be updated.
    """
    if not _ensure_registry_loaded():
        return RedirectResponse(url="/", status_code=303)

    try:
        if not field.strip():
            raise ValueError("El campo (field) no puede estar vacío.")

        view_full, issues = update_record_existing_fields(
            path=STATE["path"],
            human_id=human_id,
            fields={field: value},
        )
        STATE["view_full"] = view_full
        STATE["issues"] = issues
        STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_error"] = ""
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)
    except Exception as e:
        STATE["last_error"] = str(e)
        return RedirectResponse(url=f"/record/{canon(human_id)}", status_code=303)


@app.get("/export/view-full.csv")
def export_view_full():
    df = STATE["view_full"]
    if df is None or df.empty:
        return RedirectResponse(url="/", status_code=303)
    # Exporta la vista canónica (prefijada) SIN aliases de UI
    return df_to_csv_stream(df)


@app.get("/export/issues.csv")
def export_issues():
    df = STATE["issues"]
    if df is None or df.empty:
        return RedirectResponse(url="/", status_code=303)
    return df_to_csv_stream(df)