# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from datetime import datetime

from pathlib import Path

from fastapi import Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, PlainTextResponse


def register_record_routes(app, ctx) -> None:
    STATE = ctx.STATE
    _render = ctx._render
    _ensure_registry_loaded = ctx._ensure_registry_loaded
    canon = ctx.canon
    read_sheet = ctx.read_sheet
    lookup_options_by_level = ctx.lookup_options_by_level
    meta_for_level = ctx.meta_for_level
    generate_next_id = ctx.generate_next_id
    update_record_existing_fields = ctx.update_record_existing_fields
    add_new_field = ctx.add_new_field
    create_record = ctx.create_record
    regenerate_views = ctx.regenerate_views
    detect_level_meta = ctx.detect_level_meta
    get_row_by_id = ctx.get_row_by_id
    issues_for = ctx.issues_for
    list_children = ctx.list_children
    list_descendants_counts = ctx.list_descendants_counts
    build_record_diagram = ctx.build_record_diagram
    generate_c4_chain_report_docx = ctx.generate_c4_chain_report_docx
    generate_c4_chain_report_html = ctx.generate_c4_chain_report_html
    REPORT_TEMPLATE_DOCX = ctx.REPORT_TEMPLATE_DOCX
    REPORT_TEMPLATE_HTML = ctx.REPORT_TEMPLATE_HTML
    REPORTS_DIR = ctx.REPORTS_DIR
    require_csrf = ctx.require_csrf
    require_role = ctx.require_role
    require_user = ctx.require_user
    df_to_csv_stream = ctx.df_to_csv_stream
    read_only_fields_for_level = ctx.read_only_fields_for_level
    is_required_field = ctx.is_required_field
    is_editable_field = ctx.is_editable_field
    @app.get("/record/{id}", response_class=HTMLResponse)
    def record(request: Request, id: str, user=Depends(require_user)):
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        meta = detect_level_meta(id)
        if not meta:
            STATE["last_error"] = f"id '{id}' no reconocido (prefijo no soportado)."
            return RedirectResponse(url="/", status_code=303)

        path = STATE["path"]
        sheet = meta["sheet"]
        level = meta["level"]
        df = read_sheet(path, sheet)
        row = get_row_by_id(df, id)

        if not row:
            STATE["last_error"] = f"No se encontró '{id}' en la pestaña '{sheet}'."
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
        children = list_children(path, level, id)
        # Descendants counts (summary)
        descendant_counts = list_descendants_counts(path, level, id)

        # Issues for this id
        issues_rows = issues_for(STATE["issues"], id)

        # Overlay derived fields for display (e.g., inherited vulnerabilities_detected)
        display_record = dict(row)
        ddf = (STATE.get("views_by_level", {}) or {}).get(level)
        if ddf is not None and not ddf.empty and "id" in ddf.columns:
            match = ddf[ddf["id"].astype(str).map(canon) == canon(id)]
            if not match.empty and "vulnerabilities_detected" in match.columns:
                display_record["vulnerabilities_detected"] = str(match.iloc[0].get("vulnerabilities_detected", ""))

        # Editable fields are governed centrally by the registry schema helpers.
        editable_fields = [k for k in display_record.keys() if is_editable_field(level, k)]

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
            res = build_record_diagram(path, id, max_nodes=max_nodes)
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
                "id": canon(id),
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


    @app.post("/record/{id}/edit", dependencies=[Depends(require_csrf)])
    async def edit_record_existing_fields(request: Request, id: str, user=Depends(require_role("editor"))):
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
            fields.pop("id", None)

            # If nothing to update, just go back.
            if not fields:
                return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

            view_full, issues, views_by_level = update_record_existing_fields(
                path=STATE["path"],
                id=id,
                fields=fields,
            )
            STATE["view_full"] = view_full
            STATE["issues"] = issues
            STATE["views_by_level"] = views_by_level
            STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["last_error"] = ""
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

        except Exception as e:
            STATE["last_error"] = str(e)
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)


    @app.post("/record/{id}/add-field/preview", dependencies=[Depends(require_csrf)])
    async def preview_add_field(request: Request, id: str, user=Depends(require_role("editor"))):
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
                return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

            # Render a confirmation page (two-step confirmation)
            STATE["last_error"] = ""
            return _render(
                request,
                "add_field_confirm.html",
                {
                    "request": request,
                    "id": canon(id),
                    "field_name": field_name,
                    "value": value,
                },
            )
        except Exception as e:
            STATE["last_error"] = str(e)
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)


    @app.post("/record/{id}/add-field", dependencies=[Depends(require_csrf)])
    async def confirm_add_field(request: Request, id: str, user=Depends(require_role("editor"))):
        """Confirm and create a new field (column) in the Excel registry."""
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        try:
            form = await request.form()
            field_name = str(form.get("field_name", "") or "").strip()
            value = "" if form.get("value") is None else str(form.get("value"))

            if not field_name:
                STATE["last_error"] = "El nombre del campo no puede estar vacío."
                return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

            view_full, issues, views_by_level = add_new_field(
                path=STATE["path"],
                id=id,
                field_name=field_name,
                value=value,
            )
            STATE["view_full"] = view_full
            STATE["issues"] = issues
            STATE["views_by_level"] = views_by_level
            STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["last_error"] = ""
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

        except Exception as e:
            STATE["last_error"] = str(e)
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)


    @app.get("/report/c4/{id}.docx")
    async def report_c4_docx(request: Request, id: str, user=Depends(require_user)):
        """Generate a Word report for a C4 record (C4-xxxx) and return it as a download."""
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        run_id = canon(id)
        if not run_id.startswith("C4-"):
            return JSONResponse({"error": "Solo disponible para registros C4 (C4-xxxx)."}, status_code=400)

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
                run_id=run_id,
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


    @app.get("/report/c4/{id}.html")
    async def report_c4_html(request: Request, id: str, user=Depends(require_user)):
        """Generate an HTML report for a C4 record (C4-xxxx).

        If `raw=1` query param is provided, returns the HTML as plain text for easy copy/paste.
        """
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        run_id = canon(id)
        if not run_id.startswith("C4-"):
            return JSONResponse({"error": "Solo disponible para registros C4 (C4-xxxx)."}, status_code=400)

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
                run_id=run_id,
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








    def _parse_form_fields(form) -> dict:
        """Normalize Starlette form into a simple {str:str} dict (single-value fields)."""
        return {str(k): ("" if v is None else str(v)).strip() for k, v in (form or {}).items()}


    def _validate_create_fields(meta: dict, fields: dict) -> dict:
        """Return field-level errors for creation forms using central structural rules."""
        errors = {}
        level = str(meta.get("level", "") or "").strip().upper()
        for field_name, value in fields.items():
            if not is_required_field(level, field_name):
                continue
            if not str(value or "").strip():
                errors[field_name] = "Obligatorio."
        # Required structural fields may be absent from the submitted form entirely.
        parent_col = meta.get("parent_col")
        if parent_col and is_required_field(level, parent_col) and not str(fields.get(parent_col, "") or "").strip():
            errors[parent_col] = f"Obligatorio para {level}."
        if is_required_field(level, "name") and not str(fields.get("name", "") or "").strip():
            errors["name"] = "Obligatorio."
        return errors


    def _render_create_form(
        request: Request,
        meta: dict,
        *,
        fields: dict | None = None,
        errors: dict | None = None,
        form_error: str = "",
        status_code: int = 200,
    ):
        """Render creation form with sticky values + field errors."""
        sheet = meta["sheet"]
        df = read_sheet(STATE["path"], sheet)
        cols = [c for c in df.columns.tolist() if c != "id"]

        read_only_cols = read_only_fields_for_level(meta["level"])

        # Defaults / prefill
        prefill = {}
        parent_col = meta.get("parent_col")
        if parent_col:
            prefill[parent_col] = (fields or {}).get(parent_col, "").strip()
        prefill.setdefault("status", "draft")

        lookups = lookup_options_by_level(STATE["path"], meta["level"])

        values = dict(prefill)
        if fields:
            values.update(fields)

        return _render(
            request,
            "create_form.html",
            {
                "request": request,
                "level": meta["level"],
                "sheet": sheet,
                "columns": cols,
                "read_only_cols": sorted(list(read_only_cols)),
                "parent_col": parent_col or "",
                "prefill": prefill,
                "values": values,
                "errors": errors or {},
                "form_error": form_error,
                "lookups": lookups,
            },
            status_code=status_code,
        )
    @app.get("/create/{level}", response_class=HTMLResponse)
    def create_form(request: Request, level: str, parent: str = "", user=Depends(require_role("editor"))):
        """Render creation form for a given level (C1-C4)."""
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        meta = meta_for_level(level)
        if not meta:
            STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
            return RedirectResponse(url="/", status_code=303)

        fields = {}
        parent_col = meta.get("parent_col")
        if parent_col:
            fields[parent_col] = (parent or "").strip()

        # Clear any previous global error banner; create errors are rendered inline now.
        STATE["last_error"] = ""

        return _render_create_form(request, meta, fields=fields, errors={}, form_error="")




    @app.post("/create/{level}/preview", response_class=HTMLResponse, dependencies=[Depends(require_csrf)])
    async def create_preview(request: Request, level: str, user=Depends(require_role("editor"))):
        """Preview creation (two-step confirmation) without writing to Excel.

        On validation errors, re-render the form with sticky values and field-level messages.
        """
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        meta = meta_for_level(level)
        if not meta:
            STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
            return RedirectResponse(url="/", status_code=303)

        form = await request.form()
        fields = _parse_form_fields(form)

        # Prevent forbidden derived fields being submitted from the browser
        if meta.get("level") in ("C1", "C2"):
            fields.pop("vulnerabilities_detected", None)

        errors = _validate_create_fields(meta, fields)
        if errors:
            return _render_create_form(
                request,
                meta,
                fields=fields,
                errors=errors,
                form_error="Revisa los campos marcados.",
                status_code=400,
            )

        try:
            # Predict next id for display (not a reservation)
            next_id = generate_next_id(STATE["path"], meta["sheet"], meta["prefix"])
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
            # Technical errors (e.g. reading registry) should still keep the user's input.
            return _render_create_form(
                request,
                meta,
                fields=fields,
                errors={},
                form_error=str(e),
                status_code=400,
            )





    @app.post("/create/{level}", dependencies=[Depends(require_csrf)])
    async def create_confirm(request: Request, level: str, user=Depends(require_role("editor"))):
        """Confirm and create the record in Excel.

        On validation/creation errors, re-render the form with sticky values.
        """
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        meta = meta_for_level(level)
        if not meta:
            STATE["last_error"] = f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4."
            return RedirectResponse(url="/", status_code=303)

        form = await request.form()
        fields = _parse_form_fields(form)

        # Prevent forbidden derived fields being submitted from the browser
        if meta.get("level") in ("C1", "C2"):
            fields.pop("vulnerabilities_detected", None)

        errors = _validate_create_fields(meta, fields)
        if errors:
            return _render_create_form(
                request,
                meta,
                fields=fields,
                errors=errors,
                form_error="Revisa los campos marcados.",
                status_code=400,
            )

        try:
            new_id, view_full, issues, views_by_level = create_record(path=STATE["path"], level=meta["level"], fields=fields)
            STATE["view_full"] = view_full
            STATE["issues"] = issues
            STATE["views_by_level"] = views_by_level
            STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["last_error"] = ""

            return RedirectResponse(url=f"/record/{canon(new_id)}", status_code=303)

        except Exception as e:
            # Keep sticky values on any failure (validation, IO, etc.)
            return _render_create_form(
                request,
                meta,
                fields=fields,
                errors={},
                form_error=str(e),
                status_code=400,
            )





    @app.post("/deprecate/{id}", dependencies=[Depends(require_csrf)])
    def deprecate(id: str, user=Depends(require_role("editor"))):
        if not _ensure_registry_loaded():
            return RedirectResponse(url="/", status_code=303)

        meta = detect_level_meta(id)
        if not meta:
            STATE["last_error"] = f"id '{id}' no reconocido."
            return RedirectResponse(url="/", status_code=303)

        try:
            view_full, issues, views_by_level = update_record_existing_fields(
                path=STATE["path"], id=id, fields={"status": "deprecated"}
            )
            STATE["view_full"] = view_full
            STATE["issues"] = issues
            STATE["views_by_level"] = views_by_level
            STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["last_error"] = ""

            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)

        except Exception as e:
            STATE["last_error"] = str(e)
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)


    @app.post("/update/{id}", dependencies=[Depends(require_csrf)])
    def update_existing_fields(
        id: str,
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
                id=id,
                fields={field: value},
            )
            STATE["view_full"] = view_full
            STATE["issues"] = issues
            STATE["views_by_level"] = views_by_level
            STATE["last_regen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["last_error"] = ""
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)
        except Exception as e:
            STATE["last_error"] = str(e)
            return RedirectResponse(url=f"/record/{canon(id)}", status_code=303)


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
