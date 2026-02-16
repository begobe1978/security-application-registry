# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import base64
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from jinja2 import Environment, FileSystemLoader, select_autoescape

from sar.core.utils import canon
from sar.infra.registry_repo import read_meta_dict, read_sheet
from sar.services.diagram_service import build_record_diagram


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    # pandas NaN
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)


def _row_min(row: Dict[str, Any]) -> Dict[str, str]:
    """Return only required fields, stringified."""
    return {
        "human_id": _safe_str(row.get("human_id", "")).strip(),
        "name": _safe_str(row.get("name", "")).strip(),
        "status": _safe_str(row.get("status", "")).strip(),
    }


def _get_row(df: pd.DataFrame, human_id: str) -> Optional[Dict[str, Any]]:
    if df is None or df.empty or "human_id" not in df.columns:
        return None
    hid = canon(human_id)
    tmp = df.copy()
    tmp["__hid"] = tmp["human_id"].astype(str).map(canon)
    hit = tmp[tmp["__hid"] == hid]
    if hit.empty:
        return None
    return hit.iloc[0].drop(labels=["__hid"]).to_dict()


def _list_children_min(df: pd.DataFrame, parent_col: str, parent_hid: str) -> List[Dict[str, str]]:
    if df is None or df.empty or "human_id" not in df.columns or parent_col not in df.columns:
        return []
    pid = canon(parent_hid)
    tmp = df.copy()
    tmp["__pid"] = tmp[parent_col].astype(str).map(canon)
    tmp = tmp[tmp["__pid"] == pid].drop(columns=["__pid"])
    out = [_row_min(r.to_dict()) for _, r in tmp.iterrows()]
    out.sort(key=lambda x: canon(x.get("human_id", "")))
    return out


def _issues_for_id(issues_df: pd.DataFrame, human_id: str) -> List[Dict[str, str]]:
    """Issues for a record (match by human_id or parent_ref)."""
    if issues_df is None or issues_df.empty:
        return []
    hid = canon(human_id)
    df = issues_df.copy()
    for c in [
        "human_id",
        "parent_ref",
        "severity",
        "level",
        "issue_type",
        "message",
        "suggested_fix",
    ]:
        if c not in df.columns:
            df[c] = ""
    df["__hid"] = df["human_id"].astype(str).map(canon)
    df["__pid"] = df["parent_ref"].astype(str).map(canon)
    hit = df[(df["__hid"] == hid) | (df["__pid"] == hid)].drop(columns=["__hid", "__pid"])
    if hit.empty:
        return []
    sev_order = {"error": 0, "warning": 1, "info": 2}
    hit["__s"] = hit["severity"].astype(str).map(lambda x: sev_order.get(x, 9))
    hit = hit.sort_values(by=["__s", "issue_type"], ascending=[True, True]).drop(columns=["__s"])
    rows = []
    for _, r in hit.iterrows():
        rows.append(
            {
                "severity": _safe_str(r.get("severity", "")),
                "issue_type": _safe_str(r.get("issue_type", "")),
                "message": _safe_str(r.get("message", "")),
                "suggested_fix": _safe_str(r.get("suggested_fix", "")),
            }
        )
    return rows


def _try_render_mermaid_png(mermaid_code: str, out_png: Path) -> Tuple[bool, str]:
    """Try to render Mermaid to PNG using mermaid-cli (mmdc).

    Returns (ok, error).
    """
    if not mermaid_code.strip():
        return False, "mermaid vacío"

    mmdc = os.getenv("SAR_MMDC_PATH", "mmdc")
    if not shutil.which(mmdc) and not Path(mmdc).exists():
        return False, "mmdc no disponible (instala mermaid-cli o define SAR_MMDC_PATH)"

    out_png.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        in_mmd = td_path / "diagram.mmd"
        in_mmd.write_text(mermaid_code, encoding="utf-8")
        cfg = os.getenv("SAR_MMDC_CONFIG", "")
        args = [mmdc, "-i", str(in_mmd), "-o", str(out_png)]
        if cfg:
            args.extend(["-c", cfg])
        try:
            subprocess.run(args, check=True, capture_output=True)
            return True, ""
        except Exception as e:
            return False, f"falló mmdc: {type(e).__name__}"


def generate_c4_chain_report_docx(
    *,
    registry_path: str,
    run_human_id: str,
    issues_df: pd.DataFrame,
    template_docx_path: str,
    out_dir: str,
    max_nodes: int = 200,
) -> Path:
    """Generate a Word report for a C4 record (RUN-xxx).

    The report uses only required fields for records (human_id, name, status),
    includes the same Mermaid diagram as the UI, and a final section with
    issues for the main chain grouped by level.
    """
    # Optional dependency: keep Word export available when docxtpl is installed,
    # but don't prevent the rest of the app (HTML export, UI, etc.) from working.
    try:
        from docxtpl import DocxTemplate, InlineImage  # type: ignore
        from docx.shared import Mm  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Dependencia faltante para exportar a Word: 'docxtpl'. "
            "Instálala (pip install docxtpl) o usa el Informe HTML."
        ) from e
    rp = str(Path(registry_path).resolve())
    run_id = canon(run_human_id)
    if not run_id.startswith("RUN-"):
        raise ValueError("Este informe solo se genera desde un C4 (RUN-xxxx)")

    # Load sheets (raw)
    c1 = read_sheet(rp, "C1_Proyectos")
    c2 = read_sheet(rp, "C2_Aplicaciones")
    c3 = read_sheet(rp, "C3_Componentes")
    c4 = read_sheet(rp, "C4_Runtime")

    r4 = _get_row(c4, run_id)
    if not r4:
        raise ValueError(f"No se encontró {run_id} en C4_Runtime")
    c3_id = canon(_safe_str(r4.get("c3_human_id", "")))
    r3 = _get_row(c3, c3_id) if c3_id else None
    if not r3:
        raise ValueError(f"Cadena rota: C3 '{c3_id}' no encontrado")
    c2_id = canon(_safe_str(r3.get("c2_human_id", "")))
    r2 = _get_row(c2, c2_id) if c2_id else None
    if not r2:
        raise ValueError(f"Cadena rota: C2 '{c2_id}' no encontrado")
    c1_id = canon(_safe_str(r2.get("c1_human_id", "")))
    r1 = _get_row(c1, c1_id) if c1_id else None
    if not r1:
        raise ValueError(f"Cadena rota: C1 '{c1_id}' no encontrado")

    # Lists (context)
    components = _list_children_min(c3, "c2_human_id", c2_id)
    runtimes = _list_children_min(c4, "c3_human_id", c3_id)

    # Diagram (same as UI)
    mermaid_code, diagram_meta = build_record_diagram(rp, run_id, max_nodes=max_nodes)

    # Try to render PNG
    out_dir_p = Path(out_dir).resolve()
    out_dir_p.mkdir(parents=True, exist_ok=True)
    png_path = out_dir_p / f"{run_id}__diagram.png"
    png_ok, png_err = _try_render_mermaid_png(mermaid_code, png_path)

    # Issues by level (chain only)
    issues_by_level = {
        "C1": _issues_for_id(issues_df, c1_id),
        "C2": _issues_for_id(issues_df, c2_id),
        "C3": _issues_for_id(issues_df, c3_id),
        "C4": _issues_for_id(issues_df, run_id),
    }

    # Registry meta for footer/header
    meta = read_meta_dict(rp) or {}

    # Render DOCX
    tpl_path = Path(template_docx_path).resolve()
    if not tpl_path.exists():
        raise FileNotFoundError(f"No existe la plantilla: {tpl_path}")

    doc = DocxTemplate(str(tpl_path))
    context: Dict[str, Any] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "registry": {
            "path": rp,
            "schema_version": meta.get("schema_version", ""),
            "template_version": meta.get("template_version", ""),
        },
        "c1": _row_min(r1),
        "c2": _row_min(r2),
        "c3": _row_min(r3),
        "c4": _row_min(r4),
        "components": components,
        "runtimes": runtimes,
        "issues": issues_by_level,
        "diagram": {
            "mermaid": mermaid_code,
            "truncated": bool(diagram_meta.get("truncated")),
            "node_count": int(diagram_meta.get("node_count", 0) or 0),
            "max_nodes": int(diagram_meta.get("max_nodes", max_nodes) or max_nodes),
            "png_ok": bool(png_ok),
            "png_error": png_err,
        },
    }

    if png_ok and png_path.exists():
        context["diagram_image"] = InlineImage(doc, str(png_path), width=Mm(170))
    else:
        context["diagram_image"] = ""  # placeholder for templates

    doc.render(context)

    out_docx = out_dir_p / f"{run_id}__informe.docx"
    doc.save(str(out_docx))
    return out_docx


def generate_c4_chain_report_html(
    *,
    registry_path: str,
    run_human_id: str,
    issues_df: pd.DataFrame,
    template_html_path: str,
    out_dir: str,
    max_nodes: int = 200,
) -> Path:
    """Generate an HTML report for a C4 record (RUN-xxx).

    The HTML report mirrors the Word report structure and uses the same derived
    context (chain, siblings, Mermaid diagram, issues by level).

    Notes:
      - If Mermaid CLI (mmdc) is available, the diagram is embedded as a base64 PNG.
      - Otherwise, Mermaid code is embedded and rendered client-side via Mermaid JS.
    """
    rp = str(Path(registry_path).resolve())
    run_id = canon(run_human_id)
    if not run_id.startswith("RUN-"):
        raise ValueError("Este informe solo se genera desde un C4 (RUN-xxxx)")

    # Load sheets (raw)
    c1 = read_sheet(rp, "C1_Proyectos")
    c2 = read_sheet(rp, "C2_Aplicaciones")
    c3 = read_sheet(rp, "C3_Componentes")
    c4 = read_sheet(rp, "C4_Runtime")

    r4 = _get_row(c4, run_id)
    if not r4:
        raise ValueError(f"No se encontró {run_id} en C4_Runtime")
    c3_id = canon(_safe_str(r4.get("c3_human_id", "")))
    r3 = _get_row(c3, c3_id) if c3_id else None
    if not r3:
        raise ValueError(f"Cadena rota: C3 '{c3_id}' no encontrado")
    c2_id = canon(_safe_str(r3.get("c2_human_id", "")))
    r2 = _get_row(c2, c2_id) if c2_id else None
    if not r2:
        raise ValueError(f"Cadena rota: C2 '{c2_id}' no encontrado")
    c1_id = canon(_safe_str(r2.get("c1_human_id", "")))
    r1 = _get_row(c1, c1_id) if c1_id else None
    if not r1:
        raise ValueError(f"Cadena rota: C1 '{c1_id}' no encontrado")

    # Lists (context)
    components = _list_children_min(c3, "c2_human_id", c2_id)
    runtimes = _list_children_min(c4, "c3_human_id", c3_id)

    # Diagram (same as UI)
    mermaid_code, diagram_meta = build_record_diagram(rp, run_id, max_nodes=max_nodes)

    # Try to render PNG
    out_dir_p = Path(out_dir).resolve()
    out_dir_p.mkdir(parents=True, exist_ok=True)
    png_path = out_dir_p / f"{run_id}__diagram.png"
    png_ok, png_err = _try_render_mermaid_png(mermaid_code, png_path)

    diagram_png_data_uri = ""
    if png_ok and png_path.exists():
        b64 = base64.b64encode(png_path.read_bytes()).decode("ascii")
        diagram_png_data_uri = f"data:image/png;base64,{b64}"

    # Issues by level (chain only)
    issues_by_level = {
        "C1": _issues_for_id(issues_df, c1_id),
        "C2": _issues_for_id(issues_df, c2_id),
        "C3": _issues_for_id(issues_df, c3_id),
        "C4": _issues_for_id(issues_df, run_id),
    }

    # Registry meta
    meta = read_meta_dict(rp) or {}

    tpl_path = Path(template_html_path).resolve()
    if not tpl_path.exists():
        raise FileNotFoundError(f"No existe la plantilla: {tpl_path}")

    env = Environment(
        loader=FileSystemLoader(str(tpl_path.parent)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template(tpl_path.name)

    context: Dict[str, Any] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "registry": {
            "path": rp,
            "schema_version": meta.get("schema_version", ""),
            "template_version": meta.get("template_version", ""),
        },
        "c1": _row_min(r1),
        "c2": _row_min(r2),
        "c3": _row_min(r3),
        "c4": _row_min(r4),
        "components": components,
        "runtimes": runtimes,
        "issues": issues_by_level,
        "diagram": {
            "mermaid": mermaid_code,
            "truncated": bool(diagram_meta.get("truncated")),
            "node_count": int(diagram_meta.get("node_count", 0) or 0),
            "max_nodes": int(diagram_meta.get("max_nodes", max_nodes) or max_nodes),
            "png_ok": bool(png_ok),
            "png_error": png_err,
            "png_data_uri": diagram_png_data_uri,
        },
    }

    html = tpl.render(**context)

    out_html = out_dir_p / f"{run_id}__informe.html"
    out_html.write_text(html, encoding="utf-8")
    return out_html
