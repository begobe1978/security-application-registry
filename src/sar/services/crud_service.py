# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Any, Dict

from sar.infra.registry_repo import (
    backup_registry,
    update_fields_existing,
    add_new_field_column,
    append_row_existing_columns,
    generate_next_human_id,
    read_sheet,
    write_meta_kv,
    get_schema_map,
    schema_hash,
)
from sar.services.compute_service import regenerate_views
from sar.services.record_service import detect_level_meta
from sar.core.mapping import meta_for_level
from sar.core.utils import canon
from sar.services.record_service import get_row_by_human_id


PARENT_LEVEL = {"C2": "C1", "C3": "C2", "C4": "C3"}

SCHEMA_SHEETS = [
    "META",
    "LOOKUPS",
    "RULES",
    "C1_Proyectos",
    "C2_Aplicaciones",
    "C3_Componentes",
    "C4_Runtime",
]


def _validate_parent_ref_exists(*, path: str, child_level: str, parent_ref: str) -> None:
    """Ensure parent_ref exists and is of the correct level for the child."""
    cl = str(child_level or "").strip().upper()
    pl = PARENT_LEVEL.get(cl)
    if not pl:
        return

    pref = str(parent_ref or "").strip()
    if not pref:
        raise ValueError("El parent_id no puede estar vacío.")

    # Check prefix/level first (fast feedback)
    pm = detect_level_meta(pref)
    if not pm or pm.get("level") != pl:
        raise ValueError(f"Parent '{pref}' no es válido para {cl} (se espera un {pl}).")

    # Check existence in the correct sheet
    pmeta = meta_for_level(pl)
    if not pmeta:
        raise ValueError(f"No se pudo resolver el nivel parent '{pl}'.")
    pdf = read_sheet(path, pmeta["sheet"])
    if get_row_by_human_id(pdf, pref) is None:
        raise ValueError(f"Parent '{canon(pref)}' no existe en '{pmeta['sheet']}'.")


def update_record_existing_fields(*, path: str, human_id: str, fields: Dict[str, Any]):
    """Update existing fields for a record, backing up and regenerating views.

    This is the first CRUD step: it only allows updating columns that already
    exist in the underlying Excel sheet.
    """
    meta = detect_level_meta(human_id)
    if not meta:
        raise ValueError(f"human_id '{human_id}' no reconocido (prefijo no soportado).")

    # vulnerabilities_detected is derived in C1/C2 and must not be manually editable
    if meta.get("level") in ("C1", "C2") and "vulnerabilities_detected" in fields:
        raise ValueError("'vulnerabilities_detected' no es editable en C1/C2 (se hereda de C3/C4).")

    # If the parent field is being updated, validate it exists (prevents typos).
    parent_col = meta.get("parent_col")
    if parent_col and parent_col in fields:
        _validate_parent_ref_exists(path=path, child_level=meta.get("level", ""), parent_ref=str(fields.get(parent_col, "")))

    # Safety first
    backup_registry(path)
    update_fields_existing(path, meta["sheet"], human_id, fields)

    # Recompute derived views after each write
    return regenerate_views(path)


def add_new_field(*, path: str, human_id: str, field_name: str, value: Any):
    """Add a new column (field) to the corresponding sheet and set its value for this record.

    This is intentionally a separate operation from updates to avoid creating columns by typo.
    - If the field already exists (by normalised header), this raises an error.
    - A backup is created and derived views are regenerated.
    """
    meta = detect_level_meta(human_id)
    if not meta:
        raise ValueError(f"human_id '{human_id}' no reconocido (prefijo no soportado).")

    backup_registry(path)
    add_new_field_column(path, meta["sheet"], human_id, field_name, value)

    # Mark schema dirty on the working registry (template promotion is a separate step)
    try:
        sm = get_schema_map(path, SCHEMA_SHEETS)
        h = schema_hash(sm)
        write_meta_kv(
            path,
            {
                "schema_dirty": "yes",
                "schema_hash": h,
            },
        )
    except Exception:
        # Non-fatal: registry can still work even if META cannot be updated
        pass

    return regenerate_views(path)

def create_record(*, path: str, level: str, fields: Dict[str, Any]) -> tuple[str, Any, Any, Any]:
    """Create a new record for a given level (C1-C4) in the Excel registry.

    - Generates a new human_id for the level.
    - Writes only existing columns (no auto-creation of new fields).
    - Creates a backup and regenerates derived views.

    Returns: (new_human_id, view_full_df, issues_df)
    """
    meta = meta_for_level(level)
    if not meta:
        raise ValueError(f"Nivel '{level}' no reconocido. Usa C1, C2, C3 o C4.")

    # vulnerabilities_detected is only writable in C3/C4
    if meta.get("level") in ("C1", "C2") and "vulnerabilities_detected" in fields:
        raise ValueError("'vulnerabilities_detected' no es editable en C1/C2 (se hereda de C3/C4).")

    sheet = meta["sheet"]
    prefix = meta["prefix"]
    parent_col = meta.get("parent_col")

    # Minimal required fields
    name = str(fields.get("name", "") or "").strip()
    if not name:
        raise ValueError("El campo 'name' es obligatorio.")

    if parent_col:
        parent_ref = str(fields.get(parent_col, "") or "").strip()
        if not parent_ref:
            raise ValueError(f"El campo '{parent_col}' es obligatorio para {meta['level']}.")
        _validate_parent_ref_exists(path=path, child_level=meta.get("level", ""), parent_ref=parent_ref)
    else:
        parent_ref = ""

    # Status default
    status = str(fields.get("status", "") or "").strip() or "draft"

    # Generate new id
    new_hid = generate_next_human_id(path, sheet, prefix)

    row = dict(fields)
    row["human_id"] = new_hid
    row["status"] = status
    row["name"] = name
    if parent_col:
        row[parent_col] = parent_ref

    backup_registry(path)
    append_row_existing_columns(path, sheet, row)

    view_full, issues, views_by_level = regenerate_views(path)
    return new_hid, view_full, issues, views_by_level