# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Any, Dict

from sar.infra.registry_repo import backup_registry, update_fields_existing, add_new_field_column, append_row_existing_columns, generate_next_human_id
from sar.services.compute_service import regenerate_views
from sar.services.record_service import detect_level_meta
from sar.core.mapping import meta_for_level


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