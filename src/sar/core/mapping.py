# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Mapping between human_id prefixes and registry Excel sheets.

Centralising this avoids duplicating knowledge across routes/services and makes
future CRUD operations level-agnostic.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


# --- Level mapping (prefix -> sheet + fields) ---
LEVELS: Dict[str, Dict[str, Any]] = {
    "PRJ-": {"level": "C1", "sheet": "C1", "id_col": "human_id", "parent_col": None},
    "APP-": {"level": "C2", "sheet": "C2", "id_col": "human_id", "parent_col": "c1_human_id"},
    "CMP-": {"level": "C3", "sheet": "C3", "id_col": "human_id", "parent_col": "c2_human_id"},
    "RUN-": {"level": "C4", "sheet": "C4", "id_col": "human_id", "parent_col": "c3_human_id"},
}


CHILD_SHEETS = {
    "C1": [("C2", "C2", "c1_human_id")],
    "C2": [("C3", "C3", "c2_human_id")],
    "C3": [("C4", "C4", "c3_human_id")],
    "C4": [],
}


def detect_level(human_id: str, *, canon_fn) -> Optional[Dict[str, Any]]:
    """Return level metadata for a human_id based on its prefix."""
    hid = canon_fn(human_id)
    for prefix, meta in LEVELS.items():
        if hid.startswith(prefix):
            return meta
    return None


# Reverse mapping (level code -> metadata including prefix)
LEVEL_BY_CODE: Dict[str, Dict[str, Any]] = {
    meta["level"]: {"prefix": prefix, **meta} for prefix, meta in LEVELS.items()
}


def meta_for_level(level: str) -> Optional[Dict[str, Any]]:
    """Return metadata for a given level code (e.g. 'C1')."""
    return LEVEL_BY_CODE.get(str(level or "").strip().upper())


# --- Display labels (UI/report) ---
DEFAULT_LEVEL_LABELS: Dict[str, str] = {
    "C1": "Proyecto",
    "C2": "Aplicación",
    "C3": "Componente",
    "C4": "Runtime",
}


def level_labels_from_meta(meta: Dict[str, str] | None) -> Dict[str, str]:
    """Build UI labels for levels from META key/value pairs.

    Expected keys (case-insensitive):
      - C1_label, C2_label, C3_label, C4_label

    Falls back to DEFAULT_LEVEL_LABELS.
    """
    meta = meta or {}
    norm = {str(k).strip().lower(): str(v or "").strip() for k, v in meta.items()}
    out = dict(DEFAULT_LEVEL_LABELS)
    for code in ["C1", "C2", "C3", "C4"]:
        v = norm.get(f"{code.lower()}_label", "").strip()
        if v:
            out[code] = v
    return out
