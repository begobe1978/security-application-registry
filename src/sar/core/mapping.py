# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Mapping between registry ID prefixes and Excel sheets.

The structural hierarchy is fixed (C1..C4) while semantics remain workbook-driven.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sar.core.schema import ID_COL, PARENT_COL_BY_LEVEL, PREFIX_BY_LEVEL, LEGACY_PREFIX_BY_LEVEL


LEVELS: Dict[str, Dict[str, Any]] = {
    PREFIX_BY_LEVEL['C1']: {'level': 'C1', 'sheet': 'C1', 'id_col': ID_COL, 'parent_col': None},
    PREFIX_BY_LEVEL['C2']: {'level': 'C2', 'sheet': 'C2', 'id_col': ID_COL, 'parent_col': PARENT_COL_BY_LEVEL['C2']},
    PREFIX_BY_LEVEL['C3']: {'level': 'C3', 'sheet': 'C3', 'id_col': ID_COL, 'parent_col': PARENT_COL_BY_LEVEL['C3']},
    PREFIX_BY_LEVEL['C4']: {'level': 'C4', 'sheet': 'C4', 'id_col': ID_COL, 'parent_col': PARENT_COL_BY_LEVEL['C4']},
}

# Accept legacy prefixes for backward-compatible detection.
LEGACY_LEVELS: Dict[str, Dict[str, Any]] = {
    LEGACY_PREFIX_BY_LEVEL['C1']: LEVELS[PREFIX_BY_LEVEL['C1']],
    LEGACY_PREFIX_BY_LEVEL['C2']: LEVELS[PREFIX_BY_LEVEL['C2']],
    LEGACY_PREFIX_BY_LEVEL['C3']: LEVELS[PREFIX_BY_LEVEL['C3']],
    LEGACY_PREFIX_BY_LEVEL['C4']: LEVELS[PREFIX_BY_LEVEL['C4']],
}

CHILD_SHEETS = {
    'C1': [('C2', 'C2', PARENT_COL_BY_LEVEL['C2'])],
    'C2': [('C3', 'C3', PARENT_COL_BY_LEVEL['C3'])],
    'C3': [('C4', 'C4', PARENT_COL_BY_LEVEL['C4'])],
    'C4': [],
}


def detect_level(record_id: str, *, canon_fn) -> Optional[Dict[str, Any]]:
    """Return level metadata for a record identifier based on its prefix."""
    hid = canon_fn(record_id)
    for prefix, meta in {**LEVELS, **LEGACY_LEVELS}.items():
        if hid.startswith(prefix):
            return meta
    return None


LEVEL_BY_CODE: Dict[str, Dict[str, Any]] = {
    meta['level']: {'prefix': prefix, **meta} for prefix, meta in LEVELS.items()
}


def meta_for_level(level: str) -> Optional[Dict[str, Any]]:
    return LEVEL_BY_CODE.get(str(level or '').strip().upper())


DEFAULT_LEVEL_LABELS: Dict[str, str] = {
    'C1': 'Proyecto',
    'C2': 'Aplicación',
    'C3': 'Componente',
    'C4': 'Runtime',
}


def level_labels_from_meta(meta: Dict[str, str] | None) -> Dict[str, str]:
    meta = meta or {}
    norm = {str(k).strip().lower(): str(v or '').strip() for k, v in meta.items()}
    out = dict(DEFAULT_LEVEL_LABELS)
    for code in ['C1', 'C2', 'C3', 'C4']:
        v = norm.get(f'{code.lower()}_label', '').strip()
        if v:
            out[code] = v
    return out
