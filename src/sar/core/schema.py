# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Dict, Optional

ID_COL = "id"
NAME_COL = "name"
STATUS_COL = "status"

LEVEL_CODES = ("C1", "C2", "C3", "C4")

PARENT_COL_BY_LEVEL: Dict[str, Optional[str]] = {
    "C1": None,
    "C2": "c1_id",
    "C3": "c2_id",
    "C4": "c3_id",
}

LEGACY_ID_COL = "human_id"
LEGACY_PARENT_COLS: Dict[str, Optional[str]] = {
    "C1": None,
    "C2": "c1_human_id",
    "C3": "c2_human_id",
    "C4": "c3_human_id",
}

PREFIX_BY_LEVEL: Dict[str, str] = {
    "C1": "C1-",
    "C2": "C2-",
    "C3": "C3-",
    "C4": "C4-",
}

LEGACY_PREFIX_BY_LEVEL: Dict[str, str] = {
    "C1": "PRJ-",
    "C2": "APP-",
    "C3": "CMP-",
    "C4": "RUN-",
}

HEADER_ALIASES = {
    LEGACY_ID_COL: ID_COL,
    "human-id": ID_COL,
    "c1-human-id": "c1_id",
    "c2-human-id": "c2_id",
    "c3-human-id": "c3_id",
    LEGACY_PARENT_COLS["C2"]: PARENT_COL_BY_LEVEL["C2"],
    LEGACY_PARENT_COLS["C3"]: PARENT_COL_BY_LEVEL["C3"],
    LEGACY_PARENT_COLS["C4"]: PARENT_COL_BY_LEVEL["C4"],
}

REQUIRED_FIELDS_BY_LEVEL: Dict[str, list[str]] = {
    "C1": [ID_COL, STATUS_COL, NAME_COL],
    "C2": [PARENT_COL_BY_LEVEL["C2"], ID_COL, STATUS_COL, NAME_COL],
    "C3": [PARENT_COL_BY_LEVEL["C3"], ID_COL, STATUS_COL, NAME_COL],
    "C4": [PARENT_COL_BY_LEVEL["C4"], ID_COL, STATUS_COL, NAME_COL],
}

DERIVED_READ_ONLY_BY_LEVEL: Dict[str, set[str]] = {
    "C1": {"vulnerabilities_detected"},
    "C2": {"vulnerabilities_detected"},
    "C3": set(),
    "C4": set(),
}


def normalize_header(name: object) -> str:
    key = str(name or "").strip().replace(" ", "_").replace("-", "_").lower()
    return HEADER_ALIASES.get(key, key)


def parent_col_for_level(level: str) -> Optional[str]:
    return PARENT_COL_BY_LEVEL.get(str(level or "").strip().upper())


def required_fields_for_level(level: str) -> list[str]:
    return list(REQUIRED_FIELDS_BY_LEVEL.get(str(level or "").strip().upper(), []))


def read_only_fields_for_level(level: str) -> set[str]:
    return set(DERIVED_READ_ONLY_BY_LEVEL.get(str(level or "").strip().upper(), set()))


def is_required_field(level: str, field: str) -> bool:
    return normalize_header(field) in set(required_fields_for_level(level))


def is_editable_field(level: str, field: str) -> bool:
    norm = normalize_header(field)
    if norm == ID_COL:
        return False
    return norm not in read_only_fields_for_level(level)
