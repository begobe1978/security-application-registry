# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import io
from typing import Iterable, Optional

import pandas as pd
from fastapi.responses import StreamingResponse

from sar.core.schema import normalize_header


def canon(s: str) -> str:
    """Canonicalise IDs/keys for comparisons (trim + upper)."""
    return (s or "").strip().upper()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise dataframe column names using the registry schema aliases."""
    df = df.copy()
    df.columns = pd.Index(df.columns).map(normalize_header)
    return df


def safe_count(df: pd.DataFrame, col: str, value: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 0
    return int((df[col].astype(str) == value).sum())


def first_existing_col(df: pd.DataFrame, *candidates: str) -> str:
    """Return first existing column name from candidates, else ''."""
    for c in candidates:
        if c in df.columns:
            return c
    return ""


def level_list_columns(
    df: pd.DataFrame,
    *,
    id_col: str = "id",
    parent_col: Optional[str] = None,
    limit: int = 8,
    show_all: bool = False,
) -> list[str]:
    """Build ordered columns for level list views.

    The limited view always keeps the structural columns first (`id` and the
    parent reference column for the level, when present). The remaining visible
    columns follow the workbook order exactly, without hardcoded business
    semantics.
    """
    if df is None:
        return []

    all_cols = [c for c in df.columns.tolist() if str(c) != "__orphan"]
    if show_all:
        return all_cols

    base_cols = [c for c in [id_col, parent_col] if c and c in all_cols]
    base_cols = list(dict.fromkeys(base_cols))

    remaining_cols = [c for c in all_cols if c not in base_cols]
    limit = max(0, int(limit or 0))
    if len(base_cols) >= limit:
        return base_cols[:limit]
    return base_cols + remaining_cols[: max(0, limit - len(base_cols))]


def df_to_csv_stream(df: pd.DataFrame) -> StreamingResponse:
    """Stream a dataframe as CSV without writing to disk."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv")
