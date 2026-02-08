# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import io
from typing import Iterable

import pandas as pd
from fastapi.responses import StreamingResponse


def canon(s: str) -> str:
    """Canonicalise IDs/keys for comparisons (trim + upper)."""
    return (s or "").strip().upper()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise dataframe column names to a stable snake_case-like lower format."""
    df = df.copy()
    df.columns = (
        pd.Index(df.columns)
        .map(lambda c: str(c).strip())
        .map(lambda c: c.replace(" ", "_"))
        .map(lambda c: c.replace("-", "_"))
        .map(lambda c: c.lower())
    )
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


def df_to_csv_stream(df: pd.DataFrame) -> StreamingResponse:
    """Stream a dataframe as CSV without writing to disk."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv")
