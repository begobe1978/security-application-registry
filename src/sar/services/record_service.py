# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from sar.core.mapping import CHILD_SHEETS, detect_level
from sar.core.utils import canon
from sar.infra.registry_repo import read_sheet


def get_row_by_human_id(df: pd.DataFrame, human_id: str) -> Optional[Dict[str, Any]]:
    """Return a record dict (first match) for human_id, or None."""
    if df is None or df.empty:
        return None
    if "human_id" not in df.columns:
        return None
    hid = canon(human_id)
    tmp = df.copy()
    tmp["__hid"] = tmp["human_id"].astype(str).map(canon)
    hit = tmp[tmp["__hid"] == hid]
    if hit.empty:
        return None
    return hit.iloc[0].drop(labels=["__hid"]).to_dict()


def list_children(path: str, parent_level: str, parent_hid: str) -> List[Dict[str, Any]]:
    """List immediate children of a record."""
    out: List[Dict[str, Any]] = []
    for child_level, sheet, parent_col in CHILD_SHEETS.get(parent_level, []):
        df = read_sheet(path, sheet)
        if df.empty or parent_col not in df.columns or "human_id" not in df.columns:
            continue
        pid = canon(parent_hid)
        tmp = df.copy()
        tmp["__pid"] = tmp[parent_col].astype(str).map(canon)
        tmp = tmp[tmp["__pid"] == pid]

        for _, r in tmp.iterrows():
            out.append(
                {
                    "level": child_level,
                    "sheet": sheet,
                    "human_id": str(r.get("human_id", "")),
                    "name": str(r.get("name", "")),
                    "status": str(r.get("status", "")),
                }
            )
    out.sort(key=lambda x: canon(x.get("human_id", "")))
    return out


def list_descendants_counts(path: str, level: str, human_id: str) -> Dict[str, int]:
    """Count descendants by level (simple summary).

    C1: counts apps/components/runtimes
    C2: counts components/runtimes
    C3: counts runtimes
    """
    hid = canon(human_id)
    counts = {"C2": 0, "C3": 0, "C4": 0}

    if level == "C4":
        return counts

    if level == "C3":
        c4 = read_sheet(path, "C4_Runtime")
        if not c4.empty and "c3_human_id" in c4.columns:
            counts["C4"] = int((c4["c3_human_id"].astype(str).map(canon) == hid).sum())
        return counts

    if level == "C2":
        c3 = read_sheet(path, "C3_Componentes")
        if not c3.empty and "c2_human_id" in c3.columns:
            comps = c3[c3["c2_human_id"].astype(str).map(canon) == hid]
            counts["C3"] = int(len(comps))
            comp_ids = set(comps["human_id"].astype(str).map(canon).tolist())
        else:
            comp_ids = set()

        c4 = read_sheet(path, "C4_Runtime")
        if not c4.empty and "c3_human_id" in c4.columns and comp_ids:
            counts["C4"] = int(c4["c3_human_id"].astype(str).map(canon).isin(comp_ids).sum())
        return counts

    if level == "C1":
        c2 = read_sheet(path, "C2_Aplicaciones")
        if not c2.empty and "c1_human_id" in c2.columns:
            apps = c2[c2["c1_human_id"].astype(str).map(canon) == hid]
            counts["C2"] = int(len(apps))
            app_ids = set(apps["human_id"].astype(str).map(canon).tolist())
        else:
            app_ids = set()

        c3 = read_sheet(path, "C3_Componentes")
        if not c3.empty and "c2_human_id" in c3.columns and app_ids:
            comps = c3[c3["c2_human_id"].astype(str).map(canon).isin(app_ids)]
            counts["C3"] = int(len(comps))
            comp_ids = set(comps["human_id"].astype(str).map(canon).tolist())
        else:
            comp_ids = set()

        c4 = read_sheet(path, "C4_Runtime")
        if not c4.empty and "c3_human_id" in c4.columns and comp_ids:
            counts["C4"] = int(c4["c3_human_id"].astype(str).map(canon).isin(comp_ids).sum())

        return counts

    return counts


def issues_for(issues_df: pd.DataFrame, human_id: str) -> List[Dict[str, Any]]:
    """Return issues matching a record (by human_id or parent_ref)."""
    df = issues_df
    if df is None or df.empty:
        return []
    tmp = df.copy()
    for c in ["human_id", "parent_ref", "severity", "level", "issue_type", "message", "suggested_fix"]:
        if c not in tmp.columns:
            tmp[c] = ""
    hid = canon(human_id)
    tmp["__hid"] = tmp["human_id"].astype(str).map(canon)
    tmp["__pid"] = tmp["parent_ref"].astype(str).map(canon)
    hit = tmp[(tmp["__hid"] == hid) | (tmp["__pid"] == hid)]
    if hit.empty:
        return []
    hit = hit.drop(columns=["__hid", "__pid"])
    sev_order = {"error": 0, "warning": 1, "info": 2}
    hit["__s"] = hit["severity"].astype(str).map(lambda x: sev_order.get(x, 9))
    hit = hit.sort_values(by=["__s", "level", "issue_type"], ascending=[True, True, True]).drop(columns=["__s"])
    return hit.to_dict(orient="records")


def detect_level_meta(human_id: str) -> Optional[Dict[str, Any]]:
    """Convenience wrapper for mapping.detect_level using canonicalisation."""
    return detect_level(human_id, canon_fn=canon)
