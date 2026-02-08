# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
import pandas as pd

REQUIRED_SHEETS = ["META", "LOOKUPS", "C1_Proyectos", "C2_Aplicaciones", "C3_Componentes", "C4_Runtime"]

# --- Issues model ---
@dataclass
class Issue:
    issue_id: str
    severity: str          # error/warning/info
    level: str             # C1/C2/C3/C4
    human_id: str
    parent_ref: str
    issue_type: str        # orphan/missing_required/invalid_lookup
    message: str
    suggested_fix: str


def _split_multivalue(value: str) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def load_registry_xlsx(path: str) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    missing = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    if missing:
        raise ValueError(f"Faltan pestañas requeridas: {missing}")
    data = {name: pd.read_excel(xls, sheet_name=name, dtype=str).fillna("") for name in REQUIRED_SHEETS}
    return data


def parse_lookups(df_lookups: pd.DataFrame) -> Dict[str, set]:
    # expected columns: lookup_name | lookup_value | level | description
    req_cols = {"lookup_name", "lookup_value"}
    if not req_cols.issubset(set(df_lookups.columns)):
        return {}
    lookups: Dict[str, set] = {}
    for _, r in df_lookups.iterrows():
        ln = str(r.get("lookup_name", "")).strip()
        lv = str(r.get("lookup_value", "")).strip()
        if ln and lv:
            lookups.setdefault(ln, set()).add(lv)
    return lookups


def validate_required(df: pd.DataFrame, level: str, required_cols: List[str], issues: List[Issue]):
    for col in required_cols:
        if col not in df.columns:
            issues.append(Issue(
                issue_id=f"{level}-MISSINGCOL-{col}",
                severity="error",
                level=level,
                human_id="",
                parent_ref="",
                issue_type="missing_required",
                message=f"Falta columna requerida '{col}' en {level}",
                suggested_fix=f"Añadir columna '{col}' en la pestaña correspondiente"
            ))
            continue

    if "human_id" in df.columns:
        for idx, r in df.iterrows():
            hid = str(r.get("human_id", "")).strip()
            for col in required_cols:
                if col in df.columns:
                    v = str(r.get(col, "")).strip()
                    if not v:
                        issues.append(Issue(
                            issue_id=f"{level}-REQ-{hid or 'ROW'+str(idx)}-{col}",
                            severity="error",
                            level=level,
                            human_id=hid,
                            parent_ref="",
                            issue_type="missing_required",
                            message=f"Campo requerido vacío: {col}",
                            suggested_fix=f"Rellenar '{col}'"
                        ))


def validate_unique_human_id(df: pd.DataFrame, level: str, issues: List[Issue]):
    if "human_id" not in df.columns:
        return
    s = df["human_id"].astype(str).str.strip()
    dupes = s[s.duplicated(keep=False) & (s != "")]
    for hid in sorted(set(dupes.tolist())):
        issues.append(Issue(
            issue_id=f"{level}-DUP-{hid}",
            severity="error",
            level=level,
            human_id=hid,
            parent_ref="",
            issue_type="missing_required",
            message="human_id duplicado dentro del nivel",
            suggested_fix="Hacer human_id único en esa pestaña"
        ))


def validate_lookup_single(df: pd.DataFrame, level: str, field: str, lookup_name: str, lookups: Dict[str, set], issues: List[Issue]):
    if field not in df.columns or lookup_name not in lookups or "human_id" not in df.columns:
        return
    allowed = lookups[lookup_name]
    for _, r in df.iterrows():
        hid = str(r.get("human_id", "")).strip()
        v = str(r.get(field, "")).strip()
        if v and v not in allowed:
            issues.append(Issue(
                issue_id=f"{level}-LOOKUP-{hid}-{field}",
                severity="error",
                level=level,
                human_id=hid,
                parent_ref="",
                issue_type="invalid_lookup",
                message=f"Valor inválido en {field}: '{v}' (lookup {lookup_name})",
                suggested_fix=f"Usar uno de: {', '.join(sorted(allowed))}"
            ))


def validate_lookup_multi(df: pd.DataFrame, level: str, field: str, lookup_name: str, lookups: Dict[str, set], issues: List[Issue]):
    if field not in df.columns or lookup_name not in lookups or "human_id" not in df.columns:
        return
    allowed = lookups[lookup_name]
    for _, r in df.iterrows():
        hid = str(r.get("human_id", "")).strip()
        values = _split_multivalue(r.get(field, ""))
        for v in values:
            if v not in allowed:
                issues.append(Issue(
                    issue_id=f"{level}-LOOKUP-{hid}-{field}-{v}",
                    severity="error",
                    level=level,
                    human_id=hid,
                    parent_ref="",
                    issue_type="invalid_lookup",
                    message=f"Valor inválido en {field}: '{v}' (lookup {lookup_name})",
                    suggested_fix=f"Usar uno de: {', '.join(sorted(allowed))}"
                ))


def validate_relations(
    c1: pd.DataFrame, c2: pd.DataFrame, c3: pd.DataFrame, c4: pd.DataFrame,
    issues: List[Issue]
):
    c1_ids = set(c1.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())
    c2_ids = set(c2.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())
    c3_ids = set(c3.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())

    # C2 -> C1
    if "c1_human_id" in c2.columns and "human_id" in c2.columns:
        for _, r in c2.iterrows():
            hid = str(r.get("human_id", "")).strip()
            parent = str(r.get("c1_human_id", "")).strip()
            if not parent or parent not in c1_ids:
                issues.append(Issue(
                    issue_id=f"C2-ORPHAN-{hid}",
                    severity="error",
                    level="C2",
                    human_id=hid,
                    parent_ref=parent,
                    issue_type="orphan",
                    message="Aplicación sin proyecto (C1) asociado o C1 inexistente",
                    suggested_fix="Rellenar c1_human_id con un PRJ existente"
                ))

    # C3 -> C2
    if "c2_human_id" in c3.columns and "human_id" in c3.columns:
        for _, r in c3.iterrows():
            hid = str(r.get("human_id", "")).strip()
            parent = str(r.get("c2_human_id", "")).strip()
            if not parent or parent not in c2_ids:
                issues.append(Issue(
                    issue_id=f"C3-ORPHAN-{hid}",
                    severity="error",
                    level="C3",
                    human_id=hid,
                    parent_ref=parent,
                    issue_type="orphan",
                    message="Componente sin aplicación (C2) asociada o C2 inexistente",
                    suggested_fix="Rellenar c2_human_id con un APP existente"
                ))

    # C4 -> C3
    if "c3_human_id" in c4.columns and "human_id" in c4.columns:
        for _, r in c4.iterrows():
            hid = str(r.get("human_id", "")).strip()
            parent = str(r.get("c3_human_id", "")).strip()
            if not parent or parent not in c3_ids:
                issues.append(Issue(
                    issue_id=f"C4-ORPHAN-{hid}",
                    severity="error",
                    level="C4",
                    human_id=hid,
                    parent_ref=parent,
                    issue_type="orphan",
                    message="Runtime sin componente (C3) asociado o C3 inexistente",
                    suggested_fix="Rellenar c3_human_id con un CMP existente"
                ))


def generate_view_full(c1: pd.DataFrame, c2: pd.DataFrame, c3: pd.DataFrame, c4: pd.DataFrame) -> pd.DataFrame:
    """
    VIEW_Full (compact):
      - Incluye TODAS las columnas de C1..C4 de forma dinámica
      - Prefija por nivel: c1__ / c2__ / c3__ / c4__
      - Solo excluye los 3 campos redundantes de jerarquía:
        c2__c1_human_id, c3__c2_human_id, c4__c3_human_id
      - Mantiene cualquier columna existente (incl. placeholders de riesgo)
    """

    c1n, c2n, c3n, c4n = c1.copy(), c2.copy(), c3.copy(), c4.copy()

    # Normaliza claves (strip)
    for df in (c1n, c2n, c3n, c4n):
        if "human_id" in df.columns:
            df["human_id"] = df["human_id"].astype(str).str.strip()

    if "c1_human_id" in c2n.columns:
        c2n["c1_human_id"] = c2n["c1_human_id"].astype(str).str.strip()
    if "c2_human_id" in c3n.columns:
        c3n["c2_human_id"] = c3n["c2_human_id"].astype(str).str.strip()
    if "c3_human_id" in c4n.columns:
        c4n["c3_human_id"] = c4n["c3_human_id"].astype(str).str.strip()

    # Prefijos dinámicos
    c1p = c1n.add_prefix("c1__")
    c2p = c2n.add_prefix("c2__")
    c3p = c3n.add_prefix("c3__")
    c4p = c4n.add_prefix("c4__")

    # Merge chain desde runtime hacia arriba (solo cadenas completas)
    j = c4p.merge(
        c3p,
        left_on="c4__c3_human_id",
        right_on="c3__human_id",
        how="inner",
    )
    j = j.merge(
        c2p,
        left_on="c3__c2_human_id",
        right_on="c2__human_id",
        how="inner",
    )
    j = j.merge(
        c1p,
        left_on="c2__c1_human_id",
        right_on="c1__human_id",
        how="inner",
    )

    # Compact: elimina solo los 3 campos redundantes de jerarquía
    drop_cols = [c for c in ["c2__c1_human_id", "c3__c2_human_id", "c4__c3_human_id"] if c in j.columns]
    if drop_cols:
        j = j.drop(columns=drop_cols)

    # Orden estable
    sort_cols = [c for c in ["c1__human_id", "c2__human_id", "c3__human_id", "c4__human_id"] if c in j.columns]
    if sort_cols:
        j = j.sort_values(sort_cols, kind="mergesort")

    return j.fillna("")


def issues_to_df(issues: List[Issue]) -> pd.DataFrame:
    return pd.DataFrame([i.__dict__ for i in issues]).fillna("")


def compute(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data = load_registry_xlsx(path)
    c1, c2, c3, c4 = data["C1_Proyectos"], data["C2_Aplicaciones"], data["C3_Componentes"], data["C4_Runtime"]
    lookups = parse_lookups(data["LOOKUPS"])

    issues: List[Issue] = []

    # Required columns (MVP minimums; puedes ajustar)
    validate_required(c1, "C1", ["human_id", "status", "name"], issues)
    validate_required(c2, "C2", ["c1_human_id", "human_id", "status", "name"], issues)
    validate_required(c3, "C3", ["c2_human_id", "human_id", "status", "name"], issues)
    validate_required(c4, "C4", ["c3_human_id", "human_id", "status", "name"], issues)

    # Unique IDs
    validate_unique_human_id(c1, "C1", issues)
    validate_unique_human_id(c2, "C2", issues)
    validate_unique_human_id(c3, "C3", issues)
    validate_unique_human_id(c4, "C4", issues)

    # Relations
    validate_relations(c1, c2, c3, c4, issues)

    # Lookups (mínimos)
    validate_lookup_multi(c1, "C1", "environments", "environment", lookups, issues)
    validate_lookup_single(c1, "C1", "business_criticality", "criticality", lookups, issues)
    validate_lookup_single(c3, "C3", "component_type", "component_type", lookups, issues)
    validate_lookup_single(c3, "C3", "exposure", "exposure", lookups, issues)
    validate_lookup_single(c4, "C4", "runtime_type", "runtime_type", lookups, issues)

    view_full = generate_view_full(c1, c2, c3, c4)
    issues_df = issues_to_df(issues)
    return view_full, issues_df
