# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
import pandas as pd

REQUIRED_SHEETS = [
    "META",
    "LOOKUPS",
    "RULES",
    "C1_Proyectos",
    "C2_Aplicaciones",
    "C3_Componentes",
    "C4_Runtime",
]

VULN_FIELD = "vulnerabilities_detected"
VULN_ALLOWED = {"yes", "no", "unknown"}

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


def _split_multivalue(value: Any) -> List[str]:
    """Split a cell that may contain 1 or multiple values.

    Supports separators: comma, semicolon, pipe.
    Always returns a list of tokens (0..n).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    # normalize separators to comma
    s = s.replace(";", ",").replace("|", ",")
    return [x.strip() for x in s.split(",") if x.strip()]



def _canon_vuln(value: Any) -> str:
    """Normalize vulnerabilities_detected to {yes,no,unknown}."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "unknown"
    s = str(value).strip().lower()
    if not s:
        return "unknown"
    # allow a couple of common variants
    aliases = {
        "y": "yes",
        "true": "yes",
        "1": "yes",
        "n": "no",
        "false": "no",
        "0": "no",
        "unk": "unknown",
        "na": "unknown",
        "n/a": "unknown",
    }
    s = aliases.get(s, s)
    return s if s in VULN_ALLOWED else "__invalid__"


def _ensure_vuln_col(df: pd.DataFrame) -> pd.DataFrame:
    if VULN_FIELD not in df.columns:
        df = df.copy()
        df[VULN_FIELD] = ""
    return df


def _derive_vuln_from_children(values: List[str]) -> str:
    """Inheritance rule: any yes -> yes; else any unknown -> unknown; else no."""
    vals = [v for v in values if v]
    if any(v == "yes" for v in vals):
        return "yes"
    if any(v == "unknown" for v in vals):
        return "unknown"
    # if there are no children, stay unknown (avoids false 'no' with missing inventory)
    if not vals:
        return "unknown"
    return "no"


def load_registry_xlsx(path: str) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    missing = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    if missing:
        raise ValueError(f"Faltan pestañas requeridas: {missing}")
    data: Dict[str, pd.DataFrame] = {}
    for name in REQUIRED_SHEETS:
        df = pd.read_excel(xls, sheet_name=name, dtype=str).fillna("")
        # Normalize column headers to avoid false "missing field" issues caused by
        # trailing spaces / non-breaking spaces / accidental whitespace in Excel.
        df.columns = [str(c).strip() for c in df.columns]
        data[name] = df
    return data


def parse_lookups(df_lookups: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Parse LOOKUPS sheet.

    Expected columns:
      - lookup_name
      - lookup_value
      - level (optional; defaults to 'ALL')

    Returns:
      {lookup_name: {"values": set[str], "levels": set[str]}}
    """
    if df_lookups is None or df_lookups.empty:
        return {}

    req_cols = {"lookup_name", "lookup_value"}
    if not req_cols.issubset(set(df_lookups.columns)):
        return {}

    tmp = df_lookups.fillna("").copy()
    tmp["lookup_name"] = tmp.get("lookup_name", "").astype(str).str.strip()
    tmp["lookup_value"] = tmp.get("lookup_value", "").astype(str).str.strip()
    tmp["level"] = tmp.get("level", "ALL").astype(str).str.upper().str.strip()
    tmp.loc[tmp["level"] == "", "level"] = "ALL"

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in tmp.iterrows():
        name = str(r.get("lookup_name", "")).strip()
        val = str(r.get("lookup_value", "")).strip()
        lvl = str(r.get("level", "ALL")).strip().upper() or "ALL"
        if not name or not val:
            continue
        out.setdefault(name, {"values": set(), "levels": set()})
        out[name]["values"].add(val)
        out[name]["levels"].add(lvl)
    return out


def _lookup_names_for_level(lookups: Dict[str, Dict[str, Any]], level: str) -> List[str]:
    lvl = str(level or "").strip().upper()
    names = []
    for name, meta in lookups.items():
        lvls = set(meta.get("levels", set()))
        if "ALL" in lvls or lvl in lvls:
            names.append(name)
    return sorted(set(names))


def _validate_lookup_tokens(
    df: pd.DataFrame,
    level: str,
    field: str,
    allowed: set,
    issues: List[Issue],
):
    """Validate a field against allowed values.

    We support single or multi-value transparently:
      - if the cell contains separators (, ; |) we treat it as multi
      - otherwise it's a single token
    """
    if df is None or df.empty or field not in df.columns or "human_id" not in df.columns:
        return
    for _, r in df.iterrows():
        hid = str(r.get("human_id", "")).strip()
        raw = r.get(field, "")
        values = _split_multivalue(raw)
        # if it doesn't look like multi, _split_multivalue returns [token] for non-empty
        for v in values:
            if v and v not in allowed:
                issues.append(
                    Issue(
                        issue_id=f"{level}-LOOKUP-{hid}-{field}-{v}",
                        severity="error",
                        level=level,
                        human_id=hid,
                        parent_ref="",
                        issue_type="invalid_lookup",
                        message=f"Valor inválido en {field}: '{v}' (lookup {field})",
                        suggested_fix=f"Usar uno de: {', '.join(sorted(allowed))}",
                    )
                )


def validate_lookups_for_level(
    df: pd.DataFrame,
    level: str,
    lookups: Dict[str, Dict[str, Any]],
    issues: List[Issue],
):
    """Validate all lookup-backed fields for a level.

    Convention (Opción A): lookup_name == field name.
    Only validates fields that actually exist in the sheet.
    """
    for name in _lookup_names_for_level(lookups, level):
        if df is None or df.empty:
            continue
        if name not in df.columns:
            continue
        allowed = set(lookups.get(name, {}).get("values", set()))
        if not allowed:
            continue
        _validate_lookup_tokens(df, level, name, allowed, issues)


def validate_config_lookup_fields_exist(
    views_by_level: Dict[str, pd.DataFrame],
    lookups: Dict[str, Dict[str, Any]],
    issues: List[Issue],
):
    """Raise issues when LOOKUPS defines a field for a level but the column doesn't exist.

    - For level-specific lookups (C1..C4): if the column is missing in that level -> issue
    - For ALL: we don't warn per-level (too noisy)
    """
    if not lookups:
        return
    for name, meta in lookups.items():
        lvls = set(meta.get("levels", set()))
        for lvl in sorted(lvls):
            if lvl == "ALL":
                continue
            if lvl not in views_by_level:
                continue
            df = views_by_level[lvl]
            if df is None:
                continue
            if name not in df.columns:
                issues.append(
                    Issue(
                        issue_id=f"{lvl}-LOOKUP-MISSINGFIELD-{name}",
                        severity="warning",
                        level=lvl,
                        human_id="",
                        parent_ref="",
                        issue_type="config_missing_field",
                        message=f"LOOKUPS define '{name}' para {lvl}, pero la columna no existe en la pestaña.",
                        suggested_fix=f"Crear la columna '{name}' en {lvl} o eliminar/ajustar LOOKUPS.",
                    )
                )


def validate_config_rules_fields_exist(
    rules_df: pd.DataFrame,
    views_by_level: Dict[str, pd.DataFrame],
    issues: List[Issue],
):
    """Raise issues when RULES references a field that doesn't exist in the target level.

    These rules are skipped during evaluation to avoid false positives.
    """
    if rules_df is None or rules_df.empty:
        return
    required = {"rule_id", "level", "group_id", "logic", "when_field", "op", "value", "severity", "message", "suggested_fix"}
    if not required.issubset(set(rules_df.columns)):
        return

    tmp = rules_df.fillna("").copy()
    tmp["level"] = tmp.get("level", "").astype(str).str.upper().str.strip()
    tmp["rule_id"] = tmp.get("rule_id", "").astype(str).str.strip()
    tmp["when_field"] = tmp.get("when_field", "").astype(str).str.strip()

    for _, r in tmp.iterrows():
        lvl = str(r.get("level", "")).upper().strip()
        rid = str(r.get("rule_id", "")).strip()
        wf = str(r.get("when_field", "")).strip()
        if not lvl or not rid or not wf or wf == "_rel":
            continue
        if lvl not in views_by_level:
            continue
        df = views_by_level[lvl]
        if df is None or df.empty:
            continue
        if wf not in df.columns:
            issues.append(
                Issue(
                    issue_id=f"{lvl}-RULE-MISSINGFIELD-{rid}-{wf}",
                    severity="warning",
                    level=lvl,
                    human_id="",
                    parent_ref="",
                    issue_type="config_missing_field",
                    message=f"RULES ({rid}) referencia campo '{wf}' en {lvl}, pero la columna no existe.",
                    suggested_fix=f"Crear la columna '{wf}' en {lvl} o ajustar la regla {rid}.",
                )
            )


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


def normalize_and_derive_vulnerabilities(
    c1: pd.DataFrame,
    c2: pd.DataFrame,
    c3: pd.DataFrame,
    c4: pd.DataFrame,
    issues: List[Issue],
) -> Dict[str, pd.DataFrame]:
    """Normalize and (optionally) derive 'vulnerabilities_detected' across levels.

    Behavior is Excel-driven:
      - We ONLY operate on levels where the column already exists.
      - If the field is missing from the model, we raise an ISSUE and skip its calculation.

    Semantics (when the column exists):
      - C3/C4: factual value, normalized to yes|no|unknown.
      - C2/C1: inherited:
          any descendant yes -> yes
          else any descendant unknown -> unknown
          else no (only if there are descendants); otherwise unknown
    """
    has_any = any(
        (df is not None and not df.empty and VULN_FIELD in df.columns)
        for df in (c1, c2, c3, c4)
    )
    if not has_any:
        issues.append(
            Issue(
                issue_id="CFG-VULN-MISSINGFIELD",
                severity="warning",
                level="ALL",
                human_id="",
                parent_ref="",
                issue_type="config_missing_field",
                message=f"El motor soporta 'vulnerabilities_detected', pero el campo no existe en ninguna pestaña. Se omite su cálculo.",
                suggested_fix=f"Crear la columna 'vulnerabilities_detected' en C3/C4 (y opcionalmente en C2/C1 para herencia) o eliminar su uso.",
            )
        )
        return {"C1": c1, "C2": c2, "C3": c3, "C4": c4}

    c1n, c2n, c3n, c4n = c1.copy(), c2.copy(), c3.copy(), c4.copy()

    # normalize keys
    for df in (c1n, c2n, c3n, c4n):
        if "human_id" in df.columns:
            df["human_id"] = df["human_id"].astype(str).str.strip()
    if "c1_human_id" in c2n.columns:
        c2n["c1_human_id"] = c2n["c1_human_id"].astype(str).str.strip()
    if "c2_human_id" in c3n.columns:
        c3n["c2_human_id"] = c3n["c2_human_id"].astype(str).str.strip()
    if "c3_human_id" in c4n.columns:
        c4n["c3_human_id"] = c4n["c3_human_id"].astype(str).str.strip()

    # C3/C4: normalize and flag invalids (only if column exists)
    for level, df in (("C3", c3n), ("C4", c4n)):
        if VULN_FIELD not in df.columns:
            continue
        new_vals = []
        for _, r in df.iterrows():
            hid = str(r.get("human_id", "")).strip()
            raw = r.get(VULN_FIELD, "")
            cv = _canon_vuln(raw)
            if cv == "__invalid__":
                issues.append(
                    Issue(
                        issue_id=f"{level}-VULN-INVALID-{hid}",
                        severity="warning",
                        level=level,
                        human_id=hid,
                        parent_ref="",
                        issue_type="invalid_value",
                        message=f"Valor inválido en {VULN_FIELD}: '{str(raw).strip()}'",
                        suggested_fix="Usar uno de: yes, no, unknown",
                    )
                )
                cv = "unknown"
            new_vals.append(cv)
        df[VULN_FIELD] = new_vals

    # Build ancestry maps
    c3_to_c2 = {}
    if "human_id" in c3n.columns and "c2_human_id" in c3n.columns:
        c3_to_c2 = dict(zip(c3n["human_id"].astype(str), c3n["c2_human_id"].astype(str)))
    c4_to_c3 = {}
    if "human_id" in c4n.columns and "c3_human_id" in c4n.columns:
        c4_to_c3 = dict(zip(c4n["human_id"].astype(str), c4n["c3_human_id"].astype(str)))

    # Collect vuln values per C3 (from C4)
    vuln_by_c3: Dict[str, List[str]] = {}
    if VULN_FIELD in c4n.columns and "human_id" in c4n.columns:
        for _, r in c4n.iterrows():
            cid = str(r.get("human_id", "")).strip()
            p = str(c4_to_c3.get(cid, "")).strip()
            if not p:
                continue
            vuln_by_c3.setdefault(p, []).append(str(r.get(VULN_FIELD, "")).strip())

    # Collect vuln values per C2 (from C3)
    vuln_by_c2: Dict[str, List[str]] = {}
    if VULN_FIELD in c3n.columns and "human_id" in c3n.columns:
        for _, r in c3n.iterrows():
            cid = str(r.get("human_id", "")).strip()
            p = str(c3_to_c2.get(cid, "")).strip()
            if not p:
                continue
            vuln_by_c2.setdefault(p, []).append(str(r.get(VULN_FIELD, "")).strip())

    # If C3 has the column, we can infer from C4 for blanks
    if VULN_FIELD in c3n.columns and vuln_by_c3:
        inferred = []
        for _, r in c3n.iterrows():
            hid = str(r.get("human_id", "")).strip()
            cur = str(r.get(VULN_FIELD, "")).strip()
            inferred.append(cur or _derive_vuln_from_children(vuln_by_c3.get(hid, [])))
        c3n[VULN_FIELD] = inferred

    # Inherit to C2 only if C2 has the column
    if VULN_FIELD in c2n.columns:
        inherited = []
        for _, r in c2n.iterrows():
            hid = str(r.get("human_id", "")).strip()
            inherited.append(_derive_vuln_from_children(vuln_by_c2.get(hid, [])))
        c2n[VULN_FIELD] = inherited

    # Inherit to C1 only if C1 has the column (requires C2 inheritance too)
    if VULN_FIELD in c1n.columns and VULN_FIELD in c2n.columns and "c1_human_id" in c2n.columns:
        by_c1: Dict[str, List[str]] = {}
        for _, r in c2n.iterrows():
            pid = str(r.get("c1_human_id", "")).strip()
            if not pid:
                continue
            by_c1.setdefault(pid, []).append(str(r.get(VULN_FIELD, "")).strip())
        inherited_c1 = []
        for _, r in c1n.iterrows():
            hid = str(r.get("human_id", "")).strip()
            inherited_c1.append(_derive_vuln_from_children(by_c1.get(hid, [])))
        c1n[VULN_FIELD] = inherited_c1

    return {"C1": c1n, "C2": c2n, "C3": c3n, "C4": c4n}


def _build_relation_helpers(c1: pd.DataFrame, c2: pd.DataFrame, c3: pd.DataFrame, c4: pd.DataFrame) -> Dict[str, Any]:
    """Precompute relation data used by RULES."""
    c1_ids = set(c1.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())
    c2_ids = set(c2.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())
    c3_ids = set(c3.get("human_id", pd.Series([], dtype=str)).astype(str).str.strip())

    c2_parent = {}
    if "human_id" in c2.columns and "c1_human_id" in c2.columns:
        c2_parent = dict(zip(c2["human_id"].astype(str).str.strip(), c2["c1_human_id"].astype(str).str.strip()))
    c3_parent = {}
    if "human_id" in c3.columns and "c2_human_id" in c3.columns:
        c3_parent = dict(zip(c3["human_id"].astype(str).str.strip(), c3["c2_human_id"].astype(str).str.strip()))
    c4_parent = {}
    if "human_id" in c4.columns and "c3_human_id" in c4.columns:
        c4_parent = dict(zip(c4["human_id"].astype(str).str.strip(), c4["c3_human_id"].astype(str).str.strip()))

    # descendant counts
    counts_c2_by_c1 = {}
    if "c1_human_id" in c2.columns:
        counts_c2_by_c1 = c2.groupby(c2["c1_human_id"].astype(str).str.strip()).size().to_dict()
    counts_c3_by_c2 = {}
    if "c2_human_id" in c3.columns:
        counts_c3_by_c2 = c3.groupby(c3["c2_human_id"].astype(str).str.strip()).size().to_dict()
    counts_c4_by_c3 = {}
    if "c3_human_id" in c4.columns:
        counts_c4_by_c3 = c4.groupby(c4["c3_human_id"].astype(str).str.strip()).size().to_dict()

    # runtimes by C2 and C1
    runtimes_by_c2: Dict[str, int] = {}
    runtimes_by_c1: Dict[str, int] = {}
    if not c4.empty and "c3_human_id" in c4.columns and c3_parent and c2_parent:
        for _, r in c4.iterrows():
            c3_id = str(r.get("c3_human_id", "")).strip()
            c2_id = str(c3_parent.get(c3_id, "")).strip()
            c1_id = str(c2_parent.get(c2_id, "")).strip()
            if c2_id:
                runtimes_by_c2[c2_id] = runtimes_by_c2.get(c2_id, 0) + 1
            if c1_id:
                runtimes_by_c1[c1_id] = runtimes_by_c1.get(c1_id, 0) + 1

    return {
        "ids": {"C1": c1_ids, "C2": c2_ids, "C3": c3_ids},
        "parent": {"C2": c2_parent, "C3": c3_parent, "C4": c4_parent},
        "counts": {
            "C2_by_C1": counts_c2_by_c1,
            "C3_by_C2": counts_c3_by_c2,
            "C4_by_C3": counts_c4_by_c3,
            "C4_by_C2": runtimes_by_c2,
            "C4_by_C1": runtimes_by_c1,
        },
    }


def evaluate_rules(
    rules_df: pd.DataFrame,
    views_by_level: Dict[str, pd.DataFrame],
    rel: Dict[str, Any],
    issues: List[Issue],
):
    """Evaluate RULES sheet and append issues.

    RULES format (current MVP):
      - rule_id, level, group_id, logic (AND/OR), when_field, op, value, severity, message, suggested_fix
      - multiple rows with same rule_id+group_id are combined by `logic`
      - multiple groups for same rule_id are OR'ed (any matching group triggers the rule)
      - special when_field '_rel' supports relational ops:
          - missing_parent (value = parent level code e.g. C1/C2/C3)
          - no_descendant (value = descendant level code e.g. C4)
    """

    if rules_df is None or rules_df.empty:
        return
    required = {"rule_id", "level", "group_id", "logic", "when_field", "op", "value", "severity", "message", "suggested_fix"}
    if not required.issubset(set(rules_df.columns)):
        return

    rules_df = rules_df.fillna("")
    for rule_id, rrule in rules_df.groupby("rule_id"):
        rrule = rrule.copy()
        level = str(rrule["level"].iloc[0]).strip()
        if level not in views_by_level:
            continue
        df = views_by_level[level]
        if df is None or df.empty or "human_id" not in df.columns:
            continue

        groups = dict(tuple(rrule.groupby("group_id")))
        # evaluate per record
        for _, rec in df.iterrows():
            hid = str(rec.get("human_id", "")).strip()
            if not hid:
                continue
            triggered = False
            for _, g in groups.items():
                logic = str(g["logic"].iloc[0]).strip().upper() or "AND"
                cond_results = []
                for _, c in g.iterrows():
                    when_field = str(c.get("when_field", "")).strip()
                    op = str(c.get("op", "")).strip().lower()
                    val = str(c.get("value", "")).strip()

                    res = False
                    if when_field == "_rel":
                        # relational checks
                        if op == "missing_parent":
                            parent_level = val.strip()
                            parent_map = rel.get("parent", {}).get(level, {})
                            parent_id = str(parent_map.get(hid, "")).strip()
                            parent_ids = rel.get("ids", {}).get(parent_level, set())
                            res = (not parent_id) or (parent_id not in parent_ids)
                        elif op == "no_descendant":
                            desc_level = val.strip()
                            # implemented for descendant C4 only (runtime) from C1/C2/C3
                            if level == "C1" and desc_level == "C4":
                                res = int(rel.get("counts", {}).get("C4_by_C1", {}).get(hid, 0)) == 0
                            elif level == "C2" and desc_level == "C4":
                                res = int(rel.get("counts", {}).get("C4_by_C2", {}).get(hid, 0)) == 0
                            elif level == "C3" and desc_level == "C4":
                                res = int(rel.get("counts", {}).get("C4_by_C3", {}).get(hid, 0)) == 0
                            elif level == "C1" and desc_level == "C2":
                                res = int(rel.get("counts", {}).get("C2_by_C1", {}).get(hid, 0)) == 0
                            elif level == "C2" and desc_level == "C3":
                                res = int(rel.get("counts", {}).get("C3_by_C2", {}).get(hid, 0)) == 0
                    else:
                        # field-based checks
                        if when_field not in df.columns:
                            # Missing field: ignore condition (and avoid false positives on empty)
                            res = False
                        else:
                            field_val = str(rec.get(when_field, "")).strip()
                            if op == "eq":
                                res = field_val == val
                            elif op == "ne":
                                res = field_val != val
                            elif op == "empty":
                                res = field_val == ""
                            elif op == "not_empty":
                                res = field_val != ""
                            elif op == "contains":
                                res = val.lower() in field_val.lower()
                            elif op == "in":
                                allowed = {x.strip() for x in val.split(",") if x.strip()}
                                res = field_val in allowed
                            elif op == "not_in":
                                denied = {x.strip() for x in val.split(",") if x.strip()}
                                res = field_val not in denied

                    cond_results.append(bool(res))

                if not cond_results:
                    continue
                if logic == "OR":
                    group_ok = any(cond_results)
                else:
                    group_ok = all(cond_results)
                if group_ok:
                    triggered = True
                    break

            if triggered:
                sev = str(rrule.get("severity", "error").iloc[0]).strip() or "error"
                msg = str(rrule.get("message", "").iloc[0])
                fix = str(rrule.get("suggested_fix", "").iloc[0])
                issues.append(
                    Issue(
                        issue_id=f"RULE-{rule_id}-{hid}",
                        severity=sev,
                        level=level,
                        human_id=hid,
                        parent_ref="",
                        issue_type="rule",
                        message=msg,
                        suggested_fix=fix,
                    )
                )


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


def compute(path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
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

    # NOTE: relations (orphans, missing descendants, etc.) are evaluated via RULES.

    
    # Config/model drift checks (UX allows dynamic fields):
    # - If LOOKUPS/RULES reference fields that don't exist in the model, surface as ISSUES.
    base_views = {"C1": c1, "C2": c2, "C3": c3, "C4": c4}
    validate_config_lookup_fields_exist(base_views, lookups, issues)
    validate_config_rules_fields_exist(data.get("RULES", pd.DataFrame()), base_views, issues)

    # Lookups: validate all lookup-backed fields that exist for each level (no hardcodes)
    validate_lookups_for_level(c1, "C1", lookups, issues)
    validate_lookups_for_level(c2, "C2", lookups, issues)
    validate_lookups_for_level(c3, "C3", lookups, issues)
    validate_lookups_for_level(c4, "C4", lookups, issues)
    # Normalize + derive vulnerabilities_detected across all levels
    views_by_level = normalize_and_derive_vulnerabilities(c1, c2, c3, c4, issues)

    # Rules (from sheet RULES)
    rel = _build_relation_helpers(
        views_by_level["C1"],
        views_by_level["C2"],
        views_by_level["C3"],
        views_by_level["C4"],
    )
    evaluate_rules(data.get("RULES", pd.DataFrame()), views_by_level, rel, issues)

    view_full = generate_view_full(
        views_by_level["C1"],
        views_by_level["C2"],
        views_by_level["C3"],
        views_by_level["C4"],
    )
    issues_df = issues_to_df(issues)
    return view_full, issues_df, views_by_level
