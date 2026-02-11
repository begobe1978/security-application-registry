# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Generate Mermaid diagrams for registry records.

The UI renders the returned Mermaid text inside a ``<div class="mermaid">`` and
Mermaid.js turns it into a diagram.

Diagram strategy (record detail)
-------------------------------
We always draw exactly **one** C1 tree: the C1 ancestor of the current record.

To keep diagrams readable without "lying" about the context, we use a *local*
scope:

- Always include the full ancestor chain up to C1.
- Always expand **downwards** only for the *current branch*.
- Additionally include siblings for the C2/C3/C4 levels that exist in the
  current record's ancestor chain, but **do not** expand the children of those
  siblings.
- Siblings that have children (not shown) are highlighted with a different
  color.

We also enforce a maximum node limit (default: 200) to protect the browser.
When the limit is hit, the diagram is marked as truncated and the UI should
show a visible warning.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sar.core.mapping import meta_for_level
from sar.core.utils import canon
from sar.infra.registry_repo import read_sheet
from sar.services.record_service import detect_level_meta


_SAFE_ID_RE = re.compile(r"[^a-zA-Z0-9_]")


def _safe_node_id(level: str, human_id: str, suffix: str = "") -> str:
    base = f"{level}_{canon(human_id)}"
    base = _SAFE_ID_RE.sub("_", base)
    if suffix:
        base = f"{base}_{_SAFE_ID_RE.sub('_', suffix)}"
    return base


def _short(text: str, max_len: int = 60) -> str:
    t = (text or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _label(level: str, human_id: str, name: str) -> str:
    hid = canon(human_id)
    nm = _short(name)
    if nm:
        return f"{level} {hid}<br/>{nm}"
    return f"{level} {hid}"


@dataclass
class _Node:
    node_id: str
    label: str
    css_class: str = "main"


class _MermaidBuilder:
    """Small builder that de-duplicates nodes/edges and supports click + classes."""

    def __init__(self, max_nodes: int = 200) -> None:
        self.max_nodes = max(10, int(max_nodes or 200))
        self.nodes: Dict[str, _Node] = {}
        self.edges: Set[Tuple[str, str, str]] = set()  # (src, dst, style)
        self.clicks: Dict[str, Tuple[str, str]] = {}  # node_id -> (url, title)
        self.truncated: bool = False

    def add_node(self, node_id: str, label: str, css_class: str = "main") -> bool:
        """Return True if node was added/exists, False if skipped due to truncation."""
        if node_id in self.nodes:
            # allow upgrading class (but never downgrade focus)
            if self.nodes[node_id].css_class != "focus" and css_class == "focus":
                self.nodes[node_id].css_class = "focus"
            return True
        if len(self.nodes) >= self.max_nodes:
            self.truncated = True
            return False
        self.nodes[node_id] = _Node(node_id=node_id, label=label, css_class=css_class)
        return True

    def set_class(self, node_id: str, css_class: str) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].css_class = css_class

    def add_edge(self, src: str, dst: str, style: str = "-->") -> None:
        if src in self.nodes and dst in self.nodes:
            self.edges.add((src, dst, style))

    def add_click(self, node_id: str, url: str, title: str = "Abrir registro") -> None:
        if node_id and url:
            self.clicks[node_id] = (url, title or "Abrir registro")

    def render(self, focus_node_id: Optional[str] = None) -> str:
        lines: List[str] = ["flowchart TB"]
        # class definitions (explicit colors)
        lines.append("  classDef main fill:#E5E7EB,stroke:#374151,stroke-width:1px;")
        lines.append("  classDef focus fill:#FFE08A,stroke:#B45309,stroke-width:2px;")
        lines.append("  classDef siblingHasKids fill:#C7D2FE,stroke:#3730A3,stroke-width:2px;")
        lines.append("  classDef siblingLeaf fill:#F3F4F6,stroke:#9CA3AF,stroke-width:1px;")

        for nid in sorted(self.nodes.keys()):
            label = self.nodes[nid].label.replace("\n", "<br/>")
            lines.append(f'  {nid}["{label}"]')
        for src, dst, style in sorted(self.edges):
            lines.append(f"  {src} {style} {dst}")

        # Apply classes (after nodes)
        for nid in sorted(self.nodes.keys()):
            cls = self.nodes[nid].css_class or "main"
            lines.append(f"  class {nid} {cls};")

        # Ensure focus class is applied even if set later
        if focus_node_id and focus_node_id in self.nodes:
            lines.append(f"  class {focus_node_id} focus;")

        # Click handlers at the end
        for nid in sorted(self.clicks.keys()):
            if nid not in self.nodes:
                continue
            url, title = self.clicks[nid]
            safe_url = str(url).replace('"', r"\"")
            safe_title = str(title).replace('"', r"\"")
            lines.append(f'  click {nid} "{safe_url}" "{safe_title}"')

        # Optional truncation marker inside diagram
        if self.truncated:
            lines.append('  TRUNCATED["⚠️ TRUNCADO: límite de nodos"]')
            lines.append("  class TRUNCATED siblingHasKids;")

        return "\n".join(lines)


def _sheet(level: str, fallback: str) -> str:
    return (meta_for_level(level) or {}).get("sheet", fallback)


def _build_indexes(path: str) -> Dict[str, Dict]:
    """Load C1..C4 and build indexes.

    Returns a dict with:
      - rows[level][id] = row dict (stringified)
      - parent[level][id] = parent id (canon) (for C2/C3/C4)
      - children[level][parent_id] = list of child ids (canon)
    """
    # Sheet names
    s1 = _sheet("C1", "C1_Proyectos")
    s2 = _sheet("C2", "C2_Aplicaciones")
    s3 = _sheet("C3", "C3_Componentes")
    s4 = _sheet("C4", "C4_Runtime")

    df1 = read_sheet(path, s1)
    df2 = read_sheet(path, s2)
    df3 = read_sheet(path, s3)
    df4 = read_sheet(path, s4)

    def rows_from_df(df) -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        if df is None or df.empty or "human_id" not in df.columns:
            return out
        for _, r in df.iterrows():
            hid = canon(str(r.get("human_id", "")).strip())
            if not hid:
                continue
            out[hid] = {str(k): ("" if r.get(k) is None else str(r.get(k))) for k in df.columns}
        return out

    rows = {
        "C1": rows_from_df(df1),
        "C2": rows_from_df(df2),
        "C3": rows_from_df(df3),
        "C4": rows_from_df(df4),
    }

    parent: Dict[str, Dict[str, str]] = {"C2": {}, "C3": {}, "C4": {}}
    children: Dict[str, Dict[str, List[str]]] = {"C1": {}, "C2": {}, "C3": {}}

    # Parents by convention
    for hid, r in rows["C2"].items():
        pid = canon(r.get("c1_human_id", ""))
        if pid:
            parent["C2"][hid] = pid
            children["C1"].setdefault(pid, []).append(hid)

    for hid, r in rows["C3"].items():
        pid = canon(r.get("c2_human_id", ""))
        if pid:
            parent["C3"][hid] = pid
            children["C2"].setdefault(pid, []).append(hid)

    for hid, r in rows["C4"].items():
        pid = canon(r.get("c3_human_id", ""))
        if pid:
            parent["C4"][hid] = pid
            children["C3"].setdefault(pid, []).append(hid)

    # Sort children deterministically
    for lvl in children:
        for pid in children[lvl]:
            children[lvl][pid] = sorted(set(children[lvl][pid]))

    return {"rows": rows, "parent": parent, "children": children, "sheets": {"C1": s1, "C2": s2, "C3": s3, "C4": s4}}


def _ancestor_chain(level: str, human_id: str, idx: Dict) -> Dict[str, str]:
    """Return dict with keys C1/C2/C3/C4 for available ancestors (canon ids)."""
    hid = canon(human_id)
    out: Dict[str, str] = {}
    if level == "C1":
        out["C1"] = hid
        return out

    if level == "C2":
        out["C2"] = hid
        out["C1"] = idx["parent"]["C2"].get(hid, "")
        return out

    if level == "C3":
        out["C3"] = hid
        c2 = idx["parent"]["C3"].get(hid, "")
        out["C2"] = c2
        out["C1"] = idx["parent"]["C2"].get(c2, "") if c2 else ""
        return out

    if level == "C4":
        out["C4"] = hid
        c3 = idx["parent"]["C4"].get(hid, "")
        out["C3"] = c3
        c2 = idx["parent"]["C3"].get(c3, "") if c3 else ""
        out["C2"] = c2
        out["C1"] = idx["parent"]["C2"].get(c2, "") if c2 else ""
        return out

    return out


def _has_children(level: str, human_id: str, idx: Dict) -> bool:
    if level == "C2":
        return bool(idx["children"]["C2"].get(canon(human_id), []))
    if level == "C3":
        return bool(idx["children"]["C3"].get(canon(human_id), []))
    return False


def build_record_diagram(path: str, human_id: str, max_nodes: int = 200) -> Tuple[str, Dict[str, object]]:
    """Return (mermaid_code, meta) for a registry record."""
    meta = detect_level_meta(human_id)
    if not meta:
        return "", {"error": "human_id no reconocido", "truncated": False, "node_count": 0, "max_nodes": max_nodes}

    level = meta["level"]
    idx = _build_indexes(path)
    chain = _ancestor_chain(level, human_id, idx)
    c1_id = chain.get("C1", "")
    if not c1_id:
        # If the record exists but chain is broken, still show only the record node.
        b = _MermaidBuilder(max_nodes=max_nodes)
        nid = _safe_node_id(level, human_id)
        row = idx["rows"].get(level, {}).get(canon(human_id), {})
        b.add_node(nid, _label(level, human_id, row.get("name", "")), css_class="focus")
        b.add_click(nid, f"/record/{canon(human_id)}")
        return b.render(focus_node_id=nid), {"truncated": b.truncated, "node_count": len(b.nodes), "max_nodes": b.max_nodes}

    b = _MermaidBuilder(max_nodes=max_nodes)

    # Helpers
    def add(level_: str, hid: str, css_class: str = "main") -> Optional[str]:
        hidc = canon(hid)
        if not hidc:
            return None
        row = idx["rows"].get(level_, {}).get(hidc, {})
        nid = _safe_node_id(level_, hidc)
        ok = b.add_node(nid, _label(level_, hidc, row.get("name", "")), css_class=css_class)
        if not ok:
            return None
        b.add_click(nid, f"/record/{hidc}", "Abrir registro")
        return nid

    def children(level_: str, parent_id: str) -> List[str]:
        return idx["children"].get(level_, {}).get(canon(parent_id), [])

    # Root node (single C1)
    c1_nid = add("C1", c1_id, css_class="main")
    if not c1_nid:
        return b.render(), {"truncated": b.truncated, "node_count": len(b.nodes), "max_nodes": b.max_nodes}

    # Focus node id
    focus_level = level
    focus_id = canon(human_id)
    focus_nid = _safe_node_id(focus_level, focus_id)

    # 1) Draw ancestor chain (C1 -> C2 -> C3 -> C4 as available)
    # Ensure nodes exist and edges connect
    if chain.get("C2"):
        c2_nid = add("C2", chain["C2"], css_class="main")
        if c2_nid:
            b.add_edge(c1_nid, c2_nid)
    if chain.get("C3"):
        c3_nid = add("C3", chain["C3"], css_class="main")
        if c3_nid and chain.get("C2"):
            b.add_edge(_safe_node_id("C2", chain["C2"]), c3_nid)
    if chain.get("C4"):
        c4_nid = add("C4", chain["C4"], css_class="main")
        if c4_nid and chain.get("C3"):
            b.add_edge(_safe_node_id("C3", chain["C3"]), c4_nid)

    # 2) Expand downwards only for the current branch
    if level == "C1":
        # Full tree for this C1
        for c2_id in children("C1", c1_id):
            c2_nid = add("C2", c2_id, css_class="main")
            if not c2_nid:
                break
            b.add_edge(c1_nid, c2_nid)
            for c3_id in children("C2", c2_id):
                c3_nid = add("C3", c3_id, css_class="main")
                if not c3_nid:
                    break
                b.add_edge(c2_nid, c3_nid)
                for c4_id in children("C3", c3_id):
                    c4_nid = add("C4", c4_id, css_class="main")
                    if not c4_nid:
                        break
                    b.add_edge(c3_nid, c4_nid)
            if b.truncated:
                break

    elif level == "C2":
        cur_c2 = chain.get("C2", "")
        for c3_id in children("C2", cur_c2):
            c3_nid = add("C3", c3_id, css_class="main")
            if not c3_nid:
                break
            b.add_edge(_safe_node_id("C2", cur_c2), c3_nid)
            for c4_id in children("C3", c3_id):
                c4_nid = add("C4", c4_id, css_class="main")
                if not c4_nid:
                    break
                b.add_edge(c3_nid, c4_nid)
            if b.truncated:
                break

    elif level == "C3":
        cur_c3 = chain.get("C3", "")
        for c4_id in children("C3", cur_c3):
            c4_nid = add("C4", c4_id, css_class="main")
            if not c4_nid:
                break
            b.add_edge(_safe_node_id("C3", cur_c3), c4_nid)
        # (ancestors already drawn)

    elif level == "C4":
        # no expansion down
        pass

    # 3) Add siblings for chain levels (C2/C3/C4) without expanding their children
    # Siblings of C2 (under same C1)
    if chain.get("C2"):
        cur_c2 = chain["C2"]
        for sib_c2 in children("C1", c1_id):
            if sib_c2 == canon(cur_c2):
                continue
            cls = "siblingHasKids" if _has_children("C2", sib_c2, idx) else "siblingLeaf"
            sib_nid = add("C2", sib_c2, css_class=cls)
            if not sib_nid:
                break
            b.add_edge(c1_nid, sib_nid, "-->")
        # ensure current c2 edge exists (already added in chain)
    # Siblings of C3 (under same C2)
    if chain.get("C3") and chain.get("C2"):
        cur_c3 = chain["C3"]
        cur_c2 = chain["C2"]
        c2_nid = _safe_node_id("C2", cur_c2)
        for sib_c3 in children("C2", cur_c2):
            if sib_c3 == canon(cur_c3):
                continue
            cls = "siblingHasKids" if _has_children("C3", sib_c3, idx) else "siblingLeaf"
            sib_nid = add("C3", sib_c3, css_class=cls)
            if not sib_nid:
                break
            b.add_edge(c2_nid, sib_nid, "-->")
    # Siblings of C4 (under same C3)
    if chain.get("C4") and chain.get("C3"):
        cur_c4 = chain["C4"]
        cur_c3 = chain["C3"]
        c3_nid = _safe_node_id("C3", cur_c3)
        for sib_c4 in children("C3", cur_c3):
            if sib_c4 == canon(cur_c4):
                continue
            sib_nid = add("C4", sib_c4, css_class="siblingLeaf")
            if not sib_nid:
                break
            b.add_edge(c3_nid, sib_nid, "-->")

    # Focus styling: ensure node exists and mark as focus
    if focus_nid in b.nodes:
        b.set_class(focus_nid, "focus")
    else:
        # If focus node wasn't added due to truncation, still mark truncation.
        b.truncated = True

    code = b.render(focus_node_id=focus_nid)
    meta_out = {"truncated": b.truncated, "node_count": len(b.nodes), "max_nodes": b.max_nodes}
    return code, meta_out
