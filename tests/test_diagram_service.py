from sar.services.diagram_service import build_record_diagram


def test_build_record_diagram_contains_chain_ids(tmp_registry):
    mermaid, meta = build_record_diagram(str(tmp_registry), "RUN-0001", max_nodes=200)
    assert isinstance(mermaid, str) and mermaid.strip()
    # Must include the chain IDs
    assert "RUN-0001" in mermaid
    assert "CMP-0001" in mermaid
    assert "APP-0001" in mermaid
    assert "PRJ-0001" in mermaid
    # meta should at least expose truncation info
    assert "truncated" in meta
