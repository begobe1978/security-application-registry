from pathlib import Path

import pytest

from docx import Document

from sar.engine import compute
from sar.services.report_service import generate_c4_chain_report_docx, generate_c4_chain_report_html


def test_generate_report_docx_creates_file(tmp_registry, report_template_path, no_mmdc, tmp_path):
    pytest.importorskip("docxtpl")
    _, issues, _ = compute(str(tmp_registry))
    out = generate_c4_chain_report_docx(
        registry_path=str(tmp_registry),
        run_human_id="RUN-0001",
        issues_df=issues,
        template_docx_path=str(report_template_path),
        out_dir=str(tmp_path),
        max_nodes=200,
    )
    out_path = Path(out)
    assert out_path.exists()
    assert out_path.stat().st_size > 0

    # Light content check: document contains the main runtime ID somewhere
    doc = Document(str(out_path))
    text = "\n".join(p.text for p in doc.paragraphs)
    assert "RUN-0001" in text


def test_generate_report_html_creates_file(tmp_registry, no_mmdc, tmp_path):
    _, issues, _ = compute(str(tmp_registry))
    repo_root = Path(__file__).resolve().parents[1]
    html_tpl = repo_root / "src" / "sar" / "report_templates" / "c4_chain_report.html.j2"
    assert html_tpl.exists(), f"Missing HTML report template: {html_tpl}"

    out = generate_c4_chain_report_html(
        registry_path=str(tmp_registry),
        run_human_id="RUN-0001",
        issues_df=issues,
        template_html_path=str(html_tpl),
        out_dir=str(tmp_path),
        max_nodes=200,
    )
    out_path = Path(out)
    assert out_path.exists()
    html = out_path.read_text(encoding="utf-8")
    assert "<!doctype html>" in html.lower()
    assert "RUN-0001" in html
