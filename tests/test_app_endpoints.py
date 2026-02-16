import importlib
from pathlib import Path

from fastapi.testclient import TestClient
import pytest


def test_report_endpoint_returns_docx(tmp_registry, report_template_path, no_mmdc, tmp_path, monkeypatch):
    pytest.importorskip("docxtpl")
    # Configure app to use tmp data dir and the real report template
    data_dir = Path(tmp_registry).parent
    monkeypatch.setenv("SAR_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SAR_TEMPLATE_PATH", str(data_dir / "registry_template.xlsx"))  # not used here but safe
    monkeypatch.setenv("SAR_REPORT_TEMPLATE_DOCX", str(report_template_path))
    monkeypatch.setenv("SAR_REPORTS_DIR", str(tmp_path))

    import sar.app as app_module
    importlib.reload(app_module)

    # Seed state as if registry is loaded
    app_module.STATE["path"] = str(tmp_registry)
    view_full, issues, views_by_level = app_module.regenerate_views(app_module.STATE["path"])
    app_module.STATE["view_full"] = view_full
    app_module.STATE["issues"] = issues
    app_module.STATE["views_by_level"] = views_by_level

    client = TestClient(app_module.app)
    r = client.get("/report/c4/RUN-0001.docx")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert len(r.content) > 1000


def test_report_endpoint_returns_html(tmp_registry, tmp_path, monkeypatch):
    # Configure app to use tmp data dir
    data_dir = Path(tmp_registry).parent
    monkeypatch.setenv("SAR_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SAR_TEMPLATE_PATH", str(data_dir / "registry_template.xlsx"))
    monkeypatch.setenv("SAR_REPORTS_DIR", str(tmp_path))

    import sar.app as app_module
    importlib.reload(app_module)

    app_module.STATE["path"] = str(tmp_registry)
    view_full, issues, views_by_level = app_module.regenerate_views(app_module.STATE["path"])
    app_module.STATE["view_full"] = view_full
    app_module.STATE["issues"] = issues
    app_module.STATE["views_by_level"] = views_by_level

    client = TestClient(app_module.app)
    r = client.get("/report/c4/RUN-0001.html")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "RUN-0001" in r.text
