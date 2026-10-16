import importlib
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest


@pytest.fixture()
def authenticated_client(monkeypatch):
    """Build an authenticated client after each test configures its paths."""
    def make_client():
        monkeypatch.setenv("SECRET_KEY", "test-only-session-secret")
        monkeypatch.setenv("SAR_COOKIE_SECURE", "false")
        import sar.permissions as permissions
        from sar.auth.session import COOKIE_NAME, sign_session
        import sar.app as app_module

        monkeypatch.setattr(
            permissions,
            "get_user",
            lambda username: SimpleNamespace(username=username, role="admin", active=True)
            if username == "test_user" else None,
        )
        importlib.reload(app_module)
        client = TestClient(app_module.app)
        client.cookies.set(COOKIE_NAME, sign_session("test_user"))
        return client, app_module
    return make_client


def _seed_registry(app_module, tmp_registry):
    app_module.STATE["path"] = str(tmp_registry)
    view_full, issues, views_by_level = app_module.regenerate_views(app_module.STATE["path"])
    app_module.STATE["view_full"] = view_full
    app_module.STATE["issues"] = issues
    app_module.STATE["views_by_level"] = views_by_level


def test_report_endpoint_returns_docx(tmp_registry, report_template_path, no_mmdc, tmp_path, monkeypatch, authenticated_client):
    pytest.importorskip("docxtpl")
    data_dir = Path(tmp_registry).parent
    monkeypatch.setenv("SAR_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SAR_TEMPLATE_PATH", str(data_dir / "registry_template.xlsx"))
    monkeypatch.setenv("SAR_REPORT_TEMPLATE_DOCX", str(report_template_path))
    monkeypatch.setenv("SAR_REPORTS_DIR", str(tmp_path))

    client, app_module = authenticated_client()
    _seed_registry(app_module, tmp_registry)

    r = client.get("/report/c4/C4-0001.docx")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert len(r.content) > 1000


def test_report_endpoint_returns_html(tmp_registry, tmp_path, monkeypatch, authenticated_client):
    data_dir = Path(tmp_registry).parent
    monkeypatch.setenv("SAR_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SAR_TEMPLATE_PATH", str(data_dir / "registry_template.xlsx"))
    monkeypatch.setenv("SAR_REPORTS_DIR", str(tmp_path))

    client, app_module = authenticated_client()
    _seed_registry(app_module, tmp_registry)

    r = client.get("/report/c4/C4-0001.html")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "C4-0001" in r.text
