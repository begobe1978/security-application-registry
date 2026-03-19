import importlib
from pathlib import Path

from fastapi.testclient import TestClient
import yaml

from sar.auth.passwords import hash_password


def _build_client(tmp_path, monkeypatch):
    data_dir = tmp_path / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    users_path = data_dir / 'users.yml'
    users_path.write_text(
        yaml.safe_dump(
            {
                'version': 1,
                'users': {
                    'alice': {
                        'role': 'admin',
                        'active': True,
                        'password_hash': hash_password('wonderland'),
                    }
                },
            },
            sort_keys=False,
        ),
        encoding='utf-8',
    )
    monkeypatch.setenv('SECRET_KEY', 'test-secret-key')
    monkeypatch.setenv('SAR_USERS_PATH', str(users_path))
    monkeypatch.setenv('SAR_COOKIE_SECURE', 'true')

    import sar.auth.users as users_module
    import sar.app as app_module
    importlib.reload(users_module)
    importlib.reload(app_module)
    return TestClient(app_module.app, base_url="https://testserver"), app_module


def test_login_rejects_missing_csrf(tmp_path, monkeypatch):
    client, _ = _build_client(tmp_path, monkeypatch)

    resp = client.post(
        '/login',
        data={'username': 'alice', 'password': 'wonderland', 'next': '/'},
        follow_redirects=False,
    )

    assert resp.status_code == 403
    assert resp.json()['detail'] == 'CSRF validation failed'



def test_login_sanitizes_next_and_sets_hardened_cookies(tmp_path, monkeypatch):
    client, app_module = _build_client(tmp_path, monkeypatch)

    login_page = client.get('/login?next=https://evil.example/phish', follow_redirects=False)
    assert login_page.status_code == 200
    csrf_token = client.cookies.get(app_module.CSRF_COOKIE_NAME)
    assert csrf_token

    resp = client.post(
        '/login',
        data={
            'username': 'alice',
            'password': 'wonderland',
            'next': 'https://evil.example/phish',
            app_module.CSRF_FORM_FIELD: csrf_token,
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert resp.headers['location'] == '/'
    set_cookie = '\n'.join(resp.headers.get_list('set-cookie'))
    assert 'Secure' in set_cookie
    assert 'HttpOnly' in set_cookie
    assert 'SameSite=lax' in set_cookie or 'SameSite=Lax' in set_cookie
