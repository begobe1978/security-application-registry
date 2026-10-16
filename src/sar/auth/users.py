# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import yaml

from sar.auth.passwords import verify_password

# IMPORTANT: do not rely on current working directory.
# Anchor the default users.yml path to the project root (works well with editable installs).
BASE_DIR = Path(__file__).resolve().parents[3]  # .../sarproj
DEFAULT_USERS_PATH = Path(
    os.getenv("SAR_USERS_PATH", str(BASE_DIR / "data" / "users.yml"))
).resolve()


@dataclass(frozen=True)
class UserRecord:
    username: str
    role: str
    active: bool
    password_hash: str


_CACHE: Tuple[float, Dict[str, UserRecord]] = (0.0, {})


def _load_users_file(path: Path) -> Dict[str, UserRecord]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    users = (raw.get("users") or {}) if isinstance(raw, dict) else {}
    out: Dict[str, UserRecord] = {}
    for uname, udata in users.items():
        if not isinstance(udata, dict):
            continue
        username = str(uname).strip()
        if not username:
            continue
        role = str(udata.get("role") or "viewer").strip().lower()
        active = bool(udata.get("active", True))
        ph = str(udata.get("password_hash") or "").strip()
        out[username] = UserRecord(
            username=username,
            role=role,
            active=active,
            password_hash=ph,
        )
    return out


def get_users(*, path: Path = DEFAULT_USERS_PATH) -> Dict[str, UserRecord]:
    global _CACHE
    try:
        mtime = path.stat().st_mtime if path.exists() else 0.0
    except Exception:
        mtime = 0.0

    cached_mtime, cached_users = _CACHE
    if mtime and mtime == cached_mtime and cached_users:
        return cached_users

    users = _load_users_file(path)
    _CACHE = (mtime, users)
    return users


def get_user(username: str, *, path: Path = DEFAULT_USERS_PATH) -> Optional[UserRecord]:
    u = (username or "").strip()
    if not u:
        return None
    return get_users(path=path).get(u)


def authenticate(username: str, password: str, *, path: Path = DEFAULT_USERS_PATH) -> Optional[UserRecord]:
    u = get_user(username, path=path)
    if not u or not u.active:
        return None
    if not verify_password(u.password_hash, password):
        return None
    return u
