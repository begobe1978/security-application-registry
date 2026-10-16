# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode

from fastapi import HTTPException, Request

from sar.auth.session import COOKIE_NAME, verify_session
from sar.auth.users import get_user

ROLE_ORDER = {"viewer": 0, "editor": 1, "admin": 2}


def _rank(role: str) -> int:
    return ROLE_ORDER.get((role or "viewer").strip().lower(), 0)


@dataclass(frozen=True)
class CurrentUser:
    username: str
    role: str


def load_user_from_request(request: Request) -> Optional[CurrentUser]:
    token = request.cookies.get(COOKIE_NAME, "")
    sess = verify_session(token)
    if not sess:
        return None
    u = get_user(sess.username)
    if not u or not u.active:
        return None
    return CurrentUser(username=u.username, role=(u.role or "viewer").lower())


def current_user_optional(request: Request) -> Optional[CurrentUser]:
    u = getattr(request.state, "user", None)
    if u is not None:
        return u
    return load_user_from_request(request)


def require_user(request: Request) -> CurrentUser:
    u = current_user_optional(request)
    if u:
        return u
    next_url = str(request.url.path)
    if request.url.query:
        next_url += "?" + request.url.query
    loc = "/login?" + urlencode({"next": next_url})
    raise HTTPException(status_code=303, headers={"Location": loc})


def require_role(min_role: str):
    def _dep(request: Request) -> CurrentUser:
        u = require_user(request)
        if _rank(u.role) < _rank(min_role):
            raise HTTPException(status_code=403, detail="Forbidden")
        return u

    return _dep


def cookie_settings() -> dict:
    secure_env = os.getenv("SAR_COOKIE_SECURE", "true").strip().lower()
    secure = secure_env not in {"0", "false", "no", "n"}
    samesite = os.getenv("SAR_COOKIE_SAMESITE", "lax").strip().lower() or "lax"
    if samesite not in {"lax", "strict", "none"}:
        samesite = "lax"
    return {"httponly": True, "samesite": samesite, "secure": secure, "path": "/"}
