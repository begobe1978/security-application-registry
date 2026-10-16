# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from itsdangerous import BadSignature, BadTimeSignature, URLSafeTimedSerializer

COOKIE_NAME = os.getenv("SAR_COOKIE_NAME", "sar_session")
DEFAULT_MAX_AGE_SECONDS = int(os.getenv("SAR_SESSION_MAX_AGE", "28800"))  # 8 hours


def _serializer() -> URLSafeTimedSerializer:
    secret = os.getenv("SECRET_KEY") or os.getenv("SAR_SECRET_KEY")
    if not secret:
        raise RuntimeError("Falta SECRET_KEY (o SAR_SECRET_KEY) en entorno")
    salt = os.getenv("SAR_SESSION_SALT", "sar.session.v1")
    return URLSafeTimedSerializer(secret_key=secret, salt=salt)


@dataclass(frozen=True)
class SessionData:
    username: str


def sign_session(username: str) -> str:
    s = _serializer()
    return s.dumps({"u": username})


def verify_session(token: str, *, max_age: int = DEFAULT_MAX_AGE_SECONDS) -> Optional[SessionData]:
    if not token:
        return None
    s = _serializer()
    try:
        data = s.loads(token, max_age=max_age)
        u = (data or {}).get("u") or ""
        u = str(u).strip()
        if not u:
            return None
        return SessionData(username=u)
    except (BadSignature, BadTimeSignature):
        return None
