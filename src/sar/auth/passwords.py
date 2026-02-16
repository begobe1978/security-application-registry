# Copyright (C) 2026 Bernardo Gómez Bey
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

_PH = PasswordHasher()


def hash_password(plain: str) -> str:
    if not plain:
        raise ValueError("Password vacío")
    return _PH.hash(plain)


def verify_password(hash_value: str, plain: str) -> bool:
    if not hash_value or not plain:
        return False
    try:
        return _PH.verify(hash_value, plain)
    except VerifyMismatchError:
        return False
