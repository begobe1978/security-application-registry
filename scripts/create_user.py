#!/usr/bin/env python3
from __future__ import annotations

from getpass import getpass
from pathlib import Path

import yaml

from sar.auth.passwords import hash_password
from sar.auth.users import DEFAULT_USERS_PATH

USERS_PATH = DEFAULT_USERS_PATH


def main() -> None:
    USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if USERS_PATH.exists():
        raw = yaml.safe_load(USERS_PATH.read_text(encoding="utf-8")) or {}
    else:
        raw = {"version": 1, "users": {}}

    if "users" not in raw or not isinstance(raw["users"], dict):
        raw["users"] = {}

    username = input("Username: ").strip()
    role = (input("Role [viewer/editor/admin]: ").strip().lower() or "viewer")
    active_in = input("Active? [Y/n]: ").strip().lower()
    active = (active_in != "n")

    pw1 = getpass("Password: ")
    pw2 = getpass("Repeat password: ")
    if pw1 != pw2:
        raise SystemExit("Passwords no coinciden")

    raw["users"][username] = {
        "role": role,
        "active": active,
        "password_hash": hash_password(pw1),
    }

    USERS_PATH.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    print(f"OK -> {USERS_PATH}")


if __name__ == "__main__":
    main()
