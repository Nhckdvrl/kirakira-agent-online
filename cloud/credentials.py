"""Authenticated encryption for tenant integration credentials."""

from __future__ import annotations

import json
import os

from cryptography.fernet import Fernet, InvalidToken


class CredentialVault:
    def __init__(self, key: str | bytes | None = None) -> None:
        raw = key if key is not None else os.getenv("KIRAKIRA_CREDENTIAL_KEY", "")
        if not raw:
            raise RuntimeError("KIRAKIRA_CREDENTIAL_KEY is required for integrations")
        try:
            self._fernet = Fernet(raw.encode() if isinstance(raw, str) else raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("KIRAKIRA_CREDENTIAL_KEY must be a Fernet key") from exc

    def encrypt_json(self, value: dict[str, str]) -> str:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        return self._fernet.encrypt(payload).decode("ascii")

    def decrypt_json(self, value: str) -> dict[str, str]:
        try:
            payload = json.loads(self._fernet.decrypt(value.encode("ascii")))
        except (InvalidToken, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("integration credential cannot be decrypted") from exc
        if not isinstance(payload, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in payload.items()
        ):
            raise RuntimeError("integration credential payload is invalid")
        return payload
