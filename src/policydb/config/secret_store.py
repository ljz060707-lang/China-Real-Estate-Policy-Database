from __future__ import annotations

import os
import re
from collections.abc import Iterable
from typing import Protocol

SERVICE_NAME = "policydb"
ENV_NAMES = {
    "glm_api_key": "GLM_API_KEY",
    "tianditu_token": "TIANDITU_TOKEN",
    "search_api_key": "SEARCH_API_KEY",
    "http_proxy": "POLICYDB_HTTP_PROXY",
}


class SecretStore(Protocol):
    def get_secret(self, name: str) -> str | None: ...
    def set_secret(self, name: str, value: str) -> None: ...
    def delete_secret(self, name: str) -> None: ...
    def has_secret(self, name: str) -> bool: ...


class KeyringSecretStore:
    """OS credential-store adapter. Import is lazy so read-only deployments stay usable."""

    def __init__(self, service_name: str = SERVICE_NAME) -> None:
        self.service_name = service_name

    @staticmethod
    def _keyring():
        import keyring

        return keyring

    def get_secret(self, name: str) -> str | None:
        try:
            return self._keyring().get_password(self.service_name, name)
        except Exception:
            return None

    def set_secret(self, name: str, value: str) -> None:
        self._keyring().set_password(self.service_name, name, value)

    def delete_secret(self, name: str) -> None:
        try:
            self._keyring().delete_password(self.service_name, name)
        except Exception:
            pass

    def has_secret(self, name: str) -> bool:
        return bool(self.get_secret(name))


class EnvironmentSecretStore:
    def get_secret(self, name: str) -> str | None:
        value = os.getenv(ENV_NAMES.get(name, name.upper()), "").strip()
        return value or None

    def set_secret(self, name: str, value: str) -> None:
        os.environ[ENV_NAMES.get(name, name.upper())] = value

    def delete_secret(self, name: str) -> None:
        os.environ.pop(ENV_NAMES.get(name, name.upper()), None)

    def has_secret(self, name: str) -> bool:
        return bool(self.get_secret(name))


class StreamlitSecretsStore:
    def get_secret(self, name: str) -> str | None:
        try:
            import streamlit as st

            value = st.secrets.get(ENV_NAMES.get(name, name.upper()))
            return str(value).strip() if value else None
        except Exception:
            return None

    def set_secret(self, name: str, value: str) -> None:
        raise PermissionError("Streamlit secrets are deployment-managed")

    def delete_secret(self, name: str) -> None:
        raise PermissionError("Streamlit secrets are deployment-managed")

    def has_secret(self, name: str) -> bool:
        return bool(self.get_secret(name))


class CompositeSecretStore:
    def __init__(self, stores: Iterable[SecretStore]) -> None:
        self.stores = list(stores)

    def get_secret(self, name: str) -> str | None:
        for store in self.stores:
            value = store.get_secret(name)
            if value:
                return value
        return None

    def set_secret(self, name: str, value: str) -> None:
        if not self.stores:
            raise RuntimeError("No writable secret store configured")
        self.stores[0].set_secret(name, value)

    def delete_secret(self, name: str) -> None:
        if self.stores:
            self.stores[0].delete_secret(name)

    def has_secret(self, name: str) -> bool:
        return bool(self.get_secret(name))


def default_secret_store() -> CompositeSecretStore:
    return CompositeSecretStore(
        [KeyringSecretStore(), StreamlitSecretsStore(), EnvironmentSecretStore()]
    )


_SECRET_PATTERNS = (
    re.compile(r"(?i)(Bearer\s+)[^\s\"']+"),
    re.compile(r"(?i)((?:GLM_API_KEY|TIANDITU_TOKEN|SEARCH_API_KEY)\s*[=:]\s*)[^\s,;]+"),
    re.compile(r"\bsk-[A-Za-z0-9._-]+"),
)


def redact_secrets(text: object, secret_values: Iterable[str] = ()) -> str:
    result = str(text)
    for pattern in _SECRET_PATTERNS:
        result = pattern.sub(lambda match: (match.group(1) if match.lastindex else "") + "••••••••", result)
    for value in sorted({value for value in secret_values if value}, key=len, reverse=True):
        result = result.replace(value, "••••••••")
    return result
