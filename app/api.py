from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from aiohttp import web

from app.config import Settings
from app.database.store import Node, NodeStore
from app.keyboards import COUNTRY_CODE_ALIASES, ISO_COUNTRY_CODES


MAX_PRIVATE_KEY_BYTES = 128 * 1024
ADDNODE_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "addnode.sh"


class AddNodeApi:
    def __init__(self, settings: Settings, store: NodeStore) -> None:
        self.settings = settings
        self.store = store

    def register(self, app: web.Application) -> None:
        app.router.add_post(f"/{self.settings.addnode_path}", self.add_node)
        app.router.add_get("/scripts/addnode", self.addnode_script)
        app.router.add_get("/scripts/addnode.sh", self.addnode_script)

    async def addnode_script(self, _request: web.Request) -> web.Response:
        try:
            script = ADDNODE_SCRIPT_PATH.read_text(encoding="utf-8")
        except OSError:
            return web.json_response(
                {"ok": False, "error": "script_not_found", "message": "addnode script is not available"},
                status=404,
            )

        return web.Response(
            text=script,
            content_type="text/x-shellscript",
            charset="utf-8",
            headers={"Cache-Control": "no-store"},
        )

    async def add_node(self, request: web.Request) -> web.Response:
        try:
            payload = await self._read_payload(request)
            self._require_api_key(request, payload)
            node = self._node_from_payload(payload)
            self.store.validate_node(node)
            existing = self.store.get(node.name)

            if node.ssh_key_path:
                self._write_private_key_text(Path(node.ssh_key_path), str(payload["ssh_key"]))

            self.store.add_or_update(node)
            if existing and existing.ssh_key_path != node.ssh_key_path:
                self._delete_managed_key(existing)
        except ApiError as exc:
            return web.json_response(
                {"ok": False, "error": exc.code, "message": exc.message},
                status=exc.status,
            )
        except (OSError, ValueError) as exc:
            return web.json_response(
                {"ok": False, "error": "invalid_node", "message": str(exc)},
                status=400,
            )

        return web.json_response(
            {
                "ok": True,
                "node": {
                    "name": node.name,
                    "host": node.host,
                    "user": node.user,
                    "port": node.port,
                    "auth": node.auth_summary,
                    "country_code": node.country_code,
                },
            }
        )

    async def _read_payload(self, request: web.Request) -> dict[str, Any]:
        if request.content_type == "application/json":
            data = await request.json()
            if not isinstance(data, dict):
                raise ApiError(400, "invalid_payload", "JSON payload must be an object")
            return data

        form = await request.post()
        payload: dict[str, Any] = {}
        for key, value in form.items():
            if hasattr(value, "file"):
                payload[key] = value.file.read().decode("utf-8")
            else:
                payload[key] = value
        return payload

    def _require_api_key(self, request: web.Request, payload: dict[str, Any]) -> None:
        if not self.store.has_api_keys():
            return

        raw_key = (
            request.headers.get("X-Api-Key")
            or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            or str(payload.get("apikey") or "").strip()
        )
        if not raw_key:
            raise ApiError(401, "apikey_required", "API key is required")
        if not self.store.verify_api_key(raw_key):
            raise ApiError(403, "invalid_apikey", "API key is invalid")

    def _node_from_payload(self, payload: dict[str, Any]) -> Node:
        name = self._required_text(payload, "name")
        host = self._required_text(payload, "host")
        user = self._required_text(payload, "user")
        port = self._parse_port(payload.get("port"))
        ssh_key = str(payload.get("ssh_key") or "").strip()
        password = str(payload.get("password") or "").strip()

        if bool(ssh_key) == bool(password):
            raise ApiError(400, "invalid_auth", "Send exactly one auth field: ssh_key or password")

        key_path = self._managed_key_path(name) if ssh_key else None
        return Node(
            name=name,
            host=host,
            user=user,
            port=port,
            ssh_key_path=str(key_path) if key_path else None,
            password=password or None,
            country_code=self._infer_country_code(name),
        )

    @staticmethod
    def _required_text(payload: dict[str, Any], field: str) -> str:
        value = str(payload.get(field) or "").strip()
        if not value:
            raise ApiError(400, "missing_field", f"{field} is required")
        return value

    @staticmethod
    def _parse_port(value: Any) -> int:
        try:
            port = int(str(value or "").strip())
        except ValueError as exc:
            raise ApiError(400, "invalid_port", "port must be a number") from exc
        if not 1 <= port <= 65535:
            raise ApiError(400, "invalid_port", "port must be between 1 and 65535")
        return port

    @classmethod
    def _infer_country_code(cls, name: str) -> str | None:
        prefix = name.strip()[:2].upper()
        if len(prefix) != 2 or not prefix.isalpha():
            return None
        prefix = COUNTRY_CODE_ALIASES.get(prefix, prefix)
        return prefix if prefix in ISO_COUNTRY_CODES else None

    def _managed_key_path(self, node_name: str) -> Path:
        return self.settings.managed_ssh_keys_dir / f"{node_name}.key"

    def _write_private_key_text(self, key_path: Path, key_text: str) -> None:
        normalized = self._normalize_private_key(key_text)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_text(normalized, encoding="utf-8")
        os.chmod(key_path, 0o600)

    @staticmethod
    def _normalize_private_key(key_text: str) -> str:
        normalized = key_text.strip().replace("\r\n", "\n").replace("\r", "\n")
        encoded_len = len(normalized.encode("utf-8"))
        if encoded_len > MAX_PRIVATE_KEY_BYTES:
            raise ValueError("private key is too large")
        if not normalized.startswith("-----BEGIN ") or "PRIVATE KEY-----" not in normalized:
            raise ValueError("ssh_key does not look like a private SSH key")
        return f"{normalized}\n"

    def _delete_managed_key(self, node: Node) -> None:
        if not node.ssh_key_path:
            return

        key_path = Path(node.ssh_key_path)
        try:
            key_path.resolve(strict=False).relative_to(
                self.settings.managed_ssh_keys_dir.resolve(strict=False)
            )
        except ValueError:
            return

        try:
            key_path.unlink(missing_ok=True)
        except OSError:
            pass


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        self.status = status
        self.code = code
        self.message = message
        super().__init__(message)
