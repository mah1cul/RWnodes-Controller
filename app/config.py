from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_admin_ids(value: str | None) -> frozenset[int]:
    if not value:
        return frozenset()

    admin_ids: set[int] = set()
    for item in value.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            admin_ids.add(int(item))
        except ValueError as exc:
            raise ValueError(f"Invalid ADMIN_IDS value: {item!r}") from exc
    return frozenset(admin_ids)


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: frozenset[int]
    db_path: Path
    bot_mode: str
    webhook_url: str | None
    webhook_listen: str
    webhook_port: int
    webhook_path: str
    webhook_secret_token: str | None
    ansible_timeout: int
    ansible_host_key_checking: bool
    default_become: bool
    max_telegram_output_chars: int
    remnanode_playbook_path: Path
    managed_ssh_keys_dir: Path
    ssh_key_presets_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        if not bot_token:
            raise ValueError("BOT_TOKEN is required")

        admin_ids = _parse_admin_ids(os.getenv("ADMIN_IDS"))
        if not admin_ids:
            raise ValueError("ADMIN_IDS is required; add your Telegram numeric user id")

        bot_mode = os.getenv("BOT_MODE", "polling").strip().lower()
        if bot_mode not in {"polling", "webhook"}:
            raise ValueError("BOT_MODE must be either 'polling' or 'webhook'")

        webhook_url = os.getenv("WEBHOOK_URL", "").strip() or None
        if bot_mode == "webhook" and not webhook_url:
            raise ValueError("WEBHOOK_URL is required when BOT_MODE=webhook")

        webhook_path = os.getenv("WEBHOOK_PATH", "telegram/webhook").strip().strip("/")
        if not webhook_path:
            raise ValueError("WEBHOOK_PATH must not be empty")

        return cls(
            bot_token=bot_token,
            admin_ids=admin_ids,
            db_path=Path(os.getenv("DB_PATH", "/data/rwnodes.sqlite3")),
            bot_mode=bot_mode,
            webhook_url=webhook_url,
            webhook_listen=os.getenv("WEBHOOK_LISTEN", "0.0.0.0"),
            webhook_port=int(os.getenv("WEBHOOK_PORT", "8080")),
            webhook_path=webhook_path,
            webhook_secret_token=os.getenv("WEBHOOK_SECRET_TOKEN", "").strip() or None,
            ansible_timeout=int(os.getenv("ANSIBLE_TIMEOUT", "900")),
            ansible_host_key_checking=_parse_bool(os.getenv("ANSIBLE_HOST_KEY_CHECKING"), False),
            default_become=_parse_bool(os.getenv("DEFAULT_BECOME"), False),
            max_telegram_output_chars=int(os.getenv("MAX_TELEGRAM_OUTPUT_CHARS", "3500")),
            remnanode_playbook_path=Path(
                os.getenv("REMNANODE_PLAYBOOK_PATH", "/app/playbooks/remnanode_update.yml")
            ),
            managed_ssh_keys_dir=Path(os.getenv("MANAGED_SSH_KEYS_DIR", "/data/ssh_keys")),
            ssh_key_presets_dir=Path(os.getenv("SSH_KEY_PRESETS_DIR", "/data/ssh_key_presets")),
        )
