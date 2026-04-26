from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import Settings
from app.store import Node, NodeStore


@dataclass(frozen=True)
class AnsibleResult:
    action: str
    target: str
    returncode: int
    output: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class AnsibleRunner:
    def __init__(self, settings: Settings, store: NodeStore) -> None:
        self.settings = settings
        self.store = store

    def update_remnanode(self, target: str) -> AnsibleResult:
        playbook_path = self.settings.remnanode_playbook_path
        if not playbook_path.exists():
            raise ValueError(f"RemnaNode playbook does not exist: {playbook_path}")

        nodes = self.store.resolve_target(target)
        return self._run_playbook(
            action="update-remnanode",
            target=target,
            nodes=nodes,
            playbook_path=playbook_path,
        )

    def ping(self, target: str) -> AnsibleResult:
        nodes = self.store.resolve_target(target)
        return self._run_generated_playbook(
            action="ping",
            target=target,
            nodes=nodes,
            tasks=[
                {
                    "name": "Ping node",
                    "ansible.builtin.ping": {},
                }
            ],
        )

    def reboot(self, target: str) -> AnsibleResult:
        nodes = self.store.resolve_target(target)
        return self._run_generated_playbook(
            action="reboot",
            target=target,
            nodes=nodes,
            tasks=[
                {
                    "name": "Reboot node and wait for it",
                    "ansible.builtin.reboot": {
                        "connect_timeout": 20,
                        "reboot_timeout": 600,
                        "test_command": "whoami",
                    },
                }
            ],
        )

    def _run_generated_playbook(
        self,
        action: str,
        target: str,
        nodes: list[Node],
        tasks: list[dict[str, Any]],
    ) -> AnsibleResult:
        play = [
            {
                "name": action,
                "hosts": "managed",
                "gather_facts": False,
                "tasks": tasks,
            }
        ]
        with tempfile.TemporaryDirectory(prefix="rwnodes-ansible-") as tmp_dir:
            playbook_path = Path(tmp_dir) / "playbook.json"
            playbook_path.write_text(json.dumps(play), encoding="utf-8")
            return self._run_playbook(
                action=action,
                target=target,
                nodes=nodes,
                playbook_path=playbook_path,
                tmp_dir=Path(tmp_dir),
            )

    def _run_playbook(
        self,
        action: str,
        target: str,
        nodes: list[Node],
        playbook_path: Path,
        tmp_dir: Path | None = None,
    ) -> AnsibleResult:
        if tmp_dir is None:
            with tempfile.TemporaryDirectory(prefix="rwnodes-ansible-") as tmp_name:
                return self._run_playbook(action, target, nodes, playbook_path, Path(tmp_name))

        inventory_path = tmp_dir / "inventory.json"
        inventory_path.write_text(
            json.dumps(self._build_inventory(nodes), ensure_ascii=False),
            encoding="utf-8",
        )

        cmd = ["ansible-playbook", "-i", str(inventory_path), str(playbook_path)]
        env = os.environ.copy()
        env["ANSIBLE_HOST_KEY_CHECKING"] = (
            "True" if self.settings.ansible_host_key_checking else "False"
        )
        env["ANSIBLE_RETRY_FILES_ENABLED"] = "False"
        env["ANSIBLE_FORCE_COLOR"] = "0"

        try:
            completed = subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=self.settings.ansible_timeout,
                env=env,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("ansible-playbook was not found in PATH") from exc
        except subprocess.TimeoutExpired as exc:
            output = exc.stdout or ""
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            output = f"{output}\nTimed out after {self.settings.ansible_timeout} seconds"
            return AnsibleResult(action=action, target=target, returncode=124, output=output)

        return AnsibleResult(
            action=action,
            target=target,
            returncode=completed.returncode,
            output=completed.stdout or "",
        )

    def _build_inventory(self, nodes: list[Node]) -> dict[str, Any]:
        hosts: dict[str, dict[str, Any]] = {}
        for node in nodes:
            vars_for_node: dict[str, Any] = {
                "ansible_host": node.host,
                "ansible_user": node.user,
                "ansible_port": node.port,
                "ansible_become": node.become or self.settings.default_become,
            }
            if node.ssh_key_path:
                vars_for_node["ansible_ssh_private_key_file"] = node.ssh_key_path
            if node.password:
                vars_for_node["ansible_password"] = node.password
                vars_for_node["ansible_connection"] = "ssh"
                vars_for_node["ansible_ssh_common_args"] = (
                    "-o PreferredAuthentications=password "
                    "-o PubkeyAuthentication=no"
                )
            if node.become_password:
                vars_for_node["ansible_become_password"] = node.become_password
            hosts[node.name] = vars_for_node

        return {
            "all": {
                "children": {
                    "managed": {
                        "hosts": hosts,
                    }
                }
            }
        }
