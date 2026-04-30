from __future__ import annotations

import asyncio
import html
import os
import shlex
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, Document, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.ansible_runner import AnsibleResult, AnsibleRunner
from app.config import Settings
from app.database.store import (
    NODE_NAME_RE,
    PRESET_NAME_RE,
    RESERVED_NODE_NAMES,
    Node,
    NodeStore,
    Preset,
)
from app.keyboards import (
    COUNTRY_CODE_ALIASES,
    DEFAULT_ICONS,
    ISO_COUNTRY_CODES,
    KeyboardMixin,
    NO_COUNTRY_FLAG,
    PREMIUM_FLAG_SETS,
    PREMIUM_ICON_IDS,
)


MAX_PRIVATE_KEY_BYTES = 128 * 1024
ADD_FIELDS = ("name", "user", "host", "port", "auth")
TEXT_PRESET_FIELDS = ("name", "user", "host", "port")


class EditableCallbackMessage:
    def __init__(self, message: Message, from_user: Any) -> None:
        self._message = message
        self.from_user = from_user
        self.chat = message.chat
        self.prefers_edit = True
        self._answered_once = False

    async def answer(self, text: str, **kwargs: Any) -> Message:
        if not self._answered_once:
            self._answered_once = True
            return await self.replace(text, **kwargs)
        return await self._message.answer(text, **kwargs)

    async def replace(self, text: str, **kwargs: Any) -> Message:
        try:
            return await self._message.edit_text(text, **kwargs)
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return self._message
            return await self._message.answer(text, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._message, name)


class BotHandlers(KeyboardMixin):
    def __init__(self, settings: Settings, store: NodeStore, runner: AnsibleRunner) -> None:
        self.settings = settings
        self.store = store
        self.runner = runner
        self.operation_lock = asyncio.Lock()
        self.sessions: dict[int, dict[str, Any]] = {}
        self.bot: Bot | None = None
        self.premium_icons_checked = False
        self.premium_icons_disabled = False
        self.premium_flag_ids: dict[str, str] = {}
        self.premium_flags_loaded = False

    def set_bot(self, bot: Bot) -> None:
        self.bot = bot

    async def start(self, message: Message) -> None:
        if not await self._require_admin_message(message):
            return

        await self._ensure_premium_emoji_ready(message)
        self._clear_flow(message.from_user.id)
        await self._send_main_menu(message, "RWnodes Controller")

    async def handle_message(self, message: Message) -> None:
        if not await self._require_admin_message(message):
            return

        await self._ensure_premium_emoji_ready(message)
        user_id = message.from_user.id
        flow = self._session(user_id).get("flow")
        if not flow:
            await self._send_main_menu(message, "Используй кнопки меню.")
            return

        flow_type = flow.get("type")
        if flow_type == "add_node":
            await self._handle_add_message(message, flow)
        elif flow_type == "set_key":
            await self._handle_set_key_message(message, flow)
        elif flow_type == "preset_text":
            await self._handle_text_preset_message(message, flow)
        elif flow_type == "preset_key":
            await self._handle_key_preset_message(message, flow)
        elif flow_type == "api_key":
            await self._handle_api_key_message(message, flow)
        elif flow_type == "edit_node":
            await self._handle_edit_node_message(message, flow)
        elif flow_type == "edit_password":
            await self._handle_edit_password_message(message, flow)
        else:
            self._clear_flow(user_id)
            await self._send_main_menu(message, "Сценарий сброшен. Выбери действие.")

    async def handle_button(self, query: CallbackQuery) -> None:
        if not await self._require_admin_callback(query):
            return

        if not query.data or not query.message:
            await query.answer()
            return

        await query.answer()
        await self._ensure_premium_emoji_ready(query.message)
        message = EditableCallbackMessage(query.message, query.from_user)
        try:
            await self._dispatch_button(query, message)
        except ValueError as exc:
            await message.answer(str(exc), reply_markup=self._main_keyboard(query.from_user.id))

    async def _dispatch_button(self, query: CallbackQuery, message: Message) -> None:
        data = query.data or ""
        user_id = query.from_user.id

        if data == "menu:main":
            self._clear_flow(user_id)
            await self._send_main_menu(message)
        elif data == "flow:cancel":
            self._clear_flow(user_id)
            await self._send_main_menu(message, "Действие отменено.")
        elif data == "add:menu":
            self._clear_flow(user_id)
            await self._send_add_method_menu(message)
        elif data == "add:script":
            self._clear_flow(user_id)
            await self._send_add_script_instructions(message)
        elif data == "add:start":
            await self._begin_add_wizard(message, user_id)
        elif data.startswith("add:val:"):
            await self._handle_add_fixed_value(message, user_id, data)
        elif data.startswith("add:pre:"):
            await self._handle_add_preset_value(message, user_id, data)
        elif data.startswith("add:auth:"):
            await self._handle_add_auth_choice(message, user_id, data)
        elif data == "add:back_auth":
            await self._back_to_add_auth(message, user_id)
        elif data == "add:skip_key":
            await self._finish_add_wizard(message, user_id)
        elif data == "nodes:menu":
            await self._send_nodes_section(message)
        elif data == "nodes:list":
            await self._send_nodes_menu(message, user_id)
        elif data.startswith("node:"):
            await self._handle_node_button(message, user_id, data)
        elif data.startswith("edit:"):
            await self._handle_edit_button(message, user_id, data)
        elif data.startswith("secret:"):
            await self._handle_secret_button(message, user_id, data)
        elif data == "ops:menu":
            await self._send_operations_menu(message)
        elif data.startswith("op:"):
            await self._handle_operation_button(message, user_id, data)
        elif data == "presets:menu":
            await self._send_presets_menu(message)
        elif data.startswith("preset:"):
            await self._handle_preset_button(message, user_id, data)
        elif data == "api:menu":
            await self._send_api_keys_menu(message)
        elif data.startswith("api:"):
            await self._handle_api_key_button(message, user_id, data)
        else:
            await self._send_main_menu(message, "Кнопка устарела. Выбери действие заново.")

    async def _begin_add_wizard(self, message: Message, user_id: int) -> None:
        self._session(user_id)["flow"] = {"type": "add_node", "step": "name", "data": {}}
        await self._ask_add_step(message, user_id, "name")

    async def _handle_add_message(self, message: Message, flow: dict[str, Any]) -> None:
        user_id = message.from_user.id
        step = flow.get("step")

        if step in {"name", "user", "host", "port"}:
            await self._accept_add_text_value(message, user_id, step, message.text or "")
        elif step == "ssh_key":
            if message.text:
                await self._finish_add_wizard(message, user_id, key_text=message.text)
            elif message.document:
                await self._finish_add_wizard(message, user_id, document=message.document)
            else:
                await message.answer(
                    "Пришли приватный SSH-ключ текстом или файлом.",
                    reply_markup=self._add_step_keyboard(user_id, "ssh_key"),
                )
        elif step == "password":
            await self._finish_add_wizard(message, user_id, password=message.text or "")
        elif step == "auth":
            await message.answer("Выбери способ входа кнопкой.", reply_markup=self._add_step_keyboard(user_id, "auth"))
        else:
            self._clear_flow(user_id)
            await self._send_main_menu(message, "Мастер добавления сброшен.")

    async def _handle_add_fixed_value(self, message: Message, user_id: int, data: str) -> None:
        _, _, field, value = data.split(":", 3)
        await self._accept_add_text_value(message, user_id, field, value)

    async def _handle_add_auth_choice(self, message: Message, user_id: int, data: str) -> None:
        auth_method = data.rsplit(":", 1)[-1]
        flow = self._session(user_id).get("flow") or {}
        if flow.get("type") != "add_node" or flow.get("step") != "auth":
            await self._send_main_menu(message, "Мастер добавления неактивен.")
            return

        add_data = flow.setdefault("data", {})
        if auth_method == "key":
            add_data["auth_method"] = "key"
            flow["step"] = "ssh_key"
            await self._ask_add_step(message, user_id, "ssh_key")
        elif auth_method == "password":
            add_data["auth_method"] = "password"
            flow["step"] = "password"
            await self._ask_add_step(message, user_id, "password")
        else:
            await message.answer("Неизвестный способ входа.", reply_markup=self._add_step_keyboard(user_id, "auth"))

    async def _back_to_add_auth(self, message: Message, user_id: int) -> None:
        flow = self._session(user_id).get("flow") or {}
        if flow.get("type") != "add_node":
            await self._send_main_menu(message, "Мастер добавления неактивен.")
            return

        flow["step"] = "auth"
        flow.setdefault("data", {}).pop("auth_method", None)
        await self._ask_add_step(message, user_id, "auth")

    async def _handle_add_preset_value(self, message: Message, user_id: int, data: str) -> None:
        _, _, field, token = data.split(":", 3)
        preset_name = self._get_ref(user_id, token)
        flow = self._session(user_id).get("flow") or {}

        if flow.get("type") != "add_node" or flow.get("step") != field:
            await message.answer("Этот пресет параметров сейчас не подходит.")
            return

        preset = self.store.get_preset(field, preset_name)
        if not preset:
            await message.answer("Пресет параметров не найден.")
            return

        if field == "ssh_key":
            await self._finish_add_wizard(message, user_id, preset_key_path=Path(preset.value))
            return

        await self._accept_add_text_value(message, user_id, field, preset.value)

    async def _accept_add_text_value(
        self,
        message: Message,
        user_id: int,
        field: str,
        raw_value: str,
    ) -> None:
        flow = self._session(user_id).get("flow") or {}
        if flow.get("type") != "add_node" or flow.get("step") != field:
            await self._send_main_menu(message, "Мастер добавления неактивен.")
            return

        try:
            value = self._validate_add_value(field, raw_value)
        except ValueError as exc:
            await message.answer(
                f"Некорректное значение: {exc}",
                reply_markup=self._add_step_keyboard(user_id, field),
            )
            return

        flow.setdefault("data", {})[field] = value
        next_step = self._next_add_step(field)
        if next_step:
            flow["step"] = next_step
            await self._ask_add_step(message, user_id, next_step)
        else:
            await self._finish_add_wizard(message, user_id)

    async def _ask_add_step(self, message: Message, user_id: int, field: str) -> None:
        prompts = {
            "name": "Название ноды? Например: node1",
            "user": "Пользователь SSH? Например: root",
            "host": "IP или hostname ноды?",
            "port": "SSH-порт?",
            "auth": "Как подключаться к ноде?",
            "ssh_key": "Пришли приватный SSH-ключ текстом или файлом. Если есть пресет, нажми кнопку.",
            "password": "Напиши SSH-пароль для этой ноды.",
        }
        await message.answer(prompts[field], reply_markup=self._add_step_keyboard(user_id, field))

    async def _finish_add_wizard(
        self,
        message: Message,
        user_id: int,
        key_text: str | None = None,
        document: Document | None = None,
        preset_key_path: Path | None = None,
        password: str | None = None,
    ) -> None:
        flow = self._session(user_id).get("flow") or {}
        add_data = flow.get("data") or {}
        missing = [field for field in ("name", "user", "host", "port") if field not in add_data]
        if missing:
            self._clear_flow(user_id)
            await self._send_main_menu(message, "Мастер добавления потерял контекст. Начни заново.")
            return

        name = add_data["name"]
        password = password.strip() if password else None
        auth_method = add_data.get("auth_method")
        if auth_method not in {"key", "password"}:
            flow["step"] = "auth"
            await message.answer("Выбери способ входа.", reply_markup=self._add_step_keyboard(user_id, "auth"))
            return
        if auth_method == "password" and not password:
            await message.answer("Пароль не должен быть пустым.", reply_markup=self._add_step_keyboard(user_id, "password"))
            return
        if auth_method == "key" and not (key_text or document or preset_key_path):
            flow["step"] = "ssh_key"
            await message.answer("Нужен SSH-ключ или пресет ключа.", reply_markup=self._add_step_keyboard(user_id, "ssh_key"))
            return

        key_path = self._managed_key_path(name) if key_text or document or preset_key_path else None
        node = Node(
            name=name,
            host=add_data["host"],
            user=add_data["user"],
            port=int(add_data["port"]),
            ssh_key_path=str(key_path) if key_path else None,
            password=password,
            country_code=self._infer_country_code(name),
        )
        existing = self.store.get(name)

        try:
            if key_text and key_path:
                self._write_private_key_text(key_path, key_text)
            elif document and key_path:
                await self._download_private_key_document(self._bot(), key_path, document)
            elif preset_key_path and key_path:
                self._copy_private_key(preset_key_path, key_path)

            self.store.add_or_update(node)
        except (OSError, ValueError) as exc:
            flow["step"] = "ssh_key"
            await message.answer(
                f"Нода не сохранена: {exc}",
                reply_markup=self._add_step_keyboard(user_id, "ssh_key"),
            )
            return

        if existing and existing.ssh_key_path != node.ssh_key_path:
            self._delete_managed_key(existing)

        self._clear_flow(user_id)
        if node.ssh_key_path:
            auth = "с SSH-ключом"
        elif node.password:
            auth = "с паролем"
        else:
            auth = "без авторизации"
        await message.answer(
            f"Нода сохранена: {node.name} {node.user}@{node.host}:{node.port}, {auth}",
            reply_markup=self._back_keyboard("nodes:menu", user_id),
        )

    async def _handle_node_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "open":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_node_details(message, user_id, node_name)
        elif action in {"update", "ping"}:
            node_name = self._get_ref(user_id, parts[2])
            await self._run_node_action(message, action, node_name, back_callback=f"node:open:{parts[2]}")
        elif action == "reboot":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_reboot_warning(message, user_id, node_name, back_callback=f"node:open:{parts[2]}")
        elif action == "delete":
            node_name = self._get_ref(user_id, parts[2])
            token = self._remember_ref(user_id, node_name)
            await message.answer(
                f"Удалить ноду {node_name}?",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Удалить", callback_data=f"node:delete_yes:{token}")],
                        [InlineKeyboardButton(text="Отмена", callback_data="nodes:list")],
                    ]
                ),
            )
        elif action == "delete_yes":
            node_name = self._get_ref(user_id, parts[2])
            node = self.store.get(node_name)
            deleted = self.store.delete(node_name)
            if deleted:
                if node:
                    self._delete_managed_key(node)
                await message.answer(f"Нода {node_name} удалена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            else:
                await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
        elif action == "setkey":
            node_name = self._get_ref(user_id, parts[2])
            self._session(user_id)["flow"] = {"type": "set_key", "data": {"node": node_name}}
            await message.answer(
                f"Пришли приватный SSH-ключ для {node_name} текстом или файлом.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "edit":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_edit_node_menu(message, user_id, node_name)
        elif action == "secret":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_secret_menu(message, user_id, node_name)
        elif action == "clearkey":
            node_name = self._get_ref(user_id, parts[2])
            node = self.store.get(node_name)
            if not node:
                await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
                return

            self._delete_managed_key(node)
            self.store.add_or_update(
                Node(
                    name=node.name,
                    host=node.host,
                    user=node.user,
                    port=node.port,
                    ssh_key_path=None,
                    password=node.password,
                    become=node.become,
                    become_password=node.become_password,
                    country_code=node.country_code,
                )
            )
            await message.answer(
                f"SSH-ключ отвязан от ноды {node.name}.",
                reply_markup=self._back_keyboard(f"node:open:{parts[2]}", user_id),
            )

    async def _handle_edit_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "field":
            field = parts[2]
            node_name = self._get_ref(user_id, parts[3])
            node = self.store.get(node_name)
            if not node:
                await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
                return

            self._session(user_id)["flow"] = {
                "type": "edit_node",
                "data": {"node": node_name, "field": field},
            }
            labels = {
                "name": "новое название ноды",
                "user": "нового SSH-пользователя",
                "host": "новый IP или hostname",
                "port": "новый SSH-порт",
                "country": "код страны ноды, например RU или DE. Напиши none, чтобы сбросить",
            }
            await message.answer(
                f"Напиши {labels[field]} для {node_name}.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "auth":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_edit_auth_menu(message, user_id, node_name)
        elif action == "auth_key":
            node_name = self._get_ref(user_id, parts[2])
            self._session(user_id)["flow"] = {"type": "set_key", "data": {"node": node_name}}
            await message.answer(
                f"Пришли новый приватный SSH-ключ для {node_name} текстом или файлом.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "auth_password":
            node_name = self._get_ref(user_id, parts[2])
            self._session(user_id)["flow"] = {"type": "edit_password", "data": {"node": node_name}}
            await message.answer(
                f"Напиши новый SSH-пароль для {node_name}.",
                reply_markup=self._cancel_keyboard(),
            )

    async def _handle_secret_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]
        node_name = self._get_ref(user_id, parts[2])
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            return

        if action == "password":
            if not node.password:
                await message.answer(
                    "У этой ноды нет сохраненного пароля.",
                    reply_markup=self._back_keyboard(f"node:secret:{parts[2]}", user_id),
                )
                return
            await self._send_secret_text(
                message,
                f"Пароль для {node.name}",
                node.password,
                back_callback=f"node:secret:{self._remember_ref(user_id, node.name)}",
            )
        elif action == "key":
            if not node.ssh_key_path:
                await message.answer(
                    "У этой ноды нет сохраненного SSH-ключа.",
                    reply_markup=self._back_keyboard(f"node:secret:{parts[2]}", user_id),
                )
                return
            try:
                key_text = Path(node.ssh_key_path).read_text(encoding="utf-8")
            except OSError as exc:
                await message.answer(
                    f"Не удалось прочитать ключ: {exc}",
                    reply_markup=self._back_keyboard(f"node:secret:{parts[2]}", user_id),
                )
                return
            await self._send_secret_text(
                message,
                f"SSH-ключ для {node.name}",
                key_text,
                back_callback=f"node:secret:{self._remember_ref(user_id, node.name)}",
            )

    async def _handle_operation_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        if parts[1] in {"update", "ping", "reboot"}:
            await self._send_target_menu(message, user_id, action=parts[1])
            return

        if parts[1] == "confirm" and parts[2] == "reboot":
            target = self._get_ref(user_id, parts[3])
            await self._run_node_action(message, "reboot", target)
            return

        if parts[1] != "target":
            return

        action = parts[2]
        target = "all" if parts[3] == "all" else self._get_ref(user_id, parts[3])
        if action == "reboot":
            await self._send_reboot_warning(message, user_id, target, back_callback="ops:menu")
            return
        await self._run_node_action(message, action, target, back_callback="ops:menu")

    async def _handle_set_key_message(self, message: Message, flow: dict[str, Any]) -> None:
        node_name = flow.get("data", {}).get("node")
        node = self.store.get(node_name)
        if not node:
            self._clear_flow(message.from_user.id)
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", message.from_user.id))
            return

        key_path = self._managed_key_path(node.name)
        try:
            if message.text:
                self._write_private_key_text(key_path, message.text)
            elif message.document:
                await self._download_private_key_document(self._bot(), key_path, message.document)
            else:
                await message.answer("Пришли приватный ключ текстом или файлом.")
                return

            self.store.add_or_update(
                Node(
                    name=node.name,
                    host=node.host,
                    user=node.user,
                    port=node.port,
                    ssh_key_path=str(key_path),
                    password=None,
                    become=node.become,
                    become_password=node.become_password,
                    country_code=node.country_code,
                )
            )
        except (OSError, ValueError) as exc:
            await message.answer(f"Ключ не сохранен: {exc}", reply_markup=self._cancel_keyboard())
            return

        self._clear_flow(message.from_user.id)
        await message.answer(
            f"SSH-ключ сохранен и привязан к ноде {node.name}.",
            reply_markup=self._back_keyboard(f"node:open:{self._remember_ref(message.from_user.id, node.name)}", message.from_user.id),
        )

    async def _handle_edit_node_message(self, message: Message, flow: dict[str, Any]) -> None:
        data = flow.get("data", {})
        node_name = data.get("node")
        field = data.get("field")
        node = self.store.get(node_name)
        if not node:
            self._clear_flow(message.from_user.id)
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", message.from_user.id))
            return

        try:
            if field == "country":
                value = self._validate_country_code_value(message.text or "")
            else:
                value = self._validate_add_value(field, message.text or "")
            updated_node = self._updated_node(node, field, value)
            if field == "name" and value != node.name and self.store.get(value):
                raise ValueError("нода с таким названием уже существует")
            self._save_node_edit(node, updated_node)
        except (OSError, ValueError) as exc:
            await message.answer(f"Не удалось изменить ноду: {exc}", reply_markup=self._cancel_keyboard())
            return

        self._clear_flow(message.from_user.id)
        await message.answer(
            f"Нода обновлена: {updated_node.name} {updated_node.user}@{updated_node.host}:{updated_node.port}",
            reply_markup=self._back_keyboard(
                f"node:open:{self._remember_ref(message.from_user.id, updated_node.name)}",
                message.from_user.id,
            ),
        )

    async def _handle_edit_password_message(self, message: Message, flow: dict[str, Any]) -> None:
        node_name = flow.get("data", {}).get("node")
        node = self.store.get(node_name)
        if not node:
            self._clear_flow(message.from_user.id)
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", message.from_user.id))
            return

        password = (message.text or "").strip()
        if not password:
            await message.answer("Пароль не должен быть пустым.", reply_markup=self._cancel_keyboard())
            return

        self._delete_managed_key(node)
        self.store.add_or_update(
            Node(
                name=node.name,
                host=node.host,
                user=node.user,
                port=node.port,
                ssh_key_path=None,
                password=password,
                become=node.become,
                become_password=node.become_password,
                country_code=node.country_code,
            )
        )
        self._clear_flow(message.from_user.id)
        await message.answer(
            f"Пароль для {node.name} обновлен.",
            reply_markup=self._back_keyboard(f"node:open:{self._remember_ref(message.from_user.id, node.name)}", message.from_user.id),
        )

    async def _handle_preset_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "add_text":
            self._session(user_id)["flow"] = {"type": "preset_text", "step": "field", "data": {}}
            await self._ask_text_preset_field(message)
        elif action == "text_field":
            field = parts[2]
            self._session(user_id)["flow"] = {
                "type": "preset_text",
                "step": "name",
                "data": {"field": field},
            }
            await message.answer("Название кнопки пресета параметров?", reply_markup=self._cancel_keyboard())
        elif action == "add_key":
            self._session(user_id)["flow"] = {"type": "preset_key", "step": "name", "data": {}}
            await message.answer("Название кнопки для SSH-ключа?", reply_markup=self._cancel_keyboard())
        elif action == "list":
            await self._send_presets_list(message)
        elif action == "delete":
            await self._ask_delete_preset_field(message)
        elif action == "delete_field":
            await self._send_delete_preset_items(message, user_id, parts[2])
        elif action == "delete_item":
            field, preset_name = self._get_ref(user_id, parts[2])
            token = self._remember_ref(user_id, (field, preset_name))
            await message.answer(
                f"Удалить пресет параметров {field}/{preset_name}?",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Удалить", callback_data=f"preset:delete_yes:{token}")],
                        [InlineKeyboardButton(text="Отмена", callback_data="presets:menu")],
                    ]
                ),
            )
        elif action == "delete_yes":
            field, preset_name = self._get_ref(user_id, parts[2])
            preset = self.store.delete_preset(field, preset_name)
            if not preset:
                await message.answer("Пресет параметров не найден.", reply_markup=self._back_keyboard("presets:menu", user_id))
                return
            if preset.field == "ssh_key":
                self._delete_key_preset_file(Path(preset.value))
            await message.answer(
                f"Пресет параметров удален: {preset.field}/{preset.name}",
                reply_markup=self._back_keyboard("presets:menu", user_id),
            )

    async def _handle_text_preset_message(self, message: Message, flow: dict[str, Any]) -> None:
        step = flow.get("step")
        value = (message.text or "").strip()
        data = flow.setdefault("data", {})

        if step == "name":
            if not PRESET_NAME_RE.match(value):
                await message.answer(
                    "Название: 1-32 символа, латиница/цифры/./_/-",
                    reply_markup=self._cancel_keyboard(),
                )
                return
            data["name"] = value
            flow["step"] = "value"
            await message.answer("Значение пресета параметров?", reply_markup=self._cancel_keyboard())
            return

        if step == "value":
            field = data["field"]
            try:
                preset_value = self._validate_add_value(field, value)
                self.store.set_preset(Preset(field=field, name=data["name"], value=preset_value))
            except ValueError as exc:
                await message.answer(f"Пресет параметров не сохранен: {exc}", reply_markup=self._cancel_keyboard())
                return

            self._clear_flow(message.from_user.id)
            await message.answer(
                f"Пресет параметров сохранен: {field}/{data['name']}",
                reply_markup=self._back_keyboard("presets:menu", message.from_user.id),
            )

    async def _handle_key_preset_message(self, message: Message, flow: dict[str, Any]) -> None:
        step = flow.get("step")
        data = flow.setdefault("data", {})

        if step == "name":
            name = (message.text or "").strip()
            if not PRESET_NAME_RE.match(name):
                await message.answer(
                    "Название: 1-32 символа, латиница/цифры/./_/-",
                    reply_markup=self._cancel_keyboard(),
                )
                return
            data["name"] = name
            flow["step"] = "key"
            await message.answer("Пришли приватный SSH-ключ текстом или файлом.", reply_markup=self._cancel_keyboard())
            return

        if step != "key":
            self._clear_flow(message.from_user.id)
            await self._send_main_menu(message, "Сценарий пресета параметров сброшен.")
            return

        key_path = self._key_preset_path(data["name"])
        try:
            if message.text:
                self._write_private_key_text(key_path, message.text)
            elif message.document:
                await self._download_private_key_document(self._bot(), key_path, message.document)
            else:
                await message.answer("Пришли приватный ключ текстом или файлом.")
                return

            self.store.set_preset(Preset(field="ssh_key", name=data["name"], value=str(key_path)))
        except (OSError, ValueError) as exc:
            try:
                key_path.unlink(missing_ok=True)
            except OSError:
                pass
            await message.answer(f"Пресет ключа не сохранен: {exc}")
            return

        self._clear_flow(message.from_user.id)
        await message.answer(
            f"Пресет SSH-ключа сохранен: {data['name']}",
            reply_markup=self._back_keyboard("presets:menu", message.from_user.id),
        )

    async def _handle_api_key_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "add":
            self._session(user_id)["flow"] = {"type": "api_key"}
            await message.answer("Название API ключа? Например: node-register", reply_markup=self._cancel_keyboard())
        elif action == "list":
            await self._send_api_keys_list(message)
        elif action == "delete":
            await self._send_delete_api_key_items(message, user_id)
        elif action == "delete_item":
            key_name = self._get_ref(user_id, parts[2])
            token = self._remember_ref(user_id, key_name)
            await message.answer(
                f"Удалить API ключ {key_name}?",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [self._button("Удалить", f"api:delete_yes:{token}", icon="delete", user_id=user_id)],
                        self._back_home_row("api:menu", user_id),
                    ]
                ),
            )
        elif action == "delete_yes":
            key_name = self._get_ref(user_id, parts[2])
            deleted = self.store.delete_api_key(key_name)
            if deleted:
                await message.answer(f"API ключ удален: {key_name}", reply_markup=self._back_keyboard("api:menu", user_id))
            else:
                await message.answer("API ключ не найден.", reply_markup=self._back_keyboard("api:menu", user_id))

    async def _handle_api_key_message(self, message: Message, flow: dict[str, Any]) -> None:
        name = (message.text or "").strip()
        if not name:
            await message.answer("Название API ключа не должно быть пустым.", reply_markup=self._cancel_keyboard())
            return

        try:
            raw_key = self.store.create_api_key(name)
        except ValueError as exc:
            await message.answer(f"API ключ не создан: {exc}", reply_markup=self._cancel_keyboard())
            return

        self._clear_flow(message.from_user.id)
        await message.answer(
            (
                f"API ключ создан: {html.escape(name)}\n\n"
                "Сохрани его сейчас, потом в боте будет виден только список имен:\n"
                f"<pre>{html.escape(raw_key)}</pre>"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=self._back_keyboard("api:menu", message.from_user.id),
        )

    async def _send_main_menu(self, message: Message, text: str = "Главное меню") -> None:
        nodes = self.store.list()
        if nodes:
            node_lines = ["\nНоды:"]
            for node in nodes[:20]:
                node_lines.append(
                    f"- {self._node_flag_html(node)} {html.escape(node.name)}: {html.escape(node.host)}"
                )
            if len(nodes) > 20:
                node_lines.append(f"... и еще {len(nodes) - 20}")
            nodes_text = "\n".join(node_lines)
        else:
            nodes_text = "\n\nНоды: пока не добавлены"

        body = (
            f"{text}\n\n"
            f"{nodes_text}"
        )
        await message.answer(
            body,
            parse_mode=ParseMode.HTML,
            reply_markup=self._main_keyboard(message.from_user.id),
        )

    async def _send_nodes_section(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Раздел: Ноды",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Добавить ноду", "add:menu", icon="add", user_id=user_id)],
                    [self._button("Список нод", "nodes:list", icon="list", user_id=user_id)],
                    self._home_row(user_id),
                ]
            ),
        )

    async def _send_add_method_menu(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Как добавить ноду?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Добавить вручную", "add:start", icon="edit", user_id=user_id)],
                    [self._button("Скрипт добавления", "add:script", icon="api", user_id=user_id)],
                    self._back_home_row("nodes:menu", user_id),
                ]
            ),
        )

    async def _send_add_script_instructions(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        base_url = self._public_controller_url()
        script_url = f"{base_url}/scripts/addnode"
        api_key_arg = " --apikey APIKEY" if self.store.has_api_keys() else ""
        command_prefix = (
            f"curl -fsSL {shlex.quote(script_url)} | sudo env "
            f"RWNODES_API_URL={shlex.quote(base_url)} "
            f"RWNODES_ADDNODE_PATH={shlex.quote(self.settings.addnode_path)} "
            "bash -s --"
        )
        placeholder_note = ""
        if not self.settings.webhook_url:
            placeholder_note = (
                "\n\nWEBHOOK_URL не задан, поэтому в примерах стоит домен-заглушка. "
                "Замени его на публичный адрес, который видит добавляемая нода."
            )

        key_command = (
            f"{command_prefix} -U root --key /root/.ssh/id_ed25519{api_key_arg}"
        )
        password_command = (
            f"{command_prefix} -U root --pass 'SSHPASSWORD'{api_key_arg}"
        )
        interface_command = (
            f"{command_prefix} -U root -I wg0 --name RU-1-Node --key /root/.ssh/id_ed25519{api_key_arg}"
        )

        text = (
            "Скрипт запускается прямо на сервере, который нужно добавить. "
            "Адрес API передается через RWNODES_API_URL без флага --url. "
            "Он сам определит имя, SSH-порт и IP, если не указать их явно.\n\n"
            "С SSH-ключом:\n"
            f"<pre>{html.escape(key_command)}</pre>\n"
            "С SSH-паролем:\n"
            f"<pre>{html.escape(password_command)}</pre>\n"
            "С конкретным интерфейсом и названием:\n"
            f"<pre>{html.escape(interface_command)}</pre>\n"
            "Параметры: -U пользователь, -P порт, -I интерфейс, --name название, "
            "--key путь_к_ключу, --pass пароль, --apikey ключ API."
            f"{placeholder_note}"
        )
        await message.answer(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Добавить вручную", "add:start", icon="edit", user_id=user_id)],
                    self._back_home_row("add:menu", user_id),
                ]
            ),
        )

    async def _send_nodes_menu(self, message: Message, user_id: int) -> None:
        nodes = self.store.list()
        if not nodes:
            await message.answer("Ноды пока не добавлены.", reply_markup=self._back_keyboard("nodes:menu", user_id))
            return

        lines = ["Ноды:"]
        rows: list[list[InlineKeyboardButton]] = []
        for node in nodes:
            lines.append(
                f"- {self._node_flag_html(node)} {html.escape(node.name)}: "
                f"{html.escape(node.user)}@{html.escape(node.host)}:{node.port} ({node.auth_summary})"
            )
            token = self._remember_ref(user_id, node.name)
            rows.append(
                [
                    self._button(
                        node.name,
                        f"node:open:{token}",
                        user_id=user_id,
                        fallback_icon=self._node_flag_text(node),
                        custom_emoji_id=self._node_flag_custom_emoji_id(node),
                    )
                ]
            )

        rows.append(self._back_home_row("nodes:menu", user_id))
        await message.answer(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )

    def _public_controller_url(self) -> str:
        return (self.settings.webhook_url or "https://bot.example.com").rstrip("/")

    def _back_keyboard(self, back_callback: str, user_id: int | None = None) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[self._back_home_row(back_callback, user_id)])

    async def _send_node_details(self, message: Message, user_id: int, node_name: str) -> None:
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            return

        token = self._remember_ref(user_id, node.name)
        country_code = self._node_country_code(node)
        country_text = country_code or "не задана"
        text = (
            f"{self._node_flag_html(node)} {html.escape(node.name)}\n"
            f"SSH: {html.escape(node.user)}@{html.escape(node.host)}:{node.port}\n"
            f"Страна: {html.escape(country_text)}\n"
            f"Auth: {node.auth_summary}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    self._button("Update", f"node:update:{token}", icon="update", user_id=user_id),
                    self._button("Ping", f"node:ping:{token}", icon="ping", user_id=user_id),
                ],
                [self._button("Reboot", f"node:reboot:{token}", icon="reboot", user_id=user_id)],
                [
                    self._button("Изменить", f"node:edit:{token}", icon="edit", user_id=user_id),
                    self._button("Показать доступ", f"node:secret:{token}", icon="secret", user_id=user_id),
                ],
                [
                    self._button("Задать ключ", f"node:setkey:{token}", icon="key", user_id=user_id),
                    self._button("Убрать ключ", f"node:clearkey:{token}", icon="key", user_id=user_id),
                ],
                [self._button("Удалить", f"node:delete:{token}", icon="delete", user_id=user_id)],
                self._back_home_row("nodes:list", user_id),
            ]
        )
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

    async def _send_edit_node_menu(self, message: Message, user_id: int, node_name: str) -> None:
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            return

        token = self._remember_ref(user_id, node.name)
        await message.answer(
            f"Что изменить у {node.name}?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Название", callback_data=f"edit:field:name:{token}"),
                        InlineKeyboardButton(text="Пользователь", callback_data=f"edit:field:user:{token}"),
                    ],
                    [
                        InlineKeyboardButton(text="IP/host", callback_data=f"edit:field:host:{token}"),
                        InlineKeyboardButton(text="Порт", callback_data=f"edit:field:port:{token}"),
                    ],
                    [InlineKeyboardButton(text="Страна", callback_data=f"edit:field:country:{token}")],
                    [InlineKeyboardButton(text="Способ входа", callback_data=f"edit:auth:{token}")],
                    self._back_home_row(f"node:open:{token}"),
                ]
            ),
        )

    async def _send_edit_auth_menu(self, message: Message, user_id: int, node_name: str) -> None:
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            return

        token = self._remember_ref(user_id, node.name)
        await message.answer(
            f"Новый способ входа для {node.name}:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="SSH-ключ", callback_data=f"edit:auth_key:{token}")],
                    [InlineKeyboardButton(text="Пароль", callback_data=f"edit:auth_password:{token}")],
                    self._back_home_row(f"node:edit:{token}"),
                ]
            ),
        )

    async def _send_secret_menu(self, message: Message, user_id: int, node_name: str) -> None:
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._back_keyboard("nodes:list", user_id))
            return

        token = self._remember_ref(user_id, node.name)
        rows: list[list[InlineKeyboardButton]] = []
        if node.password:
            rows.append([InlineKeyboardButton(text="Показать пароль", callback_data=f"secret:password:{token}")])
        if node.ssh_key_path:
            rows.append([InlineKeyboardButton(text="Показать SSH-ключ", callback_data=f"secret:key:{token}")])
        if not rows:
            rows.append([InlineKeyboardButton(text="Секрет не задан", callback_data=f"node:open:{token}")])

        rows.append(self._back_home_row(f"node:open:{token}"))
        await message.answer(
            f"Доступ для {node.name}. Эти данные будут отправлены в чат.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )

    async def _send_operations_menu(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Раздел: Операции",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Обновить RemnaNode", "op:update", icon="update", user_id=user_id)],
                    [self._button("Ping", "op:ping", icon="ping", user_id=user_id)],
                    [self._button("Reboot", "op:reboot", icon="reboot", user_id=user_id)],
                    self._home_row(user_id),
                ]
            ),
        )

    async def _send_target_menu(self, message: Message, user_id: int, action: str) -> None:
        nodes = self.store.list()
        if not nodes:
            await message.answer("Ноды пока не добавлены.", reply_markup=self._back_keyboard("ops:menu", user_id))
            return

        prefix = f"op:target:{action}"
        title = "Выбери цель."

        rows = [[self._button("Все ноды", f"{prefix}:all", icon="nodes", user_id=user_id)]]
        for node in nodes:
            token = self._remember_ref(user_id, node.name)
            rows.append(
                [
                    self._button(
                        node.name,
                        f"{prefix}:{token}",
                        user_id=user_id,
                        fallback_icon=self._node_flag_text(node),
                        custom_emoji_id=self._node_flag_custom_emoji_id(node),
                    )
                ]
            )
        rows.append(self._back_home_row("ops:menu", user_id))
        await message.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_presets_menu(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Раздел: Пресеты параметров",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Добавить текстовый пресет параметров", "preset:add_text", icon="add", user_id=user_id)],
                    [self._button("Добавить SSH-ключ", "preset:add_key", icon="key", user_id=user_id)],
                    [self._button("Список пресетов параметров", "preset:list", icon="list", user_id=user_id)],
                    [self._button("Удалить пресет параметров", "preset:delete", icon="delete", user_id=user_id)],
                    self._home_row(user_id),
                ]
            ),
        )

    async def _ask_text_preset_field(self, message: Message) -> None:
        rows = [
            [InlineKeyboardButton(text=field, callback_data=f"preset:text_field:{field}")]
            for field in TEXT_PRESET_FIELDS
        ]
        rows.append(self._cancel_row())
        await message.answer("Для какого поля пресет параметров?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _ask_delete_preset_field(self, message: Message) -> None:
        rows = [
            [InlineKeyboardButton(text=field, callback_data=f"preset:delete_field:{field}")]
            for field in ("name", "user", "host", "port", "ssh_key")
        ]
        rows.append(self._back_home_row("presets:menu"))
        await message.answer("Пресеты параметров какого поля удалить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_delete_preset_items(self, message: Message, user_id: int, field: str) -> None:
        presets = self.store.list_presets(field)
        if not presets:
            await message.answer(
                "Для этого поля пресетов параметров нет.",
                reply_markup=self._back_keyboard("presets:menu", user_id),
            )
            return

        rows = []
        for preset in presets:
            token = self._remember_ref(user_id, (field, preset.name))
            rows.append([InlineKeyboardButton(text=preset.name, callback_data=f"preset:delete_item:{token}")])
        rows.append(self._back_home_row("presets:menu"))
        await message.answer("Выбери пресет параметров.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_presets_list(self, message: Message) -> None:
        presets = self.store.list_presets()
        if not presets:
            user_id = message.from_user.id if message.from_user else None
            await message.answer(
                "Пресетов параметров пока нет.",
                reply_markup=self._back_keyboard("presets:menu", user_id),
            )
            return

        lines = ["Пресеты параметров:"]
        for preset in presets:
            value = "stored private key" if preset.field == "ssh_key" else preset.value
            lines.append(f"- {preset.field}/{preset.name}: {value}")
        await message.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[self._back_home_row("presets:menu")]),
        )

    async def _send_api_keys_menu(self, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Раздел: API ключи",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Создать API ключ", "api:add", icon="add", user_id=user_id)],
                    [self._button("Список API ключей", "api:list", icon="list", user_id=user_id)],
                    [self._button("Удалить API ключ", "api:delete", icon="delete", user_id=user_id)],
                    self._home_row(user_id),
                ]
            ),
        )

    async def _send_api_keys_list(self, message: Message) -> None:
        keys = self.store.list_api_keys()
        if not keys:
            user_id = message.from_user.id if message.from_user else None
            await message.answer(
                "API ключей пока нет. /addnode будет доступен без apikey.",
                reply_markup=self._back_keyboard("api:menu", user_id),
            )
            return

        lines = ["API ключи:"]
        for key in keys:
            lines.append(f"- {key.name}: создан {key.created_at}")
        await message.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[self._back_home_row("api:menu")]),
        )

    async def _send_delete_api_key_items(self, message: Message, user_id: int) -> None:
        keys = self.store.list_api_keys()
        if not keys:
            await message.answer("API ключей пока нет.", reply_markup=self._back_keyboard("api:menu", user_id))
            return

        rows = []
        for key in keys:
            token = self._remember_ref(user_id, key.name)
            rows.append([InlineKeyboardButton(text=key.name, callback_data=f"api:delete_item:{token}")])
        rows.append(self._back_home_row("api:menu", user_id))
        await message.answer("Выбери API ключ.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_reboot_warning(
        self,
        message: Message,
        user_id: int,
        target: str,
        back_callback: str,
    ) -> None:
        token = self._remember_ref(user_id, target)
        await message.answer(
            (
                f"{DEFAULT_ICONS['warning']} Критическое действие\n\n"
                f"Цель: {html.escape(target)}\n"
                "Нода будет перезагружена через Ansible. Во время reboot соединение и сервисы будут недоступны."
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [self._button("Подтвердить reboot", f"op:confirm:reboot:{token}", icon="warning", user_id=user_id)],
                    self._back_home_row(back_callback, user_id),
                ]
            ),
        )

    async def _run_node_action(
        self,
        message: Message,
        action: str,
        target: str,
        back_callback: str = "ops:menu",
    ) -> None:
        if action == "update":
            await self._run_ansible_action(
                message,
                label=f"Обновляю RemnaNode на {target}",
                action=lambda: self.runner.update_remnanode(target),
                back_callback=back_callback,
            )
        elif action == "ping":
            await self._run_ansible_action(
                message,
                label=f"Проверяю доступ к {target}",
                action=lambda: self.runner.ping(target),
                back_callback=back_callback,
            )
        elif action == "reboot":
            await self._run_ansible_action(
                message,
                label=f"Перезагружаю {target}",
                action=lambda: self.runner.reboot(target),
                back_callback=back_callback,
            )

    async def _run_ansible_action(self, message: Message, label: str, action, back_callback: str) -> None:
        await self._bot().send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
        await message.answer(label)

        user_id = message.from_user.id if message.from_user else None
        try:
            async with self.operation_lock:
                result = await asyncio.to_thread(action)
        except Exception as exc:  # noqa: BLE001 - Telegram needs a clear operator-facing error.
            if getattr(message, "prefers_edit", False):
                await message.replace(
                    f"Ошибка: {exc}",
                    reply_markup=self._back_keyboard(back_callback, user_id),
                )
                return
            await message.answer(f"Ошибка: {exc}", reply_markup=self._back_keyboard(back_callback, user_id))
            return

        await self._send_ansible_result(message, result, back_callback)

    async def _send_ansible_result(self, message: Message, result: AnsibleResult, back_callback: str) -> None:
        status = "OK" if result.ok else "FAILED"
        header = f"{status}: {result.action} target={result.target} exit_code={result.returncode}"
        output = result.output.strip() or "(no output)"
        max_chars = max(500, self.settings.max_telegram_output_chars)
        if getattr(message, "prefers_edit", False):
            max_chars = min(max_chars, 3300)
        body = output[-max_chars:]
        if len(output) > max_chars:
            body = f"... output truncated to last {max_chars} chars ...\n{body}"

        if getattr(message, "prefers_edit", False):
            await message.replace(
                f"{html.escape(header)}\n\n<pre>{html.escape(body)}</pre>",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._back_keyboard(back_callback, message.from_user.id if message.from_user else None),
            )
            return

        await message.answer(header)
        await message.answer(
            f"<pre>{html.escape(body)}</pre>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._back_keyboard(back_callback, message.from_user.id if message.from_user else None),
        )

    async def _send_secret_text(
        self,
        message: Message,
        title: str,
        value: str,
        back_callback: str,
    ) -> None:
        max_chars = 3500
        chunks = [value[i : i + max_chars] for i in range(0, len(value), max_chars)] or [""]
        if getattr(message, "prefers_edit", False) and len(chunks) == 1:
            await message.replace(
                f"{html.escape(title)}\n\n<pre>{html.escape(chunks[0])}</pre>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[self._back_home_row(back_callback)]),
            )
            return

        await message.answer(title)
        for index, chunk in enumerate(chunks, start=1):
            suffix = f" ({index}/{len(chunks)})" if len(chunks) > 1 else ""
            await message.answer(
                f"<pre>{html.escape(chunk)}</pre>{suffix}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[self._back_home_row(back_callback)])
                if index == len(chunks)
                else None,
            )

    async def _require_admin_message(self, message: Message) -> bool:
        user = message.from_user
        if user and user.id in self.settings.admin_ids:
            return True

        user_id = user.id if user else "unknown"
        await message.answer(f"Доступ запрещен. Telegram user id: {user_id}")
        return False

    async def _require_admin_callback(self, query: CallbackQuery) -> bool:
        user = query.from_user
        if user and user.id in self.settings.admin_ids:
            return True

        await query.answer("Доступ запрещен", show_alert=True)
        if query.message:
            await query.message.answer(f"Доступ запрещен. Telegram user id: {user.id if user else 'unknown'}")
        return False

    async def _ensure_premium_emoji_ready(self, message: Message) -> None:
        if not self.settings.premium_emoji_mode or self.premium_icons_disabled:
            return

        if not self.premium_icons_checked:
            self.premium_icons_checked = True
            try:
                rows = []
                for key in PREMIUM_ICON_IDS:
                    rows.append([self._button(DEFAULT_ICONS.get(key, key), "menu:main", icon=key)])
                sent = await message.answer("Проверка premium emoji", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
                try:
                    await sent.delete()
                except Exception:
                    pass
            except TelegramBadRequest:
                self.premium_icons_disabled = True
                return

        await self._load_premium_flag_ids()

    async def _load_premium_flag_ids(self) -> None:
        if self.premium_flags_loaded or not self._can_use_premium_icons():
            return

        self.premium_flags_loaded = True
        bot = self._bot()
        for set_name in PREMIUM_FLAG_SETS:
            try:
                sticker_set = await bot.get_sticker_set(set_name)
            except Exception:
                continue

            for sticker in sticker_set.stickers:
                custom_emoji_id = getattr(sticker, "custom_emoji_id", None)
                emoji = getattr(sticker, "emoji", None)
                country_code = self._country_code_from_flag_emoji(emoji or "")
                if custom_emoji_id and country_code:
                    self.premium_flag_ids.setdefault(country_code, custom_emoji_id)

    def _can_use_premium_icons(self) -> bool:
        return self.settings.premium_emoji_mode and not self.premium_icons_disabled

    def _node_country_code(self, node: Node) -> str | None:
        return node.country_code or self._infer_country_code(node.name)

    def _node_flag_text(self, node: Node) -> str:
        country_code = self._node_country_code(node)
        return self._country_flag_emoji(country_code) if country_code else NO_COUNTRY_FLAG

    def _node_flag_custom_emoji_id(self, node: Node) -> str | None:
        if not self._can_use_premium_icons():
            return None
        country_code = self._node_country_code(node)
        if not country_code:
            return None
        return self.premium_flag_ids.get(country_code)

    def _node_flag_html(self, node: Node) -> str:
        flag = self._node_flag_text(node)
        custom_emoji_id = self._node_flag_custom_emoji_id(node)
        if custom_emoji_id:
            return f'<tg-emoji emoji-id="{html.escape(custom_emoji_id)}">{flag}</tg-emoji>'
        return flag

    @classmethod
    def _infer_country_code(cls, name: str) -> str | None:
        prefix = name.strip()[:2].upper()
        if len(prefix) != 2 or not prefix.isalpha():
            return None
        return cls._normalize_country_code(prefix)

    @staticmethod
    def _normalize_country_code(value: str) -> str | None:
        code = value.strip().upper()
        if code in {"", "-", "NONE", "NO", "OFF", "NULL"}:
            return None
        code = COUNTRY_CODE_ALIASES.get(code, code)
        return code if code in ISO_COUNTRY_CODES else None

    @classmethod
    def _validate_country_code_value(cls, raw_value: str | None) -> str:
        code = cls._normalize_country_code(raw_value or "")
        if code is None:
            raw = (raw_value or "").strip().upper()
            if raw in {"", "-", "NONE", "NO", "OFF", "NULL"}:
                return ""
            raise ValueError("код страны должен быть ISO alpha-2, например RU, DE или US")
        return code

    @staticmethod
    def _country_flag_emoji(country_code: str | None) -> str:
        if not country_code:
            return NO_COUNTRY_FLAG
        return "".join(chr(0x1F1E6 + ord(char) - ord("A")) for char in country_code[:2])

    @staticmethod
    def _country_code_from_flag_emoji(flag: str) -> str | None:
        values: list[int] = []
        for char in flag:
            value = ord(char) - 0x1F1E6
            if 0 <= value <= 25:
                values.append(value)
        if len(values) < 2:
            return None
        code = "".join(chr(ord("A") + value) for value in values[:2])
        return code if code in ISO_COUNTRY_CODES else None

    @staticmethod
    def _next_add_step(field: str) -> str | None:
        index = ADD_FIELDS.index(field)
        next_index = index + 1
        return ADD_FIELDS[next_index] if next_index < len(ADD_FIELDS) else None

    @staticmethod
    def _validate_add_value(field: str, raw_value: str | None) -> str:
        value = (raw_value or "").strip()
        if not value:
            raise ValueError("значение не должно быть пустым")

        if field == "name":
            if value in RESERVED_NODE_NAMES:
                raise ValueError(f"имя {value!r} зарезервировано")
            if not NODE_NAME_RE.match(value):
                raise ValueError("имя: 1-64 символа, латиница/цифры/./_/-")
        elif field == "port":
            try:
                port = int(value)
            except ValueError as exc:
                raise ValueError("порт должен быть числом") from exc
            if not 1 <= port <= 65535:
                raise ValueError("порт должен быть от 1 до 65535")
            value = str(port)
        elif field not in {"user", "host"}:
            raise ValueError(f"неизвестное поле {field}")

        return value

    def _updated_node(self, node: Node, field: str, value: str) -> Node:
        values: dict[str, Any] = {
            "name": node.name,
            "host": node.host,
            "user": node.user,
            "port": node.port,
            "ssh_key_path": node.ssh_key_path,
            "password": node.password,
            "become": node.become,
            "become_password": node.become_password,
            "country_code": node.country_code,
        }
        if field == "port":
            values[field] = int(value)
        elif field in {"name", "user", "host"}:
            values[field] = value
            if field == "name" and not node.country_code:
                values["country_code"] = self._infer_country_code(value)
        elif field == "country":
            values["country_code"] = value or None
        else:
            raise ValueError(f"неизвестное поле {field}")

        return Node(**values)

    def _save_node_edit(self, old_node: Node, new_node: Node) -> None:
        if old_node.name == new_node.name:
            self.store.add_or_update(new_node)
            return

        new_key_path = self._renamed_managed_key_path(old_node, new_node.name)
        old_key_path = Path(old_node.ssh_key_path) if old_node.ssh_key_path else None
        should_move_key = bool(new_key_path and old_key_path and old_key_path.exists())
        if should_move_key and new_key_path:
            if new_key_path.exists():
                raise ValueError(f"файл ключа уже существует: {new_key_path}")
            new_node = Node(
                name=new_node.name,
                host=new_node.host,
                user=new_node.user,
                port=new_node.port,
                ssh_key_path=str(new_key_path),
                password=new_node.password,
                become=new_node.become,
                become_password=new_node.become_password,
                country_code=new_node.country_code,
            )

        key_was_moved = False
        try:
            if should_move_key and new_key_path and old_key_path:
                old_key_path.rename(new_key_path)
                key_was_moved = True

            self.store.add_or_update(new_node)
            deleted = self.store.delete(old_node.name)
            if not deleted:
                raise ValueError("старая нода не найдена во время переименования")
        except Exception:
            if key_was_moved and new_key_path and old_key_path and new_key_path.exists():
                try:
                    new_key_path.rename(old_key_path)
                except OSError:
                    pass
            raise

    def _renamed_managed_key_path(self, old_node: Node, new_name: str) -> Path | None:
        if not old_node.ssh_key_path:
            return None

        old_key_path = Path(old_node.ssh_key_path)
        try:
            old_key_path.resolve(strict=False).relative_to(
                self.settings.managed_ssh_keys_dir.resolve(strict=False)
            )
        except ValueError:
            return None

        return self._managed_key_path(new_name)

    def _managed_key_path(self, node_name: str) -> Path:
        return self.settings.managed_ssh_keys_dir / f"{node_name}.key"

    def _key_preset_path(self, preset_name: str) -> Path:
        if not PRESET_NAME_RE.match(preset_name):
            raise ValueError("имя пресета: 1-32 символа, латиница/цифры/./_/-")
        return self.settings.ssh_key_presets_dir / f"{preset_name}.key"

    def _write_private_key_text(self, key_path: Path, key_text: str) -> None:
        normalized = self._normalize_private_key(key_text)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_text(normalized, encoding="utf-8")
        os.chmod(key_path, 0o600)

    def _copy_private_key(self, source_path: Path, target_path: Path) -> None:
        key_text = source_path.read_text(encoding="utf-8")
        self._write_private_key_text(target_path, key_text)

    async def _download_private_key_document(
        self,
        bot: Bot,
        key_path: Path,
        document: Document,
    ) -> None:
        if document.file_size and document.file_size > MAX_PRIVATE_KEY_BYTES:
            raise ValueError("файл ключа слишком большой")

        key_path.parent.mkdir(parents=True, exist_ok=True)
        file = await bot.get_file(document.file_id)
        await bot.download_file(file.file_path, destination=key_path)

        try:
            key_text = key_path.read_text(encoding="utf-8")
            normalized = self._normalize_private_key(key_text)
            key_path.write_text(normalized, encoding="utf-8")
            os.chmod(key_path, 0o600)
        except Exception:
            try:
                key_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    @staticmethod
    def _normalize_private_key(key_text: str) -> str:
        normalized = key_text.strip().replace("\r\n", "\n").replace("\r", "\n")
        encoded_len = len(normalized.encode("utf-8"))
        if encoded_len > MAX_PRIVATE_KEY_BYTES:
            raise ValueError("ключ слишком большой")
        if not normalized.startswith("-----BEGIN ") or "PRIVATE KEY-----" not in normalized:
            raise ValueError("это не похоже на приватный SSH-ключ")
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

    def _delete_key_preset_file(self, key_path: Path) -> None:
        try:
            key_path.resolve(strict=False).relative_to(
                self.settings.ssh_key_presets_dir.resolve(strict=False)
            )
        except ValueError:
            return

        try:
            key_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _session(self, user_id: int) -> dict[str, Any]:
        return self.sessions.setdefault(user_id, {})

    def _bot(self) -> Bot:
        if not self.bot:
            raise RuntimeError("Bot is not initialized")
        return self.bot

    def _clear_flow(self, user_id: int) -> None:
        self._session(user_id).pop("flow", None)

    def _remember_ref(self, user_id: int, value: Any) -> str:
        session = self._session(user_id)
        refs = session.setdefault("callback_refs", {})
        counter = int(session.get("callback_ref_counter", 0)) + 1
        session["callback_ref_counter"] = counter

        if len(refs) > 200:
            refs.clear()

        token = str(counter)
        refs[token] = value
        return token

    def _get_ref(self, user_id: int, token: str) -> Any:
        refs = self._session(user_id).get("callback_refs", {})
        if token not in refs:
            raise ValueError("Кнопка устарела. Открой меню заново.")
        return refs[token]


def register_handlers(dp: Dispatcher, handlers: BotHandlers) -> None:
    @dp.message(CommandStart())
    async def start(message: Message) -> None:
        await handlers.start(message)

    async def route_callback(query: CallbackQuery) -> None:
        await handlers.handle_button(query)

    @dp.callback_query(F.data == "menu:main")
    async def menu_main(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data == "flow:cancel")
    async def flow_cancel(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data == "add:start")
    async def add_start(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("add:"))
    async def add_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.in_({"nodes:menu", "nodes:list"}))
    async def nodes_menu(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("node:"))
    async def node_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("edit:"))
    async def edit_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("secret:"))
    async def secret_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data == "ops:menu")
    async def operations_menu(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("op:"))
    async def operation_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data == "presets:menu")
    async def parameter_presets_menu(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("preset:"))
    async def parameter_preset_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data == "api:menu")
    async def api_keys_menu(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query(F.data.startswith("api:"))
    async def api_key_callbacks(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.callback_query()
    async def stale_callback(query: CallbackQuery) -> None:
        await route_callback(query)

    @dp.message(F.text | F.document)
    async def text_or_document(message: Message) -> None:
        await handlers.handle_message(message)
