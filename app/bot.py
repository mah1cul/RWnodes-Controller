from __future__ import annotations

import asyncio
import html
import os
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatAction, ParseMode
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, Document, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.ansible_runner import AnsibleResult, AnsibleRunner
from app.config import Settings
from app.store import (
    NODE_NAME_RE,
    PRESET_NAME_RE,
    RESERVED_NODE_NAMES,
    Node,
    NodeStore,
    Preset,
)


MAX_PRIVATE_KEY_BYTES = 128 * 1024
ADD_FIELDS = ("name", "user", "host", "port", "auth")
TEXT_PRESET_FIELDS = ("name", "user", "host", "port")


class BotController:
    def __init__(self, settings: Settings, store: NodeStore, runner: AnsibleRunner) -> None:
        self.settings = settings
        self.store = store
        self.runner = runner
        self.operation_lock = asyncio.Lock()
        self.sessions: dict[int, dict[str, Any]] = {}
        self.bot: Bot | None = None

    def build(self) -> tuple[Bot, Dispatcher]:
        bot = Bot(token=self.settings.bot_token)
        self.bot = bot
        dispatcher = Dispatcher()
        router = Router()

        router.message.register(self.start, CommandStart())
        router.callback_query.register(self.handle_button)
        router.message.register(self.handle_message, F.text | F.document)

        dispatcher.include_router(router)
        return bot, dispatcher

    async def start(self, message: Message) -> None:
        if not await self._require_admin_message(message):
            return

        self._clear_flow(message.from_user.id)
        await self._send_main_menu(message, "RWnodes Controller")

    async def handle_message(self, message: Message) -> None:
        if not await self._require_admin_message(message):
            return

        user_id = message.from_user.id
        flow = self._session(user_id).get("flow")
        if not flow:
            await self._send_main_menu(message, "Используй кнопки меню.")
            return

        flow_type = flow.get("type")
        if flow_type == "add_node":
            await self._handle_add_message(message, flow)
        elif flow_type == "run_shell":
            await self._handle_run_shell_message(message, flow)
        elif flow_type == "set_key":
            await self._handle_set_key_message(message, flow)
        elif flow_type == "preset_text":
            await self._handle_text_preset_message(message, flow)
        elif flow_type == "preset_key":
            await self._handle_key_preset_message(message, flow)
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
        try:
            await self._dispatch_button(query)
        except ValueError as exc:
            await query.message.answer(str(exc), reply_markup=self._main_keyboard())

    async def _dispatch_button(self, query: CallbackQuery) -> None:
        data = query.data or ""
        message = query.message
        user_id = query.from_user.id

        if data == "menu:main":
            self._clear_flow(user_id)
            await self._send_main_menu(message)
        elif data == "flow:cancel":
            self._clear_flow(user_id)
            await self._send_main_menu(message, "Действие отменено.")
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
        elif data == "ops:menu":
            await self._send_operations_menu(message)
        elif data.startswith("op:"):
            await self._handle_operation_button(message, user_id, data)
        elif data == "cmd:start":
            await self._send_target_menu(message, user_id, action="run_shell")
        elif data.startswith("cmd:target:"):
            await self._handle_run_shell_target(message, user_id, data)
        elif data == "presets:menu":
            await self._send_presets_menu(message)
        elif data.startswith("preset:"):
            await self._handle_preset_button(message, user_id, data)
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
            await message.answer("Этот пресет сейчас не подходит.")
            return

        preset = self.store.get_preset(field, preset_name)
        if not preset:
            await message.answer("Пресет не найден.")
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
            reply_markup=self._main_keyboard(),
        )

    async def _handle_node_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "open":
            node_name = self._get_ref(user_id, parts[2])
            await self._send_node_details(message, user_id, node_name)
        elif action in {"update", "ping"}:
            node_name = self._get_ref(user_id, parts[2])
            await self._run_node_action(message, action, node_name)
        elif action == "run":
            node_name = self._get_ref(user_id, parts[2])
            self._session(user_id)["flow"] = {"type": "run_shell", "data": {"target": node_name}}
            await message.answer(
                f"Напиши shell-команду для {node_name}.",
                reply_markup=self._cancel_keyboard(),
            )
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
                await message.answer(f"Нода {node_name} удалена.", reply_markup=self._main_keyboard())
            else:
                await message.answer("Нода не найдена.", reply_markup=self._main_keyboard())
        elif action == "setkey":
            node_name = self._get_ref(user_id, parts[2])
            self._session(user_id)["flow"] = {"type": "set_key", "data": {"node": node_name}}
            await message.answer(
                f"Пришли приватный SSH-ключ для {node_name} текстом или файлом.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "clearkey":
            node_name = self._get_ref(user_id, parts[2])
            node = self.store.get(node_name)
            if not node:
                await message.answer("Нода не найдена.", reply_markup=self._main_keyboard())
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
                )
            )
            await message.answer(
                f"SSH-ключ отвязан от ноды {node.name}.",
                reply_markup=self._main_keyboard(),
            )

    async def _handle_operation_button(self, message: Message, user_id: int, data: str) -> None:
        parts = data.split(":")
        if parts[1] in {"update", "ping"}:
            await self._send_target_menu(message, user_id, action=parts[1])
            return

        if parts[1] != "target":
            return

        action = parts[2]
        target = "all" if parts[3] == "all" else self._get_ref(user_id, parts[3])
        await self._run_node_action(message, action, target)

    async def _handle_run_shell_target(self, message: Message, user_id: int, data: str) -> None:
        token = data.split(":", 2)[2]
        target = "all" if token == "all" else self._get_ref(user_id, token)
        self._session(user_id)["flow"] = {"type": "run_shell", "data": {"target": target}}
        await message.answer(f"Напиши shell-команду для {target}.", reply_markup=self._cancel_keyboard())

    async def _handle_run_shell_message(self, message: Message, flow: dict[str, Any]) -> None:
        command = (message.text or "").strip()
        target = flow.get("data", {}).get("target")
        if not command:
            await message.answer("Команда не должна быть пустой.")
            return

        self._clear_flow(message.from_user.id)
        await self._run_ansible_action(
            message,
            label=f"Выполняю команду на {target}",
            action=lambda: self.runner.run_shell(target, command),
        )

    async def _handle_set_key_message(self, message: Message, flow: dict[str, Any]) -> None:
        node_name = flow.get("data", {}).get("node")
        node = self.store.get(node_name)
        if not node:
            self._clear_flow(message.from_user.id)
            await message.answer("Нода не найдена.", reply_markup=self._main_keyboard())
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
                )
            )
        except (OSError, ValueError) as exc:
            await message.answer(f"Ключ не сохранен: {exc}", reply_markup=self._cancel_keyboard())
            return

        self._clear_flow(message.from_user.id)
        await message.answer(
            f"SSH-ключ сохранен и привязан к ноде {node.name}.",
            reply_markup=self._main_keyboard(),
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
            await message.answer("Название кнопки пресета?", reply_markup=self._cancel_keyboard())
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
                f"Удалить пресет {field}/{preset_name}?",
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
                await message.answer("Пресет не найден.", reply_markup=self._main_keyboard())
                return
            if preset.field == "ssh_key":
                self._delete_key_preset_file(Path(preset.value))
            await message.answer(
                f"Пресет удален: {preset.field}/{preset.name}",
                reply_markup=self._main_keyboard(),
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
            await message.answer("Значение пресета?", reply_markup=self._cancel_keyboard())
            return

        if step == "value":
            field = data["field"]
            try:
                preset_value = self._validate_add_value(field, value)
                self.store.set_preset(Preset(field=field, name=data["name"], value=preset_value))
            except ValueError as exc:
                await message.answer(f"Пресет не сохранен: {exc}", reply_markup=self._cancel_keyboard())
                return

            self._clear_flow(message.from_user.id)
            await message.answer(
                f"Пресет сохранен: {field}/{data['name']}",
                reply_markup=self._main_keyboard(),
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
            await self._send_main_menu(message, "Сценарий пресета сброшен.")
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
        await message.answer(f"Пресет SSH-ключа сохранен: {data['name']}", reply_markup=self._main_keyboard())

    async def _send_main_menu(self, message: Message, text: str = "Главное меню") -> None:
        body = (
            f"{text}\n\n"
            "Разделы:\n"
            "Ноды - добавление, список и действия с конкретной нодой.\n"
            "Операции - массовый update, ping и выполнение команд.\n"
            "Пресеты - сохраненные значения для мастера добавления."
        )
        await message.answer(body, reply_markup=self._main_keyboard())

    async def _send_nodes_section(self, message: Message) -> None:
        await message.answer(
            "Раздел: Ноды",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить ноду", callback_data="add:start")],
                    [InlineKeyboardButton(text="Список нод", callback_data="nodes:list")],
                    self._home_row(),
                ]
            ),
        )

    async def _send_nodes_menu(self, message: Message, user_id: int) -> None:
        nodes = self.store.list()
        if not nodes:
            await message.answer("Ноды пока не добавлены.", reply_markup=self._main_keyboard())
            return

        lines = ["Ноды:"]
        rows: list[list[InlineKeyboardButton]] = []
        for node in nodes:
            lines.append(f"- {node.name}: {node.user}@{node.host}:{node.port} ({node.auth_summary})")
            token = self._remember_ref(user_id, node.name)
            rows.append([InlineKeyboardButton(text=node.name, callback_data=f"node:open:{token}")])

        rows.append(self._back_home_row("nodes:menu"))
        await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_node_details(self, message: Message, user_id: int, node_name: str) -> None:
        node = self.store.get(node_name)
        if not node:
            await message.answer("Нода не найдена.", reply_markup=self._main_keyboard())
            return

        token = self._remember_ref(user_id, node.name)
        text = (
            f"{node.name}\n"
            f"SSH: {node.user}@{node.host}:{node.port}\n"
            f"Auth: {node.auth_summary}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Update", callback_data=f"node:update:{token}"),
                    InlineKeyboardButton(text="Ping", callback_data=f"node:ping:{token}"),
                ],
                [InlineKeyboardButton(text="Выполнить команду", callback_data=f"node:run:{token}")],
                [
                    InlineKeyboardButton(text="Задать ключ", callback_data=f"node:setkey:{token}"),
                    InlineKeyboardButton(text="Убрать ключ", callback_data=f"node:clearkey:{token}"),
                ],
                [InlineKeyboardButton(text="Удалить", callback_data=f"node:delete:{token}")],
                self._back_home_row("nodes:list"),
            ]
        )
        await message.answer(text, reply_markup=keyboard)

    async def _send_operations_menu(self, message: Message) -> None:
        await message.answer(
            "Раздел: Операции",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Обновить RemnaNode", callback_data="op:update")],
                    [InlineKeyboardButton(text="Ping", callback_data="op:ping")],
                    [InlineKeyboardButton(text="Выполнить команду", callback_data="cmd:start")],
                    self._home_row(),
                ]
            ),
        )

    async def _send_target_menu(self, message: Message, user_id: int, action: str) -> None:
        nodes = self.store.list()
        if not nodes:
            await message.answer("Ноды пока не добавлены.", reply_markup=self._main_keyboard())
            return

        if action == "run_shell":
            prefix = "cmd:target"
            title = "Где выполнить команду?"
        else:
            prefix = f"op:target:{action}"
            title = "Выбери цель."

        rows = [[InlineKeyboardButton(text="Все ноды", callback_data=f"{prefix}:all")]]
        for node in nodes:
            token = self._remember_ref(user_id, node.name)
            rows.append([InlineKeyboardButton(text=node.name, callback_data=f"{prefix}:{token}")])
        rows.append(self._back_home_row("ops:menu"))
        await message.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_presets_menu(self, message: Message) -> None:
        await message.answer(
            "Раздел: Пресеты",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить текстовый пресет", callback_data="preset:add_text")],
                    [InlineKeyboardButton(text="Добавить SSH-ключ", callback_data="preset:add_key")],
                    [InlineKeyboardButton(text="Список пресетов", callback_data="preset:list")],
                    [InlineKeyboardButton(text="Удалить пресет", callback_data="preset:delete")],
                    self._home_row(),
                ]
            ),
        )

    async def _ask_text_preset_field(self, message: Message) -> None:
        rows = [
            [InlineKeyboardButton(text=field, callback_data=f"preset:text_field:{field}")]
            for field in TEXT_PRESET_FIELDS
        ]
        rows.append(self._cancel_row())
        await message.answer("Для какого поля пресет?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _ask_delete_preset_field(self, message: Message) -> None:
        rows = [
            [InlineKeyboardButton(text=field, callback_data=f"preset:delete_field:{field}")]
            for field in ("name", "user", "host", "port", "ssh_key")
        ]
        rows.append(self._back_home_row("presets:menu"))
        await message.answer("Пресеты какого поля удалить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_delete_preset_items(self, message: Message, user_id: int, field: str) -> None:
        presets = self.store.list_presets(field)
        if not presets:
            await message.answer("Для этого поля пресетов нет.", reply_markup=self._main_keyboard())
            return

        rows = []
        for preset in presets:
            token = self._remember_ref(user_id, (field, preset.name))
            rows.append([InlineKeyboardButton(text=preset.name, callback_data=f"preset:delete_item:{token}")])
        rows.append(self._back_home_row("presets:menu"))
        await message.answer("Выбери пресет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    async def _send_presets_list(self, message: Message) -> None:
        presets = self.store.list_presets()
        if not presets:
            await message.answer("Пресетов пока нет.", reply_markup=self._main_keyboard())
            return

        lines = ["Пресеты:"]
        for preset in presets:
            value = "stored private key" if preset.field == "ssh_key" else preset.value
            lines.append(f"- {preset.field}/{preset.name}: {value}")
        await message.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[self._back_home_row("presets:menu")]),
        )

    async def _run_node_action(self, message: Message, action: str, target: str) -> None:
        if action == "update":
            await self._run_ansible_action(
                message,
                label=f"Обновляю RemnaNode на {target}",
                action=lambda: self.runner.update_remnanode(target),
            )
        elif action == "ping":
            await self._run_ansible_action(
                message,
                label=f"Проверяю доступ к {target}",
                action=lambda: self.runner.ping(target),
            )

    async def _run_ansible_action(self, message: Message, label: str, action) -> None:
        await self._bot().send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
        await message.answer(label)

        try:
            async with self.operation_lock:
                result = await asyncio.to_thread(action)
        except Exception as exc:  # noqa: BLE001 - Telegram needs a clear operator-facing error.
            await message.answer(f"Ошибка: {exc}", reply_markup=self._main_keyboard())
            return

        await self._send_ansible_result(message, result)

    async def _send_ansible_result(self, message: Message, result: AnsibleResult) -> None:
        status = "OK" if result.ok else "FAILED"
        header = f"{status}: {result.action} target={result.target} exit_code={result.returncode}"
        output = result.output.strip() or "(no output)"
        max_chars = max(500, self.settings.max_telegram_output_chars)
        body = output[-max_chars:]
        if len(output) > max_chars:
            body = f"... output truncated to last {max_chars} chars ...\n{body}"

        await message.answer(header)
        await message.answer(
            f"<pre>{html.escape(body)}</pre>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._main_keyboard(),
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

    def _add_step_keyboard(self, user_id: int, field: str) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        for preset in self.store.list_presets(field):
            token = self._remember_ref(user_id, preset.name)
            rows.append([InlineKeyboardButton(text=preset.name, callback_data=f"add:pre:{field}:{token}")])

        if field == "port":
            rows.insert(0, [InlineKeyboardButton(text="22", callback_data="add:val:port:22")])
        if field == "auth":
            rows.extend(
                [
                    [InlineKeyboardButton(text="SSH-ключ", callback_data="add:auth:key")],
                    [InlineKeyboardButton(text="Пароль", callback_data="add:auth:password")],
                ]
            )
        if field == "ssh_key":
            rows.append([InlineKeyboardButton(text="Назад к способу входа", callback_data="add:back_auth")])
        if field == "password":
            rows.append([InlineKeyboardButton(text="Назад к способу входа", callback_data="add:back_auth")])

        rows.append(self._cancel_row())
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def _main_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Ноды", callback_data="nodes:menu"),
                    InlineKeyboardButton(text="Операции", callback_data="ops:menu"),
                ],
                [InlineKeyboardButton(text="Пресеты", callback_data="presets:menu")],
            ]
        )

    @staticmethod
    def _cancel_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[BotController._cancel_row()])

    @staticmethod
    def _home_row() -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton(text="Главное меню", callback_data="menu:main")]

    @staticmethod
    def _cancel_row() -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton(text="Отмена", callback_data="flow:cancel")]

    @staticmethod
    def _back_home_row(back_callback: str) -> list[InlineKeyboardButton]:
        return [
            InlineKeyboardButton(text="Назад", callback_data=back_callback),
            InlineKeyboardButton(text="Главное меню", callback_data="menu:main"),
        ]

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
