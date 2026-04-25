from __future__ import annotations

import asyncio
import html
import os
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.ansible_runner import AnsibleResult, AnsibleRunner
from app.config import Settings
from app.store import (
    NODE_NAME_RE,
    PRESET_FIELDS,
    PRESET_NAME_RE,
    RESERVED_NODE_NAMES,
    Node,
    NodeStore,
    Preset,
)


MAX_PRIVATE_KEY_BYTES = 128 * 1024
ADD_FIELDS = ("name", "user", "host", "port", "ssh_key")
TEXT_PRESET_FIELDS = ("name", "user", "host", "port")
PRESET_FIELD_ALIASES = {
    "name": "name",
    "node": "name",
    "user": "user",
    "username": "user",
    "host": "host",
    "ip": "host",
    "address": "host",
    "port": "port",
    "key": "ssh_key",
    "ssh_key": "ssh_key",
    "ssh-key": "ssh_key",
}


class BotController:
    def __init__(self, settings: Settings, store: NodeStore, runner: AnsibleRunner) -> None:
        self.settings = settings
        self.store = store
        self.runner = runner
        self.operation_lock = asyncio.Lock()

    def build_application(self) -> Application:
        application = Application.builder().token(self.settings.bot_token).build()
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CallbackQueryHandler(self.handle_button))
        application.add_handler(MessageHandler(filters.TEXT | filters.Document.ALL, self.handle_message))
        return application

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return

        self._clear_flow(context)
        await self._send_main_menu(update, "RWnodes Controller")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return

        flow = context.user_data.get("flow")
        if not flow:
            await self._send_main_menu(update, "Используй кнопки меню.")
            return

        flow_type = flow.get("type")
        if flow_type == "add_node":
            await self._handle_add_message(update, context, flow)
        elif flow_type == "run_shell":
            await self._handle_run_shell_message(update, context, flow)
        elif flow_type == "set_key":
            await self._handle_set_key_message(update, context, flow)
        elif flow_type == "preset_text":
            await self._handle_text_preset_message(update, context, flow)
        elif flow_type == "preset_key":
            await self._handle_key_preset_message(update, context, flow)
        else:
            self._clear_flow(context)
            await self._send_main_menu(update, "Сценарий сброшен. Выбери действие.")

    async def handle_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return

        query = update.callback_query
        if not query or not query.data:
            return

        await query.answer()
        try:
            await self._dispatch_button(update, context, query.data)
        except ValueError as exc:
            await update.effective_message.reply_text(str(exc), reply_markup=self._main_keyboard())

    async def _dispatch_button(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        if data == "menu:main":
            self._clear_flow(context)
            await self._send_main_menu(update)
        elif data == "flow:cancel":
            self._clear_flow(context)
            await self._send_main_menu(update, "Действие отменено.")
        elif data == "add:start":
            await self._begin_add_wizard(update, context)
        elif data.startswith("add:val:"):
            await self._handle_add_fixed_value(update, context, data)
        elif data.startswith("add:pre:"):
            await self._handle_add_preset_value(update, context, data)
        elif data == "add:skip_key":
            await self._finish_add_wizard(update, context)
        elif data == "nodes:list":
            await self._send_nodes_menu(update, context)
        elif data.startswith("node:"):
            await self._handle_node_button(update, context, data)
        elif data == "ops:menu":
            await self._send_operations_menu(update)
        elif data.startswith("op:"):
            await self._handle_operation_button(update, context, data)
        elif data == "cmd:start":
            await self._send_target_menu(update, context, action="run_shell")
        elif data.startswith("cmd:target:"):
            await self._handle_run_shell_target(update, context, data)
        elif data == "presets:menu":
            await self._send_presets_menu(update)
        elif data.startswith("preset:"):
            await self._handle_preset_button(update, context, data)
        else:
            await self._send_main_menu(update, "Кнопка устарела. Выбери действие заново.")

    async def _begin_add_wizard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data["flow"] = {"type": "add_node", "step": "name", "data": {}}
        await self._ask_add_step(update, context, "name")

    async def _handle_add_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: dict[str, Any],
    ) -> None:
        step = flow.get("step")
        message = update.effective_message

        if step in {"name", "user", "host", "port"}:
            await self._accept_add_text_value(update, context, step, message.text or "")
        elif step == "ssh_key":
            if message.text:
                await self._finish_add_wizard(update, context, key_text=message.text)
            elif message.document:
                await self._finish_add_wizard(update, context, document=message.document)
            else:
                await message.reply_text(
                    "Пришли приватный SSH-ключ текстом или файлом.",
                    reply_markup=self._add_step_keyboard(context, "ssh_key"),
                )
        else:
            self._clear_flow(context)
            await self._send_main_menu(update, "Мастер добавления сброшен.")

    async def _handle_add_fixed_value(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        _, _, field, value = data.split(":", 3)
        await self._accept_add_text_value(update, context, field, value)

    async def _handle_add_preset_value(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        _, _, field, token = data.split(":", 3)
        preset_name = self._get_ref(context, token)
        flow = context.user_data.get("flow") or {}

        if flow.get("type") != "add_node" or flow.get("step") != field:
            await update.effective_message.reply_text("Этот пресет сейчас не подходит.")
            return

        preset = self.store.get_preset(field, preset_name)
        if not preset:
            await update.effective_message.reply_text("Пресет не найден.")
            return

        if field == "ssh_key":
            await self._finish_add_wizard(update, context, preset_key_path=Path(preset.value))
            return

        await self._accept_add_text_value(update, context, field, preset.value)

    async def _accept_add_text_value(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
        raw_value: str,
    ) -> None:
        flow = context.user_data.get("flow") or {}
        if flow.get("type") != "add_node" or flow.get("step") != field:
            await self._send_main_menu(update, "Мастер добавления неактивен.")
            return

        try:
            value = self._validate_add_value(field, raw_value)
        except ValueError as exc:
            await update.effective_message.reply_text(
                f"Некорректное значение: {exc}",
                reply_markup=self._add_step_keyboard(context, field),
            )
            return

        flow.setdefault("data", {})[field] = value
        next_step = self._next_add_step(field)
        if next_step:
            flow["step"] = next_step
            await self._ask_add_step(update, context, next_step)
        else:
            await self._finish_add_wizard(update, context)

    async def _ask_add_step(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
    ) -> None:
        prompts = {
            "name": "Название ноды? Например: node1",
            "user": "Пользователь SSH? Например: root",
            "host": "IP или hostname ноды?",
            "port": "SSH-порт?",
            "ssh_key": "Пришли приватный SSH-ключ текстом или файлом. Если есть пресет, нажми кнопку.",
        }
        await update.effective_message.reply_text(
            prompts[field],
            reply_markup=self._add_step_keyboard(context, field),
        )

    async def _finish_add_wizard(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        key_text: str | None = None,
        document=None,
        preset_key_path: Path | None = None,
    ) -> None:
        flow = context.user_data.get("flow") or {}
        add_data = flow.get("data") or {}
        missing = [field for field in ("name", "user", "host", "port") if field not in add_data]
        if missing:
            self._clear_flow(context)
            await self._send_main_menu(update, "Мастер добавления потерял контекст. Начни заново.")
            return

        name = add_data["name"]
        key_path = self._managed_key_path(name) if key_text or document or preset_key_path else None
        node = Node(
            name=name,
            host=add_data["host"],
            user=add_data["user"],
            port=int(add_data["port"]),
            ssh_key_path=str(key_path) if key_path else None,
        )
        existing = self.store.get(name)

        try:
            if key_text and key_path:
                self._write_private_key_text(key_path, key_text)
            elif document and key_path:
                await self._download_private_key_document(key_path, document)
            elif preset_key_path and key_path:
                self._copy_private_key(preset_key_path, key_path)

            self.store.add_or_update(node)
        except (OSError, ValueError) as exc:
            await update.effective_message.reply_text(
                f"Нода не сохранена: {exc}",
                reply_markup=self._add_step_keyboard(context, "ssh_key"),
            )
            flow["step"] = "ssh_key"
            return

        if existing and existing.ssh_key_path != node.ssh_key_path:
            self._delete_managed_key(existing)

        self._clear_flow(context)
        auth = "с ключом" if node.ssh_key_path else "без ключа"
        await update.effective_message.reply_text(
            f"Нода сохранена: {node.name} {node.user}@{node.host}:{node.port}, {auth}",
            reply_markup=self._main_keyboard(),
        )

    async def _handle_node_button(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "open":
            node_name = self._get_ref(context, parts[2])
            await self._send_node_details(update, context, node_name)
        elif action in {"update", "ping"}:
            node_name = self._get_ref(context, parts[2])
            await self._run_node_action(update, action, node_name)
        elif action == "run":
            node_name = self._get_ref(context, parts[2])
            context.user_data["flow"] = {
                "type": "run_shell",
                "data": {"target": node_name},
            }
            await update.effective_message.reply_text(
                f"Напиши shell-команду для {node_name}.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "delete":
            node_name = self._get_ref(context, parts[2])
            token = self._remember_ref(context, node_name)
            await update.effective_message.reply_text(
                f"Удалить ноду {node_name}?",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("Удалить", callback_data=f"node:delete_yes:{token}")],
                        [InlineKeyboardButton("Отмена", callback_data="nodes:list")],
                    ]
                ),
            )
        elif action == "delete_yes":
            node_name = self._get_ref(context, parts[2])
            node = self.store.get(node_name)
            deleted = self.store.delete(node_name)
            if deleted:
                if node:
                    self._delete_managed_key(node)
                await update.effective_message.reply_text(
                    f"Нода {node_name} удалена.",
                    reply_markup=self._main_keyboard(),
                )
            else:
                await update.effective_message.reply_text("Нода не найдена.")
        elif action == "setkey":
            node_name = self._get_ref(context, parts[2])
            context.user_data["flow"] = {"type": "set_key", "data": {"node": node_name}}
            await update.effective_message.reply_text(
                f"Пришли приватный SSH-ключ для {node_name} текстом или файлом.",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "clearkey":
            node_name = self._get_ref(context, parts[2])
            node = self.store.get(node_name)
            if not node:
                await update.effective_message.reply_text("Нода не найдена.")
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
            await update.effective_message.reply_text(
                f"SSH-ключ отвязан от ноды {node.name}.",
                reply_markup=self._main_keyboard(),
            )

    async def _handle_operation_button(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        parts = data.split(":")
        if parts[1] in {"update", "ping"}:
            await self._send_target_menu(update, context, action=parts[1])
            return

        if parts[1] != "target":
            return

        action = parts[2]
        target = "all" if parts[3] == "all" else self._get_ref(context, parts[3])
        await self._run_node_action(update, action, target)

    async def _handle_run_shell_target(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        token = data.split(":", 2)[2]
        target = "all" if token == "all" else self._get_ref(context, token)
        context.user_data["flow"] = {"type": "run_shell", "data": {"target": target}}
        await update.effective_message.reply_text(
            f"Напиши shell-команду для {target}.",
            reply_markup=self._cancel_keyboard(),
        )

    async def _handle_run_shell_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: dict[str, Any],
    ) -> None:
        command = (update.effective_message.text or "").strip()
        target = flow.get("data", {}).get("target")
        if not command:
            await update.effective_message.reply_text("Команда не должна быть пустой.")
            return

        self._clear_flow(context)
        await self._run_ansible_action(
            update,
            label=f"Выполняю команду на {target}",
            action=lambda: self.runner.run_shell(target, command),
        )

    async def _handle_set_key_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: dict[str, Any],
    ) -> None:
        node_name = flow.get("data", {}).get("node")
        node = self.store.get(node_name)
        if not node:
            self._clear_flow(context)
            await update.effective_message.reply_text("Нода не найдена.", reply_markup=self._main_keyboard())
            return

        key_path = self._managed_key_path(node.name)
        message = update.effective_message
        try:
            if message.text:
                self._write_private_key_text(key_path, message.text)
            elif message.document:
                await self._download_private_key_document(key_path, message.document)
            else:
                await message.reply_text("Пришли приватный ключ текстом или файлом.")
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
            await message.reply_text(f"Ключ не сохранен: {exc}", reply_markup=self._cancel_keyboard())
            return

        self._clear_flow(context)
        await message.reply_text(
            f"SSH-ключ сохранен и привязан к ноде {node.name}.",
            reply_markup=self._main_keyboard(),
        )

    async def _handle_preset_button(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: str,
    ) -> None:
        parts = data.split(":")
        action = parts[1]

        if action == "add_text":
            context.user_data["flow"] = {"type": "preset_text", "step": "field", "data": {}}
            await self._ask_text_preset_field(update)
        elif action == "text_field":
            field = parts[2]
            context.user_data["flow"] = {
                "type": "preset_text",
                "step": "name",
                "data": {"field": field},
            }
            await update.effective_message.reply_text(
                "Название кнопки пресета?",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "add_key":
            context.user_data["flow"] = {"type": "preset_key", "step": "name", "data": {}}
            await update.effective_message.reply_text(
                "Название кнопки для SSH-ключа?",
                reply_markup=self._cancel_keyboard(),
            )
        elif action == "list":
            await self._send_presets_list(update)
        elif action == "delete":
            await self._ask_delete_preset_field(update)
        elif action == "delete_field":
            await self._send_delete_preset_items(update, context, parts[2])
        elif action == "delete_item":
            field, preset_name = self._get_ref(context, parts[2])
            token = self._remember_ref(context, (field, preset_name))
            await update.effective_message.reply_text(
                f"Удалить пресет {field}/{preset_name}?",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("Удалить", callback_data=f"preset:delete_yes:{token}")],
                        [InlineKeyboardButton("Отмена", callback_data="presets:menu")],
                    ]
                ),
            )
        elif action == "delete_yes":
            field, preset_name = self._get_ref(context, parts[2])
            preset = self.store.delete_preset(field, preset_name)
            if not preset:
                await update.effective_message.reply_text("Пресет не найден.")
                return
            if preset.field == "ssh_key":
                self._delete_key_preset_file(Path(preset.value))
            await update.effective_message.reply_text(
                f"Пресет удален: {preset.field}/{preset.name}",
                reply_markup=self._main_keyboard(),
            )

    async def _handle_text_preset_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: dict[str, Any],
    ) -> None:
        step = flow.get("step")
        value = (update.effective_message.text or "").strip()
        data = flow.setdefault("data", {})

        if step == "name":
            if not PRESET_NAME_RE.match(value):
                await update.effective_message.reply_text(
                    "Название: 1-32 символа, латиница/цифры/./_/-",
                    reply_markup=self._cancel_keyboard(),
                )
                return
            data["name"] = value
            flow["step"] = "value"
            await update.effective_message.reply_text(
                "Значение пресета?",
                reply_markup=self._cancel_keyboard(),
            )
            return

        if step == "value":
            field = data["field"]
            try:
                preset_value = self._validate_add_value(field, value)
                self.store.set_preset(Preset(field=field, name=data["name"], value=preset_value))
            except ValueError as exc:
                await update.effective_message.reply_text(
                    f"Пресет не сохранен: {exc}",
                    reply_markup=self._cancel_keyboard(),
                )
                return

            self._clear_flow(context)
            await update.effective_message.reply_text(
                f"Пресет сохранен: {field}/{data['name']}",
                reply_markup=self._main_keyboard(),
            )

    async def _handle_key_preset_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: dict[str, Any],
    ) -> None:
        step = flow.get("step")
        data = flow.setdefault("data", {})
        message = update.effective_message

        if step == "name":
            name = (message.text or "").strip()
            if not PRESET_NAME_RE.match(name):
                await message.reply_text(
                    "Название: 1-32 символа, латиница/цифры/./_/-",
                    reply_markup=self._cancel_keyboard(),
                )
                return
            data["name"] = name
            flow["step"] = "key"
            await message.reply_text(
                "Пришли приватный SSH-ключ текстом или файлом.",
                reply_markup=self._cancel_keyboard(),
            )
            return

        if step != "key":
            self._clear_flow(context)
            await self._send_main_menu(update, "Сценарий пресета сброшен.")
            return

        key_path = self._key_preset_path(data["name"])
        try:
            if message.text:
                self._write_private_key_text(key_path, message.text)
            elif message.document:
                await self._download_private_key_document(key_path, message.document)
            else:
                await message.reply_text("Пришли приватный ключ текстом или файлом.")
                return

            self.store.set_preset(Preset(field="ssh_key", name=data["name"], value=str(key_path)))
        except (OSError, ValueError) as exc:
            try:
                key_path.unlink(missing_ok=True)
            except OSError:
                pass
            await message.reply_text(f"Пресет ключа не сохранен: {exc}")
            return

        self._clear_flow(context)
        await message.reply_text(
            f"Пресет SSH-ключа сохранен: {data['name']}",
            reply_markup=self._main_keyboard(),
        )

    async def _send_main_menu(self, update: Update, text: str = "Главное меню") -> None:
        await update.effective_message.reply_text(text, reply_markup=self._main_keyboard())

    async def _send_nodes_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        nodes = self.store.list()
        if not nodes:
            await update.effective_message.reply_text(
                "Ноды пока не добавлены.",
                reply_markup=self._main_keyboard(),
            )
            return

        lines = ["Ноды:"]
        rows: list[list[InlineKeyboardButton]] = []
        for node in nodes:
            lines.append(f"- {node.name}: {node.user}@{node.host}:{node.port} ({node.auth_summary})")
            token = self._remember_ref(context, node.name)
            rows.append([InlineKeyboardButton(node.name, callback_data=f"node:open:{token}")])

        rows.append([InlineKeyboardButton("Назад", callback_data="menu:main")])
        await update.effective_message.reply_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _send_node_details(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        node_name: str,
    ) -> None:
        node = self.store.get(node_name)
        if not node:
            await update.effective_message.reply_text("Нода не найдена.", reply_markup=self._main_keyboard())
            return

        token = self._remember_ref(context, node.name)
        text = (
            f"{node.name}\n"
            f"SSH: {node.user}@{node.host}:{node.port}\n"
            f"Auth: {node.auth_summary}"
        )
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Update", callback_data=f"node:update:{token}"),
                    InlineKeyboardButton("Ping", callback_data=f"node:ping:{token}"),
                ],
                [InlineKeyboardButton("Выполнить команду", callback_data=f"node:run:{token}")],
                [
                    InlineKeyboardButton("Задать ключ", callback_data=f"node:setkey:{token}"),
                    InlineKeyboardButton("Убрать ключ", callback_data=f"node:clearkey:{token}"),
                ],
                [InlineKeyboardButton("Удалить", callback_data=f"node:delete:{token}")],
                [InlineKeyboardButton("Назад", callback_data="nodes:list")],
            ]
        )
        await update.effective_message.reply_text(text, reply_markup=keyboard)

    async def _send_operations_menu(self, update: Update) -> None:
        await update.effective_message.reply_text(
            "Выбери операцию.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Обновить RemnaNode", callback_data="op:update")],
                    [InlineKeyboardButton("Ping", callback_data="op:ping")],
                    [InlineKeyboardButton("Назад", callback_data="menu:main")],
                ]
            ),
        )

    async def _send_target_menu(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        action: str,
    ) -> None:
        nodes = self.store.list()
        if not nodes:
            await update.effective_message.reply_text("Ноды пока не добавлены.", reply_markup=self._main_keyboard())
            return

        if action == "run_shell":
            prefix = "cmd:target"
            title = "Где выполнить команду?"
        else:
            prefix = f"op:target:{action}"
            title = "Выбери цель."

        rows = [[InlineKeyboardButton("Все ноды", callback_data=f"{prefix}:all")]]
        for node in nodes:
            token = self._remember_ref(context, node.name)
            rows.append([InlineKeyboardButton(node.name, callback_data=f"{prefix}:{token}")])
        rows.append([InlineKeyboardButton("Назад", callback_data="menu:main")])
        await update.effective_message.reply_text(title, reply_markup=InlineKeyboardMarkup(rows))

    async def _send_presets_menu(self, update: Update) -> None:
        await update.effective_message.reply_text(
            "Пресеты",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Добавить текстовый пресет", callback_data="preset:add_text")],
                    [InlineKeyboardButton("Добавить SSH-ключ", callback_data="preset:add_key")],
                    [InlineKeyboardButton("Список пресетов", callback_data="preset:list")],
                    [InlineKeyboardButton("Удалить пресет", callback_data="preset:delete")],
                    [InlineKeyboardButton("Назад", callback_data="menu:main")],
                ]
            ),
        )

    async def _ask_text_preset_field(self, update: Update) -> None:
        rows = [
            [InlineKeyboardButton(field, callback_data=f"preset:text_field:{field}")]
            for field in TEXT_PRESET_FIELDS
        ]
        rows.append([InlineKeyboardButton("Отмена", callback_data="flow:cancel")])
        await update.effective_message.reply_text("Для какого поля пресет?", reply_markup=InlineKeyboardMarkup(rows))

    async def _ask_delete_preset_field(self, update: Update) -> None:
        rows = [
            [InlineKeyboardButton(field, callback_data=f"preset:delete_field:{field}")]
            for field in ("name", "user", "host", "port", "ssh_key")
        ]
        rows.append([InlineKeyboardButton("Отмена", callback_data="presets:menu")])
        await update.effective_message.reply_text(
            "Пресеты какого поля удалить?",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _send_delete_preset_items(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
    ) -> None:
        presets = self.store.list_presets(field)
        if not presets:
            await update.effective_message.reply_text("Для этого поля пресетов нет.", reply_markup=self._main_keyboard())
            return

        rows = []
        for preset in presets:
            token = self._remember_ref(context, (field, preset.name))
            rows.append([InlineKeyboardButton(preset.name, callback_data=f"preset:delete_item:{token}")])
        rows.append([InlineKeyboardButton("Назад", callback_data="presets:menu")])
        await update.effective_message.reply_text("Выбери пресет.", reply_markup=InlineKeyboardMarkup(rows))

    async def _send_presets_list(self, update: Update) -> None:
        presets = self.store.list_presets()
        if not presets:
            await update.effective_message.reply_text("Пресетов пока нет.", reply_markup=self._main_keyboard())
            return

        lines = ["Пресеты:"]
        for preset in presets:
            value = "stored private key" if preset.field == "ssh_key" else preset.value
            lines.append(f"- {preset.field}/{preset.name}: {value}")
        await update.effective_message.reply_text("\n".join(lines), reply_markup=self._main_keyboard())

    async def _run_node_action(self, update: Update, action: str, target: str) -> None:
        if action == "update":
            await self._run_ansible_action(
                update,
                label=f"Обновляю RemnaNode на {target}",
                action=lambda: self.runner.update_remnanode(target),
            )
        elif action == "ping":
            await self._run_ansible_action(
                update,
                label=f"Проверяю доступ к {target}",
                action=lambda: self.runner.ping(target),
            )

    async def _run_ansible_action(
        self,
        update: Update,
        label: str,
        action,
    ) -> None:
        message = update.effective_message
        await message.chat.send_action(action=ChatAction.TYPING)
        await message.reply_text(label)

        try:
            async with self.operation_lock:
                result = await asyncio.to_thread(action)
        except Exception as exc:  # noqa: BLE001 - Telegram needs a clear operator-facing error.
            await message.reply_text(f"Ошибка: {exc}", reply_markup=self._main_keyboard())
            return

        await self._send_ansible_result(message, result)

    async def _send_ansible_result(self, message, result: AnsibleResult) -> None:
        status = "OK" if result.ok else "FAILED"
        header = (
            f"{status}: {result.action} target={result.target} "
            f"exit_code={result.returncode}"
        )
        output = result.output.strip() or "(no output)"
        max_chars = max(500, self.settings.max_telegram_output_chars)
        body = output[-max_chars:]
        if len(output) > max_chars:
            body = f"... output truncated to last {max_chars} chars ...\n{body}"

        await message.reply_text(header)
        await message.reply_text(
            f"<pre>{html.escape(body)}</pre>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._main_keyboard(),
        )

    async def _require_admin(self, update: Update) -> bool:
        user = update.effective_user
        if user and user.id in self.settings.admin_ids:
            return True

        message = update.effective_message
        if message:
            user_id = user.id if user else "unknown"
            await message.reply_text(f"Доступ запрещен. Telegram user id: {user_id}")
        return False

    def _add_step_keyboard(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
    ) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        for preset in self.store.list_presets(field):
            token = self._remember_ref(context, preset.name)
            rows.append([InlineKeyboardButton(preset.name, callback_data=f"add:pre:{field}:{token}")])

        if field == "port":
            rows.insert(0, [InlineKeyboardButton("22", callback_data="add:val:port:22")])
        if field == "ssh_key":
            rows.append([InlineKeyboardButton("Без ключа", callback_data="add:skip_key")])

        rows.append([InlineKeyboardButton("Отмена", callback_data="flow:cancel")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _main_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Добавить ноду", callback_data="add:start")],
                [
                    InlineKeyboardButton("Ноды", callback_data="nodes:list"),
                    InlineKeyboardButton("Операции", callback_data="ops:menu"),
                ],
                [
                    InlineKeyboardButton("Выполнить команду", callback_data="cmd:start"),
                    InlineKeyboardButton("Пресеты", callback_data="presets:menu"),
                ],
            ]
        )

    @staticmethod
    def _cancel_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="flow:cancel")]])

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

    @staticmethod
    def _normalize_preset_field(field: str) -> str:
        normalized = PRESET_FIELD_ALIASES.get(field.strip().lower())
        if not normalized:
            allowed = ", ".join(sorted(PRESET_FIELD_ALIASES))
            raise ValueError(f"Неизвестное поле пресета. Доступно: {allowed}")
        return normalized

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

    async def _download_private_key_document(self, key_path: Path, document) -> None:
        if document.file_size and document.file_size > MAX_PRIVATE_KEY_BYTES:
            raise ValueError("файл ключа слишком большой")

        key_path.parent.mkdir(parents=True, exist_ok=True)
        telegram_file = await document.get_file()
        await telegram_file.download_to_drive(custom_path=str(key_path))

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

    @staticmethod
    def _clear_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop("flow", None)

    @staticmethod
    def _remember_ref(context: ContextTypes.DEFAULT_TYPE, value: Any) -> str:
        refs = context.user_data.setdefault("callback_refs", {})
        counter = int(context.user_data.get("callback_ref_counter", 0)) + 1
        context.user_data["callback_ref_counter"] = counter

        if len(refs) > 200:
            refs.clear()

        token = str(counter)
        refs[token] = value
        return token

    @staticmethod
    def _get_ref(context: ContextTypes.DEFAULT_TYPE, token: str) -> Any:
        refs = context.user_data.get("callback_refs", {})
        if token not in refs:
            raise ValueError("Кнопка устарела. Открой меню заново.")
        return refs[token]
