from __future__ import annotations

from aiogram import Bot, Dispatcher

from app.ansible_runner import AnsibleRunner
from app.config import Settings
from app.database.store import NodeStore
from app.handlers import BotHandlers, register_handlers


class BotController:
    def __init__(self, settings: Settings, store: NodeStore, runner: AnsibleRunner) -> None:
        self.settings = settings
        self.handlers = BotHandlers(settings=settings, store=store, runner=runner)

    def build(self) -> tuple[Bot, Dispatcher]:
        bot = Bot(token=self.settings.bot_token)
        dispatcher = Dispatcher()

        self.handlers.set_bot(bot)
        register_handlers(dispatcher, self.handlers)

        return bot, dispatcher
