from __future__ import annotations

import asyncio
import logging

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from app.ansible_runner import AnsibleRunner
from app.bot import BotController
from app.config import Settings
from app.database.store import NodeStore


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def run_webhook(bot: Bot, dispatcher: Dispatcher, settings: Settings) -> None:
    webhook_path = f"/{settings.webhook_path}"
    await bot.set_webhook(
        url=settings.webhook_url,
        secret_token=settings.webhook_secret_token,
        allowed_updates=dispatcher.resolve_used_update_types(),
    )

    app = web.Application()
    SimpleRequestHandler(
        dispatcher=dispatcher,
        bot=bot,
        secret_token=settings.webhook_secret_token,
    ).register(app, path=webhook_path)
    setup_application(app, dispatcher, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=settings.webhook_listen, port=settings.webhook_port)
    await site.start()

    logging.info(
        "Webhook server started on %s:%s%s",
        settings.webhook_listen,
        settings.webhook_port,
        webhook_path,
    )

    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


async def run_polling(bot: Bot, dispatcher: Dispatcher) -> None:
    await bot.delete_webhook(drop_pending_updates=True)
    await dispatcher.start_polling(
        bot,
        allowed_updates=dispatcher.resolve_used_update_types(),
    )


async def async_main() -> None:
    configure_logging()
    settings = Settings.from_env()

    store = NodeStore(settings.db_path)
    store.init()

    runner = AnsibleRunner(settings=settings, store=store)
    bot, dispatcher = BotController(settings, store, runner).build()

    try:
        if settings.bot_mode == "webhook":
            await run_webhook(bot, dispatcher, settings)
        else:
            await run_polling(bot, dispatcher)
    finally:
        await bot.session.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
