from __future__ import annotations

import asyncio
import logging
from urllib.parse import urljoin

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from app.ansible_runner import AnsibleRunner
from app.api import AddNodeApi
from app.bot import BotController
from app.config import Settings
from app.database.store import NodeStore


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def create_http_app(
    bot: Bot,
    dispatcher: Dispatcher,
    settings: Settings,
    store: NodeStore,
    *,
    include_telegram_webhook: bool,
) -> web.Application:
    app = web.Application()
    AddNodeApi(settings=settings, store=store, bot=bot).register(app)

    if include_telegram_webhook:
        webhook_path = f"/{settings.webhook_path}"
        SimpleRequestHandler(
            dispatcher=dispatcher,
            bot=bot,
            secret_token=settings.webhook_secret_token,
        ).register(app, path=webhook_path)
        setup_application(app, dispatcher, bot=bot)

    return app


async def start_http_server(app: web.Application, settings: Settings) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=settings.webhook_listen, port=settings.webhook_port)
    await site.start()
    logging.info(
        "HTTP server started on %s:%s",
        settings.webhook_listen,
        settings.webhook_port,
    )
    logging.info("Add-node API listening on /%s", settings.addnode_path)
    return runner


async def run_webhook(bot: Bot, dispatcher: Dispatcher, settings: Settings, store: NodeStore) -> None:
    webhook_path = f"/{settings.webhook_path}"
    telegram_webhook_url = urljoin(f"{settings.webhook_url.rstrip('/')}/", settings.webhook_path)
    await bot.set_webhook(
        url=telegram_webhook_url,
        secret_token=settings.webhook_secret_token,
        allowed_updates=dispatcher.resolve_used_update_types(),
    )

    app = create_http_app(
        bot,
        dispatcher,
        settings,
        store,
        include_telegram_webhook=True,
    )
    runner = await start_http_server(app, settings)

    logging.info(
        "Webhook server started on %s:%s%s",
        settings.webhook_listen,
        settings.webhook_port,
        webhook_path,
    )
    logging.info("Telegram webhook URL set to %s", telegram_webhook_url)

    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


async def run_polling(bot: Bot, dispatcher: Dispatcher, settings: Settings, store: NodeStore) -> None:
    await bot.delete_webhook(drop_pending_updates=True)
    app = create_http_app(
        bot,
        dispatcher,
        settings,
        store,
        include_telegram_webhook=False,
    )
    runner = await start_http_server(app, settings)
    try:
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        await runner.cleanup()


async def async_main() -> None:
    configure_logging()
    settings = Settings.from_env()

    store = NodeStore(settings.db_path)
    store.init()

    runner = AnsibleRunner(settings=settings, store=store)
    bot, dispatcher = BotController(settings, store, runner).build()

    try:
        if settings.bot_mode == "webhook":
            await run_webhook(bot, dispatcher, settings, store)
        else:
            await run_polling(bot, dispatcher, settings, store)
    finally:
        await bot.session.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
