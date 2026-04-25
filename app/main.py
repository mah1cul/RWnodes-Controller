from __future__ import annotations

import logging

from telegram import Update

from app.ansible_runner import AnsibleRunner
from app.bot import BotController
from app.config import Settings
from app.store import NodeStore


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    configure_logging()
    settings = Settings.from_env()

    store = NodeStore(settings.db_path)
    store.init()

    runner = AnsibleRunner(settings=settings, store=store)
    application = BotController(settings, store, runner).build_application()

    if settings.bot_mode == "webhook":
        application.run_webhook(
            listen=settings.webhook_listen,
            port=settings.webhook_port,
            url_path=settings.webhook_path,
            webhook_url=settings.webhook_url,
            secret_token=settings.webhook_secret_token,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

