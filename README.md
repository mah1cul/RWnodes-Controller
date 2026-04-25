# RWnodes Controller

Python + Ansible controller for managing remote nodes from an aiogram-based
Telegram bot.

The first built-in operation is RemnaNode update:

```bash
cd /opt/remnanode && docker compose pull && docker compose down && docker compose up -d
```

The controller runs inside Docker, stores nodes and presets in SQLite, generates
a temporary Ansible inventory for each operation, and supports Telegram bot
polling or webhook mode.

## Features

- Button-only Telegram UI for operational actions.
- Add nodes with a step-by-step inline wizard.
- Add, list and delete nodes.
- Upload per-node SSH private keys through the bot.
- Add presets for node name, user, host, port and SSH key.
- Run Ansible ping against one node or all nodes.
- Update RemnaNode on one node or all nodes.
- Run arbitrary shell commands through Ansible.
- Admin-only access by Telegram numeric user id.

## Quick Start

1. Create `.env` from the example:

```bash
cp .env.example .env
```

2. Edit `.env`:

```env
BOT_TOKEN=123456:telegram-bot-token
ADMIN_IDS=123456789
BOT_MODE=polling
```

`ADMIN_IDS` must contain your numeric Telegram user id. If you do not know it,
open the bot and send any message: unauthorized users receive a reply with their
id.

3. Build and run:

```bash
docker compose up -d --build
```

Logs:

```bash
docker compose logs -f rwnodes-controller
```

## Telegram UI

The bot uses inline buttons for all working actions. The only text entry points
are values that the bot explicitly asks for, such as node name, IP address,
shell command or private key.

Main menu buttons:

```text
Добавить ноду
Ноды
Операции
Выполнить команду
Пресеты
```

Telegram may still show its standard Start button for opening the bot. After
that, use the inline menu.

## Add Node Wizard

Press `Добавить ноду`. The bot asks, one step at a time:

```text
Название ноды
Пользователь SSH
IP или hostname
SSH-порт
Приватный SSH-ключ
```

Each step accepts manual input. If presets exist for that field, the bot also
shows buttons that fill the value automatically. The port step always includes a
`22` button.

At the key step, send the private key as text or as a file, choose a saved key
preset, or press `Без ключа`.

Uploaded node keys are stored in:

```text
/data/ssh_keys/<node>.key
```

## Presets

Open `Пресеты` from the main menu.

Available actions:

```text
Добавить текстовый пресет
Добавить SSH-ключ
Список пресетов
Удалить пресет
```

Text presets can be created for:

```text
name
user
host
port
```

SSH key presets are stored separately as private key files:

```text
/data/ssh_key_presets/<preset>.key
```

When the add-node wizard asks for a field with presets, the saved preset names
appear as inline buttons.

## Nodes

Open `Ноды` to see saved nodes. Each node has buttons for:

```text
Update
Ping
Выполнить команду
Задать ключ
Убрать ключ
Удалить
```

`Update` runs the RemnaNode update playbook on that node.

## Operations

Open `Операции` for node-wide actions:

```text
Обновить RemnaNode
Ping
```

After choosing an operation, select either `Все ноды` or a specific node.

## Run Commands

Press `Выполнить команду`, choose the target, then type the shell command when
the bot asks for it. The command is executed through Ansible.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `BOT_TOKEN` | required | Telegram bot token from BotFather. |
| `ADMIN_IDS` | required | Comma-separated Telegram user ids allowed to use the bot. |
| `BOT_MODE` | `polling` | `polling` or `webhook`. |
| `DB_PATH` | `/data/rwnodes.sqlite3` | SQLite database path. |
| `MANAGED_SSH_KEYS_DIR` | `/data/ssh_keys` | Directory for private keys uploaded for nodes. |
| `SSH_KEY_PRESETS_DIR` | `/data/ssh_key_presets` | Directory for private key presets. |
| `ANSIBLE_TIMEOUT` | `900` | Max seconds for one Ansible run. |
| `ANSIBLE_HOST_KEY_CHECKING` | `false` | Enables/disables SSH host key checking. |
| `DEFAULT_BECOME` | `false` | Enables Ansible become for every node unless overridden by node config. |
| `MAX_TELEGRAM_OUTPUT_CHARS` | `3500` | Max output chars sent back to Telegram. |
| `WEBHOOK_URL` | empty | Required in webhook mode. |
| `WEBHOOK_LISTEN` | `0.0.0.0` | Webhook bind address inside the container. |
| `WEBHOOK_PORT` | `8080` | Webhook port inside and outside the container. |
| `WEBHOOK_PATH` | `telegram/webhook` | Webhook URL path. |
| `WEBHOOK_SECRET_TOKEN` | empty | Optional Telegram webhook secret token. |

## Polling Mode

Polling is the default mode and does not require a public HTTPS endpoint.

```env
BOT_MODE=polling
```

## Webhook Mode

Webhook mode requires a public HTTPS URL that forwards requests to the container.

```env
BOT_MODE=webhook
WEBHOOK_URL=https://example.com/telegram/webhook
WEBHOOK_LISTEN=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=telegram/webhook
WEBHOOK_SECRET_TOKEN=change-me
CADDY_DOCKER_NETWORK=caddy
```

The compose file does not publish `WEBHOOK_PORT` to the host. It only exposes
the port inside Docker and attaches the app to an external Docker network used
by Caddy. Create or reuse that network:

```bash
docker network create caddy
```

If your Caddy container uses another network name, set `CADDY_DOCKER_NETWORK` to
that exact name. In Caddy, proxy to:

```text
rwnodes-controller:8080
```

The URL path in `WEBHOOK_URL` must match `WEBHOOK_PATH`.

## Security Notes

- Keep `ADMIN_IDS` strict. The bot can execute shell commands on nodes.
- Prefer SSH keys over passwords.
- Private keys uploaded for nodes are stored in the Docker volume under `/data/ssh_keys`.
- Private key presets are stored in the Docker volume under `/data/ssh_key_presets`.
- Do not commit `.env`, `ssh_keys` or the SQLite database.
