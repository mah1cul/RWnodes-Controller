# RWnodes Controller

Python + Ansible controller for managing remote nodes from an aiogram-based
Telegram bot.

The first built-in operation is RemnaNode update:

```bash
cd /opt/remnanode && docker compose pull && docker compose down && docker compose up -d
```

The controller runs inside Docker, stores nodes and parameter presets in SQLite, generates
a temporary Ansible inventory for each operation, and supports Telegram bot
polling or webhook mode.

## Features

- Button-only Telegram UI for operational actions.
- Add nodes with a step-by-step inline wizard.
- Add, list and delete nodes.
- Add nodes from the node itself through the `/addnode` HTTP API.
- Upload per-node SSH private keys through the bot.
- Use SSH password auth when adding a node.
- Add parameter presets for node name, user, host, port and SSH key.
- Run Ansible ping against one node or all nodes.
- Update RemnaNode on one node or all nodes.
- Reboot one node or all nodes through a confirmation screen.
- Detect a node country from the first two letters of its name and show flags.
- Admin-only access by Telegram numeric user id.
- Optional API keys for `/addnode` registration.

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

## Project Structure

```text
app/bot.py                  aiogram Bot/Dispatcher assembly
app/handlers.py             message and callback handlers registered with @dp decorators
app/keyboards.py            inline keyboard builders and emoji metadata
app/api.py                  HTTP API for node self-registration
app/database/store.py       SQLite store and models
app/database/migrations/    SQL migrations applied on startup
scripts/addnode.sh          node-side registration script
```

## Telegram UI

The bot uses inline buttons for all working actions. The only text entry points
are values that the bot explicitly asks for, such as node name, IP address,
country code or private key.

Main menu buttons:

```text
Ноды
Операции
Пресеты параметров
```

`Ноды` contains adding nodes, node list and per-node actions. `Операции`
contains predefined playbooks. `Пресеты параметров` contains saved wizard values.
Nested screens include navigation buttons back to the previous section and to the
main menu.

The main menu also shows a compact node summary:

```text
Ноды:
- 🇷🇺 RU-1-Node: 100.88.1.3
```

Telegram may still show its standard Start button for opening the bot. After
that, use the inline menu.

## Add Node

Press `Добавить ноду`, then choose one of two modes:

```text
Добавить вручную
Скрипт добавления
```

`Добавить вручную` starts the usual bot wizard. The bot asks, one step at a
time:

```text
Название ноды
Пользователь SSH
IP или hostname
SSH-порт
Способ входа: SSH-ключ или пароль
Приватный SSH-ключ или SSH-пароль
```

Each text step accepts manual input. If presets exist for that field, the bot
also shows buttons that fill the value automatically. The port step always
includes a `22` button.

At the auth step, choose `SSH-ключ` or `Пароль`.

If you choose `SSH-ключ`, send the private key as text or as a file, or choose a
saved key preset. If you choose `Пароль`, type the SSH password when the bot asks
for it. Passwords are stored in SQLite.

Uploaded node keys are stored in:

```text
/data/ssh_keys/<node>.key
```

`Скрипт добавления` shows ready commands for registering the current server
through the HTTP API without downloading the script manually.

## Add Node API

The app exposes `POST /addnode` on the same HTTP server as webhook traffic. It is
available in both `polling` and `webhook` modes.

Accepted fields:

```text
name      node name in the bot
host      IP/host used by Ansible
user      SSH username
port      SSH port
ssh_key   private SSH key text
password  SSH password
apikey    optional API key, also accepted as X-Api-Key header
```

Send exactly one auth field: `ssh_key` or `password`.

API keys are managed from the bot in `API ключи`. If no API keys exist,
`/addnode` accepts requests without `apikey`. Once at least one key exists, every
request must include a valid key.

The node-side script is stored at:

```text
/app/scripts/addnode.sh
```

The same script is served over HTTP:

```text
GET /scripts/addnode
GET /scripts/addnode.sh
```

Example with SSH key:

```bash
curl -fsSL https://hooks.example.com/scripts/addnode | sudo bash -s -- --url https://hooks.example.com -U root --key /root/.ssh/id_ed25519
```

Example with password and a specific interface:

```bash
curl -fsSL https://hooks.example.com/scripts/addnode | sudo bash -s -- --url https://hooks.example.com -U root -I wg0 --name RU-1-Node --pass 'SSHPASSWORD' --apikey 'APIKEY'
```

`--url` can be the base controller URL; the script appends `/addnode`. If you
changed `ADDNODE_PATH`, set `RWNODES_ADDNODE_PATH` for the script. If `-P` is not
set, it reads the SSH port from sshd config and falls back to `22`. If `-I` is
not set, it uses a public IPv4 lookup.

## Parameter Presets

Open `Пресеты параметров` from the main menu.

Available actions:

```text
Добавить текстовый пресет параметров
Добавить SSH-ключ
Список пресетов параметров
Удалить пресет параметров
```

Text parameter presets can be created for:

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
Reboot
Изменить
Показать доступ
Задать ключ
Убрать ключ
Удалить
```

`Update` runs the RemnaNode update playbook on that node.

`Изменить` lets you update:

```text
Название
Пользователь
IP/host
Порт
Страна
Способ входа
```

`Показать доступ` sends the saved SSH password or SSH private key into the chat.
Use it only in a trusted admin chat.

Country is stored as an ISO alpha-2 code such as `RU`, `DE` or `US`. During
node creation the bot takes the first two letters of the node name and uses them
as the country when they match a known country code. If there is no match, the
node is shown with `🏳️‍🌈`. You can change the country later from the node edit
menu.

## Operations

Open `Операции` for node-wide actions:

```text
Обновить RemnaNode
Ping
Reboot
```

After choosing an operation, select either `Все ноды` or a specific node.

`Reboot` is treated as a critical action. The bot shows a warning and requires a
separate confirmation before Ansible starts the reboot playbook.

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
| `WEBHOOK_URL` | empty | Required in webhook mode. Base public URL without path, for example `https://hooks.example.com`. |
| `WEBHOOK_LISTEN` | `0.0.0.0` | Webhook bind address inside the container. |
| `WEBHOOK_PORT` | `8080` | Webhook port inside and outside the container. |
| `WEBHOOK_PATH` | `telegram/webhook` | Webhook path appended to `WEBHOOK_URL`. |
| `WEBHOOK_SECRET_TOKEN` | empty | Optional Telegram webhook secret token. |
| `ADDNODE_PATH` | `addnode` | HTTP path for node self-registration API. |
| `PREMIUM_EMOJI_MODE` | `false` | `true` uses hardcoded Telegram custom emoji ids; `false` uses Unicode icons. |

## Premium Emoji Icons

The bot has hardcoded Telegram custom emoji ids for inline button icons:

```env
PREMIUM_EMOJI_MODE=true
```

When enabled, the bot probes custom emoji once in the current chat. If Telegram
rejects the request, for example because the bot owner does not have Telegram
Premium, the bot disables custom emoji until restart and falls back to regular
Unicode icons.

For country flags, the bot tries to load custom emoji ids from:

```text
worldroundflags1_by_fStikBot
worldroundflags2_by_fStikBot
```

If those sets cannot be loaded or a flag is not found, the bot uses the regular
Unicode flag.

## Polling Mode

Polling is the default mode and does not require a public HTTPS endpoint.

```env
BOT_MODE=polling
```

## Webhook Mode

Webhook mode requires a public HTTPS URL that forwards requests to the container.

```env
BOT_MODE=webhook
WEBHOOK_URL=https://hooks.example.com
WEBHOOK_LISTEN=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=webhook
WEBHOOK_SECRET_TOKEN=change-me
ADDNODE_PATH=addnode
CADDY_DOCKER_NETWORK=caddy
```

The bot registers Telegram webhook as `WEBHOOK_URL + "/" + WEBHOOK_PATH`. With
the example above Telegram receives:

```text
https://hooks.example.com/webhook
```

With `ADDNODE_PATH=addnode`, the node registration API is:

```text
https://hooks.example.com/addnode
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

The external proxy must forward both `WEBHOOK_PATH` and `ADDNODE_PATH` to the
controller container.

## Security Notes

- Keep `ADMIN_IDS` strict. The bot can run operational Ansible playbooks on nodes.
- Prefer SSH keys over passwords.
- Private keys uploaded for nodes are stored in the Docker volume under `/data/ssh_keys`.
- Private key presets are stored in the Docker volume under `/data/ssh_key_presets`.
- SSH passwords are stored in the SQLite database.
- API keys are stored as SHA-256 hashes; the raw key is shown only once when created.
- `scripts/addnode.sh --pass` puts the SSH password in shell history/process args; prefer `--key`.
- Do not commit `.env`, `ssh_keys` or the SQLite database.
