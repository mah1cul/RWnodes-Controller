#!/usr/bin/env bash
set -euo pipefail

DEFAULT_API_URL="__RWNODES_DEFAULT_API_URL__"
DEFAULT_ADDNODE_PATH="__RWNODES_DEFAULT_ADDNODE_PATH__"
if [[ "$DEFAULT_API_URL" == "__RWNODES_DEFAULT_API_URL__" ]]; then
  DEFAULT_API_URL=""
fi
if [[ "$DEFAULT_ADDNODE_PATH" == "__RWNODES_DEFAULT_ADDNODE_PATH__" ]]; then
  DEFAULT_ADDNODE_PATH="addnode"
fi

API_URL="${RWNODES_API_URL:-$DEFAULT_API_URL}"
ADDNODE_PATH="${RWNODES_ADDNODE_PATH:-$DEFAULT_ADDNODE_PATH}"
SSH_PORT=""
SSH_USER=""
NODE_NAME=""
INTERFACE=""
KEY_PATH=""
SSH_PASSWORD=""
API_KEY=""

usage() {
  cat <<'EOF'
Usage:
  addnode.sh -U USER [options] (--key /path/to/key | --pass PASSWORD)

Options:
  --url URL          Base controller URL. Can also be set with RWNODES_API_URL.
                    Not needed when the script is loaded from /scripts/addnode.
                    The script appends /addnode unless URL already ends with it.
  -P sshport        SSH port. If empty, reads sshd config, fallback 22.
  -U username       SSH username. Required.
  --name name       Node name in bot. Defaults to hostname.
  -I interface      Interface to take IPv4 from, for example wg0.
                    If omitted, script uses public IPv4.
  --key path        Path to private SSH key.
  --pass password   SSH password.
  --apikey key      API key. Required only when keys exist in bot.
  -h, --help        Show this help.
EOF
}

fail() {
  echo "error: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

normalize_api_url() {
  local url="${1%/}"
  local path="${ADDNODE_PATH#/}"
  if [[ "$url" == */"$path" ]]; then
    echo "$url"
  else
    echo "$url/$path"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      API_URL="${2:-}"
      shift 2
      ;;
    -P)
      SSH_PORT="${2:-}"
      shift 2
      ;;
    -U)
      SSH_USER="${2:-}"
      shift 2
      ;;
    --name)
      NODE_NAME="${2:-}"
      shift 2
      ;;
    -I)
      INTERFACE="${2:-}"
      shift 2
      ;;
    --key)
      KEY_PATH="${2:-}"
      shift 2
      ;;
    --pass)
      SSH_PASSWORD="${2:-}"
      shift 2
      ;;
    --apikey)
      API_KEY="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

need_cmd curl
need_cmd awk

[[ -n "$API_URL" ]] || fail "RWNODES_API_URL is required when the script is not served with an embedded controller URL"
API_URL="$(normalize_api_url "$API_URL")"
[[ -n "$SSH_USER" ]] || fail "-U username is required"

if [[ -n "$KEY_PATH" && -n "$SSH_PASSWORD" ]]; then
  fail "use either --key or --pass, not both"
fi
if [[ -z "$KEY_PATH" && -z "$SSH_PASSWORD" ]]; then
  fail "one auth option is required: --key or --pass"
fi
if [[ -n "$KEY_PATH" && ! -r "$KEY_PATH" ]]; then
  fail "private key is not readable: $KEY_PATH"
fi

detect_ssh_port() {
  local port=""
  if command -v sshd >/dev/null 2>&1; then
    port="$(sshd -T 2>/dev/null | awk '$1 == "port" {print $2; exit}')"
  fi
  if [[ -z "$port" ]]; then
    port="$(
      awk 'tolower($1) == "port" {print $2; exit}' \
        /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf 2>/dev/null || true
    )"
  fi
  echo "${port:-22}"
}

detect_name() {
  hostname -f 2>/dev/null || hostname
}

detect_interface_ip() {
  local iface="$1"
  ip -4 -o addr show dev "$iface" scope global 2>/dev/null \
    | awk '{split($4, a, "/"); print a[1]; exit}'
}

detect_public_ip() {
  local ip_addr=""
  ip_addr="$(curl -fsS --max-time 8 https://api.ipify.org 2>/dev/null || true)"
  if [[ -z "$ip_addr" ]]; then
    ip_addr="$(curl -fsS --max-time 8 https://ifconfig.me/ip 2>/dev/null || true)"
  fi
  if [[ -z "$ip_addr" ]]; then
    ip_addr="$(curl -fsS --max-time 8 https://icanhazip.com 2>/dev/null | tr -d '[:space:]' || true)"
  fi
  echo "$ip_addr"
}

if [[ -z "$SSH_PORT" ]]; then
  SSH_PORT="$(detect_ssh_port)"
fi
if [[ -z "$NODE_NAME" ]]; then
  NODE_NAME="$(detect_name)"
fi

if [[ -n "$INTERFACE" ]]; then
  need_cmd ip
  NODE_HOST="$(detect_interface_ip "$INTERFACE")"
  [[ -n "$NODE_HOST" ]] || fail "could not detect IPv4 on interface $INTERFACE"
else
  NODE_HOST="$(detect_public_ip)"
  [[ -n "$NODE_HOST" ]] || fail "could not detect public IPv4"
fi

curl_args=(
  -sS
  -X POST "$API_URL"
  --form-string "name=$NODE_NAME"
  --form-string "host=$NODE_HOST"
  --form-string "user=$SSH_USER"
  --form-string "port=$SSH_PORT"
)

if [[ -n "$API_KEY" ]]; then
  curl_args+=(--header "X-Api-Key: $API_KEY")
fi
if [[ -n "$KEY_PATH" ]]; then
  curl_args+=(--form "ssh_key=<$KEY_PATH")
else
  curl_args+=(--form-string "password=$SSH_PASSWORD")
fi

response="$(curl "${curl_args[@]}" -w $'\n%{http_code}')"
http_code="${response##*$'\n'}"
body="${response%$'\n'*}"

echo "$body"
if [[ ! "$http_code" =~ ^2 ]]; then
  exit 1
fi
