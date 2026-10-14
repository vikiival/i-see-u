#!/usr/bin/env bash
# Fetch Transfer logs for one account over a block range, save the raw JSON and report its size.
#
# Usage: scripts/call.sh <address> [to|from|both|all] [fromBlock] [blocks] [outDir]
#   to    account received (topic2)        from  account sent (topic1)
#   both  runs "from" and "to"             all   no address filter (topic0 only; keep blocks small!)
# Defaults: direction=both, fromBlock=25933634, blocks=10000, outDir=out
# Reads RPC_URL from the environment or .env (never printed).
set -euo pipefail

ADDRESS=${1:?usage: scripts/call.sh <address> [to|from|both|all] [fromBlock] [blocks] [outDir]}
DIRECTION=${2:-both}
FROM=${3:-25933634}
BLOCKS=${4:-10000}
OUT=${5:-out}
TRANSFER=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

cd "$(dirname "$0")/.."
if [[ -z "${RPC_URL:-}" && -f .env ]]; then set -a; source .env; set +a; fi
: "${RPC_URL:?RPC_URL not set (env or .env)}"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

addr=$(tr '[:upper:]' '[:lower:]' <<<"${ADDRESS#0x}")
[[ $addr =~ ^[0-9a-f]{40}$ ]] || { echo "invalid address: $ADDRESS" >&2; exit 1; }
padded="0x000000000000000000000000$addr"
to_block=$((FROM + BLOCKS - 1))
from_hex=$(printf '0x%x' "$FROM")
to_hex=$(printf '0x%x' "$to_block")
mkdir -p "$OUT"

fetch() {
  local dir=$1 topics
  case $dir in
    from) topics="[\"$TRANSFER\",\"$padded\"]" ;;
    to)   topics="[\"$TRANSFER\",null,\"$padded\"]" ;;
    all)  topics="[\"$TRANSFER\"]" ;;
  esac
  local file="$OUT/${ADDRESS}_${dir}_${FROM}-${to_block}.json"
  local body="{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getLogs\",\"params\":[{\"fromBlock\":\"$from_hex\",\"toBlock\":\"$to_hex\",\"topics\":$topics}]}"

  local meta
  meta=$(curl -sS -X POST "$RPC_URL" -H 'Content-Type: application/json' -d "$body" \
    -o "$file" -w '%{http_code} %{time_total} %{size_download}')
  read -r http secs bytes <<<"$meta"

  echo "── $dir  blocks $FROM..$to_block ($BLOCKS)  →  $file"
  echo "   http $http  time ${secs}s  size $(awk -v b="$bytes" 'BEGIN { split("B KB MB GB", u); i = 1; while (b >= 1024 && i < 4) { b /= 1024; i++ } printf "%.1f %s", b, u[i] }') ($bytes bytes)"
  if jq -e '.error' "$file" >/dev/null 2>&1; then
    echo "   RPC error: $(jq -c '.error' "$file")"
    return
  fi
  jq -r '.result
    | "   logs \(length)  erc20 \(map(select(.topics|length==3))|length)  erc721 \(map(select(.topics|length==4))|length)  tokens \(map(.address)|unique|length)"' "$file"
}

case $DIRECTION in
  both) fetch from; fetch to ;;
  to|from|all) fetch "$DIRECTION" ;;
  *) echo "direction must be to|from|both|all" >&2; exit 1 ;;
esac
