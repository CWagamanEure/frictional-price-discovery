#!/usr/bin/env bash
set -euo pipefail

HOSTS=(
  "gateway.thegraph.com"
  "api.exchange.coinbase.com"
  "eth-mainnet.g.alchemy.com"
)

echo "== Connectivity Diagnostics =="
echo "Date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo

echo "-- Proxy-related env vars --"
env | egrep -i '^(http_proxy|https_proxy|all_proxy|no_proxy|HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY)=' || true
echo

echo "-- DNS resolution (Python socket.getaddrinfo) --"
DNS_FAIL=0
poetry run python - <<'PY' || DNS_FAIL=1
import socket
hosts = [
    "gateway.thegraph.com",
    "api.exchange.coinbase.com",
    "eth-mainnet.g.alchemy.com",
]
failed = False
for host in hosts:
    try:
        infos = socket.getaddrinfo(host, 443)
        addrs = sorted({item[4][0] for item in infos if item and item[4]})
        preview = ", ".join(addrs[:3])
        print(f"[OK]   {host} -> {preview}")
    except Exception as exc:
        failed = True
        print(f"[FAIL] {host} -> {exc!r}")
if failed:
    raise SystemExit(1)
PY

echo

echo "-- HTTPS HEAD checks (curl -I) --"
HTTP_FAIL=0
for host in "${HOSTS[@]}"; do
  url="https://${host}"
  if curl -sS -I --connect-timeout 8 --max-time 15 "$url" >/tmp/connectivity_head.$$ 2>/tmp/connectivity_err.$$; then
    status_line=$(head -n 1 /tmp/connectivity_head.$$ || true)
    echo "[OK]   ${url} (${status_line})"
  else
    HTTP_FAIL=1
    err=$(head -n 1 /tmp/connectivity_err.$$ || true)
    echo "[FAIL] ${url} (${err})"
  fi
  rm -f /tmp/connectivity_head.$$ /tmp/connectivity_err.$$ || true
done

echo
if [[ "$DNS_FAIL" -eq 0 && "$HTTP_FAIL" -eq 0 ]]; then
  echo "Result: all connectivity checks passed."
  exit 0
fi

echo "Result: one or more connectivity checks failed."
exit 1
