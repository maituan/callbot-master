#!/usr/bin/env bash
# Quick health probe for the bot endpoint configured in MASTER_BOT_URL.
# Run this on the master host BEFORE inferring container/network issues —
# if the host can't reach the bot, neither can the container.
#
# Usage: ./deploy/check-bot.sh [URL]

set -u
URL="${1:-${MASTER_BOT_URL:-http://160.250.216.28:11005/api/v1/call/}}"
echo "[probe] $URL"

echo
echo "--- 1. TCP reachability ---"
host_port="${URL#http://}"
host_port="${host_port#https://}"
host_port="${host_port%%/*}"
host="${host_port%:*}"
port="${host_port##*:}"
[[ "$host" == "$port" ]] && port=80
timeout 3 bash -c "</dev/tcp/$host/$port" \
  && echo "  TCP $host:$port reachable" \
  || { echo "  TCP $host:$port UNREACHABLE — check route/firewall"; exit 1; }

echo
echo "--- 2. HTTP reachability (5s timeout) ---"
curl -sS --max-time 5 -o /dev/null -w "  http_code=%{http_code} ttfb=%{time_starttransfer}s total=%{time_total}s\n" \
  "$URL" || echo "  curl failed"

echo
echo "--- 3. POST a real bot request (matches what master sends) ---"
curl -sS --max-time 10 -X POST "$URL" \
  -H "Content-Type: application/json" \
  -H "Accept: text/plain" \
  -d '{"conversation_id":"diag-001","message":""}' \
  -o /tmp/bot-probe.out \
  -w "\n  http_code=%{http_code} ttfb=%{time_starttransfer}s total=%{time_total}s\n"
echo "  body (first 200 bytes):"
head -c 200 /tmp/bot-probe.out 2>/dev/null
echo

echo
echo "If TTFB > 5s → bot is slow / cold-starting; bump MASTER_BOT_FIRST_BYTE_TIMEOUT."
echo "If 4xx/5xx → bot side error; check bot logs."
echo "If connection refused → wrong host:port or bot is down."
