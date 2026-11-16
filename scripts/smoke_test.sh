#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
BIN="$ROOT/target/debug"

cleanup() {
  [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null || true
  [[ -n "${HUB_PID:-}" ]] && kill "$HUB_PID" 2>/dev/null || true
  [[ -n "${ECHO_PID:-}" ]] && kill "$ECHO_PID" 2>/dev/null || true
}
trap cleanup EXIT

python3 - <<'PY' > /tmp/pb-echo.log 2>&1 &
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 9999))
s.listen(5)
print("echo up on 9999", flush=True)
while True:
    c, addr = s.accept()
    print(f"accept {addr}", flush=True)
    while True:
        chunk = c.recv(4096)
        if not chunk:
            break
        print(f"recv {chunk!r}", flush=True)
        c.sendall(chunk)
    c.close()
    print("closed", flush=True)
PY
ECHO_PID=$!
sleep 0.3

HUB_TOKEN=dev-token \
HUB_WSS_ADDR=127.0.0.1:8700 \
HUB_FORWARD_ADDR=127.0.0.1:15432 \
HUB_DIAL_HOST=127.0.0.1 \
HUB_DIAL_PORT=9999 \
"$BIN/hub" > /tmp/pb-hub.log 2>&1 &
HUB_PID=$!
sleep 0.4

POCKETBI_TOKEN=dev-token \
POCKETBI_URL=ws://127.0.0.1:8700/agent \
"$BIN/agent" > /tmp/pb-agent.log 2>&1 &
AGENT_PID=$!
sleep 1

echo "=== ports ==="
ss -ltn | grep -E '8700|15432|9999' || true
echo "=== hub ==="
cat /tmp/pb-hub.log
echo "=== agent ==="
cat /tmp/pb-agent.log
echo "=== echo ==="
cat /tmp/pb-echo.log

echo "=== probe ==="
python3 - <<'PY'
import socket
s = socket.create_connection(("127.0.0.1", 15432), timeout=5)
s.sendall(b"hello-from-tunnel\n")
s.settimeout(3)
try:
    data = s.recv(4096)
    print("RECV:", repr(data))
except Exception as e:
    print("RECV_ERR:", type(e).__name__, e)
s.close()
PY

sleep 0.5
echo "=== after probe ==="
echo "--- hub ---"; cat /tmp/pb-hub.log
echo "--- agent ---"; cat /tmp/pb-agent.log
echo "--- echo ---"; cat /tmp/pb-echo.log
