# pocketbi-agent

**Public, open-source outbound connector** for [Pocket BI](https://pocketbi.ai).

When your database is not reachable from the internet, you run this agent on a host that *can* reach the DB. It only opens an **outbound** WebSocket to Pocket BI and relays TCP — no inbound ports, no exposing Postgres/MySQL publicly.

| | |
|--|--|
| Source | Public GitHub (this repo) |
| Image | `pocketbi/agent` on Docker Hub |
| License | MIT |

The agent does **not** include SQL drivers, credentials storage, or sync logic. Those stay in Pocket BI / Sauron. This binary is a small authenticated reverse TCP tunnel you can audit.

## Install (customers)

```bash
docker run --rm \
  -e POCKETBI_TOKEN=<install-token-from-wizard> \
  -e POCKETBI_URL=wss://app.pocketbi.ai/api/agent \
  pocketbi/agent:latest
```

Leave it running. In the Pocket BI wizard, wait until the connector shows **online**, then enter DB host/port **as seen from this machine** (e.g. `localhost`, Docker service name, or private IP).

### Env

| Var | Required | Meaning |
|-----|----------|---------|
| `POCKETBI_TOKEN` | yes | Install token from the connect wizard |
| `POCKETBI_URL` | yes in prod | Control-plane WebSocket URL (`wss://…`) |

## What it does / does not do

**Does:** dial out, authenticate with token, open TCP to `host:port` when asked, relay bytes.

**Does not:** listen on the public internet, store DB passwords, run SQL, or require inbound firewall rules.

## Local development

```bash
cargo build
./target/debug/echo-server          # terminal 1 — fake private DB
# terminal 2
HUB_TOKEN=dev-token HUB_WSS_ADDR=127.0.0.1:8700 HUB_FORWARD_ADDR=127.0.0.1:15432 HUB_DIAL_HOST=127.0.0.1 HUB_DIAL_PORT=9999 ./target/debug/hub
# terminal 3
POCKETBI_TOKEN=dev-token POCKETBI_URL=ws://127.0.0.1:8700/agent ./target/debug/agent
# terminal 4
printf 'hello world\n' | nc -N 127.0.0.1 15432
```

`hub` and `echo-server` are **dev-only** binaries for local demos. The published Docker image contains only `agent`.

### Build the public image locally

```bash
docker build -t pocketbi/agent:latest .
docker run --rm -e POCKETBI_TOKEN=dev-token -e POCKETBI_URL=ws://host.docker.internal:8700/agent pocketbi/agent:latest
```

## Tests

```bash
cargo test --all
```

- Unit tests cover the WebSocket JSON protocol (`hello` / `dial` / `data` / …).
- `tests/tunnel_e2e.rs` boots `echo-server` + `hub` + `agent` and asserts a TCP echo through the reverse tunnel.

## CI (GitHub Actions)

[`.github/workflows/ci.yml`](.github/workflows/ci.yml):

| Job | When | What |
|-----|------|------|
| `test` | every push / PR | `fmt`, `clippy`, unit + tunnel e2e |
| `docker` | `main` / `develop` / `v*` tags (after tests pass) | push `pocketbi/agent` to Docker Hub |

Secrets: `DOCKER_USERNAME`, `DOCKER_PASSWORD`.

## Transparency

- All agent source lives in this repository.
- Releases are tagged; Docker Hub / registry tags should match git tags (`v0.1.0`, etc.).
- Prefer pulling by digest in production when you pin versions.

## Security notes

- Treat `POCKETBI_TOKEN` like a secret; rotate/revoke from the Pocket BI UI if leaked.
- The agent only dials targets requested by an authenticated control-plane session for that token.
- Run it on a host that should be allowed to reach the database (DB VM or private-network jump host).
