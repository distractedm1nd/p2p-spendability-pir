# Deploy setup

This guide covers deployment for the **combined PIR server** (`pir-server`), which runs both nullifier and witness PIR in a single process. Standalone `spend-server` and `witness-server` binaries are also available if you only need one subsystem.

Two deployment paths:

- **[Binary setup](#binary-setup-operators)** — Download a pre-built binary and run the service. No Rust toolchain or git clone required.
- **[Source setup](#source-setup-developers)** — Build from source with CI/CD-driven deployment.

---

## Hardware requirements

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| **CPU** | x86-64 with AVX-512 | x86-64 with AVX-512 or ARM64 | x86-64 builds require AVX-512F (Intel Skylake-X / AMD Zen 4+). ARM64 builds have no special CPU requirement. |
| **RAM** | 16 GB | 32 GB | The combined server loads ~56 MB nullifier PIR data and ~64 MB witness PIR data, plus YPIR internal structures for both. Peak usage during initialization is higher. |
| **Disk** | 10 GB free | 20 GB free | Snapshot data, PIR databases, plus headroom. |
| **OS** | Linux (x86-64) | Ubuntu 22.04+ / Debian 12+ | macOS (arm64/amd64) binaries are also published but not recommended for production serving. |
| **Network** | Outbound HTTPS | Static IP or DNS A record | Needs outbound access to a lightwalletd gRPC endpoint for syncing. Inbound access on the serve port for wallet clients. |

---

## Binary setup (operators)

This path is for operators who want to run `pir-server` without cloning the repository or installing the Rust toolchain.

### 1. Download the binary

Grab the latest release from GitHub:

```bash
# Pick the asset for your platform
PLATFORM="linux-amd64"   # or: linux-arm64, darwin-arm64
VERSION=$(curl -s https://api.github.com/repos/valargroup/spendability-pir/releases/latest | grep tag_name | cut -d'"' -f4)

sudo mkdir -p /opt/pir-server
cd /opt/pir-server

# Download the binary and systemd unit
curl -fLO "https://github.com/valargroup/spendability-pir/releases/download/${VERSION}/pir-server-${PLATFORM}"
curl -fLO "https://github.com/valargroup/spendability-pir/releases/download/${VERSION}/pir-server.service"

sudo mv "pir-server-${PLATFORM}" pir-server
sudo chmod +x pir-server
```

### 2. Create the data directory

```bash
sudo mkdir -p /opt/pir-server/data
```

The server creates `nullifier/` and `witness/` subdirectories automatically.

### 3. Install the systemd service

```bash
sudo cp /opt/pir-server/pir-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable pir-server
sudo systemctl start pir-server
```

Verify the service is running:

```bash
sudo systemctl status pir-server
curl http://localhost:8080/health
```

The server syncs both subsystems concurrently from lightwalletd, builds both YPIR databases, and starts serving. During sync, `GET /health` returns 503 with per-subsystem progress. Once both are ready, it returns 200.

### API routes

The combined server exposes both subsystems under path prefixes:

| Route | Description |
|-------|-------------|
| `GET /health` | Combined health (200 when both serving, 503 during sync) |
| `GET /nullifier/health` | Nullifier subsystem health |
| `GET /nullifier/metadata` | Nullifier metadata |
| `GET /nullifier/params` | Nullifier YPIR parameters |
| `POST /nullifier/query` | Nullifier PIR query |
| `GET /witness/health` | Witness subsystem health |
| `GET /witness/metadata` | Witness metadata |
| `GET /witness/broadcast` | Witness broadcast data (shard roots + sub-shard roots) |
| `GET /witness/params` | Witness YPIR parameters |
| `POST /witness/query` | Witness PIR query |

Wallet clients connect using base URLs `https://server/nullifier` and `https://server/witness`.

---

## Source setup (developers)

This path is for contributors and operators who want to build from source with CI/CD-driven deployment.

### GitHub repository secrets

In the repo: **Settings -> Secrets and variables -> Actions**, add:

| Secret | Description |
|--------|-------------|
| `DEPLOY_HOST` | Remote hostname or IP (e.g. `pir.example.com` or `164.92.137.124`). If an IP, Caddy is configured with a `sslip.io` domain (e.g. `164-92-137-124.sslip.io`) for automatic TLS. If a domain name, it's used as-is. |
| `DEPLOY_USER` | SSH user on that host (for the DigitalOcean host, `deploy`). |
| `DEPLOY_SSH_KEY` | Private SSH key authorized for `DEPLOY_USER`. |

### One-time setup on the remote host

**Directory and permissions**

- Create the deploy directory: `sudo mkdir -p /opt/spend-server/data`
- Ensure the SSH user can write to that directory.
- Allow the SSH user to run the deployment `sudo` commands without a password.

**Systemd service**

The `pir-server` binary runs both nullifier and witness PIR in a single process. It needs:

- **lightwalletd endpoint**: Configured via `--lwd-url` (default in the service file: `https://us.zec.stardust.rest:443`).
- **Data directory**: For snapshots, configured via `--data-dir`. The server creates `nullifier/` and `witness/` subdirectories.
- **Port**: Configurable via `--listen` (default `127.0.0.1:8080`, behind Caddy).

A systemd unit file is provided at `docs/spend-server.service`. Copy to `/etc/systemd/system/`:

```bash
sudo cp /opt/spend-server/spend-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable spend-server
sudo systemctl start spend-server
```

**Caddy reverse proxy**

Caddy handles TLS termination and reverse-proxies HTTPS traffic to `pir-server` on `localhost:8080`. Install Caddy once on the remote host:

```bash
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install caddy
```

Caddy is enabled on boot by default. The CI pipeline deploys the Caddyfile (with the domain from the `DEPLOY_DOMAIN` secret) to `/etc/caddy/Caddyfile` and reloads the service on every deploy. Caddy automatically provisions and renews TLS certificates via Let's Encrypt.

Ensure the server's DNS A record points to this host and that ports 80 and 443 are open (Caddy needs port 80 for the ACME HTTP challenge).

### Changing deploy path or restart command

- **Deploy path**: Edit the `env.DEPLOY_PATH` in `.github/workflows/deploy.yml` (default `/opt/spend-server`).
- **Restart command**: Edit the "Install config and restart services" step in that workflow if you use a different service name.

### Manual runs

`deploy.yml` supports `workflow_dispatch`, so you can trigger it from **Actions -> Run workflow** without pushing to `main`.

### Test locally

From the workspace root:

```bash
# Build the combined server
make build

# Run with default settings
make run

# Or run with custom lightwalletd endpoint
make run LWD_URL=http://localhost:9067

# Build/run individual servers if needed
make build-nullifier
make run-nullifier
make build-witness
make run-witness
```

Then check `http://localhost:8080/health`.

---

## CI/CD workflows

The workflows in `.github/workflows/` handle building and deploying:

- **`ci.yml`** — Runs format checks, clippy, and tests on every push/PR to `main`.
- **`deploy.yml`** — Builds on every push to `main` and deploys to a remote host via SSH.
- **`release.yml`** — Builds multi-platform binaries and publishes a GitHub Release on version tags.

---

## Migrating from spend-server to pir-server

If you're running the standalone `spend-server` and want to switch to the combined `pir-server`:

1. Stop the old service: `sudo systemctl stop spend-server`
2. Move existing nullifier snapshots: `sudo mkdir -p /opt/pir-server/data/nullifier && sudo mv /opt/spend-server/data/snapshot.bin /opt/pir-server/data/nullifier/`
3. Install the new binary and service file as described above.
4. The witness subsystem will sync from scratch on first start (takes a few minutes with subtree root acceleration).
