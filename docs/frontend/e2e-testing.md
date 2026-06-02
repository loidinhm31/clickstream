# Frontend E2E Testing Guide

Playwright-based live-stack E2E guidance for the Clickstream frontend and its dependent services.

## Recommended Command

```bash
bash scripts/run-e2e.sh
```

Or via Make:

```bash
make test-e2e
make test-e2e-headed
```

## What `run-e2e.sh` Does

The wrapper is the supported entrypoint. It is more reliable than starting services manually and then invoking `npx playwright test`.

Current flow:

1. Verifies infrastructure services with `scripts/verify-setup.sh`
2. Ensures host application services are reachable:
   - Ingestion API: `http://127.0.0.1:9051/actuator/health`
   - Real-time Analytics: `http://127.0.0.1:9052/api/realtime/health`
   - Frontend: `http://127.0.0.1:9059`
3. Restarts Raw Archiver on `9053` with `FLUSH_INTERVAL_SECONDS=10`
4. Installs E2E dependencies if `tests/e2e-playwright/node_modules` is missing
5. Detects a cached Playwright Chromium headless-shell fallback if the pinned Playwright browser revision is unavailable
6. Runs the Playwright suite

The wrapper fails fast and prints recent service logs when a required service does not become ready.

## Infrastructure Prerequisites

`run-e2e.sh` expects infrastructure to exist already:

- Kafka
- MongoDB
- Kafka UI
- Spark ETL

The backend modules should also be built at least once so the Raw Archiver jar exists:

```bash
make build-all
```

Or, if you only need the backend artifacts:

```bash
mvn clean install -DskipTests --settings .mvn/settings.xml
```

Recommended bootstrap:

```bash
make start-infra
make start-spark
```

If you already use the full development stack:

```bash
make start-all
```

## Ports Used

| Service | Port | Managed by |
|---|---:|---|
| Frontend (Vite) | 9059 | `run-e2e.sh` checks/starts |
| Ingestion API | 9051 | `run-e2e.sh` checks/starts |
| Real-time Analytics | 9052 | `run-e2e.sh` checks/starts |
| Raw Archiver | 9053 | `run-e2e.sh` restarts |
| MongoDB | 9055 | Infrastructure |
| Kafka | 9056 | Infrastructure |

## Fedora / Podman Notes

Fedora development hosts commonly use Podman via the Docker-compatible CLI.

Working setup:

```bash
export XDG_RUNTIME_DIR=/run/user/$(id -u)
```

`scripts/verify-setup.sh` now:

- accepts either `docker` or `podman`
- bootstraps `XDG_RUNTIME_DIR` when possible
- verifies infrastructure only

## Playwright on Fedora

### Problem

On this project, Playwright browser installation may hang after download on Fedora hosts, even when the download reaches 100%.

Observed pattern:

- `npx playwright install`
- `npx playwright install chromium`
- `npx playwright install --only-shell chromium`

can stall after the archive download step without producing a usable `1217` browser binary.

The same behavior may also appear inside an Ubuntu `distrobox`, so `distrobox` is not guaranteed to fix it.

### Supported Workaround

The project now supports a Chromium override through:

```bash
PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/path/to/chrome-headless-shell
```

`scripts/run-e2e.sh` automatically looks for the newest cached binary under:

```text
~/.cache/ms-playwright/chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell
```

If one exists, the script exports `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` before launching Playwright.

### Manual Override Example

```bash
export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$HOME/.cache/ms-playwright/chromium_headless_shell-1223/chrome-headless-shell-linux64/chrome-headless-shell"
bash scripts/run-e2e.sh
```

## Distrobox Fallback

If you want to test the browser installation path in a supported Ubuntu userspace:

```bash
distrobox list
distrobox enter ubuntu
cd /mnt/data/ws/sharing/clickstream/tests/e2e-playwright
npx playwright install --only-shell chromium
```

Use this for diagnosis only. The preferred project workflow is still `bash scripts/run-e2e.sh` on the host.

## Troubleshooting

### `sg: group 'docker' does not exist`

Cause:
- old Docker-group assumption on Podman/Fedora

Resolution:
- fixed in `scripts/verify-setup.sh`
- rerun `bash scripts/run-e2e.sh`

### `page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:9059/`

Cause:
- frontend not listening on `9059`

Resolution:
- use `bash scripts/run-e2e.sh`
- the wrapper now checks and starts the frontend automatically

### Playwright says browser executable does not exist

Cause:
- pinned Playwright revision missing from local cache

Resolution:
1. try `bash scripts/run-e2e.sh`
2. if needed, set `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` manually
3. if no cached browser exists, attempt `npx playwright install --only-shell chromium`

### MongoDB or Parquet assertions time out

Cause:
- downstream consumers not ready, Spark ETL behind, or Raw Archiver not restarted cleanly

Resolution:
- rerun through `bash scripts/run-e2e.sh`
- inspect:

```bash
tail -n 80 logs/ingestion-api.log
tail -n 80 logs/realtime-analytics.log
tail -n 80 logs/raw-archiver.log
docker compose ps
```

## Validation Commands

```bash
cd tests/e2e-playwright && npm run typecheck
bash scripts/verify-setup.sh
bash scripts/run-e2e.sh
```
