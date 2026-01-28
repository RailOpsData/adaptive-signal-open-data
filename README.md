# Tram Delay Reduction Management

A GTFS data collection, analysis, and optimization system designed to reduce tram delays.

## Overview

 - **GTFS Data Collection**: Real-time data acquisition at 20-second intervals
 - **Simulation**: Optional external traffic simulation tools (not included)
 - **Optimization**: Delay reduction through optimization and experimental methods

## Quick Start

### 1. Install Dependencies
```bash
sudo apt update

# Docker (optional if you prefer Podman)
sudo apt install docker.io docker-compose-plugin -y

# Podman (optional alternative runtime)
sudo apt install podman podman-compose -y
```

### 2. Run
```bash
# Fetch GTFS static feeds (tram + bus) once
make run-ingest-static

# Fetch GTFS real-time feeds once (containerized)
make run-ingest-realtime

# Continuous GTFS real-time ingestion (pauses 00:00–04:59 JST)
make run-ingest-realtime-loop

# Use compose helpers (single-run containers)
make compose-ingest-realtime

# Compose helper for continuous ingestion
make compose-ingest-realtime-loop REALTIME_INTERVAL=20

# Example using Podman instead of Docker
CONTAINER_RUNTIME=podman COMPOSE_CMD="podman compose" make run-ingest-static
> If your Podman installation uses `podman-compose`, set `COMPOSE_CMD="podman-compose"` instead.
```

### 3. Cross-Platform Notes
- The Makefile is the canonical entry point for builds and one-off tasks.  
- macOS/Linux include `make` by default. On Windows, install a POSIX shell with `make` support (e.g. WSL, Git Bash, MSYS2) before running the commands above.  
- If you need to invoke the underlying shell scripts directly, see `scripts/`, but `make` keeps workflows consistent across laptops, servers, and CI/cloud runners.

## Directory Structure

```
adaptive-signal-open-data/
├── configs/                # Configuration templates (ingestion, simulation)
├── data/                   # Local storage layers for collected feeds
│   ├── bronze/
│   ├── raw_GCP/
│   ├── raw_test/
│   └── silver/
├── docker/                 # Container definitions and compose manifests
├── docs/                   # Project documentation (setup guides, notes)
├── logs/                   # Runtime and ingestion logs
├── requirements/           # Python dependency lockfiles per component
├── results/                # Analysis outputs, evaluation artefacts
 
├── scripts/                # Operational utilities (schedulers, helpers)
├── src/                    # Application source code
│   ├── gtfs_pipeline/      # GTFS ingestion CLI, config, persistence glue
│   ├── sim_bridge/         # Interfaces bridging to external simulators
│   └── training/           # RL/optimisation experiments and notebooks
├── Makefile                # Top-level automation and shortcuts
├── README.md
└── http-server.log         # Local dev HTTP server output (optional)
```

<small>Note: Some auxiliary or runtime directories are not tracked as part of the repository by default and are created or populated when needed. Examples include:
- `.github/` — CI configuration and workflow files maintained by the project or created when enabling CI.
- `logs/`, `data/bronze/`, `data/raw/`, `results/` — created and populated by ingestion, runtime tasks, or analysis jobs.
- `docker/` build artifacts or temporary files — produced when building container images or running compose flows.

Treat these directories as local/runtime artifacts. Use the container-based development workflow described above to keep environments reproducible and avoid committing local-only artifacts.
</small>

## Key Features

### GTFS Data Collection
- **Interval**: 20 seconds (real-time feeds)
- **Data**: GTFS Static (manual trigger), Trip Updates, Vehicle Positions
- **Storage**: Local filesystem

### Simulation
- **Engine**: Optional external traffic simulators (not included)
- **Purpose**: Traffic flow simulation (external tools)
- **Output**: Delay analysis results

### Optimization
- **Methods**: Optimization and experimental methods (no specific model is assumed)
- **Goal**: Delay reduction
- **Output**: Optimized operation plans

## Usage

### Data Collection
```bash
# GTFS static feeds (tram & bus). Set CLEAN_PREVIOUS=1 to clear old snapshots.
make run-ingest-static

# GTFS real-time feeds (one-off snapshot)
make run-ingest-realtime

# GTFS real-time feeds with raw protobuf/ZIP archives
make run-ingest-realtime-raw

# Compose helpers (one-off containers)
make compose-ingest-realtime

# Continuous real-time ingestion via scheduler (skips 00:00–04:59 JST)
make run-ingest-realtime-loop

# Compose helper for manual continuous run (no quiet hours, runs until stopped)
make compose-ingest-realtime-loop REALTIME_INTERVAL=20
```
> To run with Podman instead of Docker, set `CONTAINER_RUNTIME=podman` and `COMPOSE_CMD="podman compose"` (or `podman-compose`) before invoking `make`.

### Simulation
```bash
make run-sim
```

### Training
```bash
make run-train
```

## Configuration

### Environment Variables
- `GTFS_RT_SAVE_PROTO`: Set to `1` to archive raw GTFS-RT protobuf (`.pb`) alongside parsed JSON
- `GTFS_STATIC_SAVE_ZIP`: Set to `1` to archive raw GTFS Static ZIP payloads alongside parsed JSON
- `REALTIME_INTERVAL`: Override the loop interval (seconds) for compose-based continuous runs
- `CLEAN_PREVIOUS`: Set to `1` when running `make run-ingest-static` to purge existing snapshots before download
- `CONTAINER_RUNTIME`: Override container runtime (`docker`, `podman`, etc.). Defaults to `docker`.
- `COMPOSE_CMD`: Explicit compose command (e.g. `podman compose` or `podman-compose`). Defaults to `${CONTAINER_RUNTIME} compose`.

### Documentation
- `docs/REQUIREMENTS.md`: Dependencies management guide

## Troubleshooting

### Build Errors
```bash
make clean
make build-all
```

### Log Checking
```bash
# Application logs
tail -f logs/ingest.log
tail -f logs/scheduler-realtime.log
```

## Development

### Container-based development (start from the container)

This project is designed to be developed from within a containerized development environment. The instructions below cover common, reproducible workflows: opening the repository in VS Code (Dev Containers), starting JupyterLab from a container, running and attaching to containers via the Docker CLI, and using Jupytext to pair/sync notebooks with text files.

Prerequisites
- Docker or Podman installed and usable by your user (or via sudo).
- VS Code with the "Dev Containers" (Remote - Containers) extension for container attach workflows.
- A container image with project dependencies (or use a Python base image and install requirements inside the container).

Open the repository in VS Code (recommended)
- In VS Code: open Command Palette -> "Dev Containers: Open Folder in Container..." and choose the workspace folder.
- If you prefer the CLI, create and run a development container and then attach from VS Code:

```bash
# Quick dev container (one-off) — mounts repo and opens an interactive shell
docker run --rm -it \
	-v "$(pwd)":/workspace -w /workspace \
	--name as-dev python:3.11-slim /bin/bash

# Attach to a running container from VS Code: Command Palette -> Remote-Containers: Attach to Running Container...
```

Docker CLI: run and exec examples
- Run an interactive shell inside a temporary container that mounts the repository:

```bash
docker run --rm -it -v "$(pwd)":/app -w /app python:3.11-slim /bin/bash
```

- Start a named container (so you can re-attach) and keep it running:

```bash
docker run -d --name as-dev -v "$(pwd)":/app -w /app python:3.11-slim tail -f /dev/null

# Attach a shell to the running container
docker exec -it as-dev /bin/bash
```

JupyterLab in a container
- If you want to start JupyterLab directly from a container image that has dependencies installed:

```bash
docker run --rm -p 8888:8888 -v "$(pwd)":/workspace -w /workspace python:3.11-slim \
	bash -lc "pip install -r requirements/ingest.txt && jupyter lab --no-browser --ip=0.0.0.0 --allow-root"
```

- If a container is already running and contains the environment, start JupyterLab inside it:

```bash
docker exec -it as-dev bash -lc "jupyter lab --no-browser --ip=0.0.0.0 --allow-root --NotebookApp.token=''"
```

Note: binding ports and the token configuration above are examples for local development only. Protect Jupyter instances if they are reachable from other hosts.

Using Jupytext (pairing notebooks with text files)
- Install Jupytext in the container environment (once):

```bash
pip install jupytext
```

- Pair an existing notebook with a light-weight Python script (one-time pairing):

```bash
jupytext --set-formats ipynb,py notebooks/example.ipynb
```

- Sync between paired formats (keeps both files updated):

```bash
jupytext --sync notebooks/example.ipynb
```

- Recommended Git workflow: commit both the .py/.md text version and the .ipynb (or choose to commit only the text form). A minimal `.jupytext.toml` to keep ipynb <-> py pairing by convention:

```toml
[jupytext]
formats = "ipynb,py:percent"
default_jupytext_formats = "ipynb,py:percent"
```

Activating the virtual environment inside the container
- If you use micromamba (scripts/install_micromamba.sh is provided), activate the environment inside the container:

```bash
# example (after micromamba environment creation)
micromamba activate myenv
```

Quick tips
- If you use `docker compose` for local dev, prefer `docker compose run --service-ports` or `docker compose up` and then `docker exec -it <service> /bin/bash` to attach.
- Keep long-running dev containers named (e.g. `as-dev`) so you can re-attach and keep caches installed across sessions.
- For reproducibility, document the image name and tag used for development in `dev/` or in the `docker/` folder.

### Adding Dependencies
```bash
# Common for all jobs
echo "package>=1.0.0" >> requirements/base.txt

# Specific job only
echo "package>=1.0.0" >> requirements/ingest.txt
```

### Building
```bash
# Individual builds
make build-ingest
make build-sim
make build-train

# Build all
make build-all
```

## License

MIT License

## Contributing

Pull requests and issue reports are welcome.

## References

 - [GTFS Specification](https://developers.google.com/transit/gtfs)
