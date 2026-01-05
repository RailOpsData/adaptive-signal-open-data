#!/bin/bash
# install_micromamba.sh - Install Micromamba and create gtfs-sumo-rl environment

set -e  # Exit immediately on error

echo "Installing micromamba (1–2 min)..."

# 1. Download and extract micromamba binary
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba

# 2. Initialize shell integration for bash with custom root prefix
./bin/micromamba shell init --shell bash --root-prefix ~/micromamba

# # 3. Reload shell configuration to enable micromamba in this session
# source ~/.bashrc

# # 4. Create project environment from environment.yml
# echo "Creating gtfs-sumo-rl environment..."
# micromamba env create -f ~/adaptive-signal-open-data/requirements/environment.yml

# # 5. Activate environment and run a basic check
# micromamba activate gtfs-sumo-rl
# micromamba list | grep -E 'pandas|numpy' || echo "Warning: core packages not found in environment."

# echo "Installation complete."
# echo "Next steps:"
# echo "  micromamba activate gtfs-sumo-rl"
# echo "  jupyter lab  # start development"

