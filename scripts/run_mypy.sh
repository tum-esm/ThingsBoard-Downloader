#!/bin/bash

set -o errexit  # Exit on any command failure
set -o nounset  # Treat unset variables as an error
set -o pipefail # Catch errors in pipelines

# Get the absolute path of the script's parent directory
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Navigate to the project root (assuming the script is in a subfolder of the project)
cd "$SCRIPT_DIR/.." || exit 1

echo "Removing old mypy cache..."
rm -rf .mypy_cache

export MYPYPATH="$PWD"
MY_FILES="."
echo "No src folder found. MYPYPATH set to project root: $MYPYPATH"

echo "Checking files using mypy..."
mypy --explicit-package-bases $MY_FILES

echo "Mypy check completed successfully!"