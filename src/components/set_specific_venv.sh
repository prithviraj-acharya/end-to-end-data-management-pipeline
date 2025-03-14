#!/bin/bash

# Check if the requirements file argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <requirements_file>"
    exit 1
fi

REQUIREMENTS_FILE=$1
VENV_DIR="venv"

# Function to check if a virtual environment is active
deactivate_venv() {
    if [[ "$VIRTUAL_ENV" != "" ]]; then
        echo "Deactivating existing virtual environment..."
        deactivate
    fi
}

# Deactivate any active virtual environment
deactivate_venv

# Create virtual environment if not exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv $VENV_DIR
fi

# Activate virtual environment
source $VENV_DIR/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies from the provided requirements file
pip install -r "$REQUIREMENTS_FILE"

echo "Environment setup complete!"
