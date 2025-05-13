#!/bin/bash

# --- Configuration ---
PYTHON_VERSION="3.13"
VENV_NAME=".venv"
REQ_FILE="requirements.txt"
LOG_DIR="logs"
DATA_DIR="api/v1/market"
DICT_DIR="dictionaries"

# --- Helper Functions ---
print_success() {
    echo -e "\033[32m✓ $1\033[0m"
}
print_error() {
    echo -e "\033[31m✗ $1\033[0m"
}
print_info() {
    echo -e "\033[34m• $1\033[0m"
}
print_warning() {
    echo -e "\033[33m⚠️ $1\033[0m"
}

# --- Ensure script stops on error ---
set -e

# --- Main Setup Logic ---
print_info "Starting project setup..."

# 1. Check/Install Python
print_info "Checking for Python ${PYTHON_VERSION}..."
if ! command -v python${PYTHON_VERSION%.*} &> /dev/null || ! python${PYTHON_VERSION%.*} --version | grep -q "${PYTHON_VERSION}"; then
    print_warning "Python ${PYTHON_VERSION} not found or incorrect version. Attempting to install/update..."
    sudo apt-get update
    sudo apt-get install -y software-properties-common
    # Consider using deadsnakes PPA for specific Python versions if needed
    # sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev build-essential
    print_success "Python ${PYTHON_VERSION} installed/updated."
else
    print_success "Python ${PYTHON_VERSION} found."
fi

# 2. Create Virtual Environment
print_info "Creating virtual environment '${VENV_NAME}'..."
if [ ! -d "${VENV_NAME}" ]; then
    python${PYTHON_VERSION%.*} -m venv ${VENV_NAME}
    print_success "Virtual environment created."
else
    print_warning "Virtual environment '${VENV_NAME}' already exists. Skipping creation."
fi

# 3. Activate Virtual Environment (Informational - Activation needs to happen in the calling shell)
print_info "To activate the virtual environment, run: source ${VENV_NAME}/bin/activate"

# 4. Install Requirements
print_info "Installing dependencies from ${REQ_FILE}..."
# Ensure pip is up-to-date within the venv
"${VENV_NAME}/bin/pip" install --upgrade pip
if [ -f "${REQ_FILE}" ]; then
    "${VENV_NAME}/bin/pip" install -r "${REQ_FILE}"
    print_success "Dependencies installed successfully."
else
    print_error "${REQ_FILE} not found. Cannot install dependencies."
    exit 1
fi

# 5. Verify Installations
print_info "Verifying installations..."
"${VENV_NAME}/bin/python" --version
"${VENV_NAME}/bin/pip" list

# 6. Create necessary directories
print_info "Creating required directories..."
mkdir -p "${LOG_DIR}"
mkdir -p "${DATA_DIR}"
mkdir -p "${DICT_DIR}"
print_success "Directories '${LOG_DIR}', '${DATA_DIR}', and '${DICT_DIR}' ensured."

print_success "Project setup completed successfully!"
print_info "Remember to activate the virtual environment: source ${VENV_NAME}/bin/activate"

exit 0
