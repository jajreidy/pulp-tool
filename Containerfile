# Containerfile for pulp-tool
# Base image: Fedora 42
FROM registry.fedoraproject.org/fedora:42@sha256:1a8a7625e48614e174e7a7bdf7bf5bd1c08ea02a11b918ef257cca5fd415fe94

# Install Python 3 and pip
RUN dnf install -y python3 python3-pip && dnf clean all

# Set working directory
WORKDIR /app

# Copy project files needed for installation
COPY setup.py pyproject.toml README.md MANIFEST.in VERSION ./
COPY pulp_tool/ ./pulp_tool/

# Install pulp-tool and its runtime dependencies
RUN pip install --no-cache-dir .

# The pulp-tool command is now available in PATH
# No entrypoint specified - users can run commands as needed
