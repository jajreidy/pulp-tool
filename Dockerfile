# Dockerfile for pulp-tool
# Base image: Fedora 42
FROM registry.fedoraproject.org/fedora:45@sha256:542ae438dc75170c22b20416279277122e75f38e9cd8aa3ce69af36071c9fed1

# Install Python 3 and pip
RUN dnf install -y python3 python3-pip jq && dnf clean all

# Set working directory
WORKDIR /app

# Copy project files needed for installation
COPY setup.py pyproject.toml README.md MANIFEST.in VERSION ./
COPY pulp_tool/ ./pulp_tool/

# Install pulp-tool and its runtime dependencies
RUN pip install --no-cache-dir .

# The pulp-tool command is now available in PATH
# No entrypoint specified - users can run commands as needed
