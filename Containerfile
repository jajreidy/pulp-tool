# Containerfile for pulp-tool
# Base image: Fedora 42
FROM registry.fedoraproject.org/fedora:44@sha256:840ca148f79254df4f6fa229f6e8c28df7e6ddcc0a637d9a8d3d359ac2f2808b

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
