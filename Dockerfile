# Dockerfile for pulp-tool
# Base image: UBI 10 minimal (system python3 is 3.12)
FROM registry.access.redhat.com/ubi10-minimal

ARG VERSION=1.0.0
ARG RELEASE=1

LABEL name="pulp-tool-container" \
      description="Konflux container image for pulp-tool Pulp API client operations" \
      summary="pulp-tool container image for uploading RPMs and artifacts to Pulp" \
      maintainer="Rok Artifact Storage Team <jreidy@redhat.com>" \
      io.k8s.description="Konflux container image for pulp-tool Pulp API client operations" \
      com.redhat.component="pulp-tool-container" \
      distribution-scope="public" \
      release="${RELEASE}" \
      version="${VERSION}" \
      url="https://github.com/konflux-ci/pulp-tool/" \
      vendor="Red Hat, Inc."

# OpenShift preflight check requires licensing files under /licenses
COPY LICENSE /licenses/LICENSE

RUN microdnf update -y && \
    microdnf install -y \
        python3 \
        python3-pip \
        jq \
        shadow-utils && \
    microdnf clean all && \
    pip3 install --no-cache-dir --root-user-action=ignore uv

WORKDIR /app

# Runtime install from uv.lock (regenerate with: make lock).
COPY pyproject.toml uv.lock README.md MANIFEST.in VERSION ./
COPY pulp_tool/ ./pulp_tool/

ENV UV_SYSTEM_PYTHON=1
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt && \
    pip3 install --no-cache-dir --root-user-action=ignore -r /tmp/requirements.txt && \
    SETUPTOOLS_SCM_PRETEND_VERSION="${VERSION}" pip3 install --no-cache-dir --root-user-action=ignore --no-deps . && \
    rm -rf /root/.cache /root/.local /tmp/requirements.txt

RUN useradd -lms /bin/bash -u 1001 -g 0 pulp-tool && \
    chown -R 1001:0 /app && \
    chmod -R g=u /app

USER 1001

# The pulp-tool command is available in PATH; no entrypoint — Tekton invokes it directly.
