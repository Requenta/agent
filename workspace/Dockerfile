# Extend the supplier-approved Debian/Ubuntu CUDA/PyTorch image, pinned by digest.
# CPU integration uses ubuntu:24.04, never published as GPU capacity.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server python3 coreutils \
 && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_* \
 && mkdir -p /run/sshd /etc/requenta \
 && (if getent passwd 1000 >/dev/null; then userdel "$(getent passwd 1000 | cut -d: -f1)"; fi) \
 && useradd --uid 1000 --user-group --home-dir /workspace/home --shell /bin/bash requenta \
 && passwd -d requenta
COPY sshd_config /etc/requenta/sshd_config
ENV HOME=/workspace/home
USER 1000:1000
WORKDIR /workspace
