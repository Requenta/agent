FROM python:3.13-alpine
ARG TARGETARCH
ARG KUBECTL_VERSION=v1.35.0
RUN apk add --no-cache ca-certificates curl \
 && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
 && curl -fsSLo /tmp/kubectl.sha256 "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl.sha256" \
 && echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c - \
 && chmod 755 /usr/local/bin/kubectl && rm /tmp/kubectl.sha256 && apk del curl
WORKDIR /app
COPY *.py ./
RUN chmod 755 /app/workspace_driver.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
USER 65532:65532
ENTRYPOINT ["python3", "/app/in_cluster.py"]
