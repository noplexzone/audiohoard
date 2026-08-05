FROM python:3.12-slim

LABEL org.opencontainers.image.title="Audiohoard" \
      org.opencontainers.image.description="Self-hosted music acquisition, metadata enrichment, and library naming" \
      org.opencontainers.image.source="https://github.com/noplexzone/audiohoard" \
      org.opencontainers.image.version="0.18.1"

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg \
      libchromaprint-tools \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]" || pip install --no-cache-dir -e .

COPY . .

RUN pip install --no-cache-dir -e .

RUN pip install --no-cache-dir yt-dlp

RUN mkdir -p /app/data

EXPOSE 8000

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8000/health/ready || exit 1

ENTRYPOINT ["/entrypoint.sh"]
