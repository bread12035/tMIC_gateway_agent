FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# App source
COPY gateway/ ./gateway/
COPY agent/ ./agent/
COPY tools/ ./tools/
COPY skills/ ./skills/

ENV SKILLS_BASE_PATH=/app/skills \
    WORKSPACE_TMP_DIR=/tmp/workspace \
    PYTHONPATH=/app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "gateway.main"]
