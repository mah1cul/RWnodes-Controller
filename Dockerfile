FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        openssh-client \
        sshpass \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ansible.cfg .
COPY app ./app
COPY playbooks ./playbooks

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data/ssh_keys /data/ssh_key_presets /ssh_keys \
    && chown -R appuser:appuser /app /data

USER appuser

CMD ["python", "-m", "app.main"]
