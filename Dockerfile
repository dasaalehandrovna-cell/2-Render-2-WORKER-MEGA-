FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# WORKER owns normal MEGA backup/restore.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && wget -q https://mega.nz/linux/repo/Debian_12/amd64/megacmd-Debian_12_amd64.deb -O /tmp/megacmd.deb \
    && apt-get install -y /tmp/megacmd.deb \
    && rm -f /tmp/megacmd.deb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 10000
CMD ["python", "worker_service.py"]
