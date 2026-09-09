FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg openssl \
 && mkdir -p /etc/apt/keyrings \
 && curl -fsSL https://mega.nz/linux/repo/Debian_12/Release.key | gpg --dearmor -o /etc/apt/keyrings/mega.gpg \
 && echo "deb [signed-by=/etc/apt/keyrings/mega.gpg] https://mega.nz/linux/repo/Debian_12/ ./" > /etc/apt/sources.list.d/mega.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends megacmd \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# R45: copy the complete HEAVY application into /app before CMD.
COPY worker_service.py runtime_config.py ./

# R48: compile is not enough; execute module import after dependencies are installed.
# This catches use-before-definition / bad final-owner aliases during Docker build.
RUN python -m py_compile worker_service.py runtime_config.py \
 && python -c "import worker_service as w; assert w.process_file_job.__name__ == 'process_file_job_r43'; assert w.process_google_job.__name__ == 'process_google_job_r43'; assert not w._R48_WORKERS_STARTED; print('R48 HEAVY startup smoke PASS')"

CMD ["python", "worker_service.py"]
