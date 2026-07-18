FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --break-system-packages pyarrow numpy
COPY runner_worker.py /runner_worker.py
ENTRYPOINT ["python3", "/runner_worker.py"]
