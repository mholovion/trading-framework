FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY compile.sh /compile.sh
RUN chmod +x /compile.sh
ENTRYPOINT ["/compile.sh"]
