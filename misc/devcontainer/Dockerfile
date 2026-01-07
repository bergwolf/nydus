FROM rust:1.84.0

ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV PATH=$PATH:/usr/local/go/bin/
ENV DOCKER=false
ENV GOPROXY=https://goproxy.cn
ENV REGISTRY_PORT=5077
ENV DISABLE_REGISTRY=true

# dockerfile tools
RUN sed -ie 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources
RUN apt update && apt install -y curl ca-certificates btrfs-progs

# golang
RUN wget https://go.dev/dl/go1.24.11.linux-amd64.tar.gz; tar xzf go1.24.11.linux-amd64.tar.gz; mv go /usr/local/; rm -f go1.24.11.linux-amd64.tar.gz
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/bin v1.64.8
RUN ls /usr/local/go/ ; go version
RUN git config --global --add safe.directory /nydus

# rust
COPY cargo-config.toml /usr/local/cargo/config.toml
RUN rustup default 1.84.0-x86_64-unknown-linux-gnu
RUN rustup component add clippy rustfmt

# docker
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -sSfL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/debian
Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF
COPY docker-daemon.json /etc/docker/daemon.json
#RUN cd root; truncate -s 2G btrfs-disk; mkfs.btrfs btrfs-disk; rm -rf /var/lib/docker/*
RUN apt update && apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# nydus tools
RUN apt install -y vim cmake sudo libcap2-bin jq
COPY prepare-snapshotter.sh /root/
COPY containerd_config.toml /etc/containerd/config.toml
COPY snapshotter_config.toml /etc/nydus/snapshotter_config.toml
COPY nydusd_config.json /etc/nydus/nydusd-config.fusedev.json
COPY prepare-old-nydus-bianry.sh /root/prepare-old-nydus-bianry.sh
RUN /root/prepare-snapshotter.sh
RUN /root/prepare-old-nydus-bianry.sh
ENV PATH=$PATH:/usr/bin/nydus-latest/

WORKDIR /nydus

COPY stop-service.sh /stop-service.sh
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
