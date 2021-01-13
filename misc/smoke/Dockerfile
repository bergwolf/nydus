FROM rust:1.49

RUN rustup component add clippy
RUN rustup component add rustfmt
RUN rustup update

RUN cat /etc/apt/sources.list
RUN apt-get update && apt-get install -yq tree

WORKDIR /nydus-rs

CMD make test
