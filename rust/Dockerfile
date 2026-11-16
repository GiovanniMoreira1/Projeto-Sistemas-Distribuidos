# contexto de build: raiz do repositorio (usa proto/client.proto compartilhado)
FROM rust:slim-bookworm AS build

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libzmq3-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/rust

COPY proto/client.proto /app/proto/client.proto
COPY rust/Cargo.toml rust/build.rs ./
COPY rust/src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libzmq5 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/rust/target/release/servidor /app/rust/target/release/client /usr/local/bin/
COPY rust/usuarios.json ./

CMD ["servidor"]
