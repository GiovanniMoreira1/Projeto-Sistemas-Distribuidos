FROM rust:slim-bookworm

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libzmq3-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml build.rs client.proto ./
COPY src ./src
RUN cargo build --release

COPY usuarios.json canais.json login.json mensagens.json publicacoes.json ./

ENV PATH="/app/target/release:${PATH}"

CMD ["servidor"]
