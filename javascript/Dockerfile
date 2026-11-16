# contexto de build: raiz do repositorio (usa proto/client.proto compartilhado)
FROM node:18-bullseye

# Dependências de build para compilar o binding nativo do zeromq
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/javascript

COPY javascript/package*.json ./
RUN npm install

COPY proto/client.proto /app/proto/client.proto
COPY javascript/ ./

CMD ["node", "servidor.js"]
