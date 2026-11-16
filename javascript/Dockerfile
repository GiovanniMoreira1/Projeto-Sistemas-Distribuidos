FROM node:18-bookworm

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
