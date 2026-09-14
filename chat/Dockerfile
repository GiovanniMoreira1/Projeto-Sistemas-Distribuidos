FROM node:18-bullseye

# Dependências de build para compilar o binding nativo do zeromq
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

CMD ["node", "servidor.js"]