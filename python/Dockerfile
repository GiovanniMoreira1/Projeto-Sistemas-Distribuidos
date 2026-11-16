# contexto de build: raiz do repositorio (usa proto/client.proto compartilhado)
FROM python:3.13-slim AS build

WORKDIR /src

RUN pip install --no-cache-dir grpcio-tools==1.76.0

COPY proto/client.proto proto/
RUN python -m grpc_tools.protoc -I proto --python_out=. proto/client.proto

FROM python:3.13-slim

WORKDIR /app

COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=build /src/client_pb2.py ./gen/
COPY python/broker ./broker
COPY python/proxy ./proxy
COPY python/referencia ./referencia
COPY python/servidor ./servidor
COPY python/cliente ./cliente
COPY python/seed ./seed

ENV PYTHONPATH=/app/gen

CMD ["python", "servidor/main.py"]
