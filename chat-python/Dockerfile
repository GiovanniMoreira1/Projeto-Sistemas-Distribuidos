FROM python:3.13-slim AS build

WORKDIR /src

RUN pip install --no-cache-dir grpcio-tools==1.76.0

COPY proto/client.proto proto/
RUN python -m grpc_tools.protoc -I proto --python_out=. proto/client.proto

FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=build /src/client_pb2.py ./gen/
COPY broker ./broker
COPY proxy ./proxy
COPY referencia ./referencia
COPY servidor ./servidor
COPY cliente ./cliente
COPY seed ./seed

ENV PYTHONPATH=/app/gen

CMD ["python", "servidor/main.py"]
