FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src/csharp

COPY proto/client.proto /src/proto/client.proto
COPY csharp/ChatCSharp.slnx .
COPY csharp/Broker/Broker.csproj Broker/
COPY csharp/Proxy/Proxy.csproj Proxy/
COPY csharp/Referencia/Referencia.csproj Referencia/
COPY csharp/Servidor/Servidor.csproj Servidor/
COPY csharp/Cliente/Cliente.csproj Cliente/
RUN dotnet restore

COPY csharp/ .

RUN dotnet publish Servidor -c Release -o /out/servidor --no-self-contained
RUN dotnet publish Cliente -c Release -o /out/cliente --no-self-contained

FROM mcr.microsoft.com/dotnet/runtime:10.0

WORKDIR /app

COPY --from=build /out/servidor ./servidor
COPY --from=build /out/cliente ./cliente
COPY csharp/usuarios.json ./

CMD ["dotnet", "servidor/Servidor.dll"]
