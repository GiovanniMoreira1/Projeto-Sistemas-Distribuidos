FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ChatCSharp.slnx .
COPY Broker/Broker.csproj Broker/
COPY Servidor/Servidor.csproj Servidor/
COPY Cliente/Cliente.csproj Cliente/
RUN dotnet restore

COPY . .

RUN dotnet publish Broker -c Release -o /out/broker --no-self-contained
RUN dotnet publish Servidor -c Release -o /out/servidor --no-self-contained
RUN dotnet publish Cliente -c Release -o /out/cliente --no-self-contained

FROM mcr.microsoft.com/dotnet/runtime:10.0

WORKDIR /app

COPY --from=build /out/broker ./broker
COPY --from=build /out/servidor ./servidor
COPY --from=build /out/cliente ./cliente
COPY usuarios.json canais.json login.json ./

CMD ["dotnet", "servidor/Servidor.dll"]
