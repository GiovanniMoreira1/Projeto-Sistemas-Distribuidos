# Chat distribuido (Partes 1 a 4) - implementacao C#

Sistema de chat distribuido em que bots (clientes), servidores e intermediarios conversam por ZeroMQ (NetMQ).
O projeto do grupo usa varias linguagens; **o que une todas elas e o contrato de comunicacao** (`Proto/client.proto`, portas, nomes das acoes e formato dos topicos), identico em todas as implementacoes.
Esta e a implementacao em C#: cliente, servidor, broker, proxy e servico de referencia.

## Como executar

```
docker compose up --build
```

Sobe broker, proxy, referencia, 2 servidores C# e 2 clientes. Os dados dos servidores ficam em `./dados/<nome-do-servidor>/`.
Para outras linguagens do grupo, acrescente os servicos delas no compose; **o nome de cada servidor (servico = `NOME_SERVIDOR`) precisa ser unico**, pois e usado como endereco pelos outros servidores.

## Arquitetura

```
Req-Rep:   cliente (REQ) -> broker (ROUTER :5559 / DEALER :5560) -> servidor (REP)
Pub-Sub:   servidor (PUB) -> proxy (XSUB :5557 / XPUB :5558) -> cliente/servidor (SUB)
Referencia: servidor (REQ) -> referencia (REP :5556)
Servidor-servidor: servidor (REQ) -> outro servidor (REP :5559)   (eleicao e relogio)
```

| Processo | Projeto | Papel |
|---|---|---|
| `broker` | `Broker/` | balanceia as requisicoes dos clientes entre os servidores |
| `proxy` | `Proxy/` | repassa publicacoes (canais dos bots e topico `servers`) |
| `referencia` | `Referencia/` | da o rank a cada servidor, guarda a lista de servidores e remove os que ficam 30 s sem heartbeat |
| `servidor` | `Servidor/` | atende os clientes, publica nos canais, participa da eleicao e sincroniza o relogio |
| `cliente` | `Cliente/` | bot: login, garante >= 5 canais e >= 3 inscricoes, publica 10 mensagens por rodada |

## Serializacao

Protocol Buffers em binario (`Proto/client.proto`, o mesmo do grupo). Toda mensagem carrega `timestamp` (ms, `double`) e `contadorMensagem` (relogio logico).

| Mensagem | Uso |
|---|---|
| `Mensagem` | cliente/servidor -> servidor/referencia (`timestamp`, `acao`, `canal`, `conteudo`, `contadorMensagem`) |
| `Resposta` | resposta de servidor (`status`, `resposta`, `canais`, `contadorMensagem`, `timestamp`) |
| `RespostaSub` | publicacoes e anuncio de coordenador |
| `RespostaReferencia` | resposta da referencia (`rank`, `servidores`, ...) |

Acoes: `login`, `listar`, `cadastrar`, `publicar` (clientes); `consultar_rank`, `listar_servidores`, `heartbeat` (servidor -> referencia); `eleicao`, `sincronizar` (servidor -> servidor).
Topicos: frame `"<canal>|"` + mensagem serializada; o anuncio do coordenador usa o topico `servers|`.

## Parte 3: relogios e heartbeat

**Relogio logico** (cliente e servidor): o contador sobe em 1 antes de cada envio e vai na mensagem; ao receber, vira `max(local, recebido)`. Vale para toda mensagem trocada, inclusive com a referencia e entre servidores.

**Referencia:** o servidor pede o rank (`consultar_rank`) ao iniciar; a referencia guarda nome e rank sem repeticao. Servidores pedem a lista (`listar_servidores`). A cada 10 mensagens de clientes o servidor envia `heartbeat`; se o servidor ficar mais de 30 s sem enviar, a referencia o remove. Se o servidor foi removido e volta, ele se recadastra.

## Parte 4: eleicao e sincronizacao (algoritmo Bully)

1. Ao iniciar (e sempre que o coordenador falha) o servidor pede a lista de servidores a referencia e envia `eleicao` **apenas aos de rank maior**.
2. Quem recebe `eleicao` responde `OK` e tambem inicia a sua propria eleicao.
3. Se algum servidor de rank maior respondeu `OK`, este espera o anuncio (ate 10 s; se nao vier, refaz a eleicao). Se ninguem respondeu, ele e o coordenador: zera o offset do relogio e publica o nome no topico `servers` (duas vezes, para tolerar a perda da primeira por conexao recem-aberta).
4. Todos mantem a variavel `coordenador` com o nome do eleito (atualizada pelo topico `servers`).
5. A cada 15 mensagens de clientes, o servidor pede a hora ao coordenador (`sincronizar`) e ajusta o seu relogio: `offset = hora_do_coordenador + RTT/2 - hora_local`. Se o coordenador nao responder em 3 s ou disser que nao e coordenador, o servidor inicia uma eleicao.

O servico de referencia **nao devolve hora** no heartbeat; a hora vem somente do coordenador.

Observacao: o fluxo segue os diagramas do enunciado (`REQ relogio -> REP hora correta`), isto e, o servidor pede a hora ao coordenador e corrige o proprio relogio; nao e o Berkeley completo, em que o coordenador consultaria todos e calcularia a media.

## Outras escolhas

**Acoes e status** (iguais aos demais servidores do grupo): `login` (200 / 404), `listar` (200), `cadastrar` (200 / 404 canal ja existe / 400 nome invalido), `publicar` (200 / 400 / 500).

**Persistencia** (um diretorio por servidor, nada compartilhado): `usuarios.json`, `canais.json`, `login.json`, `mensagens.jsonl` (cada requisicao e resposta) e `publicacoes.jsonl` (cada publicacao). Os `.jsonl` so recebem linhas novas.

**Parametros:** `NOME_SERVIDOR` (variavel de ambiente, padrao = nome da maquina). Heartbeat a cada 10 mensagens, sincronizacao a cada 15 e porta servidor-servidor 5559 sao constantes em `Servidor/Program.cs`.

**Organizacao do codigo:** cada `Program.cs` e dividido em `#region`s (configuracao, comunicacao, eleicao, acoes dos clientes etc.) para facilitar a leitura.

## Limitacoes conhecidas

- Cada servidor tem seus proprios canais e nao ha replicacao: a listagem depende de qual servidor atendeu. Por isso `publicar` nao exige que o canal exista no servidor que recebeu o pedido.
- Fora do Docker, a porta servidor-servidor 5559 colide com a do broker no mesmo host, e nao ha variavel de ambiente para trocar a porta.
