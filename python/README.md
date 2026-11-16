# Chat distribuido (Partes 1 a 4) - implementacao Python

Sistema de chat distribuido em que bots (clientes), servidores e intermediarios conversam por ZeroMQ.
O projeto do grupo usa varias linguagens; **o que une todas elas e o contrato de comunicacao** (`proto/client.proto`, portas, nomes das acoes e formato dos topicos), identico em todas as implementacoes.
Esta e a implementacao em Python: cliente, servidor, broker, proxy e (o mesmo) servico de referencia.

## Como executar

```
docker compose up --build
```

Sobe broker, proxy, referencia, 2 servidores Python e 2 clientes. Os dados dos servidores ficam em `./dados/<nome-do-servidor>/`.
Para outras linguagens do grupo, acrescente os servicos delas no compose; **o nome de cada servidor (servico = `NOME_SERVIDOR`) precisa ser unico**, pois e usado como endereco pelos outros servidores.

## Arquitetura

```
Req-Rep:   cliente (REQ) -> broker (ROUTER :5559 / DEALER :5560) -> servidor (REP)
Pub-Sub:   servidor (PUB) -> proxy (XSUB :5557 / XPUB :5558) -> cliente/servidor (SUB)
Referencia: servidor (REQ) -> referencia (REP :5556)
Servidor-servidor: servidor (REQ) -> outro servidor (REP :5559)   (eleicao e relogio)
```

| Processo | Papel |
|---|---|
| `broker` | balanceia as requisicoes dos clientes entre os servidores |
| `proxy` | repassa publicacoes (canais dos bots e topico `servers`) |
| `referencia` | da o rank a cada servidor, guarda a lista de servidores e remove os que ficam 30 s sem heartbeat |
| `servidor` | atende os clientes, publica nos canais, participa da eleicao e sincroniza o relogio |
| `cliente` | bot: login, garante >= 5 canais e >= 3 inscricoes, publica 10 mensagens por rodada |

## Serializacao

Protocol Buffers em binario (`proto/client.proto`, o mesmo do grupo). Toda mensagem carrega `timestamp` (ms, `double`) e `contadorMensagem` (relogio logico).

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

Observacao: o fluxo segue os diagramas do enunciado (`REQ relogio -> REP hora correta`, igual ao servidor JS), isto e, o servidor pede a hora ao coordenador e corrige o proprio relogio; nao e o Berkeley completo, em que o coordenador consultaria todos e calcularia a media.

## Outras escolhas

**Acoes e status** (iguais ao servidor JS): `login` (200 / 404), `listar` (200), `cadastrar` (200 / 404 canal ja existe / 400 nome invalido), `publicar` (200 / 400 / 500).

**Persistencia** (um diretorio por servidor, nada compartilhado): `usuarios.json`, `canais.json`, `login.json`, `mensagens.jsonl` (cada requisicao e resposta) e `publicacoes.jsonl` (cada publicacao). Os `.jsonl` so recebem linhas novas.

**Parametros por variavel de ambiente** (padroes seguem o enunciado): `HEARTBEAT_A_CADA=10`, `SINCRONIZAR_A_CADA=15`, `NOME_SERVIDOR`, `PORTA_SERVIDORES=5559`. `DESVIO_RELOGIO_MS` (padrao 0) existe so para testes: simula um relogio desalinhado.

## Limitacoes conhecidas

- Cada servidor tem seus proprios canais e nao ha replicacao: a listagem depende de qual servidor atendeu. Por isso `publicar` nao exige que o canal exista no servidor que recebeu o pedido.
- Dentro de um mesmo host (fora do Docker) a porta servidor-servidor 5559 colide com a do broker; use `PORTA_SERVIDORES` diferente nesse caso.
