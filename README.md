# Projeto de Sistemas Distribuídos: troca de mensagens em canais

Versão simplificada de um BBS/IRC feita para a disciplina de Sistemas Distribuídos. Bots fazem login,
criam canais e publicam mensagens; os servidores atendem os bots, sincronizam os relógios, elegem um
coordenador e replicam os dados entre si. Toda a comunicação é feita com ZeroMQ e tudo roda em containers.

Cada integrante do grupo implementou cliente e servidor em uma linguagem diferente:

| Linguagem | Pasta | Cliente | Servidor |
|---|---|---|---|
| JavaScript (Node 18) | `javascript/` | `client.js` | `servidor.js` |
| Python 3.13 | `python/` | `cliente/main.py` | `servidor/main.py` |
| Rust | `rust/` | `src/bin/client.rs` | `src/bin/servidor.rs` |
| C# (.NET 10) | `csharp/` | `Cliente/Program.cs` | `Servidor/Program.cs` |
| Java 21 | `java/` | `sd/projeto/client/Client.java` | `sd/projeto/server/Server.java` |

Todos rodam ao mesmo tempo e conversam entre si: um bot Java pode ser atendido por um servidor Rust, a
publicação chega a um bot Python, e a eleição envolve servidores das cinco linguagens.

## Como rodar

```
docker compose up --build
```

O compose sobe 1 broker, 1 proxy, 1 serviço de referência e, para cada linguagem, 2 servidores e 2 bots.
Os bots funcionam sozinhos, sem interação.

Os dados de cada servidor ficam em `dados/<nome-do-servidor>/`. Para começar do zero:

```
docker compose down
rm -rf dados
```

Broker, proxy e referência usados no compose são os da pasta `python/`. As outras linguagens também têm
a sua versão desses processos (por exemplo `javascript/broker.js` e `csharp/Referencia/`), que podem ser
usadas no lugar, mas no compose sobe só uma instância de cada.

## Arquitetura

```
bots (REQ) ──> broker ROUTER :5559 / DEALER :5560 ──> servidores (REP)
servidores (PUB) ──> proxy XSUB :5557 / XPUB :5558 ──> bots e servidores (SUB)
servidores (REQ) ──> referência (REP :5556)
servidor (REQ) ──> outro servidor (REP :5559)
```

- **Broker:** distribui os pedidos dos bots entre os servidores (round-robin). Por isso dá para adicionar ou
  remover servidores sem parar o sistema.
- **Proxy:** repassa as publicações. Os bots assinam os canais por ele, e os servidores também o usam para
  avisar quem é o coordenador e para replicar dados.
- **Referência:** dá o rank de cada servidor, guarda a lista de servidores ativos e tira da lista quem fica
  30 s sem mandar heartbeat.
- **Servidor ↔ servidor:** usado para eleição, sincronização do relógio e transferência de estado.

O nome de cada servidor é o nome do serviço no compose (variável `NOME_SERVIDOR`, por exemplo
`server-rust-1`). Os outros servidores usam esse nome como endereço (`tcp://server-rust-1:5559`), então ele
precisa ser único.

## Mensagens

As cinco linguagens geram o código a partir do mesmo arquivo, `proto/client.proto`. A serialização é
Protocol Buffers em binário: o enunciado não permite JSON nem texto puro, e com um `.proto` só ninguém
precisa reimplementar o formato à mão.

| Mensagem | Uso |
|---|---|
| `Mensagem` | todo pedido: bot → servidor, servidor → referência, servidor → servidor e operações replicadas |
| `Resposta` | resposta de um servidor |
| `RespostaSub` | publicação num canal e anúncio de coordenador |
| `RespostaReferencia` | resposta da referência (rank e lista de servidores) |

Toda mensagem leva `timestamp` (ms) e `contadorMensagem` (relógio lógico).

Ações e respostas:

| Ação | Quem envia | Status |
|---|---|---|
| `login` | bot | 200 ok, 404 usuário não existe |
| `listar` | bot | 200, lista em `canais` |
| `cadastrar` | bot | 200 ok, 404 canal já existe, 400 nome inválido |
| `publicar` | bot | 200 ok, 400 canal ou texto vazio |
| `consultar_rank`, `listar_servidores`, `heartbeat` | servidor → referência | 200, 404 servidor não cadastrado |
| `eleicao`, `sincronizar`, `obter_estado` | servidor → servidor | 200, 400 não sou o coordenador, 503 ainda sincronizando |

Tópicos do Pub/Sub:

- `<canal>|`: publicações dos bots. O `|` no fim evita que quem assina `canal1` receba também `canal10`,
  já que o filtro do ZeroMQ é por prefixo.
- `servers|`: anúncio do coordenador eleito.
- `replicacao|<servidor>`: escritas replicadas (Parte 5).

Não há senha. O login só aceita os usuários de `usuarios.json` (`teste`, `teste2` ... `teste10`). Cada bot
tenta primeiro um nome que não existe, para mostrar a repetição do login em caso de erro.

## O bot

Todos os bots seguem o comportamento definido na Parte 2:

1. faz login, repetindo com outro nome se der erro;
2. se existirem menos de 5 canais, cria um;
3. se estiver inscrito em menos de 3 canais, se inscreve em mais um;
4. escolhe um canal e manda 10 mensagens, uma por segundo, e volta para o passo 2.

Enquanto espera entre as mensagens, o bot mostra tudo o que chega nos canais assinados: canal, mensagem,
horário de envio, horário de recebimento e relógio lógico.

## Persistência

Cada servidor grava só no seu diretório; nenhum arquivo é compartilhado. O formato varia por linguagem,
mas todos guardam as mesmas informações:

| Linguagem | Arquivos |
|---|---|
| JavaScript, Rust | `canais.json`, `login.json`, `publicacoes.json`, `mensagens.json` |
| Python, C# | `canais.json`, `login.json`, `publicacoes.jsonl`, `mensagens.jsonl` |
| Java | `canais.txt`, `login.txt`, `publicacoes.txt`, `mensagens.txt` (CSV simples) |

- `login`: usuário, timestamp e status de cada tentativa de login, inclusive as que falharam.
- `canais`: canais criados.
- `publicacoes`: canal, texto e timestamp de cada mensagem publicada.
- `mensagens`: tudo o que o servidor recebeu dos bots e respondeu.

O `.jsonl` (uma linha por registro) é usado onde o arquivo só cresce, para não reescrever tudo a cada
mensagem.

## Relógios e eleição (Partes 3 e 4)

**Relógio lógico.** Bots e servidores têm um contador que começa em 0. Antes de enviar qualquer mensagem
o contador soma 1 e vai junto; ao receber, o processo fica com o maior valor entre o seu e o recebido.

**Heartbeat.** A cada 10 mensagens de bots o servidor manda `heartbeat` para a referência. Se a
referência tiver removido o servidor (30 s sem heartbeat), ele se cadastra de novo e recebe um rank novo.

**Eleição.** Usamos o algoritmo do valentão (bully). O servidor pede a lista à referência e manda `eleicao`
para quem tem rank maior. Se alguém responde `OK`, ele espera até 10 s pelo anúncio do vencedor; se
ninguém responde, ele se declara coordenador e publica o próprio nome em `servers|`. O anúncio é
publicado duas vezes porque a primeira pode se perder logo após a conexão com o proxy. Quem recebe um
pedido de eleição responde `OK` e começa a sua própria eleição. A eleição acontece quando o servidor sobe
e sempre que o coordenador para de responder.

**Relógio físico.** A cada 15 mensagens de bots o servidor pede a hora ao coordenador (`sincronizar`),
soma metade do tempo de ida e volta e guarda a diferença em relação ao relógio local. Todos os
timestamps gerados depois usam essa correção. Se o coordenador não responde em 3 s, começa uma eleição.
A referência não devolve mais a hora no heartbeat.

Um detalhe que só apareceu nos testes com servidores caindo: o ZeroMQ resolve o nome do destino dentro
da sua própria thread de I/O, e quando um container para, o nome dele leva uns 8 s para dar erro no DNS do
Docker. Durante esse tempo o processo inteiro fica travado e a eleição dá resultado errado. Por isso o
compose configura `dns_opt` com timeout de 1 s nos servidores.

## Parte 5: consistência e replicação

### O problema

Como o broker reparte os pedidos em round-robin, sem replicação cada servidor fica só com o pedaço do
histórico que passou por ele. Dois `listar` seguidos podem trazer listas diferentes, e se um servidor cai,
o que só ele tinha se perde. O enunciado pede que todos os servidores tenham todos os dados.

### Método escolhido

Usamos **replicação ativa** (escrita replicada), vista em aula: toda escrita é executada por todas as
réplicas, e não só por quem recebeu o pedido. Não existe réplica primária. Qualquer servidor aceita a
escrita e a repassa para os outros.

Escolhemos esse método porque ele se encaixa no que já tínhamos. O proxy Pub/Sub já entrega uma mensagem
a todos os servidores, então replicar é publicar mais uma mensagem. A alternativa seria primário-backup
usando o coordenador como primário, mas aí as escritas ficariam paradas durante cada eleição e cada
servidor teria que encaminhar os pedidos ao coordenador, o que daria bem mais trabalho em cinco linguagens.

### Como funciona

As escritas são `login`, `cadastrar` e `publicar`. Os dados replicados são os logins, os canais e as
publicações.

1. O servidor que atende a escrita grava localmente e publica a operação no tópico
   `replicacao|<nome do servidor>`. A mensagem é a própria `Mensagem` do proto, com a mesma ação, canal,
   conteúdo e o timestamp original do bot, então o `.proto` não precisou mudar. Só escritas aceitas são
   replicadas: um `cadastrar` de canal repetido, por exemplo, não é.
2. Todos os servidores assinam `replicacao|` e aplicam o que chega: registram o login, criam o canal ou
   guardam a publicação. Eles não respondem a nenhum bot e não publicam de novo no canal, porque isso o
   servidor de origem já fez.
3. Um servidor que acabou de subir, novo ou voltando de uma queda, pede o estado completo a outro
   servidor com `obter_estado`. A resposta vem em JSON no campo `resposta`, no mesmo formato em todas as
   linguagens:

   ```json
   {"canais": ["canal1"],
    "logins": [{"usuario": "teste", "timestamp": 1794802162630, "status": "200"}],
    "publicacoes": [{"canal": "canal1", "conteudo": "ola", "timestamp": 1794802162631}]}
   ```

   O servidor junta isso com o que já tinha em disco e só então começa a atender.

