# Canais - Partes 1 e 2 (Java)

Essa é a parte em Java do trabalho de Sistemas Distribuídos (CC7261), do Centro Universitário FEI, professor Leonardo Anjoletto Ferreira. O grupo tem 5 integrantes, cada um implementando o projeto inteiro (cliente e servidor) numa linguagem diferente, e no final todas as partes conversam entre si. Esse repositório é só a minha parte, em Java.

## O que essa parte faz

**Parte 1:** login (só nome de usuário, sem senha, repete se der erro), criar canal e listar canais. Tudo via Request-Reply.

**Parte 2:** publicar mensagens em canais e receber as mensagens dos canais em que o bot está inscrito, usando Publisher-Subscriber.

## Por que ZeroMQ

A comunicação entre cliente, servidor, broker e proxy usa ZeroMQ. Em Java, isso é feito com a lib JeroMQ, que é a implementação de ZeroMQ pra Java (o "core" oficial do ZeroMQ é em C).

## Por que Protobuf

A serialização usada é Protobuf, binária. O grupo decidiu usar isso porque dá pra definir um `.proto` só e cada um gera o código pra sua própria linguagem a partir dele, sem precisar ficar combinando formato na mão toda hora entre as 5 linguagens.

A mensagem usada é bem genérica, o mesmo formato serve pra tudo:

```proto
message Envelope {
  int64 timestamp = 1;
  string acao = 2;
  string conteudo = 3;
}
```

`acao` diz o que é (por exemplo "login", "criar_canal", "listar_canais", "publicar") e `conteudo` carrega os dados daquela ação. Quando `conteudo` precisa carregar mais de uma coisa (por exemplo a lista de canais), o grupo decidiu separar por vírgula dentro da própria string, em vez de criar um campo novo pra cada caso - fica mais simples de manter igual entre as 5 linguagens, mesmo perdendo um pouco de tipagem.

As respostas do servidor sempre seguem o padrão `sucesso,mensagem` ou `erro,mensagem` dentro do `conteudo`. Todas as mensagens têm o timestamp de envio no campo `timestamp`.

## Por que tem um Broker no meio (Request-Reply)

Cliente e servidor não conversam direto. Existe um Broker no meio que recebe as mensagens dos clientes num socket ROUTER (porta 5559) e repassa pros servidores conectados num socket DEALER (porta 5560), que ficam disponíveis em round-robin. Isso permite ter mais de um servidor rodando ao mesmo tempo, entrando ou saindo sem precisar parar o sistema, sem o cliente precisar saber quantos servidores existem ou qual deles vai responder.

## Por que tem um Proxy (Publisher-Subscriber)

As publicações passam por um Proxy separado do Broker. Ele tem dois sockets:

- **XSUB na porta 5557**: os servidores se conectam aqui (com um socket PUB) pra entregar as publicações.
- **XPUB na porta 5558**: os clientes se conectam aqui (com um socket SUB) pra receber as publicações.

O proxy só repassa de um lado pro outro, ele não lê nem guarda o conteúdo. Usei o `ZMQ.proxy()`, que já faz esse repasse sozinho (inclusive as inscrições, que andam no sentido contrário).

### Como uma publicação acontece

1. O bot manda pro servidor um pedido (Request-Reply) com `acao = "publicar"` e `conteudo = "canal,mensagem"`.
2. O servidor corta o `conteudo` na primeira vírgula (assim a mensagem pode ter vírgula dentro). Se o formato estiver errado, responde `erro,formato invalido`.
3. Se estiver certo, o servidor publica no proxy usando 2 frames: o primeiro é o tópico e o segundo é um `Envelope` em bytes (`acao = "publicacao"`, `conteudo = "canal,mensagem"` e o timestamp do momento do envio).
4. O servidor responde pro bot `sucesso,mensagem publicada`.

### Tópico

O tópico é o nome do canal com um `|` no final (`canal1|`). Isso porque o filtro do ZeroMQ funciona por prefixo: se o bot assinasse só `canal1`, ele também receberia as mensagens de `canal10`. Com o `|`, só bate o canal certo.

### Recebendo

O bot assina os canais com `subscribe`, e uma thread separada fica só lendo o socket SUB e imprimindo na tela o canal, a mensagem, o timestamp do envio (que vem dentro do `Envelope`) e o timestamp do recebimento (pego na hora que chega). A thread é separada porque o bot ao mesmo tempo fica publicando em loop.

## Como o bot funciona

1. Faz login (repete até dar certo).
2. Lista os canais. Se tiver menos de 5, cria um novo.
3. Assina canais sorteados (sem repetir) até chegar em 3, ou em todos que existirem se forem menos de 3.
4. Fica em loop infinito: lista os canais, sorteia um e manda 10 mensagens aleatórias com 1 segundo de intervalo entre elas.

## Persistência

Cada servidor guarda seus próprios dados, sem compartilhar com o outro:

- `login.txt`: log de todo login feito, só vai acrescentando linha. Formato `timestamp,usuario`.
- `canais.txt`: lista de canais criados, reescrito por completo toda vez que um canal novo é criado, e recarregado quando o servidor sobe.
- `publicacoes.txt`: uma linha por publicação feita, só acrescentando. Formato `timestamp,canal,mensagem`.
- `mensagens.txt`: log de tudo que entra e sai do servidor, só acrescentando. Cada linha começa com `req` (veio do cliente) ou `resp` (resposta do servidor), seguido de `timestamp,acao,conteudo`.

Dá pra recuperar tudo que aconteceu lendo esses arquivos, já que as linhas guardam o timestamp, a ação e o conteúdo.

No Docker, esses arquivos ficam na pasta `/data` de cada servidor, que é um volume (cada servidor tem o seu). O `app.jar` fica em `/app`, separado, pra o volume não esconder o jar novo quando o código muda.

### Limitação conhecida

Como cada servidor só conhece os canais que passaram por ele, e os pedidos são distribuídos em round-robin, dois pedidos seguidos do mesmo bot podem cair em servidores diferentes e a lista de canais pode vir diferente. Por isso o servidor não confere se o canal existe na hora de publicar, só confere o formato. Sincronizar os canais entre os servidores fica pra uma parte futura.

## Como rodar

```bash
docker compose up --build
```

Isso sobe o broker, o proxy, dois servidores (cada um com seu próprio volume) e dois clientes (bots). Os bots ficam rodando em loop até você parar com `Ctrl+C`.

Pra limpar tudo, inclusive os arquivos dos servidores:

```bash
docker compose down -v
```

## Estrutura

- `proto/envelope.proto` - definição da mensagem
- `src/main/java/sd/projeto/broker/Broker.java`
- `src/main/java/sd/projeto/proxy/Proxy.java`
- `src/main/java/sd/projeto/server/Server.java`
- `src/main/java/sd/projeto/client/Client.java`

## O que falta

As Partes 1 e 2 foram implementadas. As Partes 3 a 5 ainda não foram feitas.
