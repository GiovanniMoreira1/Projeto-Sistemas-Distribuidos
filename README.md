# Canais - Parte 1 (Java)

Essa é a parte em Java do trabalho de Sistemas Distribuídos (CC7261), do Centro Universitário FEI, professor Leonardo Anjoletto Ferreira. O grupo tem 5 integrantes, cada um implementando o projeto inteiro (cliente e servidor) numa linguagem diferente, e no final todas as partes conversam entre si. Esse repositório é só a minha parte, em Java.

## O que essa parte faz

É a Parte 1 do enunciado: login (só nome de usuário, sem senha, repete se der erro), criar canal e listar canais. Tudo via Request-Reply.

## Por que ZeroMQ

A comunicação entre cliente, servidor e broker usa ZeroMQ. Em Java, isso é feito com a lib JeroMQ, que é a implementação de ZeroMQ pra Java (o "core" oficial do ZeroMQ é em C).

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

`acao` diz o que é (por exemplo "login", "criar_canal", "listar_canais") e `conteudo` carrega os dados daquela ação. Quando `conteudo` precisa carregar mais de uma coisa (por exemplo a lista de canais), o grupo decidiu separar por vírgula dentro da própria string, em vez de criar um campo novo pra cada caso - fica mais simples de manter igual entre as 5 linguagens, mesmo perdendo um pouco de tipagem.

As respostas do servidor sempre seguem o padrão `sucesso,mensagem` ou `erro,mensagem` dentro do `conteudo`.

## Por que tem um Broker no meio

Cliente e servidor não conversam direto. Existe um Broker no meio que recebe as mensagens dos clientes num socket ROUTER (porta 5559) e repassa pros servidores conectados num socket DEALER (porta 5560), que ficam disponíveis em round-robin. Isso permite ter mais de um servidor rodando ao mesmo tempo, entrando ou saindo sem precisar parar o sistema, sem o cliente precisar saber quantos servidores existem ou qual deles vai responder.

## Persistência

Cada servidor guarda seus próprios dados em dois arquivos:

- `login.txt`: log de todo login feito, só vai acrescentando linha, nunca apaga nada
- `canais.txt`: lista de canais criados, reescrito por completo toda vez que um canal novo é criado, e recarregado quando o servidor sobe

Os arquivos não são compartilhados entre os servidores, cada um só sabe dos canais/logins que passaram por ele mesmo.

## Como rodar

```bash
docker compose up --build
```

Isso sobe o broker, dois servidores (cada um com seu próprio volume, então cada um cria seus próprios `login.txt`/`canais.txt` sem misturar com o outro) e dois clientes. Os clientes fazem login, criam um canal de teste e listam os canais, e depois terminam - ainda não tem loop contínuo mandando mensagem, isso só entra na Parte 2.

## Estrutura

- `proto/envelope.proto` - definição da mensagem
- `src/main/java/sd/projeto/broker/Broker.java`
- `src/main/java/sd/projeto/server/Server.java`
- `src/main/java/sd/projeto/client/Client.java`

## O que falta

Só a Parte 1 foi implementada até agora. As Partes 2 a 5 ainda não foram feitas.