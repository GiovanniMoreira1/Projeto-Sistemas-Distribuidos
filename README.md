# Sistema distribuído de troca de mensagens (Partes 1 a 5)

Projeto de Sistemas Distribuídos: uma versão simples de um BBS/IRC em que bots (clientes) fazem login,
criam canais e publicam mensagens, e servidores atendem esses bots, sincronizam relógios, elegem um
coordenador e replicam os dados entre si. Tudo roda em containers e conversa por ZeroMQ.

O grupo implementou **cliente e servidor em 5 linguagens**, e todos funcionam juntos ao mesmo tempo:
qualquer cliente pode ser atendido por qualquer servidor, e os servidores de linguagens diferentes
participam da mesma eleição e da mesma replicação.

| Linguagem | Pasta | Cliente | Servidor |
|---|---|---|---|
| JavaScript (Node.js) | `javascript/` | `client.js` | `servidor.js` |
| Python | `python/` | `cliente/main.py` | `servidor/main.py` |
| Rust | `rust/` | `src/bin/client.rs` | `src/bin/servidor.rs` |
| C# (.NET) | `csharp/` | `Cliente/Program.cs` | `Servidor/Program.cs` |
| Java | `java/` | `sd/projeto/client/Client.java` | `sd/projeto/server/Server.java` |

Broker, proxy e serviço de referência usados no `docker-compose.yml` são os da implementação em
Python (`python/broker`, `python/proxy`, `python/referencia`). As outras linguagens também têm as suas
versões desses processos nas próprias pastas, mas só uma instância de cada sobe no sistema.

## Como executar

```
docker compose up --build
```

Sobe 1 broker, 1 proxy, 1 referência e, para cada linguagem, 2 servidores e 2 clientes
(10 servidores e 10 clientes no total). Os dados de cada servidor ficam em
`./dados/<nome-do-servidor>/`. Para começar do zero, apague a pasta `dados/`.

## Contrato comum

O que une as 5 implementações é o contrato de comunicação, igual em todas:

- **Serialização:** Protocol Buffers em binário, a partir de um único arquivo, `proto/client.proto`.
  Cada linguagem gera o seu código a partir dele durante o build.
- **Mensagens:** `Mensagem` (requisição), `Resposta` (resposta de servidor), `RespostaSub`
  (publicação e anúncio de coordenador) e `RespostaReferencia` (resposta da referência).
  Todas carregam o `timestamp` do envio (ms) e o `contadorMensagem` (relógio lógico).
- **Portas e padrões:**

```
Req-Rep:           cliente (REQ) -> broker (ROUTER :5559 / DEALER :5560) -> servidor (REP)
Pub-Sub:           servidor (PUB) -> proxy (XSUB :5557 / XPUB :5558) -> cliente/servidor (SUB)
Referência:        servidor (REQ) -> referência (REP :5556)
Servidor-servidor: servidor (REQ) -> outro servidor (REP :5559)
```

- **Ações:**
  - clientes: `login`, `listar`, `cadastrar`, `publicar`
  - servidor → referência: `consultar_rank`, `listar_servidores`, `heartbeat`
  - servidor → servidor: `eleicao`, `sincronizar`, `obter_estado`
- **Status:** `login` 200/404, `listar` 200, `cadastrar` 200/404 (já existe)/400 (nome inválido),
  `publicar` 200/400.
- **Tópicos:** `"<canal>|"` para as publicações dos bots (o `|` evita que `canal1` também receba
  `canal10`), `servers|` para o anúncio do coordenador e `replicacao|<servidor>` para a replicação.
- **Nome do servidor:** é o nome do serviço no compose (variável `NOME_SERVIDOR`) e também o endereço
  usado pelos outros servidores (`tcp://<nome>:5559`). Por isso precisa ser único.

## Partes 1 a 4 (resumo)

- **Parte 1:** login (sem senha, repetido em caso de erro), criação e listagem de canais via Req-Rep
  pelo broker. Cada servidor persiste os seus dados no próprio diretório.
- **Parte 2:** publicação em canais via Pub-Sub pelo proxy. O bot segue o comportamento padronizado:
  cria um canal se houver menos de 5, inscreve-se em mais um se tiver menos de 3 inscrições e, em
  loop, escolhe um canal e envia 10 mensagens com 1 s de intervalo.
- **Parte 3:** relógio lógico em todas as mensagens (incrementa antes de enviar e assume o máximo ao
  receber). O serviço de referência dá o rank, guarda a lista de servidores e remove quem fica 30 s
  sem heartbeat (enviado a cada 10 mensagens de clientes).
- **Parte 4:** eleição pelo algoritmo do valentão (bully): vence o maior rank, que se anuncia no
  tópico `servers`. A cada 15 mensagens de clientes cada servidor pede a hora ao coordenador e corrige
  o seu relógio físico (somando metade do tempo de ida e volta). Se o coordenador não responde em 3 s,
  começa uma nova eleição.

## Parte 5: consistência e replicação

### O problema

O broker distribui as requisições entre os servidores em round-robin. Sem replicação, cada servidor
guarda só a parte do histórico que passou por ele: um cliente que lista canais ou pede o histórico
recebe respostas diferentes conforme o servidor que o atendeu, e se um servidor para de funcionar a
parte dele se perde. O enunciado pede que **todos os servidores tenham todos os dados**.

### Método escolhido: replicação ativa (escrita replicada)

Escolhemos a **replicação ativa**, um protocolo de escrita replicada visto em aula: toda operação de
escrita é executada por **todas** as réplicas, e não só pela que recebeu o pedido. Não existe uma
réplica primária: qualquer servidor aceita escritas e as repassa para os outros.

Como resolve o problema do projeto: não importa para qual servidor o broker mandou a requisição, a
escrita chega a todos. Assim todos ficam com o mesmo conjunto de logins, canais e publicações, e a
queda de um servidor não perde dados, porque os outros têm a cópia.

### Como foi implementado

As escritas são `login`, `cadastrar` e `publicar`. Os dados replicados são os logins (usuário,
timestamp e status), os canais e as publicações (canal, conteúdo e timestamp).

1. **Difusão das escritas.** O servidor que atende a escrita grava localmente, como antes, e também a
   publica no proxy, no tópico `replicacao|<nome do servidor de origem>`. A mensagem é a própria
   `Mensagem` do proto, com a mesma `acao`, `canal`, `conteudo` e o `timestamp` original do cliente,
   então o `.proto` não precisou mudar. Só escritas aceitas são difundidas: um canal repetido ou uma
   publicação inválida não são replicados. Todos os logins são replicados, inclusive os que falharam,
   porque o enunciado pede guardar todos os logins.
2. **Aplicação nas outras réplicas.** Todo servidor assina o prefixo `replicacao|` e aplica as escritas
   que chegam: registra o login, cria o canal se ainda não existir ou guarda a publicação. Ele não
   responde a nenhum cliente e não publica de novo no canal dos bots, porque isso já foi feito pelo
   servidor de origem.
3. **Transferência de estado.** A difusão só alcança quem já está no ar. Um servidor que sobe depois,
   ou que volta de uma queda, pede o estado completo a outro servidor com a ação `obter_estado`
   (porta 5559). A resposta traz, no campo `resposta`, um JSON no mesmo formato para todas as
   linguagens:

   ```json
   {"canais": ["..."],
    "logins": [{"usuario": "...", "timestamp": 0, "status": "200"}],
    "publicacoes": [{"canal": "...", "conteudo": "...", "timestamp": 0}]}
   ```

   O servidor junta esse estado com o que já tinha em disco e segue normalmente.

### Mudanças necessárias para o método funcionar no projeto

- **Ignorar o próprio eco.** No Pub-Sub o servidor também recebe o que ele mesmo publicou. Por isso o
  nome do servidor de origem vai no tópico, e cada servidor descarta as mensagens com o próprio nome.
- **Aplicar a mesma escrita mais de uma vez não duplica dados.** Cada login é identificado por
  (usuário, timestamp) e cada publicação por (canal, conteúdo, timestamp), e um canal só é criado se
  ainda não existir. Isso permite juntar o estado recebido com o local sem duplicar registros, inclusive
  quando a mesma escrita chega pela difusão e pela transferência de estado.
- **Assinar antes de pedir o estado.** Ao iniciar, o servidor primeiro assina `replicacao|` e só depois
  pede o estado, para não perder as escritas feitas nesse meio tempo.
- **A quem pedir o estado.** A ideia inicial era pedir sempre ao coordenador. Mas no algoritmo do
  valentão quem acabou de subir recebe da referência o maior rank e normalmente vence a eleição, ou
  seja, muitas vezes o coordenador seria o próprio servidor que está sem os dados. Por isso o servidor
  pede ao coordenador quando ele é outro servidor e, se não houver ou ele não responder, ao próximo da
  lista de servidores da referência.

### Limitações

- A consistência é **eventual**: uma escrita aparece nos outros servidores alguns milissegundos depois
  de aparecer no servidor que a atendeu, e escritas concorrentes podem ser guardadas em ordem diferente
  em servidores diferentes. Os dados ficam iguais, mas a ordem de inserção pode variar. O timestamp e o
  relógio lógico de cada registro permitem reordenar se for necessário.
- O Pub-Sub do ZeroMQ não garante entrega: se o proxy cair, as escritas difundidas nesse intervalo se
  perdem para quem não as recebeu. Um servidor reiniciado recupera tudo pela transferência de estado.
- O registro de requisições e respostas (`mensagens`) é um log de auditoria de cada servidor e não é
  replicado. Os dados do sistema (logins, canais e publicações) são replicados.
