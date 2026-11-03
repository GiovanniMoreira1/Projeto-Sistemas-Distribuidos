
import time
import zmq

from client_pb2 import Mensagem, RespostaReferencia

context = zmq.Context()

socket = context.socket(zmq.REP)
socket.bind("tcp://*:5556")

# Dicionário de servidores disponíveis
# nome: rank e último heartbeat
servidores = {}

proximo_rank = 1

# Tempo máximo sem heartbeat (segundos)
TEMPO_LIMITE = 30

# Relógio lógico da referência
contador_logico = 0


def remover_servidores_inativos():
    agora = time.time()

    inativos = []

    for nome, dados in servidores.items():
        if agora - dados["ultimo_heartbeat"] > TEMPO_LIMITE:
            inativos.append(nome)

    for nome in inativos:
        print(f"Servidor removido: {nome}", flush=True)
        del servidores[nome]


def criar_resposta(status, resposta, rank=0):
    global contador_logico

    contador_logico += 1

    return RespostaReferencia(
        status=status,
        resposta=resposta,
        rank=rank,
        timestamp=time.time() * 1000,
        contadorMensagem=contador_logico
    )


poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

print("Referência aguardando mensagens...", flush=True)

while True:
    # recebimento da mensagem
    print("Mensagem recebida pela referência!", flush=True)

    eventos = dict(poller.poll(1000))

    # Verifica servidores inativos mesmo sem novas requisições
    remover_servidores_inativos()

    if socket not in eventos:
        continue

    try:
        # Recebe a mensagem em Protocol Buffers
        dados = socket.recv()

        mensagem = Mensagem()
        mensagem.ParseFromString(dados)

        # Atualiza relógio lógico ao receber
        contador_logico = max(
            contador_logico,
            mensagem.contadorMensagem
        )

        print(
            f"Ação recebida: {mensagem.acao}",
            flush=True
        )

        resposta = None

        # 1. Consultar rank e cadastrar servidor
        if mensagem.acao == "consultar_rank":

            nome = mensagem.conteudo

            if nome not in servidores:

                rank = proximo_rank
                proximo_rank += 1

                servidores[nome] = {
                    "rank": rank,
                    "ultimo_heartbeat": time.time()
                }

                print(
                    f"Servidor cadastrado: {nome} | Rank: {rank}",
                    flush=True
                )

            else:
                rank = servidores[nome]["rank"]

                servidores[nome]["ultimo_heartbeat"] = time.time()

            resposta = criar_resposta(
                "200",
                "Servidor cadastrado com sucesso!",
                rank
            )

        # 2. Listar servidores disponíveis
        elif mensagem.acao == "listar":

            remover_servidores_inativos()

            resposta = criar_resposta(
                "200",
                "Servidores listados!"
            )

            for nome, dados_servidor in servidores.items():

                resposta.servidores.add(
                    nome=nome,
                    rank=dados_servidor["rank"]
                )

        # 3. Heartbeat e sincronização física
        elif mensagem.acao == "heartbeat":

            nome = mensagem.conteudo

            if nome in servidores:

                servidores[nome]["ultimo_heartbeat"] = time.time()

                resposta = criar_resposta(
                    "200",
                    "Heartbeat recebido!",
                    servidores[nome]["rank"]
                )

                print(
                    f"Heartbeat recebido de {nome}",
                    flush=True
                )

            else:

                resposta = criar_resposta(
                    "404",
                    "Servidor não cadastrado!"
                )

        else:

            resposta = criar_resposta(
                "400",
                "Ação desconhecida!"
            )

        # Envia a resposta em Protocol Buffers
        socket.send(resposta.SerializeToString())

    except Exception as erro:

        print(f"Erro na referência: {erro}", flush=True)

        resposta = criar_resposta(
            "500",
            "Erro interno na referência!"
        )

        socket.send(resposta.SerializeToString())
