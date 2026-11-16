import os
import random
import sys
import time

import zmq

from client_pb2 import Mensagem, Resposta, RespostaSub

BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")

MIN_CANAIS = 5
MIN_INSCRICOES = 3
MENSAGENS_POR_RODADA = 10
INTERVALO_S = 1

# O primeiro nome e proposital: nao existe, para exercitar o retry de login.
USUARIOS = ["usuario_inexistente", "teste"] + [f"teste{i}" for i in range(2, 11)]
PALAVRAS = ["ola", "bom dia", "alguem ai?", "teste", "sistemas distribuidos", "zeromq", "protobuf", "python"]

contador = 0  # relogio logico (contadorMensagem)


def agora_ms():
    return float(int(time.time() * 1000))


def atualizar_contador(recebido):
    global contador
    contador = max(contador, recebido)


contexto = zmq.Context()
requester = contexto.socket(zmq.REQ)
requester.connect(f"tcp://{BROKER_HOST}:5559")
subscriber = contexto.socket(zmq.SUB)
subscriber.connect(f"tcp://{PROXY_HOST}:5558")
print("Cliente conectado no broker e no proxy...", flush=True)

inscritos = set()


def requisitar(acao, canal="", conteudo=""):
    global contador
    contador += 1
    mensagem = Mensagem(
        timestamp=agora_ms(), acao=acao, canal=canal, conteudo=conteudo, contadorMensagem=contador
    )
    requester.send(mensagem.SerializeToString())
    resposta = Resposta()
    resposta.ParseFromString(requester.recv())
    atualizar_contador(resposta.contadorMensagem)
    return resposta


def mostrar_publicacao():
    _topico, corpo = subscriber.recv_multipart()
    publicacao = RespostaSub()
    publicacao.ParseFromString(corpo)
    atualizar_contador(publicacao.contadorMensagem)
    print({
        "canal": publicacao.canal,
        "mensagem": publicacao.conteudo,
        "timestampEnvio": publicacao.timestampEnvio,
        "timestampRecebimento": agora_ms(),
        "contadorMensagem": publicacao.contadorMensagem,
    }, flush=True)


def esperar(segundos):
    """Espera `segundos` recebendo (e exibindo) as publicacoes que chegarem nesse tempo."""
    fim = time.time() + segundos
    while (restante := fim - time.time()) > 0:
        if subscriber.poll(int(restante * 1000)):
            mostrar_publicacao()


def inscrever(canal):
    subscriber.setsockopt(zmq.SUBSCRIBE, f"{canal}|".encode())
    inscritos.add(canal)
    print(f"[{usuario}] inscrito no {canal}", flush=True)

candidatos = [USUARIOS[0]] + random.sample(USUARIOS[1:], len(USUARIOS) - 1)
usuario = None
for candidato in candidatos:
    resposta = requisitar("login", conteudo=candidato)
    if resposta.status == "200":
        usuario = candidato
        print(f"[{usuario}] {resposta.resposta}", flush=True)
        break
    print(f"[{candidato}] Erro {resposta.status}: {resposta.resposta}", flush=True)

if usuario is None:
    print("Nenhum usuario da lista funcionou.", flush=True)
    sys.exit(1)

while True:
    canais = list(requisitar("listar").canais)

    if len(canais) < MIN_CANAIS:
        novo = f"canal{random.randint(1000, 9999)}"
        resposta = requisitar("cadastrar", conteudo=novo)
        print(f"[{usuario}] criar {novo}: {resposta.status} {resposta.resposta}", flush=True)
        canais = list(requisitar("listar").canais)

    if len(inscritos) < MIN_INSCRICOES:
        disponiveis = [c for c in canais if c not in inscritos]
        if disponiveis:
            inscrever(random.choice(disponiveis))

    if not canais:
        esperar(INTERVALO_S)
        continue

    canal = random.choice(canais)
    for i in range(1, MENSAGENS_POR_RODADA + 1):
        texto = f"{usuario}: {random.choice(PALAVRAS)} ({i}/{MENSAGENS_POR_RODADA})"
        resposta = requisitar("publicar", canal=canal, conteudo=texto)
        if resposta.status != "200":
            print(f"[{usuario}] erro ao publicar em {canal}: {resposta.status} {resposta.resposta}", flush=True)
        esperar(INTERVALO_S)
