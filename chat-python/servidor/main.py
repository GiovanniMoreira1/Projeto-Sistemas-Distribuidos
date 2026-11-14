import json
import os
import socket
import threading
import time

import zmq

from client_pb2 import Mensagem, Resposta, RespostaReferencia, RespostaSub

# ---------------------------------------------------------------- configuracao
BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
REFERENCIA_HOST = os.environ.get("REFERENCIA_HOST", "referencia")
SEED_USUARIOS = os.environ.get("SEED_USUARIOS", "seed/usuarios.json")

# Nome do servidor = nome do servico no docker-compose: os outros servidores usam esse nome
# como endereco (tcp://<nome>:5559) para pedir eleicao e sincronizacao.
NOME_SERVIDOR = os.environ.get("NOME_SERVIDOR", socket.gethostname())
PORTA_SERVIDORES = int(os.environ.get("PORTA_SERVIDORES", "5559"))
BIND_HOST = os.environ.get("BIND_HOST", "*")

HEARTBEAT_A_CADA = int(os.environ.get("HEARTBEAT_A_CADA", "10"))        # mensagens de clientes (Parte 3)
SINCRONIZAR_A_CADA = int(os.environ.get("SINCRONIZAR_A_CADA", "15"))    # mensagens de clientes (Parte 4)
TEMPO_LIMITE_MS = 3000
ESPERA_ANUNCIO_S = 10

URL_REFERENCIA = f"tcp://{REFERENCIA_HOST}:5556"

# cada servidor tem o seu proprio diretorio de dados: nada e compartilhado entre servidores.
DIRETORIO = os.path.join(os.environ.get("DADOS_DIR", "dados"), NOME_SERVIDOR)
os.makedirs(DIRETORIO, exist_ok=True)

CAMINHO_USUARIOS = os.path.join(DIRETORIO, "usuarios.json")
CAMINHO_CANAIS = os.path.join(DIRETORIO, "canais.json")
CAMINHO_LOGINS = os.path.join(DIRETORIO, "login.json")
CAMINHO_MENSAGENS = os.path.join(DIRETORIO, "mensagens.jsonl")
CAMINHO_PUBLICACOES = os.path.join(DIRETORIO, "publicacoes.jsonl")

trava = threading.RLock()  # protege contador, offset, rank e coordenador (varias threads)
contador = 0               # relogio logico
#offset_ms = float(os.environ.get("DESVIO_RELOGIO_MS", "0"))
rank = 0
coordenador = ""           # nome do servidor eleito
em_eleicao = False
anuncio_recebido = threading.Event()
mensagens_recebidas = 0

contexto = zmq.Context()
trava_pub = threading.Lock()  # o socket PUB e usado por mais de uma thread
publicador = contexto.socket(zmq.PUB)
publicador.connect(f"tcp://{PROXY_HOST}:5557")


def log(texto):
    print(f"[{NOME_SERVIDOR}] {texto}", flush=True)


def agora_ms():
    with trava:
        return float(int(time.time() * 1000 + offset_ms))


def proximo_contador():
    """Regra 1: incrementa antes de cada envio."""
    global contador
    with trava:
        contador += 1
        return contador


def atualizar_contador(recebido):
    """Regra 2: ao receber, assume o maximo entre o valor local e o recebido."""
    global contador
    with trava:
        contador = max(contador, recebido)


def ler_json(caminho, padrao):
    if not os.path.exists(caminho):
        return padrao
    with open(caminho, encoding="utf-8") as arquivo:
        return json.load(arquivo)


def salvar_json(caminho, dados):
    temporario = caminho + ".tmp" 
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, separators=(",", ":"))
    os.replace(temporario, caminho)


def anexar_jsonl(caminho, registro):
    with open(caminho, "a", encoding="utf-8") as arquivo:
        arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")


if not os.path.exists(CAMINHO_USUARIOS):
    salvar_json(CAMINHO_USUARIOS, ler_json(SEED_USUARIOS, {"usuarios": []}))

usuarios = ler_json(CAMINHO_USUARIOS, {"usuarios": []})
canais = ler_json(CAMINHO_CANAIS, {"canais": []})
logins = ler_json(CAMINHO_LOGINS, [])


def requisitar(url, acao, conteudo="", tipo=Resposta):
    """REQ/REP com limite de tempo. Devolve None se o destino nao responder."""
    mensagem = Mensagem(
        timestamp=agora_ms(), acao=acao, conteudo=conteudo, contadorMensagem=proximo_contador()
    )
    requester = contexto.socket(zmq.REQ)
    requester.setsockopt(zmq.LINGER, 0)
    requester.setsockopt(zmq.SNDTIMEO, TEMPO_LIMITE_MS)
    requester.setsockopt(zmq.RCVTIMEO, TEMPO_LIMITE_MS)
    try:
        requester.connect(url)
        requester.send(mensagem.SerializeToString())
        resposta = tipo()
        resposta.ParseFromString(requester.recv())
        atualizar_contador(resposta.contadorMensagem)
        return resposta
    except zmq.ZMQError:
        return None
    finally:
        requester.close()


def publicar_no_proxy(topico, payload):
    with trava_pub:
        publicador.send_multipart([topico, payload])


def criar_resposta(status, texto, lista_canais=()):
    return Resposta(
        status=status,
        resposta=texto,
        canais=lista_canais,
        contadorMensagem=proximo_contador(),
        timestamp=agora_ms(),
    )


def tentar_rank():
    resposta = requisitar(URL_REFERENCIA, "consultar_rank", NOME_SERVIDOR, RespostaReferencia)
    if resposta is not None and resposta.status == "200":
        return resposta.rank
    return None


def consultar_rank():
    while True:
        valor = tentar_rank()
        if valor is not None:
            return valor
        log("aguardando o servico de referencia...")
        time.sleep(1)


def listar_servidores():
    resposta = requisitar(URL_REFERENCIA, "listar_servidores", NOME_SERVIDOR, RespostaReferencia)
    if resposta is None or resposta.status != "200":
        return []
    return list(resposta.servidores)


def heartbeat():
    global rank
    resposta = requisitar(URL_REFERENCIA, "heartbeat", NOME_SERVIDOR, RespostaReferencia)
    if resposta is None:
        log("heartbeat sem resposta da referencia")
    elif resposta.status == "200":
        log("heartbeat confirmado")
    else:
        # a referencia nos removeu da lista (ficamos muito tempo sem heartbeat): cadastra de novo
        novo = tentar_rank()
        if novo is not None:
            with trava:
                rank = novo
            log(f"recadastrado na referencia, rank {novo}")


def iniciar_eleicao():
    global em_eleicao
    with trava:
        if em_eleicao:
            return
        em_eleicao = True
        anuncio_recebido.clear()
    threading.Thread(target=executar_eleicao, daemon=True).start()


def executar_eleicao():
    global em_eleicao
    refazer = False
    try:
        log("iniciando eleicao...")
        with trava:
            meu_rank = rank
        maiores = [s for s in listar_servidores() if s.rank > meu_rank and s.nome != NOME_SERVIDOR]

        algum_respondeu = False
        for alvo in maiores:
            resposta = requisitar(f"tcp://{alvo.nome}:{PORTA_SERVIDORES}", "eleicao", NOME_SERVIDOR)
            if resposta is not None and resposta.resposta == "OK":
                algum_respondeu = True

        if algum_respondeu:
            log("servidor de rank maior assumiu a eleicao; aguardando anuncio...")
            if not anuncio_recebido.wait(ESPERA_ANUNCIO_S):
                log("anuncio nao chegou; refazendo eleicao")
                refazer = True
        else:
            tornar_se_coordenador()
    finally:
        with trava:
            em_eleicao = False
    if refazer:
        iniciar_eleicao()


def tornar_se_coordenador():
    global coordenador, offset_ms
    with trava:
        coordenador = NOME_SERVIDOR
        offset_ms = 0.0  # o coordenador e a referencia de hora
    anuncio_recebido.set()
    log(f"eleito coordenador (rank {rank})")
    for _ in range(2):
        anuncio = RespostaSub(
            timestampEnvio=agora_ms(),
            canal="servers",
            conteudo=NOME_SERVIDOR,
            contadorMensagem=proximo_contador(),
        )
        publicar_no_proxy(b"servers|", anuncio.SerializeToString())
        time.sleep(1)


def ouvir_anuncios():
    global coordenador
    sub = contexto.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:5558")
    sub.setsockopt(zmq.SUBSCRIBE, b"servers|")
    while True:
        _topico, corpo = sub.recv_multipart()
        anuncio = RespostaSub()
        anuncio.ParseFromString(corpo)
        atualizar_contador(anuncio.contadorMensagem)
        with trava:
            mudou = coordenador != anuncio.conteudo
            coordenador = anuncio.conteudo
        anuncio_recebido.set()
        if mudou:
            log(f"novo coordenador anunciado: {anuncio.conteudo}")


def atender_servidores():
    responder = contexto.socket(zmq.REP)
    responder.bind(f"tcp://{BIND_HOST}:{PORTA_SERVIDORES}")
    while True:
        dados = responder.recv()
        iniciar = False
        try:
            requisicao = Mensagem()
            requisicao.ParseFromString(dados)
            atualizar_contador(requisicao.contadorMensagem)

            if requisicao.acao == "eleicao":
                resposta = criar_resposta("200", "OK")
                iniciar = True  # quem recebe pedido de eleicao tambem participa (Bully)
            elif requisicao.acao == "sincronizar":
                with trava:
                    sou_coordenador = coordenador == NOME_SERVIDOR
                if sou_coordenador:
                    resposta = criar_resposta("200", "Hora correta")  # timestamp = hora do coordenador
                else:
                    resposta = criar_resposta("400", "Não sou o coordenador!")
            else:
                resposta = criar_resposta("400", "Ação desconhecida!")
        except Exception as erro:
            log(f"erro ao atender servidor: {erro}")
            resposta = criar_resposta("500", "Erro interno no servidor!")

        responder.send(resposta.SerializeToString())
        if iniciar:
            iniciar_eleicao()


def sincronizar_relogio():
    """Pede a hora ao coordenador (a cada SINCRONIZAR_A_CADA mensagens). Se ele nao responder,
    inicia uma eleicao."""
    global offset_ms
    with trava:
        alvo = coordenador

    if not alvo:
        iniciar_eleicao()
        return
    if alvo == NOME_SERVIDOR:
        log("sou o coordenador: relogio mantido")
        return

    t0 = time.time() * 1000
    resposta = requisitar(f"tcp://{alvo}:{PORTA_SERVIDORES}", "sincronizar")
    t1 = time.time() * 1000

    if resposta is None or resposta.status != "200":
        log("coordenador indisponivel; iniciando eleicao")
        iniciar_eleicao()
        return

    hora_coordenador = resposta.timestamp + (t1 - t0) / 2  # compensa metade do tempo de ida e volta
    with trava:
        anterior = offset_ms
        offset_ms = hora_coordenador - t1
        valor = offset_ms
    log(f"relogio sincronizado com {alvo} (offset {anterior:+.0f} -> {valor:+.0f} ms)")


def tratar_login(requisicao):
    usuario = requisicao.conteudo.strip()

    if usuario in usuarios["usuarios"]:
        resposta = criar_resposta("200", "Login realizado com sucesso!")
    else:
        resposta = criar_resposta("404", "Nome de usuario incorreto!")

    logins.append({
        "usuario": usuario,
        "timestamp": requisicao.timestamp,
        "status": resposta.status,
    })
    salvar_json(CAMINHO_LOGINS, logins)
    return resposta


def tratar_listar(_requisicao):
    return criar_resposta("200", "Canais listados", canais["canais"])


def tratar_cadastrar(requisicao):
    canal = requisicao.conteudo.strip()

    if not canal or len(canal) > 50 or "|" in canal:
        return criar_resposta("400", "Nome de canal invalido!")
    if canal in canais["canais"]:
        return criar_resposta("404", "Canal já existe!")  # mesmo codigo do servidor JS

    canais["canais"].append(canal)
    salvar_json(CAMINHO_CANAIS, canais)
    return criar_resposta("200", f"Canal {canal} cadastrado com sucesso!")


def tratar_publicar(requisicao):
    canal = requisicao.canal.strip()
    texto = requisicao.conteudo

    if not canal or not texto:
        return criar_resposta("400", "Canal e mensagem sao obrigatorios!")

    publicacao = RespostaSub(
        timestampEnvio=agora_ms(),
        canal=canal,
        conteudo=texto,
        contadorMensagem=proximo_contador(),
    )
    publicar_no_proxy(f"{canal}|".encode(), publicacao.SerializeToString())

    anexar_jsonl(CAMINHO_PUBLICACOES, {
        "canal": canal,
        "conteudo": texto,
        "timestampEnvioCliente": requisicao.timestamp,
        "timestampPublicacao": publicacao.timestampEnvio,
        "contadorMensagem": publicacao.contadorMensagem,
    })
    return criar_resposta("200", f"Mensagem publicada no canal {canal} com sucesso!")


ACOES = {
    "login": tratar_login,
    "listar": tratar_listar,
    "cadastrar": tratar_cadastrar,
    "publicar": tratar_publicar,
}


def atender_clientes():
    global mensagens_recebidas
    responder = contexto.socket(zmq.REP)
    responder.connect(f"tcp://{BROKER_HOST}:5560")

    while True:
        dados = responder.recv()

        try:
            requisicao = Mensagem()
            requisicao.ParseFromString(dados)
            atualizar_contador(requisicao.contadorMensagem)

            tratador = ACOES.get(requisicao.acao)
            if tratador is None:
                resposta = criar_resposta("400", "Acao desconhecida!")
            else:
                resposta = tratador(requisicao)

            anexar_jsonl(CAMINHO_MENSAGENS, {
                "requisicao": {
                    "acao": requisicao.acao,
                    "canal": requisicao.canal,
                    "conteudo": requisicao.conteudo,
                    "timestamp": requisicao.timestamp,
                    "contadorMensagem": requisicao.contadorMensagem,
                },
                "resposta": {
                    "status": resposta.status,
                    "resposta": resposta.resposta,
                    "canais": list(resposta.canais),
                    "timestamp": resposta.timestamp,
                    "contadorMensagem": resposta.contadorMensagem,
                },
            })
        except Exception as erro:  # o REP precisa sempre responder, senao trava
            log(f"erro ao processar mensagem: {erro}")
            resposta = criar_resposta("500", "Erro interno no servidor!")

        responder.send(resposta.SerializeToString())

        mensagens_recebidas += 1
        if mensagens_recebidas % HEARTBEAT_A_CADA == 0:
            heartbeat()
        if mensagens_recebidas % SINCRONIZAR_A_CADA == 0:
            sincronizar_relogio()


log(f"dados em {DIRETORIO}")

threading.Thread(target=atender_servidores, daemon=True).start()
threading.Thread(target=ouvir_anuncios, daemon=True).start()

rank = consultar_rank()
log(f"rank {rank}")

iniciar_eleicao()
atender_clientes()
