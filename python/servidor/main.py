import json
import os
import socket
import threading
import time

import zmq

from client_pb2 import Mensagem, Resposta, RespostaReferencia, RespostaSub

BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
REFERENCIA_HOST = os.environ.get("REFERENCIA_HOST", "referencia")
SEED_USUARIOS = os.environ.get("SEED_USUARIOS", "seed/usuarios.json")

NOME_SERVIDOR = os.environ.get("NOME_SERVIDOR", socket.gethostname())
PORTA_SERVIDORES = int(os.environ.get("PORTA_SERVIDORES", "5559"))
BIND_HOST = os.environ.get("BIND_HOST", "*")

HEARTBEAT_A_CADA = int(os.environ.get("HEARTBEAT_A_CADA", "10"))
SINCRONIZAR_A_CADA = int(os.environ.get("SINCRONIZAR_A_CADA", "15"))
TEMPO_LIMITE_MS = 3000
ESPERA_ANUNCIO_S = 10
ESPERA_ASSINATURA_S = 1

URL_REFERENCIA = f"tcp://{REFERENCIA_HOST}:5556"

DIRETORIO = os.path.join(os.environ.get("DADOS_DIR", "dados"), NOME_SERVIDOR)
os.makedirs(DIRETORIO, exist_ok=True)

CAMINHO_USUARIOS = os.path.join(DIRETORIO, "usuarios.json")
CAMINHO_CANAIS = os.path.join(DIRETORIO, "canais.json")
CAMINHO_LOGINS = os.path.join(DIRETORIO, "login.json")
CAMINHO_MENSAGENS = os.path.join(DIRETORIO, "mensagens.jsonl")
CAMINHO_PUBLICACOES = os.path.join(DIRETORIO, "publicacoes.jsonl")

trava = threading.RLock()
contador = 0
offset_ms = float(os.environ.get("DESVIO_RELOGIO_MS", "0"))
rank = 0
coordenador = ""
em_eleicao = False
anuncio_recebido = threading.Event()
sincronizado = threading.Event()
mensagens_recebidas = 0

contexto = zmq.Context()
trava_pub = threading.Lock()
publicador = contexto.socket(zmq.PUB)
publicador.connect(f"tcp://{PROXY_HOST}:5557")


def log(texto):
    print(f"[{NOME_SERVIDOR}] {texto}", flush=True)


def agora_ms():
    with trava:
        return float(int(time.time() * 1000 + offset_ms))


def proximo_contador():
    global contador
    with trava:
        contador += 1
        return contador


def atualizar_contador(recebido):
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


def ler_jsonl(caminho):
    if not os.path.exists(caminho):
        return []
    with open(caminho, encoding="utf-8") as arquivo:
        return [json.loads(linha) for linha in arquivo if linha.strip()]


def salvar_jsonl(caminho, registros):
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    os.replace(temporario, caminho)


if not os.path.exists(CAMINHO_USUARIOS):
    salvar_json(CAMINHO_USUARIOS, ler_json(SEED_USUARIOS, {"usuarios": []}))


usuarios = ler_json(CAMINHO_USUARIOS, {"usuarios": []})
canais = ler_json(CAMINHO_CANAIS, {"canais": []})
logins = ler_json(CAMINHO_LOGINS, [])
publicacoes = ler_jsonl(CAMINHO_PUBLICACOES)

trava_dados = threading.RLock()


def chave_login(registro):
    return (registro["usuario"], registro["timestamp"])


def chave_publicacao(registro):
    return (registro["canal"], registro["conteudo"], registro["timestamp"])


logins_vistos = {chave_login(r) for r in logins}
publicacoes_vistas = {chave_publicacao(r) for r in publicacoes}


def registrar_login(usuario, timestamp, status):
    registro = {"usuario": usuario, "timestamp": timestamp, "status": status}
    with trava_dados:
        if chave_login(registro) in logins_vistos:
            return
        logins_vistos.add(chave_login(registro))
        logins.append(registro)
        salvar_json(CAMINHO_LOGINS, logins)


def registrar_canal(canal):
    with trava_dados:
        if canal in canais["canais"]:
            return False
        canais["canais"].append(canal)
        salvar_json(CAMINHO_CANAIS, canais)
        return True


def registrar_publicacao(canal, conteudo, timestamp):
    registro = {"canal": canal, "conteudo": conteudo, "timestamp": timestamp}
    with trava_dados:
        if chave_publicacao(registro) in publicacoes_vistas:
            return
        publicacoes_vistas.add(chave_publicacao(registro))
        publicacoes.append(registro)
        anexar_jsonl(CAMINHO_PUBLICACOES, registro)


def requisitar(url, acao, conteudo="", tipo=Resposta):
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
        offset_ms = 0.0
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

            if requisicao.acao == "obter_estado" and not sincronizado.is_set():
                resposta = criar_resposta("503", "Servidor ainda sincronizando")
            elif requisicao.acao == "obter_estado":
                resposta = criar_resposta("200", exportar_estado())
                log(f"estado enviado para {requisicao.conteudo}")
            elif requisicao.acao == "eleicao":
                resposta = criar_resposta("200", "OK")
                iniciar = True
            elif requisicao.acao == "sincronizar":
                with trava:
                    sou_coordenador = coordenador == NOME_SERVIDOR
                if sou_coordenador:
                    resposta = criar_resposta("200", "Hora correta")
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

    hora_coordenador = resposta.timestamp + (t1 - t0) / 2
    with trava:
        anterior = offset_ms
        offset_ms = hora_coordenador - t1
        valor = offset_ms
    log(f"relogio sincronizado com {alvo} (offset {anterior:+.0f} -> {valor:+.0f} ms)")


TOPICO_REPLICACAO = "replicacao|"


def replicar(acao, canal, conteudo, timestamp):
    operacao = Mensagem(
        timestamp=timestamp, acao=acao, canal=canal, conteudo=conteudo,
        contadorMensagem=proximo_contador(),
    )
    publicar_no_proxy(f"{TOPICO_REPLICACAO}{NOME_SERVIDOR}".encode(), operacao.SerializeToString())


def aplicar_operacao(operacao):
    if operacao.acao == "login":
        usuario = operacao.conteudo
        status = "200" if usuario in usuarios["usuarios"] else "404"
        registrar_login(usuario, operacao.timestamp, status)
    elif operacao.acao == "cadastrar":
        registrar_canal(operacao.conteudo)
    elif operacao.acao == "publicar":
        registrar_publicacao(operacao.canal, operacao.conteudo, operacao.timestamp)


def ouvir_replicacao(pronto):
    sub = contexto.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:5558")
    sub.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICACAO.encode())
    pronto.set()
    proprio = f"{TOPICO_REPLICACAO}{NOME_SERVIDOR}".encode()
    while True:
        topico, corpo = sub.recv_multipart()
        if topico == proprio:
            continue
        operacao = Mensagem()
        operacao.ParseFromString(corpo)
        atualizar_contador(operacao.contadorMensagem)
        aplicar_operacao(operacao)
        origem = topico.decode()[len(TOPICO_REPLICACAO):]
        log(f"replicado de {origem}: {operacao.acao} {operacao.canal} {operacao.conteudo}")


def exportar_estado():
    with trava_dados:
        return json.dumps({
            "canais": canais["canais"],
            "logins": logins,
            "publicacoes": publicacoes,
        }, ensure_ascii=False)


def importar_estado(texto):
    estado = json.loads(texto)
    with trava_dados:
        for canal in estado.get("canais", []):
            registrar_canal(canal)
        for r in estado.get("logins", []):
            registrar_login(r["usuario"], r["timestamp"], r.get("status", ""))
        for r in estado.get("publicacoes", []):
            registrar_publicacao(r["canal"], r["conteudo"], r["timestamp"])
        salvar_jsonl(CAMINHO_PUBLICACOES, publicacoes)
        return len(canais["canais"]), len(logins), len(publicacoes)


def obter_estado_inicial():
    with trava:
        atual = coordenador
    outros = [s.nome for s in listar_servidores() if s.nome != NOME_SERVIDOR]
    candidatos = ([atual] if atual and atual != NOME_SERVIDOR else []) + [n for n in outros if n != atual]

    for alvo in candidatos:
        resposta = requisitar(f"tcp://{alvo}:{PORTA_SERVIDORES}", "obter_estado", NOME_SERVIDOR)
        if resposta is not None and resposta.status == "200":
            n_canais, n_logins, n_pub = importar_estado(resposta.resposta)
            log(f"estado recebido de {alvo}: {n_canais} canais, {n_logins} logins, {n_pub} publicacoes")
            return
    log("nenhum outro servidor respondeu: mantendo os dados locais")


def tratar_login(requisicao):
    usuario = requisicao.conteudo.strip()

    if usuario in usuarios["usuarios"]:
        resposta = criar_resposta("200", "Login realizado com sucesso!")
    else:
        resposta = criar_resposta("404", "Nome de usuario incorreto!")

    registrar_login(usuario, requisicao.timestamp, resposta.status)
    replicar("login", "", usuario, requisicao.timestamp)
    return resposta


def tratar_listar(_requisicao):
    with trava_dados:
        lista = list(canais["canais"])
    return criar_resposta("200", "Canais listados", lista)


def tratar_cadastrar(requisicao):
    canal = requisicao.conteudo.strip()

    if not canal or len(canal) > 50 or "|" in canal:
        return criar_resposta("400", "Nome de canal invalido!")
    if not registrar_canal(canal):
        return criar_resposta("404", "Canal já existe!")

    replicar("cadastrar", "", canal, requisicao.timestamp)
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

    registrar_publicacao(canal, texto, requisicao.timestamp)
    replicar("publicar", canal, texto, requisicao.timestamp)
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
            log(f"recebido: {requisicao.acao} canal={requisicao.canal} conteudo={requisicao.conteudo} "
                f"timestamp={requisicao.timestamp:.0f} contador={requisicao.contadorMensagem}")

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
        except Exception as erro:
            log(f"erro ao processar mensagem: {erro}")
            resposta = criar_resposta("500", "Erro interno no servidor!")

        log(f"enviado: {resposta.status} {resposta.resposta} "
            f"timestamp={resposta.timestamp:.0f} contador={resposta.contadorMensagem}")
        responder.send(resposta.SerializeToString())

        mensagens_recebidas += 1
        if mensagens_recebidas % HEARTBEAT_A_CADA == 0:
            heartbeat()
        if mensagens_recebidas % SINCRONIZAR_A_CADA == 0:
            sincronizar_relogio()


log(f"dados em {DIRETORIO}")

threading.Thread(target=atender_servidores, daemon=True).start()
threading.Thread(target=ouvir_anuncios, daemon=True).start()

replicacao_pronta = threading.Event()
threading.Thread(target=ouvir_replicacao, args=(replicacao_pronta,), daemon=True).start()
replicacao_pronta.wait()

rank = consultar_rank()
log(f"rank {rank}")

time.sleep(ESPERA_ASSINATURA_S)
obter_estado_inicial()
sincronizado.set()
iniciar_eleicao()
atender_clientes()
