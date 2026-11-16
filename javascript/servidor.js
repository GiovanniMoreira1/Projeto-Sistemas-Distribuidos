import { Reply, Publisher, Request, Subscriber } from "zeromq";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import protobuf from "protobufjs";

const NOME_SERVIDOR = process.env.NOME_SERVIDOR || os.hostname();
const PORTA_SERVIDORES = 5559;
const URL_REFERENCIA = "tcp://referencia:5556";
const HEARTBEAT_A_CADA = 10;
const SINCRONIZAR_A_CADA = 15;
const TEMPO_LIMITE_MS = 3000;
const ESPERA_ANUNCIO_MS = 10000;
const ESPERA_ASSINATURA_MS = 1000;
const TOPICO_REPLICACAO = "replicacao|";

const PASTA_CODIGO = path.dirname(fileURLToPath(import.meta.url));
const DIRETORIO = path.join(process.env.DADOS_DIR || "dados", NOME_SERVIDOR);
const ARQUIVO_CANAIS = path.join(DIRETORIO, "canais.json");
const ARQUIVO_LOGINS = path.join(DIRETORIO, "login.json");
const ARQUIVO_PUBLICACOES = path.join(DIRETORIO, "publicacoes.json");
const ARQUIVO_MENSAGENS = path.join(DIRETORIO, "mensagens.json");

const proto = await protobuf.load(path.join(PASTA_CODIGO, "..", "proto", "client.proto"));
const Mensagem = proto.lookupType("Mensagem");
const Resposta = proto.lookupType("Resposta");
const RespostaSub = proto.lookupType("RespostaSub");
const RespostaReferencia = proto.lookupType("RespostaReferencia");

await fs.mkdir(DIRETORIO, { recursive: true });

const usuarios = (await lerJson(path.join(PASTA_CODIGO, "usuarios.json"), { usuarios: [] })).usuarios;
const canais = await lerJson(ARQUIVO_CANAIS, { canais: [] });
const logins = await lerJson(ARQUIVO_LOGINS, []);
const publicacoes = await lerJson(ARQUIVO_PUBLICACOES, []);
const mensagens = await lerJson(ARQUIVO_MENSAGENS, []);

const chaveLogin = (usuario, timestamp) => JSON.stringify([usuario, Number(timestamp)]);
const chavePublicacao = (canal, conteudo, timestamp) => JSON.stringify([canal, conteudo, Number(timestamp)]);
const loginsVistos = new Set(logins.map(l => chaveLogin(l.usuario, l.timestamp)));
const publicacoesVistas = new Set(publicacoes.map(p => chavePublicacao(p.canal, p.conteudo, p.timestamp)));

let contador = 0;
let offsetRelogio = 0;
let rank = 0;
let coordenador = "";
let emEleicao = false;
let anuncioRecebido = false;
let sincronizado = false;

const pubSocket = new Publisher();
let filaPub = Promise.resolve();

function log(...texto) {
    console.log(`[${NOME_SERVIDOR}]`, ...texto);
}

async function lerJson(caminho, padrao) {
    try {
        return JSON.parse(await fs.readFile(caminho, "utf-8"));
    } catch {
        return padrao;
    }
}

const filasArquivos = new Map();

function salvarArquivo(caminho, dados) {
    const texto = JSON.stringify(dados);
    const anterior = filasArquivos.get(caminho) || Promise.resolve();
    const proxima = anterior.catch(() => {}).then(() => fs.writeFile(caminho, texto, "utf-8"));
    filasArquivos.set(caminho, proxima);
    return proxima;
}

function agora() {
    return Math.floor(Date.now() + offsetRelogio);
}

function proximoContador() {
    return ++contador;
}

function atualizarContador(recebido) {
    contador = Math.max(contador, Number(recebido));
}

function publicarNoProxy(topico, corpo) {
    filaPub = filaPub.catch(() => {}).then(() => pubSocket.send([topico, corpo]));
    return filaPub;
}

function decodificar(Tipo, bytes) {
    return Tipo.toObject(Tipo.decode(bytes), { longs: Number, defaults: true });
}

function novaMensagem(acao, conteudo = NOME_SERVIDOR, canal = "") {
    return Mensagem.encode(Mensagem.create({
        timestamp: agora(),
        acao: acao,
        canal: canal,
        conteudo: conteudo,
        contadorMensagem: proximoContador()
    })).finish();
}

function criarResposta(status, texto, lista = []) {
    return {
        status: status,
        resposta: texto,
        canais: lista,
        timestamp: agora(),
        contadorMensagem: proximoContador()
    };
}

async function requisitar(url, acao, Tipo, conteudo = NOME_SERVIDOR) {
    const socket = new Request({ sendTimeout: TEMPO_LIMITE_MS, receiveTimeout: TEMPO_LIMITE_MS, linger: 0 });
    try {
        socket.connect(url);
        await socket.send(novaMensagem(acao, conteudo));
        const [bytes] = await socket.receive();
        const resposta = decodificar(Tipo, bytes);
        atualizarContador(resposta.contadorMensagem);
        return resposta;
    } catch {
        return null;
    } finally {
        socket.close();
    }
}

function requisitarServidor(alvo, acao) {
    return requisitar(`tcp://${alvo}:${PORTA_SERVIDORES}`, acao, Resposta);
}

function requisitarReferencia(acao) {
    return requisitar(URL_REFERENCIA, acao, RespostaReferencia);
}

async function registrarLogin(usuario, timestamp, status) {
    const chave = chaveLogin(usuario, timestamp);
    if (loginsVistos.has(chave)) {
        return;
    }
    loginsVistos.add(chave);
    logins.push({ usuario: usuario, timestamp: Number(timestamp), status: status });
    await salvarArquivo(ARQUIVO_LOGINS, logins);
}

async function registrarCanal(canal) {
    if (canais.canais.includes(canal)) {
        return false;
    }
    canais.canais.push(canal);
    await salvarArquivo(ARQUIVO_CANAIS, canais);
    return true;
}

async function registrarPublicacao(canal, conteudo, timestamp) {
    const chave = chavePublicacao(canal, conteudo, timestamp);
    if (publicacoesVistas.has(chave)) {
        return;
    }
    publicacoesVistas.add(chave);
    publicacoes.push({ canal: canal, conteudo: conteudo, timestamp: Number(timestamp) });
    await salvarArquivo(ARQUIVO_PUBLICACOES, publicacoes);
}

async function tentarRank() {
    const resposta = await requisitarReferencia("consultar_rank");
    return resposta !== null && resposta.status === "200" ? resposta.rank : null;
}

async function heartbeat() {
    const resposta = await requisitarReferencia("heartbeat");
    if (resposta === null) {
        log("heartbeat sem resposta da referência");
    } else if (resposta.status === "200") {
        log("heartbeat confirmado");
    } else {
        const novo = await tentarRank();
        if (novo !== null) {
            rank = novo;
            log("recadastrado na referência, rank", rank);
        }
    }
}

async function iniciarEleicao() {
    if (emEleicao) {
        return;
    }
    emEleicao = true;
    anuncioRecebido = false;
    log("iniciando eleição...");

    try {
        const lista = await requisitarReferencia("listar_servidores");
        const maiores = (lista?.servidores || []).filter(s => s.rank > rank && s.nome !== NOME_SERVIDOR);

        let algumRespondeu = false;
        for (const alvo of maiores) {
            const resposta = await requisitarServidor(alvo.nome, "eleicao");
            if (resposta !== null && resposta.resposta === "OK") {
                algumRespondeu = true;
            }
        }

        if (algumRespondeu) {
            log("servidor de rank maior assumiu a eleição; aguardando anúncio...");
            setTimeout(() => {
                if (!anuncioRecebido) {
                    log("anúncio não chegou; refazendo eleição");
                    iniciarEleicao();
                }
            }, ESPERA_ANUNCIO_MS);
        } else {
            await tornarSeCoordenador();
        }
    } finally {
        emEleicao = false;
    }
}

async function tornarSeCoordenador() {
    coordenador = NOME_SERVIDOR;
    offsetRelogio = 0;
    anuncioRecebido = true;
    log(`eleito coordenador (rank ${rank})`);

    for (let i = 0; i < 2; i++) {
        const anuncio = RespostaSub.encode(RespostaSub.create({
            timestampEnvio: agora(),
            canal: "servers",
            conteudo: NOME_SERVIDOR,
            contadorMensagem: proximoContador()
        })).finish();
        await publicarNoProxy("servers|", anuncio);
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
}

async function ouvirAnuncios() {
    const sub = new Subscriber();
    sub.connect("tcp://proxy:5558");
    sub.subscribe("servers|");

    for await (const [, corpo] of sub) {
        const anuncio = decodificar(RespostaSub, corpo);
        atualizarContador(anuncio.contadorMensagem);
        const mudou = coordenador !== anuncio.conteudo;
        coordenador = anuncio.conteudo;
        anuncioRecebido = true;
        if (mudou) {
            log("novo coordenador anunciado:", coordenador);
        }
    }
}

async function sincronizarRelogio() {
    const alvo = coordenador;
    if (alvo === "") {
        iniciarEleicao();
        return;
    }
    if (alvo === NOME_SERVIDOR) {
        log("sou o coordenador: relógio mantido");
        return;
    }

    const t0 = Date.now();
    const resposta = await requisitarServidor(alvo, "sincronizar");
    const t1 = Date.now();

    if (resposta === null || resposta.status !== "200") {
        log("coordenador indisponível; iniciando eleição");
        iniciarEleicao();
        return;
    }

    offsetRelogio = Number(resposta.timestamp) + (t1 - t0) / 2 - t1;
    log(`relógio sincronizado com ${alvo} (offset ${Math.round(offsetRelogio)} ms)`);
}

async function atenderServidores() {
    const socket = new Reply();
    await socket.bind(`tcp://*:${PORTA_SERVIDORES}`);

    for await (const [bytes] of socket) {
        let resposta;
        let iniciar = false;
        try {
            const pedido = decodificar(Mensagem, bytes);
            atualizarContador(pedido.contadorMensagem);

            if (pedido.acao === "obter_estado" && !sincronizado) {
                resposta = criarResposta("503", "Servidor ainda sincronizando");
            } else if (pedido.acao === "obter_estado") {
                resposta = criarResposta("200", exportarEstado());
                log("estado enviado para", pedido.conteudo);
            } else if (pedido.acao === "eleicao") {
                resposta = criarResposta("200", "OK");
                iniciar = true;
            } else if (pedido.acao === "sincronizar") {
                resposta = coordenador === NOME_SERVIDOR
                    ? criarResposta("200", "Hora correta")
                    : criarResposta("400", "Não sou o coordenador!");
            } else {
                resposta = criarResposta("400", "Ação desconhecida!");
            }
        } catch (erro) {
            log("erro ao atender servidor:", erro);
            resposta = criarResposta("500", "Erro interno no servidor!");
        }

        await socket.send(Resposta.encode(Resposta.create(resposta)).finish());
        if (iniciar) {
            iniciarEleicao();
        }
    }
}

async function replicar(acao, canal, conteudo, timestamp) {
    const operacao = Mensagem.encode(Mensagem.create({
        timestamp: Number(timestamp),
        acao: acao,
        canal: canal,
        conteudo: conteudo,
        contadorMensagem: proximoContador()
    })).finish();
    await publicarNoProxy(TOPICO_REPLICACAO + NOME_SERVIDOR, operacao);
}

async function aplicarOperacao(operacao) {
    if (operacao.acao === "login") {
        const status = usuarios.includes(operacao.conteudo) ? "200" : "404";
        await registrarLogin(operacao.conteudo, operacao.timestamp, status);
    } else if (operacao.acao === "cadastrar") {
        await registrarCanal(operacao.conteudo);
    } else if (operacao.acao === "publicar") {
        await registrarPublicacao(operacao.canal, operacao.conteudo, operacao.timestamp);
    }
}

async function ouvirReplicacao(sub) {
    for await (const [topico, corpo] of sub) {
        const origem = topico.toString().slice(TOPICO_REPLICACAO.length);
        if (origem === NOME_SERVIDOR) {
            continue;
        }
        try {
            const operacao = decodificar(Mensagem, corpo);
            atualizarContador(operacao.contadorMensagem);
            await aplicarOperacao(operacao);
            log(`replicado de ${origem}:`, operacao.acao, operacao.canal, operacao.conteudo);
        } catch (erro) {
            log("erro ao aplicar replicação:", erro);
        }
    }
}

function exportarEstado() {
    return JSON.stringify({ canais: canais.canais, logins: logins, publicacoes: publicacoes });
}

async function importarEstado(texto) {
    const estado = JSON.parse(texto);
    for (const canal of estado.canais || []) {
        await registrarCanal(canal);
    }
    for (const l of estado.logins || []) {
        await registrarLogin(l.usuario, l.timestamp, l.status || "");
    }
    for (const p of estado.publicacoes || []) {
        await registrarPublicacao(p.canal, p.conteudo, p.timestamp);
    }
}

async function obterEstadoInicial() {
    const lista = await requisitarReferencia("listar_servidores");
    const candidatos = [];
    if (coordenador !== "" && coordenador !== NOME_SERVIDOR) {
        candidatos.push(coordenador);
    }
    for (const s of lista?.servidores || []) {
        if (s.nome !== NOME_SERVIDOR && s.nome !== coordenador) {
            candidatos.push(s.nome);
        }
    }

    for (const alvo of candidatos) {
        const resposta = await requisitarServidor(alvo, "obter_estado");
        if (resposta !== null && resposta.status === "200") {
            await importarEstado(resposta.resposta);
            log(`estado recebido de ${alvo}: ${canais.canais.length} canais, ${logins.length} logins, ${publicacoes.length} publicações`);
            return;
        }
    }
    log("nenhum outro servidor respondeu: mantendo os dados locais");
}

async function tratarLogin(req) {
    const usuario = req.conteudo.trim();
    const resposta = usuarios.includes(usuario)
        ? criarResposta("200", "Login realizado com sucesso!")
        : criarResposta("404", "Nome de usuario incorreto!");

    await registrarLogin(usuario, req.timestamp, resposta.status);
    await replicar("login", "", usuario, req.timestamp);
    return resposta;
}

function tratarListar() {
    return criarResposta("200", "Canais listados", [...canais.canais]);
}

async function tratarCadastrar(req) {
    const canal = req.conteudo.trim();
    if (canal === "" || canal.length > 50 || canal.includes("|")) {
        return criarResposta("400", "Nome de canal invalido!");
    }
    if (!(await registrarCanal(canal))) {
        return criarResposta("404", "Canal já existe!");
    }

    await replicar("cadastrar", "", canal, req.timestamp);
    return criarResposta("200", `Canal ${canal} cadastrado com sucesso!`);
}

async function tratarPublicar(req) {
    const canal = req.canal.trim();
    if (canal === "" || req.conteudo === "") {
        return criarResposta("400", "Canal e mensagem sao obrigatorios!");
    }

    const publicacao = RespostaSub.encode(RespostaSub.create({
        timestampEnvio: agora(),
        canal: canal,
        conteudo: req.conteudo,
        contadorMensagem: proximoContador()
    })).finish();
    await publicarNoProxy(`${canal}|`, publicacao);

    await registrarPublicacao(canal, req.conteudo, req.timestamp);
    await replicar("publicar", canal, req.conteudo, req.timestamp);
    return criarResposta("200", `Mensagem publicada no canal ${canal} com sucesso!`);
}

const ACOES = {
    login: tratarLogin,
    listar: tratarListar,
    cadastrar: tratarCadastrar,
    publicar: tratarPublicar
};

async function atenderClientes() {
    const socket = new Reply();
    socket.connect("tcp://broker:5560");

    let recebidas = 0;
    for await (const [bytes] of socket) {
        let resposta;
        try {
            const req = decodificar(Mensagem, bytes);
            atualizarContador(req.contadorMensagem);
            log(`recebido: ${req.acao} canal=${req.canal} conteudo=${req.conteudo} timestamp=${req.timestamp} contador=${req.contadorMensagem}`);
            mensagens.push({ usuario: "cliente", ...req });

            const tratar = ACOES[req.acao];
            resposta = tratar ? await tratar(req) : criarResposta("400", "Ação desconhecida!");
        } catch (erro) {
            log("erro ao processar mensagem:", erro);
            resposta = criarResposta("500", "Erro interno no servidor!");
        }

        log(`enviado: ${resposta.status} ${resposta.resposta} timestamp=${resposta.timestamp} contador=${resposta.contadorMensagem}`);
        await socket.send(Resposta.encode(Resposta.create(resposta)).finish());

        mensagens.push({ usuario: "servidor", ...resposta });
        await salvarArquivo(ARQUIVO_MENSAGENS, mensagens);

        recebidas++;
        if (recebidas % HEARTBEAT_A_CADA === 0) {
            await heartbeat();
        }
        if (recebidas % SINCRONIZAR_A_CADA === 0) {
            await sincronizarRelogio();
        }
    }
}

async function main() {
    log("dados em", DIRETORIO);
    pubSocket.connect("tcp://proxy:5557");

    const subReplicacao = new Subscriber();
    subReplicacao.connect("tcp://proxy:5558");
    subReplicacao.subscribe(TOPICO_REPLICACAO);
    ouvirReplicacao(subReplicacao);

    ouvirAnuncios();
    atenderServidores();

    let inicial;
    while ((inicial = await tentarRank()) === null) {
        log("aguardando o serviço de referência...");
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
    rank = inicial;
    log("rank", rank);

    await new Promise(resolve => setTimeout(resolve, ESPERA_ASSINATURA_MS));
    await obterEstadoInicial();
    sincronizado = true;
    iniciarEleicao();
    await atenderClientes();
}

main().catch(erro => {
    console.error("Erro ao iniciar servidor:", erro);
    process.exit(1);
});

process.on("SIGINT", () => process.exit(0));
