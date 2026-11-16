import { Reply, Publisher, Request, Subscriber } from "zeromq";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import protobuf from "protobufjs";

// Nome do servidor = nome do servico no docker-compose: os outros servidores usam esse nome
// como endereco (tcp://<nome>:5559) para pedir eleicao e sincronizacao.
const NOME_SERVIDOR = process.env.NOME_SERVIDOR || os.hostname();
const PORTA_SERVIDORES = 5559;

const PASTA_CODIGO = path.dirname(fileURLToPath(import.meta.url));
const ARQUIVO_PROTO = path.join(PASTA_CODIGO, "..", "proto", "client.proto");

// cada servidor tem o seu proprio diretorio de dados: nada e compartilhado entre servidores
const DIRETORIO = path.join(process.env.DADOS_DIR || "dados", NOME_SERVIDOR);
await fs.mkdir(DIRETORIO, { recursive: true });

const ARQUIVO_CANAIS = path.join(DIRETORIO, "canais.json");
const ARQUIVO_LOGINS = path.join(DIRETORIO, "login.json");
const ARQUIVO_PUBLICACOES = path.join(DIRETORIO, "publicacoes.json");
const ARQUIVO_MENSAGENS = path.join(DIRETORIO, "mensagens.json");

async function lerJson(caminho, padrao) {
    try {
        return JSON.parse(await fs.readFile(caminho, "utf-8"));
    } catch {
        return padrao;
    }
}

var usuarios = await lerJson(path.join(PASTA_CODIGO, "usuarios.json"), { usuarios: [] });
var canais = await lerJson(ARQUIVO_CANAIS, { canais: [] });
var logins = await lerJson(ARQUIVO_LOGINS, []);
var publicacoes = await lerJson(ARQUIVO_PUBLICACOES, []);
var mensagens = await lerJson(ARQUIVO_MENSAGENS, []);

// os arquivos sao gravados pelo atendimento dos clientes e pela replicacao ao mesmo tempo:
// cada arquivo tem uma fila de gravacoes para que duas escritas nao se misturem
const filasArquivos = new Map();

function salvarArquivo(caminho, dados) {
    const texto = JSON.stringify(dados);
    const anterior = filasArquivos.get(caminho) || Promise.resolve();
    const proxima = anterior.catch(() => {}).then(() => fs.writeFile(caminho, texto, "utf-8"));
    filasArquivos.set(caminho, proxima);
    return proxima;
}

// chaves do que ja esta guardado: aplicar a mesma operacao duas vezes nao duplica o registro
const chaveLogin = (usuario, timestamp) => JSON.stringify([usuario, Number(timestamp)]);
const chavePublicacao = (canal, conteudo, timestamp) => JSON.stringify([canal, conteudo, Number(timestamp)]);
const loginsVistos = new Set(logins.map(l => chaveLogin(l.usuario, l.timestamp)));
const publicacoesVistas = new Set(publicacoes.map(p => chavePublicacao(p.canal, p.conteudo, p.timestamp)));

async function registrarLogin(usuario, timestamp, status) {
    const chave = chaveLogin(usuario, timestamp);
    if (loginsVistos.has(chave)) {
        return;
    }
    loginsVistos.add(chave);
    logins.push({ usuario: usuario, timestamp: Number(timestamp), status: status });
    await salvarArquivo(ARQUIVO_LOGINS, logins);
}

// devolve false se o canal ja existia
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

// ---------------------------------------------------------------- Parte 5: replicacao
// Replicacao ativa: cada escrita aceita de um cliente (login, cadastrar, publicar) e aplicada
// localmente e difundida no topico "replicacao|<servidor de origem>"; todos os servidores assinam
// "replicacao|" e aplicam as escritas dos outros. Quem entra (ou volta) pede o estado completo a
// outro servidor com "obter_estado".

const TOPICO_REPLICACAO = "replicacao|";
var pubReplicacao = null;

async function replicar(acao, canal, conteudo, timestamp) {

    contadorMensagens++;

    const operacao = Mensagem.create({
        timestamp: Number(timestamp),
        acao: acao,
        canal: canal,
        conteudo: conteudo,
        contadorMensagem: contadorMensagens
    });

    await pubReplicacao.send([
        TOPICO_REPLICACAO + NOME_SERVIDOR,
        Mensagem.encode(operacao).finish()
    ]);
}

// aplica uma escrita feita por outro servidor (sem responder cliente nem republicar no canal)
async function aplicarOperacao(operacao) {

    if (operacao.acao === "login") {
        const status = usuarios.usuarios.includes(operacao.conteudo) ? "200" : "404";
        await registrarLogin(operacao.conteudo, operacao.timestamp, status);
    }

    else if (operacao.acao === "cadastrar") {
        await registrarCanal(operacao.conteudo);
    }

    else if (operacao.acao === "publicar") {
        await registrarPublicacao(operacao.canal, operacao.conteudo, operacao.timestamp);
    }
}

async function ouvirReplicacao(subReplicacao) {

    for await (const [topico, msg] of subReplicacao) {

        try {

            const origem = topico.toString().slice(TOPICO_REPLICACAO.length);

            if (origem === NOME_SERVIDOR) {
                continue;  // eco da propria escrita: ja foi aplicada
            }

            const operacao = Mensagem.toObject(Mensagem.decode(msg), {
                longs: Number,
                defaults: true
            });

            contadorMensagens = Math.max(contadorMensagens, Number(operacao.contadorMensagem));

            await aplicarOperacao(operacao);

            console.log("Replicado de " + origem + ":", operacao.acao, operacao.canal, operacao.conteudo);

        } catch (error) {

            console.log("Erro ao aplicar replicação:", error);
        }
    }
}

function exportarEstado() {

    return JSON.stringify({
        canais: canais.canais,
        logins: logins,
        publicacoes: publicacoes
    });
}

// junta o estado recebido com o local (sem duplicar)
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

// pede o estado ao coordenador (se for outro servidor) ou ao proximo servidor da lista
async function obterEstadoInicial() {

    const servidores = await listarServidores();

    const candidatos = [];

    if (servidorEleito !== "" && servidorEleito !== NOME_SERVIDOR) {
        candidatos.push(servidorEleito);
    }

    for (const s of servidores) {
        if (s.nome !== NOME_SERVIDOR && s.nome !== servidorEleito) {
            candidatos.push(s.nome);
        }
    }

    for (const alvo of candidatos) {

        contadorMensagens++;

        const payload = Mensagem.create({
            timestamp: Date.now() + offsetRelogio,
            acao: "obter_estado",
            conteudo: NOME_SERVIDOR,
            contadorMensagem: contadorMensagens
        });

        const objeto = await requisitar(
            "tcp://" + alvo + ":" + PORTA_SERVIDORES,
            payload,
            Resposta
        );

        if (objeto !== null && objeto.status === "200") {

            await importarEstado(objeto.resposta);

            console.log(
                "Estado recebido de " + alvo + ":",
                canais.canais.length, "canais,",
                logins.length, "logins,",
                publicacoes.length, "publicacoes"
            );
            return;
        }
    }

    console.log("Nenhum outro servidor respondeu: mantendo os dados locais");
}

var contadorMensagens = 0;

var mensagensRecebidas = 0;

var offsetRelogio = 0;

const TEMPO_LIMITE_MS = 3000;

var Mensagem;
var Resposta;
var RespostaSub;
var RespostaReferencia;

var rankServidor = 0;

var servidorEleito = "";

var emEleicao = false;
var anuncioRecebido = false;

async function run() {

    const root = await protobuf.load(ARQUIVO_PROTO);

    Mensagem = root.lookupType("Mensagem");
    Resposta = root.lookupType("Resposta");
    RespostaSub = root.lookupType("RespostaSub");
    RespostaReferencia = root.lookupType("RespostaReferencia");

    const responder = new Reply();

    const respostaServidores = new Reply();

    const pubSocket = new Publisher();

    // o PUB e usado ao mesmo tempo pelas requisicoes, pela eleicao e pela replicacao, e o
    // zeromq.js nao aceita dois envios simultaneos no mesmo socket: os envios entram numa fila
    let filaPub = Promise.resolve();
    const pub = {
        send: (frames) => (filaPub = filaPub.catch(() => {}).then(() => pubSocket.send(frames)))
    };

    const sub = new Subscriber();

    const subReplicacao = new Subscriber();

    const referencia = new Request();

    pubSocket.connect("tcp://proxy:5557");

    sub.connect("tcp://proxy:5558");
    sub.subscribe("servers");

    // Parte 5: assina a replicacao antes de pedir o estado, para nao perder escritas no meio
    subReplicacao.connect("tcp://proxy:5558");
    subReplicacao.subscribe(TOPICO_REPLICACAO);
    ouvirReplicacao(subReplicacao);
    pubReplicacao = pub;

    responder.connect("tcp://broker:5560");

    await respostaServidores.bind("tcp://*:" + PORTA_SERVIDORES);

    referencia.connect("tcp://referencia:5556");

    console.log("Servidor conectado ao broker...");
    console.log("Servidor conectado ao proxy...");
    console.log("Servidor conectado à referência...");

    rankServidor = await consultarRank(
        referencia,
        Mensagem,
        RespostaReferencia
    );

    console.log("Rank do servidor:", rankServidor);

    ouvirAnuncios(sub);
    atenderServidores(respostaServidores, pub);

    await obterEstadoInicial();

    iniciarEleicao(pub);

    for await (const [request] of responder) {

        try {

            const requisicao = Mensagem.decode(request);

            const objetoOriginal = Mensagem.toObject(requisicao, {
                longs: Number,
                enums: String,
                bytes: String,
                defaults: true
            });

            contadorMensagens = Math.max(
                contadorMensagens,
                Number(objetoOriginal.contadorMensagem)
            );

            mensagensRecebidas++;

            if (mensagensRecebidas % 10 === 0) {

                await heartbeat(
                    referencia,
                    Mensagem,
                    RespostaReferencia
                );
            }

            if (mensagensRecebidas % 15 === 0) {

                await sincronizarComServidorEleito(pub);
            }

            mensagens.push({
                usuario: "cliente",
                acao: objetoOriginal.acao,
                conteudo: objetoOriginal.conteudo,
                canal: objetoOriginal.canal,
                timestamp: objetoOriginal.timestamp,
                contadorMensagem: objetoOriginal.contadorMensagem
            });

            await salvarArquivo(ARQUIVO_MENSAGENS, mensagens);

            // LOGIN
            if (objetoOriginal.acao === "login") {

                if (usuarios.usuarios.includes(objetoOriginal.conteudo)) {

                    const payload = criarResposta(
                        Resposta,
                        "200",
                        "Login realizado com sucesso!"
                    );

                    await registrarLogin(objetoOriginal.conteudo, objetoOriginal.timestamp, "200");

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "200",
                        conteudo: "Login realizado com sucesso!"
                    });

                } else {

                    const payload = criarResposta(
                        Resposta,
                        "404",
                        "Nome de usuario incorreto!"
                    );

                    await registrarLogin(objetoOriginal.conteudo, objetoOriginal.timestamp, "404");

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "404",
                        conteudo: "Nome de usuario incorreto!"
                    });
                }

                await replicar("login", "", objetoOriginal.conteudo, objetoOriginal.timestamp);

                await salvarMensagens();
            }

            // LISTAR CANAIS
            else if (objetoOriginal.acao === "listar") {

                const payload = criarResposta(
                    Resposta,
                    "200",
                    "Canais listados",
                    canais.canais
                );

                await enviarResposta(responder, payload);

                mensagens.push({
                    usuario: "servidor",
                    status: "200",
                    conteudo: "Canais listados",
                    canais: canais.canais
                });

                await salvarMensagens();
            }

            // CADASTRAR CANAL
            else if (objetoOriginal.acao === "cadastrar") {

                if (await registrarCanal(objetoOriginal.conteudo)) {

                    await replicar("cadastrar", "", objetoOriginal.conteudo, objetoOriginal.timestamp);

                    const payload = criarResposta(
                        Resposta,
                        "200",
                        "Canal " + objetoOriginal.conteudo + " cadastrado com sucesso!"
                    );

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "200",
                        conteudo: "Canal " + objetoOriginal.conteudo + " cadastrado com sucesso!"
                    });

                } else {

                    const payload = criarResposta(
                        Resposta,
                        "404",
                        "Canal já existe!"
                    );

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "404",
                        conteudo: "Canal já existe!"
                    });
                }

                await salvarMensagens();
            }

            // PUBLICAR MENSAGEM
            else if (objetoOriginal.acao === "publicar") {

                try {

                    await publicar(
                        pub,
                        objetoOriginal,
                        RespostaSub
                    );

                    const payload = criarResposta(
                        Resposta,
                        "200",
                        "Mensagem publicada no canal " +
                        objetoOriginal.canal + " com sucesso!"
                    );

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "200",
                        conteudo: "Mensagem publicada no canal " +
                        objetoOriginal.canal + " com sucesso!"
                    });

                } catch (error) {

                    console.log("Erro ao publicar:", error);

                    const payload = criarResposta(
                        Resposta,
                        "500",
                        "Erro ao publicar mensagem!"
                    );

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "500",
                        conteudo: "Erro ao publicar mensagem!"
                    });
                }

                await salvarMensagens();
            }

        } catch (error) {

            console.log("Erro ao processar mensagem:", error);

            const payload = criarResposta(
                Resposta,
                "500",
                "Erro interno no servidor!"
            );

            await enviarResposta(responder, payload);
        }
    }
}

function criarResposta(Resposta, status, resposta, listaCanais = []) {

    contadorMensagens++;

    return Resposta.create({
        status: status,
        resposta: resposta,
        canais: listaCanais,
        timestamp: Date.now() + offsetRelogio,
        contadorMensagem: contadorMensagens
    });
}

async function enviarResposta(socket, payload) {

    await socket.send(
        RespostaEncode(payload)
    );
}

function RespostaEncode(payload) {
    return Resposta.encode(payload).finish();
}

// Salva histórico de mensagens
async function salvarMensagens() {

    await salvarArquivo(ARQUIVO_MENSAGENS, mensagens);
}

async function requisitar(url, payload, Tipo) {

    const socket = new Request({
        sendTimeout: TEMPO_LIMITE_MS,
        receiveTimeout: TEMPO_LIMITE_MS,
        linger: 0
    });

    try {

        socket.connect(url);

        await socket.send(
            Mensagem.encode(payload).finish()
        );

        const [reply] = await socket.receive();

        const objeto = Tipo.toObject(Tipo.decode(reply), {
            longs: Number,
            defaults: true
        });

        contadorMensagens = Math.max(
            contadorMensagens,
            Number(objeto.contadorMensagem)
        );

        return objeto;

    } catch (error) {

        return null;

    } finally {

        socket.close();
    }
}

async function consultarRank(
    requester,
    Mensagem,
    RespostaReferencia
) {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now() + offsetRelogio,
        acao: "consultar_rank",
        conteudo: NOME_SERVIDOR,
        contadorMensagem: contadorMensagens
    });

    await requester.send(
        Mensagem.encode(payload).finish()
    );

    const [reply] = await requester.receive();

    const resposta = RespostaReferencia.decode(reply);

    const objeto = RespostaReferencia.toObject(resposta, {
        defaults: true
    });

    contadorMensagens = Math.max(
        contadorMensagens,
        Number(objeto.contadorMensagem)
    );

    if (objeto.status !== "200") {
        throw new Error(objeto.resposta);
    }

    return objeto.rank;
}

async function listarServidores() {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now() + offsetRelogio,
        acao: "listar_servidores",
        conteudo: NOME_SERVIDOR,
        contadorMensagem: contadorMensagens
    });

    const objeto = await requisitar(
        "tcp://referencia:5556",
        payload,
        RespostaReferencia
    );

    if (objeto === null || objeto.status !== "200") {
        return [];
    }

    return objeto.servidores;
}

async function iniciarEleicao(pub) {

    if (emEleicao) {
        return;
    }

    emEleicao = true;
    anuncioRecebido = false;

    console.log("Iniciando eleição...");

    try {

        const servidores = await listarServidores();

        const maiores = servidores.filter(
            s => s.rank > rankServidor && s.nome !== NOME_SERVIDOR
        );

        let algumRespondeu = false;

        for (const alvo of maiores) {

            contadorMensagens++;

            const payload = Mensagem.create({
                timestamp: Date.now() + offsetRelogio,
                acao: "eleicao",
                conteudo: NOME_SERVIDOR,
                contadorMensagem: contadorMensagens
            });

            const objeto = await requisitar(
                "tcp://" + alvo.nome + ":" + PORTA_SERVIDORES,
                payload,
                Resposta
            );

            if (objeto !== null && objeto.resposta === "OK") {
                algumRespondeu = true;
            }
        }

        if (algumRespondeu) {

            console.log("Servidor de rank maior assumiu a eleição. Aguardando anúncio...");

            setTimeout(() => {
                if (!anuncioRecebido) {
                    iniciarEleicao(pub);
                }
            }, 10000);

        } else {

            servidorEleito = NOME_SERVIDOR;
            offsetRelogio = 0;
            anuncioRecebido = true;

            console.log("Servidor eleito como coordenador:", servidorEleito);

            await anunciarCoordenador(pub);
        }

    } finally {

        emEleicao = false;
    }
}

async function anunciarCoordenador(pub) {

    contadorMensagens++;

    const payloadSub = RespostaSub.create({
        timestampEnvio: Date.now() + offsetRelogio,
        canal: "servers",
        conteudo: NOME_SERVIDOR,
        contadorMensagem: contadorMensagens
    });

    await pub.send([
        "servers|",
        RespostaSub.encode(payloadSub).finish()
    ]);
}

async function ouvirAnuncios(sub) {

    for await (const [topico, msg] of sub) {

        try {

            const objeto = RespostaSub.toObject(RespostaSub.decode(msg), {
                longs: Number,
                defaults: true
            });

            contadorMensagens = Math.max(
                contadorMensagens,
                Number(objeto.contadorMensagem)
            );

            servidorEleito = objeto.conteudo;
            anuncioRecebido = true;

            console.log("Novo coordenador anunciado:", servidorEleito);

        } catch (error) {

            console.log("Erro ao processar anúncio:", error);
        }
    }
}

async function atenderServidores(socket, pub) {

    for await (const [request] of socket) {

        try {

            const requisicao = Mensagem.decode(request);

            const objeto = Mensagem.toObject(requisicao, {
                longs: Number,
                defaults: true
            });

            contadorMensagens = Math.max(
                contadorMensagens,
                Number(objeto.contadorMensagem)
            );

            // Parte 5: transferencia de estado para um servidor que esta entrando
            if (objeto.acao === "obter_estado") {

                const payload = criarResposta(Resposta, "200", exportarEstado());

                await enviarResposta(socket, payload);

                console.log("Estado enviado para", objeto.conteudo);
            }

            else if (objeto.acao === "eleicao") {

                const payload = criarResposta(Resposta, "200", "OK");

                await enviarResposta(socket, payload);

                iniciarEleicao(pub);
            }

            else if (objeto.acao === "sincronizar") {

                if (servidorEleito === NOME_SERVIDOR) {

                    const payload = criarResposta(
                        Resposta,
                        "200",
                        "Hora correta"
                    );

                    await enviarResposta(socket, payload);

                } else {

                    const payload = criarResposta(
                        Resposta,
                        "400",
                        "Não sou o coordenador!"
                    );

                    await enviarResposta(socket, payload);
                }
            }

            else {

                const payload = criarResposta(
                    Resposta,
                    "400",
                    "Ação desconhecida!"
                );

                await enviarResposta(socket, payload);
            }

        } catch (error) {

            console.log("Erro ao atender servidor:", error);

            const payload = criarResposta(
                Resposta,
                "500",
                "Erro interno no servidor!"
            );

            await enviarResposta(socket, payload);
        }
    }
}

async function sincronizarComServidorEleito(pub) {

    if (servidorEleito === "") {
        iniciarEleicao(pub);
        return;
    }

    if (servidorEleito === NOME_SERVIDOR) {
        console.log("Este servidor é o coordenador. Relógio mantido.");
        return;
    }

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now() + offsetRelogio,
        acao: "sincronizar",
        conteudo: NOME_SERVIDOR,
        contadorMensagem: contadorMensagens
    });

    const t0 = Date.now();

    const objeto = await requisitar(
        "tcp://" + servidorEleito + ":" + PORTA_SERVIDORES,
        payload,
        Resposta
    );

    const t1 = Date.now();

    if (objeto === null || objeto.status !== "200") {

        console.log("Coordenador indisponível. Iniciando eleição...");

        iniciarEleicao(pub);
        return;
    }

    const horaCoordenador = Number(objeto.timestamp) + (t1 - t0) / 2;

    offsetRelogio = horaCoordenador - t1;

    console.log(
        "Relógio sincronizado com " + servidorEleito + ":",
        new Date(Date.now() + offsetRelogio).toISOString()
    );
}

async function heartbeat(
    requester,
    Mensagem,
    RespostaReferencia
) {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now() + offsetRelogio,
        acao: "heartbeat",
        conteudo: NOME_SERVIDOR,
        contadorMensagem: contadorMensagens
    });

    await requester.send(
        Mensagem.encode(payload).finish()
    );

    const [reply] = await requester.receive();

    const resposta = RespostaReferencia.decode(reply);

    const objeto = RespostaReferencia.toObject(resposta, {
        defaults: true
    });

    contadorMensagens = Math.max(
        contadorMensagens,
        Number(objeto.contadorMensagem)
    );

    if (objeto.status === "200") {

        console.log("Heartbeat confirmado.");

    } else {

        // a referencia removeu este servidor (ficou muito tempo sem heartbeat): cadastra de novo
        console.log("Erro no heartbeat:", objeto.resposta);

        rankServidor = await consultarRank(requester, Mensagem, RespostaReferencia);

        console.log("Recadastrado na referência, rank", rankServidor);
    }

    return objeto;
}

// publica uma vez no canal (topico "<canal>|"); o bot e quem manda 10 mensagens por rodada
async function publicar(pub, objetoOriginal, RespostaSub) {

    contadorMensagens++;

    const payloadSub = RespostaSub.create({
        timestampEnvio: Date.now() + offsetRelogio,
        canal: objetoOriginal.canal,
        conteudo: objetoOriginal.conteudo,
        contadorMensagem: contadorMensagens
    });

    await pub.send([
        objetoOriginal.canal + "|",
        RespostaSub.encode(payloadSub).finish()
    ]);

    await registrarPublicacao(objetoOriginal.canal, objetoOriginal.conteudo, objetoOriginal.timestamp);

    await replicar("publicar", objetoOriginal.canal, objetoOriginal.conteudo, objetoOriginal.timestamp);

    console.log(
        "Mensagem publicada:",
        objetoOriginal.conteudo
    );
}

run().catch(error => {
    console.error("Erro ao iniciar servidor:", error);
});

process.on("SIGINT", function () {
    process.exit(0);
});