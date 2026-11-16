import { Reply, Publisher, Request, Subscriber } from "zeromq";
import fs from "fs/promises";
import protobuf from "protobufjs";

var usuarios = JSON.parse(await fs.readFile("usuarios.json", "utf-8"));
var canais = JSON.parse(await fs.readFile("canais.json", "utf-8"));
var logins = JSON.parse(await fs.readFile("login.json", "utf-8"));
var publicacoes = JSON.parse(await fs.readFile("publicacoes.json", "utf-8"));
var mensagens = JSON.parse(await fs.readFile("mensagens.json", "utf-8"));

var contadorMensagens = 0;

var mensagensRecebidas = 0;

var offsetRelogio = 0;

const NOME_SERVIDOR = "servidorJavaScript";
// const NOME_SERVIDOR = process.env.NOME_SERVIDOR || "servidorJavaScript";
const PORTA_SERVIDORES = 5559;
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

    const root = await protobuf.load("client.proto");

    Mensagem = root.lookupType("Mensagem");
    Resposta = root.lookupType("Resposta");
    RespostaSub = root.lookupType("RespostaSub");
    RespostaReferencia = root.lookupType("RespostaReferencia");

    const responder = new Reply();

    const respostaServidores = new Reply();

    const pub = new Publisher();

    const sub = new Subscriber();

    const referencia = new Request();

    pub.connect("tcp://proxy:5557");

    sub.connect("tcp://proxy:5558");
    sub.subscribe("servers");

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

            await fs.writeFile(
                "mensagens.json",
                JSON.stringify(mensagens),
                "utf-8"
            );

            // LOGIN
            if (objetoOriginal.acao === "login") {

                if (usuarios.usuarios.includes(objetoOriginal.conteudo)) {

                    const payload = criarResposta(
                        Resposta,
                        "200",
                        "Login realizado com sucesso!"
                    );

                    logins.push({
                        usuario: objetoOriginal.conteudo,
                        timestamp: objetoOriginal.timestamp,
                        status: "200"
                    });

                    await fs.writeFile(
                        "login.json",
                        JSON.stringify(logins),
                        "utf-8"
                    );

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

                    logins.push({
                        usuario: objetoOriginal.conteudo,
                        timestamp: objetoOriginal.timestamp,
                        status: "404"
                    });

                    await fs.writeFile(
                        "login.json",
                        JSON.stringify(logins),
                        "utf-8"
                    );

                    await enviarResposta(responder, payload);

                    mensagens.push({
                        usuario: "servidor",
                        status: "404",
                        conteudo: "Nome de usuario incorreto!"
                    });
                }

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

                if (!canais.canais.includes(objetoOriginal.conteudo)) {

                    canais.canais.push(objetoOriginal.conteudo);

                    await fs.writeFile(
                        "canais.json",
                        JSON.stringify(canais),
                        "utf-8"
                    );

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

    await fs.writeFile(
        "mensagens.json",
        JSON.stringify(mensagens),
        "utf-8"
    );
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

            if (objeto.acao === "eleicao") {

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

        console.log("Erro no heartbeat:", objeto.resposta);
    }

    return objeto;
}

async function publicar(pub, objetoOriginal, RespostaSub) {

    let contador = 1;

    while (contador <= 10) {

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

        publicacoes.push({
            conteudo: objetoOriginal.conteudo,
            canal: objetoOriginal.canal
        });

        await fs.writeFile(
            "publicacoes.json",
            JSON.stringify(publicacoes),
            "utf-8"
        );

        console.log(
            "Mensagem publicada:",
            objetoOriginal.conteudo
        );

        contador++;

        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

run().catch(error => {
    console.error("Erro ao iniciar servidor:", error);
});

process.on("SIGINT", function () {
    process.exit(0);
});