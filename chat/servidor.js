
import { Reply, Publisher, Request } from "zeromq";
import fs from "fs/promises";
import protobuf from "protobufjs";

// Dados persistidos
var usuarios = JSON.parse(await fs.readFile("usuarios.json", "utf-8"));
var canais = JSON.parse(await fs.readFile("canais.json", "utf-8"));
var logins = JSON.parse(await fs.readFile("login.json", "utf-8"));
var publicacoes = JSON.parse(await fs.readFile("publicacoes.json", "utf-8"));
var mensagens = JSON.parse(await fs.readFile("mensagens.json", "utf-8"));

// Relógio lógico
var contadorMensagens = 0;

// Quantidade de mensagens recebidas dos bots
var mensagensRecebidas = 0;

// Relógio físico sincronizado
var timestampReferencia = Date.now();

// Identificação do servidor
const NOME_SERVIDOR = "servidorJavaScript";

var Resposta;

async function run() {

    const root = await protobuf.load("client.proto");

    const Mensagem = root.lookupType("Mensagem");
    Resposta = root.lookupType("Resposta");
    const RespostaSub = root.lookupType("RespostaSub");
    const RespostaReferencia = root.lookupType("RespostaReferencia");

    // Socket para receber requisições dos bots
    const responder = new Reply();

    // Socket para publicar mensagens
    const pub = new Publisher();

    // Socket exclusivo para comunicação com a referência
    const referencia = new Request();

    pub.connect("tcp://proxy:5557");

    responder.connect("tcp://broker:5560");

    referencia.connect("tcp://referencia:5556");

    console.log("Servidor conectado ao broker...");
    console.log("Servidor conectado ao proxy...");
    console.log("Servidor conectado à referência...");

    // Consulta o rank antes de começar a receber clientes
    const rankServidor = await consultarRank(
        referencia,
        Mensagem,
        RespostaReferencia
    );

    console.log("Rank do servidor:", rankServidor);

    for await (const [request] of responder) {

        try {

            // Recebe mensagem do bot
            const requisicao = Mensagem.decode(request);

            const objetoOriginal = Mensagem.toObject(requisicao, {
                longs: Number,
                enums: String,
                bytes: String,
                defaults: true
            });

            // Atualiza relógio lógico ao receber
            contadorMensagens = Math.max(
                contadorMensagens,
                Number(objetoOriginal.contadorMensagem)
            );

            // Contabiliza somente mensagens dos clientes
            mensagensRecebidas++;

            // Heartbeat a cada 10 mensagens recebidas
            if (mensagensRecebidas % 10 === 0) {

                await heartbeat(
                    referencia,
                    Mensagem,
                    RespostaReferencia
                );
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

// Cria resposta normal para os bots
function criarResposta(Resposta, status, resposta, listaCanais = []) {

    contadorMensagens++;

    return Resposta.create({
        status: status,
        resposta: resposta,
        canais: listaCanais,
        timestamp: Date.now(),
        contadorMensagem: contadorMensagens
    });
}

// Envia resposta ao bot
async function enviarResposta(socket, payload) {

    await socket.send(
        RespostaEncode(payload)
    );
}

// Codificação da resposta
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

// Consulta rank e cadastra o servidor na referência
async function consultarRank(
    requester,
    Mensagem,
    RespostaReferencia
) {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now(),
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

    timestampReferencia = Number(objeto.timestamp);

    return objeto.rank;
}

// Consulta a lista de servidores disponíveis
async function listarServidores(
    requester,
    Mensagem,
    RespostaReferencia
) {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now(),
        acao: "listar",
        contadorMensagem: contadorMensagens
    });

    await requester.send(
        Mensagem.encode(payload).finish()
    );

    const reply = await requester.receive();

    const resposta = RespostaReferencia.decode(reply);

    const objeto = RespostaReferencia.toObject(resposta, {
        defaults: true
    });

    contadorMensagens = Math.max(
        contadorMensagens,
        Number(objeto.contadorMensagem)
    );

    return objeto.servidores;
}

// Heartbeat e sincronização do relógio físico
async function heartbeat(
    requester,
    Mensagem,
    RespostaReferencia
) {

    contadorMensagens++;

    const payload = Mensagem.create({
        timestamp: Date.now(),
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

        timestampReferencia = Number(objeto.timestamp);

        console.log(
            "Heartbeat confirmado. Horário sincronizado:",
            new Date(timestampReferencia).toISOString()
        );

    } else {

        console.log("Erro no heartbeat:", objeto.resposta);
    }

    return objeto;
}

// Publicação de mensagens nos canais
async function publicar(pub, objetoOriginal, RespostaSub) {

    let contador = 1;

    while (contador <= 10) {

        contadorMensagens++;

        const payloadSub = RespostaSub.create({
            timestampEnvio: timestampReferencia,
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
