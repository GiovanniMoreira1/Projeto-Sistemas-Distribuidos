import { Request, Subscriber } from "zeromq";
import path from "path";
import { fileURLToPath } from "url";
import protobuf from "protobufjs";

const ARQUIVO_PROTO = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "proto", "client.proto");

const MIN_CANAIS = 5;
const MIN_INSCRICOES = 3;
const MENSAGENS_POR_RODADA = 10;
const INTERVALO_MS = 1000;

var contadorMensagem = 0;

const inscritos = new Set();

async function run() {
  var usuarios = ["usuario1", "usuario2", "usuario3", "teste", "teste2", "teste3", "teste4", "teste5"];
  const root = await protobuf.load(ARQUIVO_PROTO);

  const Mensagem = root.lookupType("Mensagem");
  const Resposta = root.lookupType("Resposta");
  const RespostaSub = root.lookupType("RespostaSub");

  const requester = new Request();
  const subscriber = new Subscriber();

  subscriber.connect("tcp://proxy:5558");
  requester.connect("tcp://broker:5559");

  console.log("Cliente conectado no broker e no proxy...");

  var indiceUsuario = 0;

  var reply = await login(requester, Mensagem, Resposta, usuarios[indiceUsuario]);

  while (reply.status != "200") {
    console.log(`[${usuarios[indiceUsuario]}] Erro ${reply.status}: ${reply.resposta}`);

    indiceUsuario++;

    if (indiceUsuario >= usuarios.length) {
      console.log("Nenhum usuario da lista funcionou.");
      requester.close();
      process.exit(1);
    }

    reply = await login(requester, Mensagem, Resposta, usuarios[indiceUsuario]);
  }

  const usuario = usuarios[indiceUsuario];
  console.log(`[${usuario}] ${reply.resposta}`);

  receberPublicacoes(subscriber, RespostaSub).catch(console.error);

  while (true) {
    var canais = (await listarCanais(requester, Mensagem, Resposta)).canais;

    if (canais.length < MIN_CANAIS) {
      const novo = `canal${Math.floor(1000 + Math.random() * 9000)}`;
      const resposta = await cadastrarCanal(requester, Mensagem, Resposta, novo);
      console.log(`[${usuario}] criar ${novo}: ${resposta.status} ${resposta.resposta}`);
      canais = (await listarCanais(requester, Mensagem, Resposta)).canais;
    }

    if (inscritos.size < MIN_INSCRICOES) {
      const disponiveis = canais.filter(c => !inscritos.has(c));
      if (disponiveis.length > 0) {
        inscreverCanal(disponiveis[Math.floor(Math.random() * disponiveis.length)], subscriber, usuario);
      }
    }

    if (canais.length === 0) {
      await esperar(INTERVALO_MS);
      continue;
    }

    const canal = canais[Math.floor(Math.random() * canais.length)];
    for (let i = 1; i <= MENSAGENS_POR_RODADA; i++) {
      const texto = `${usuario}: mensagem de teste para o canal ${canal} (${i}/${MENSAGENS_POR_RODADA})`;
      const resposta = await publicarMensagem(requester, Mensagem, Resposta, canal, texto);
      if (resposta.status != "200") {
        console.log(`[${usuario}] erro ao publicar em ${canal}: ${resposta.status} ${resposta.resposta}`);
      }
      await esperar(INTERVALO_MS);
    }
  }
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});

function esperar(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function login(requester, Mensagem, Resposta, usuario) {
  var timestamp = Date.now();
  contadorMensagem++;
  const payload = Mensagem.create({ timestamp: timestamp, acao: "login", conteudo: usuario, contadorMensagem: contadorMensagem });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

async function listarCanais(requester, Mensagem, Resposta) {
  var timestamp = Date.now();
  contadorMensagem++;
  const payload = Mensagem.create({ timestamp: timestamp, acao: "listar", contadorMensagem: contadorMensagem });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

async function cadastrarCanal(requester, Mensagem, Resposta, canal) {
  var timestamp = Date.now();
  contadorMensagem++;
  const payload = Mensagem.create({ timestamp: timestamp, acao: "cadastrar", conteudo: canal, contadorMensagem: contadorMensagem });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

async function deserializarMensagem(Resposta, request) {
  const buffer = Array.isArray(request) ? request[0] : request;

  const requisicao = Resposta.decode(buffer);

  const objetoOriginal = Resposta.toObject(requisicao, {
    longs: String,
    enums: String,
    bytes: String,
    int64: Number,
    defaults: true,
  });

  contadorMensagem = Math.max(contadorMensagem, Number(objetoOriginal.contadorMensagem));

  return objetoOriginal;
}

async function publicarMensagem(requester, Mensagem, Resposta, canal, mensagem) {
  var timestamp = Date.now();
  contadorMensagem++;
  const payload = Mensagem.create({ timestamp: timestamp, acao: "publicar", canal: canal, conteudo: mensagem, contadorMensagem: contadorMensagem });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

function inscreverCanal(canal, subscriber, usuario) {
  subscriber.subscribe(canal + "|");
  inscritos.add(canal);
  console.log(`[${usuario}] inscrito no ${canal}`);
}

async function receberPublicacoes(subscriber, RespostaSub) {
  for await (const [topic, msg] of subscriber) {
    const dados = RespostaSub.toObject(RespostaSub.decode(msg), { longs: String, defaults: true });
    console.log({
      canal: dados.canal,
      mensagem: dados.conteudo,
      timestampEnvio: dados.timestampEnvio,
      timestampRecebimento: Date.now(),
      contadorMensagem: dados.contadorMensagem
    });

    contadorMensagem = Math.max(contadorMensagem, Number(dados.contadorMensagem));
  }
}
