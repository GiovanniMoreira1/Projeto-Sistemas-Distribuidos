import { Request, Subscriber } from "zeromq";

import protobuf from "protobufjs";


async function run() {
  var usuarios = ["usuario1", "usuario2", "usuario3","teste", "usuario4", "usuario5"]
  const root = await protobuf.load("client.proto");

  const Mensagem = root.lookupType("Mensagem");
  const Resposta = root.lookupType("Resposta");
  const RespostaSub = root.lookupType("RespostaSub");

  console.log("Cliente conectado no broker...");
  
  const requester = new Request();
  const subscriber = new Subscriber();
  
  subscriber.connect("tcp://proxy:5558");
  requester.connect("tcp://broker:5559");

  var indiceUsuario = 0;
  var indiceCanal = 0;

  var reply = await login(requester, Mensagem, Resposta, usuarios[indiceUsuario]);

  while (reply.status != "200") {
    console.log("Nome de usuario incorreto!");

    indiceUsuario++;

    if (indiceUsuario >= usuarios.length) {
      console.log("Nenhum usuario da lista funcionou.");
      requester.close();
      process.exit(1);
    }
    
    reply = await login(requester, Mensagem, Resposta, usuarios[indiceUsuario]);
  }
  
  var canais = await listarCanais(requester, Mensagem, Resposta);
  indiceCanal = canais.canais.length;
  indiceCanal += 1;
  if (canais.canais.length < 5) {
    console.log("Erro: quantidade de canais insuficiente. É necessário ter pelo menos 5 canais cadastrados.");
    indiceCanal += 1;
    await cadastrarCanal(requester, Mensagem, Resposta, `canal${indiceCanal}`);
  }
  console.log("Canais disponiveis: " + canais.canais.join(", "));

  let valores = [];
  for (var i = 0; i < 3; i++) {
    const aleatorio = canais.canais[Math.floor(Math.random() * canais.canais.length)];
    
    if (valores.includes(aleatorio)) {
      i--;
      continue;
    }else{
      valores.push(aleatorio);
      inscreverCanal(aleatorio, subscriber);
    }
  }

  receberPublicacoes(subscriber, RespostaSub).catch(console.error);

  while (true) {
    const aleatorio = canais.canais[Math.floor(Math.random() * canais.canais.length)];
  
    await publicarMensagem(requester, Mensagem, Resposta, aleatorio, `Mensagem de teste para o canal ${aleatorio}`);
  }


  requester.close();
  process.exit(0);
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});

async function login(requester, Mensagem, Resposta, usuario) {
  var timestamp = Date.now();

  const payload = Mensagem.create({ timestamp: timestamp, acao: "login", conteudo: usuario });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

async function listarCanais(requester, Mensagem, Resposta) {
  var timestamp = Date.now();

  const payload = Mensagem.create({ timestamp: timestamp, acao: "listar" });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

async function cadastrarCanal(requester, Mensagem, Resposta, canal) {
  var timestamp = Date.now();

  const payload = Mensagem.create({ timestamp: timestamp, acao: "cadastrar", conteudo: canal });

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
    defaults: true,
  });

  return objetoOriginal;
}

async function publicarMensagem(requester, Mensagem, Resposta, canal, mensagem) {
  var timestamp = Date.now();

  const payload = Mensagem.create({ timestamp: timestamp, acao: "publicar", canal: canal, conteudo: mensagem });

  const bufferBinario = Mensagem.encode(payload).finish();

  await requester.send(bufferBinario);

  const reply = await requester.receive();

  return deserializarMensagem(Resposta, reply);
}

function inscreverCanal(canal, subscriber) {
  subscriber.subscribe(canal + "|");
  console.log(`cliente inscrito no ${canal}`);
}


async function receberPublicacoes(subscriber, RespostaSub) {
  for await (const [topic, msg] of subscriber) {
    const dados = RespostaSub.toObject(RespostaSub.decode(msg), { longs: String, defaults: true });
    console.log({
      canal: dados.canal,
      mensagem: dados.conteudo,
      timestampEnvio: dados.timestampEnvio,
      timestampRecebimento: Date.now(),
    });
  }
}
