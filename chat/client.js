import { Request } from "zeromq";

import protobuf from "protobufjs";

async function run() {
  var usuarios = ["usuario1", "usuario2", "usuario3","teste", "usuario4", "usuario5"]
  const root = await protobuf.load("client.proto");

  const Mensagem = root.lookupType("Mensagem");
  const Resposta = root.lookupType("Resposta");

  console.log("Cliente conectado no broker...");
  const requester = new Request();

  requester.connect("tcp://broker:5559");

  var indiceUsuario = 0;
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
  console.log("Canais disponiveis: " + canais.resposta);

  var teste2 = await cadastrarCanal(requester, Mensagem, Resposta, "canal14");
  if (teste2.status == "200") {
    console.log(teste2.resposta);
  }
  else{
    console.log("Erro:" + teste2.resposta);
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

  const requisicao = Resposta.decode(buffer); // também adicionei o "const" que faltava aqui

  const objetoOriginal = Resposta.toObject(requisicao, {
    longs: String,
    enums: String,
    bytes: String,
    defaults: true,
  });

  return objetoOriginal;
}
