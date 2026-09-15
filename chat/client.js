import { Request } from "zeromq";

async function run() {
  console.log("Cliente conectado no broker...");
  const requester = new Request();

  requester.connect("tcp://broker:5559");

  var reply = await login(requester);
  console.log(reply);

  while (reply[0] != "400") {
    console.log("Nome de usuario incorreto!");
    reply = await login(requester);
  }
  
  var canais = await listarCanais(requester);
  console.log("Canais disponiveis: " + canais[1]);

  var teste2 = await cadastrarCanal(requester, "canal12");
  console.log("Resposta do servidor: " + teste2[1]);


  await requester.close();
  process.exit(0);
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});

async function login(requester) {
  var timestamp = Date.now();
  var mensagemLogin = timestamp + "|login|teste";

  await requester.send(mensagemLogin);

  const [reply] = await requester.receive();

  return reply.toString().split("|");
}

async function listarCanais(requester) {
  var timestamp = Date.now();
  var mensagemListar = timestamp + "|listar";
  await requester.send(mensagemListar);

  const [reply] = await requester.receive();

  return reply.toString().split("|");
}

async function cadastrarCanal(requester, canal) {
  var timestamp = Date.now();
  var mensagemCadastrar = timestamp + "|cadastrar|" + canal;
  await requester.send(mensagemCadastrar);
  const [reply] = await requester.receive();

  return reply.toString().split("|");
}