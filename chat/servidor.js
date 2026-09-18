import { Reply } from "zeromq";
import fs from "fs/promises";

import protobuf from "protobufjs";

var usuarios = JSON.parse(await fs.readFile("usuarios.json", "utf-8"));

var canais = JSON.parse(await fs.readFile("canais.json", "utf-8"));

var logins = JSON.parse(await fs.readFile("login.json", "utf-8"));

async function run() {

  const root = await protobuf.load("client.proto");

  const Mensagem = root.lookupType("Mensagem");
  const Resposta = root.lookupType("Resposta");

  const responder = new Reply();

  await responder.connect("tcp://broker:5560");
  console.log("Servidor conectado no broker...");
  var requisicao = "";
  for await (const [request] of responder) {

    requisicao = Mensagem.decode(request);

    const objetoOriginal = Mensagem.toObject(requisicao, {
    longs: String,
    enums: String,
    bytes: String,
    defaults: true,
  });
    
    if (objetoOriginal.acao == "login") {
      
      if (usuarios.usuarios.includes(objetoOriginal.conteudo)) {
        const payload = Resposta.create({ status: "200", resposta: "Login realizado com sucesso!" });
        logins.push({ usuario: objetoOriginal.conteudo, timestamp: objetoOriginal.timestamp, status: "200" });
        await fs.writeFile("login.json", JSON.stringify(logins), "utf-8");
        await responder.send(Resposta.encode(payload).finish());
      } 
      else {
        const payload = Resposta.create({ status: "404", resposta: "Nome de usuario incorreto!" });
        logins.push({ usuario: objetoOriginal.conteudo, timestamp: objetoOriginal.timestamp, status: "404" });
        await fs.writeFile("login.json", JSON.stringify(logins), "utf-8");
        await responder.send(Resposta.encode(payload).finish());
      }
      
    }
    
    else if (objetoOriginal.acao == "listar") {
      const payload = Resposta.create({ status: "200", resposta: "Canais disponiveis: " + canais.canais.join(", ") });
      await responder.send(Resposta.encode(payload).finish());
    }
    
    else if (objetoOriginal.acao == "cadastrar") {
      if (!canais.canais.includes(objetoOriginal.conteudo)) {
        canais.canais.push(objetoOriginal.conteudo);
        await fs.writeFile("canais.json", JSON.stringify(canais), "utf-8");
        const payload = Resposta.create({ status: "200", resposta: "Canal " + objetoOriginal.conteudo + " cadastrado com sucesso!" });
        await responder.send(Resposta.encode(payload).finish());
      } else {
        const payload = Resposta.create({ status: "404", resposta: "Canal já existe!" });
        await responder.send(Resposta.encode(payload).finish());
      }
    }
  }
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});