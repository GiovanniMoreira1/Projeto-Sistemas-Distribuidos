import { Reply } from "zeromq";
import fs from "fs/promises";

var usuarios = JSON.parse(await fs.readFile("usuarios.json", "utf-8"));


var canais = JSON.parse(await fs.readFile("canais.json", "utf-8"));

async function run() {
  const responder = new Reply();

  await responder.connect("tcp://broker:5560");
  console.log("Servidor conectado no broker...");
  var requisicao = "";
  for await (const [request] of responder) {

    requisicao = request.toString();
    requisicao = requisicao.split("|");
    
    if (requisicao[1] == "login") {
      
      if (usuarios.usuarios.includes(requisicao[2])) {
        await responder.send("400|Login realizado com sucesso!");
      } else {
        await responder.send("404|Nome de usuario incorreto!");
      }
      
    }
    
    else if (requisicao[1] == "listar") {
      await responder.send("400|Canais disponiveis: " + canais.canais.join(", "));
    }
    
    else if (requisicao[1] == "cadastrar") {
      if (!canais.canais.includes(requisicao[2])) {
        canais.canais.push(requisicao[2]);
        await fs.writeFile("canais.json", JSON.stringify(canais), "utf-8");
        await responder.send("400|Canal " + requisicao[2] + " cadastrado com sucesso!");
      } else {
        await responder.send("404|Canal já existe!");
      }
    }
  }
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});