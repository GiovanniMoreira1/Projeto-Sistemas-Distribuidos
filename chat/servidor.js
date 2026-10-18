import { Reply, Publisher } from "zeromq";
import fs from "fs/promises";

import protobuf from "protobufjs";

var usuarios = JSON.parse(await fs.readFile("usuarios.json", "utf-8"));

var canais = JSON.parse(await fs.readFile("canais.json", "utf-8"));

var logins = JSON.parse(await fs.readFile("login.json", "utf-8"));

var publicacoes = JSON.parse(await fs.readFile("publicacoes.json", "utf-8"));

var mensagens = JSON.parse(await fs.readFile("mensagens.json", "utf-8"));

async function run() {

  const root = await protobuf.load("client.proto");

  const Mensagem = root.lookupType("Mensagem");
  const Resposta = root.lookupType("Resposta");
  const RespostaSub = root.lookupType("RespostaSub");

  const responder = new Reply();
  var pub = new Publisher();

  pub.connect("tcp://proxy:5557")

  responder.connect("tcp://broker:5560");
  console.log("Servidor conectado no broker...");
  var requisicao = "";
  for await (const [request] of responder) {

    requisicao = Mensagem.decode(request);

    const objetoOriginal = Mensagem.toObject(requisicao, {
    longs: String,
    enums: String,
    bytes: String,
    bytes: String,
    defaults: true,
  });

    mensagens.push({usuario: "cliente", acao: objetoOriginal.acao, conteudo: objetoOriginal.conteudo, canal: objetoOriginal.canal, timestamp: objetoOriginal.timestamp});
    await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
    
    if (objetoOriginal.acao == "login") {
      
      if (usuarios.usuarios.includes(objetoOriginal.conteudo)) {
        const payload = Resposta.create({ status: "200", resposta: "Login realizado com sucesso!" });

        logins.push({ usuario: objetoOriginal.conteudo, timestamp: objetoOriginal.timestamp, status: "200" });

        await fs.writeFile("login.json", JSON.stringify(logins), "utf-8");
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "200", conteudo: "Login realizado com sucesso!"});

        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
      } 
      else {
        const payload = Resposta.create({ status: "404", resposta: "Nome de usuario incorreto!" });
        logins.push({ usuario: objetoOriginal.conteudo, timestamp: objetoOriginal.timestamp, status: "404" });
        await fs.writeFile("login.json", JSON.stringify(logins), "utf-8");
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "404", conteudo: "Nome de usuario incorreto!"});

        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
      }
      
    }
    
    else if (objetoOriginal.acao == "listar") {
      const payload = Resposta.create({ status: "200", resposta: "Canais listados", canais: canais.canais });
      await responder.send(Resposta.encode(payload).finish());

      mensagens.push({usuario: "servidor", status: "200", conteudo: "Canais listados", canais: canais.canais});

      await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
    }
    
    else if (objetoOriginal.acao == "cadastrar") {
      if (!canais.canais.includes(objetoOriginal.conteudo)) {
        canais.canais.push(objetoOriginal.conteudo);
        await fs.writeFile("canais.json", JSON.stringify(canais), "utf-8");
        const payload = Resposta.create({ status: "200", resposta: "Canal " + objetoOriginal.conteudo + " cadastrado com sucesso!" });
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "200", conteudo: "Canal " + objetoOriginal.conteudo + " cadastrado com sucesso!"});
        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");

      } else {
        const payload = Resposta.create({ status: "404", resposta: "Canal já existe!" });
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "404", conteudo: "Canal já existe!"});
        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
      }
    }

    else if (objetoOriginal.acao == "publicar") {
      try{
        await publicar(pub, objetoOriginal, RespostaSub);
        
        const payload = Resposta.create({ status: "200", resposta: "Mensagem publicada no canal " + objetoOriginal.canal + " com sucesso!" });
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "200", conteudo: "Mensagem publicada no canal " + objetoOriginal.canal + " com sucesso!"});
        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");

      }catch (error) {
        const payload = Resposta.create({ status: "500", resposta: "Erro ao publicar mensagem!" });
        await responder.send(Resposta.encode(payload).finish());

        mensagens.push({usuario: "servidor", status: "500", conteudo: "Erro ao publicar mensagem!"});
        await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");
      }
    }
  }
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});

async function publicar(pub, objetoOriginal, RespostaSub) {
  let contador = 1;
  while(contador <= 10){
    const payloadSub = RespostaSub.create({ timestampEnvio: Date.now(), canal: objetoOriginal.canal, conteudo: objetoOriginal.conteudo });

    await pub.send([objetoOriginal.canal + "|", RespostaSub.encode(payloadSub).finish()]);

    publicacoes.push({conteudo: objetoOriginal.conteudo, canal: objetoOriginal.canal});
    await fs.writeFile("mensagens.json", JSON.stringify(mensagens), "utf-8");

    console.log("Mensagem publicada: " + objetoOriginal.conteudo);

    contador++;

    await new Promise(resolve => {
      setTimeout(resolve, 100)
    })
  }
}