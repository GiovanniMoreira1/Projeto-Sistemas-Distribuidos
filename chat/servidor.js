import { Reply } from "zeromq";

async function run() {
  const responder = new Reply();

  await responder.connect("tcp://broker:5560");
  console.log("Servidor conectado no broker...");

  for await (const [request] of responder) {
    console.log("Received request: [", request.toString(), "]");

    await responder.send("World");
  }
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});