import { Router, Dealer } from "zeromq";

async function run() {
  const client = new Router();
  const server = new Dealer();

  await client.bind("tcp://*:5559");
  await server.bind("tcp://*:5560");

  async function handleClient() {
    for await (const frames of client) {
      await server.send(frames);
    }
  }

  async function handleServer() {
    for await (const frames of server) {
      await client.send(frames);
    }
  }

  await Promise.all([handleClient(), handleServer()]);
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});