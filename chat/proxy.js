import { Proxy, XPublisher, XSubscriber } from "zeromq";

async function run() {
  const proxy = new Proxy(new XSubscriber(), new XPublisher());

  await proxy.frontEnd.bind("tcp://*:5557");
  await proxy.backEnd.bind("tcp://*:5558");

  console.log("Proxy Pub/Sub rodando (XSUB 5557 / XPUB 5558)");
  await proxy.run();
}

run();

process.on("SIGINT", () => process.exit(0));