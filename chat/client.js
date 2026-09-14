import { Request } from "zeromq";

async function run() {
  console.log("Connecting to server...");
  const requester = new Request();

  requester.connect("tcp://broker:5559");

  await requester.send("Hello");

  const [reply] = await requester.receive();
  console.log("Received reply:", reply.toString());

  requester.close();
  process.exit(0);
}

run();

process.on("SIGINT", function () {
  process.exit(0);
});