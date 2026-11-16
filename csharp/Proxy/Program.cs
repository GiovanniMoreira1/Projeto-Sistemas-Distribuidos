using NetMQ;
using NetMQ.Sockets;

using var xsub = new XSubscriberSocket();
using var xpub = new XPublisherSocket();

xsub.Bind("tcp://*:5557");
xpub.Bind("tcp://*:5558");

Console.WriteLine("Proxy Pub/Sub rodando (XSUB 5557 / XPUB 5558)");

var proxy = new Proxy(xsub, xpub);
proxy.Start();
