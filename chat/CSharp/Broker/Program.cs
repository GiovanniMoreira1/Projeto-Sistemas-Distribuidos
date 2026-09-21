using NetMQ;
using NetMQ.Sockets;

using var clientes = new RouterSocket();
using var servidores = new DealerSocket();

clientes.Bind("tcp://*:5559");
servidores.Bind("tcp://*:5560");

Console.WriteLine("Broker escutando em 5559 (clientes) e 5560 (servidores)...");

var proxy = new Proxy(clientes, servidores);
proxy.Start();
