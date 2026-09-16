package sd.projeto.broker;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class Broker {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {

            ZMQ.Socket frontend = context.createSocket(SocketType.ROUTER);
            frontend.bind("tcp://*:5559");

            ZMQ.Socket backend = context.createSocket(SocketType.DEALER);
            backend.bind("tcp://*:5560");

            ZMQ.Poller poller = context.createPoller(2);
            poller.register(frontend, ZMQ.Poller.POLLIN);
            poller.register(backend, ZMQ.Poller.POLLIN);

            int clientMessages = 0;
            int serverMessages = 0;

            System.out.println("[BROKER] Rodando, aguardando conexoes...");

            while (!Thread.currentThread().isInterrupted()) {
                poller.poll();

                if (poller.pollin(0)) {
                    clientMessages++;
                    repassarMensagem(frontend, backend);
                    System.out.println("[BROKER] Mensagens de clientes: " + clientMessages);
                }

                if (poller.pollin(1)) {
                    serverMessages++;
                    repassarMensagem(backend, frontend);
                    System.out.println("[BROKER] Mensagens de servidores: " + serverMessages);
                }
            }
        }
    }

    private static void repassarMensagem(ZMQ.Socket origem, ZMQ.Socket destino) {
        while (true) {
            byte[] frame = origem.recv(0);
            boolean more = origem.hasReceiveMore();
            destino.send(frame, more ? ZMQ.SNDMORE : 0);
            if (!more) {
                break;
            }
        }
    }
}
