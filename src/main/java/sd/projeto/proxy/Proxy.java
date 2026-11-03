package sd.projeto.proxy;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class Proxy {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {

            ZMQ.Socket frontend = context.createSocket(SocketType.XSUB);
            frontend.bind("tcp://*:5557");

            ZMQ.Socket backend = context.createSocket(SocketType.XPUB);
            backend.bind("tcp://*:5558");

            System.out.println("[PROXY] Rodando ...");

            ZMQ.proxy(frontend, backend, null);
        }
    }
}