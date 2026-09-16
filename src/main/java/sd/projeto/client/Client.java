package sd.projeto.client;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import sd.projeto.protos.Envelope;

public class Client
{
    public static void main(String[] args) throws Exception
    {
        try (ZContext context = new ZContext()) {
            
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5559");

            while (true) {

                
                Envelope envelope = Envelope.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setAcao("login")
                    .setConteudo("Felipe")
                    .build();
                byte[] data = envelope.toByteArray();
                socket.send(data, 0);

                
                byte[] reply = socket.recv(0);

                Envelope resposta = Envelope.parseFrom(reply);

                if (resposta.getConteudo().startsWith("sucesso")){
                    System.out.println("[CLIENTE] Login realizado com sucesso");
                    break;
                }else {
                    System.out.println("[CLIENTE] Erro no login, repetindo o login...");
                }
            }

            Envelope canal = Envelope.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setAcao("criar_canal")
                .setConteudo("teste")
                .build();
            byte[] data_canal = canal.toByteArray();
            socket.send(data_canal, 0);

            byte[] reply_canal = socket.recv(0);

            System.out.println(
                    "[CLIENTE] Recebido: [" + Envelope.parseFrom(reply_canal) + "]"
            );

            Envelope listar_canais = Envelope.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setAcao("listar_canais")
                .setConteudo("")
                .build();

            byte[] data_lista = listar_canais.toByteArray();
            socket.send(data_lista, 0);

            byte[] reply_lista_canal = socket.recv(0);

            System.out.println(
                    "[CLIENTE] Recebido: [" + Envelope.parseFrom(reply_lista_canal) + "]"
            );
        }
    }
}