package sd.projeto.client;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;
import sd.projeto.protos.Envelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Client
{
    public static void main(String[] args) throws Exception
    {
        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5559");

            ZMQ.Socket sub = context.createSocket(SocketType.SUB);
            sub.connect("tcp://proxy:5558");

            Random random = new Random();

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

            List<String> canais = listarCanais(socket);
            if (canais.size() < 5) {
                String novoCanal = "canal" + random.nextInt(10000);
                Envelope criado = enviar(socket, "criar_canal", novoCanal);
                System.out.println("[CLIENTE] Criar canal " + novoCanal + ": " + criado.getConteudo());
                canais = listarCanais(socket);
            }

            List<String> inscritos = new ArrayList<>();
            while (inscritos.size() < 3 && inscritos.size() < canais.size()) {
                String sorteado = canais.get(random.nextInt(canais.size()));
                if (!inscritos.contains(sorteado)) {
                    sub.subscribe(sorteado + "|");
                    inscritos.add(sorteado);
                    System.out.println("[CLIENTE] Inscrito no canal " + sorteado);
                }
            }
            Thread.sleep(1000);

            Thread receptor = new Thread(() -> {
                while (true) {
                    try {
                        String topico = sub.recvStr();
                        byte[] corpo = sub.recv(0);

                        Envelope pub = Envelope.parseFrom(corpo);
                        String[] partes = pub.getConteudo().split(",", 2);

                        System.out.println(
                            "[CLIENTE] Canal: " + partes[0] + " | Mensagem: " + partes[1]
                            + " | Envio: " + pub.getTimestamp()
                            + " | Recebimento: " + System.currentTimeMillis()
                        );
                    } catch (ZMQException e) {
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            receptor.setDaemon(true);
            receptor.start();

            while (true) {
                canais = listarCanais(socket);
                if (canais.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                String canal = canais.get(random.nextInt(canais.size()));
                for (int i = 1; i <= 10; i++) {
                    String mensagem = "msg" + random.nextInt(100000);
                    Envelope r = enviar(socket, "publicar", canal + "," + mensagem);
                    System.out.println("[CLIENTE] Publicou em " + canal + ": " + r.getConteudo());
                    Thread.sleep(1000);
                }
            }
        }
    }

    private static Envelope enviar(ZMQ.Socket socket, String acao, String conteudo) throws Exception
    {
        Envelope envelope = Envelope.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setAcao(acao)
            .setConteudo(conteudo)
            .build();
        socket.send(envelope.toByteArray(), 0);

        byte[] reply = socket.recv(0);
        return Envelope.parseFrom(reply);
    }

    private static List<String> listarCanais(ZMQ.Socket socket) throws Exception
    {
        Envelope r = enviar(socket, "listar_canais", "");
        List<String> canais = new ArrayList<>();
        if (!r.getConteudo().isBlank()) {
            canais.addAll(Arrays.asList(r.getConteudo().split(",")));
        }
        return canais;
    }
}
