package sd.projeto.referencia;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import sd.projeto.protos.Envelope;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Referencia {

    private static final long TEMPO_LIMITE = 30000;

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:5556");

            ZMQ.Poller poller = context.createPoller(1);
            poller.register(socket, ZMQ.Poller.POLLIN);

            Map<String, Integer> ranks = new LinkedHashMap<>();
            Map<String, Long> ultimoHeartbeat = new LinkedHashMap<>();
            int proximoRank = 1;
            long relogio = 0;

            System.out.println("[REFERENCIA] Rodando, aguardando servidores...");

            while (!Thread.currentThread().isInterrupted()) {
                poller.poll(1000);

                // tira da lista quem ficou muito tempo sem heartbeat
                long agora = System.currentTimeMillis();
                List<String> inativos = new ArrayList<>();
                for (String nome : ultimoHeartbeat.keySet()) {
                    if (agora - ultimoHeartbeat.get(nome) > TEMPO_LIMITE) {
                        inativos.add(nome);
                    }
                }
                for (String nome : inativos) {
                    ranks.remove(nome);
                    ultimoHeartbeat.remove(nome);
                    System.out.println("[REFERENCIA] Servidor removido por falta de heartbeat: " + nome);
                }

                if (!poller.pollin(0)) {
                    continue;
                }

                Envelope envelope = Envelope.parseFrom(socket.recv(0));
                relogio = Math.max(relogio, envelope.getContador());

                String conteudo;
                if (envelope.getAcao().equals("rank")) {
                    String nome = envelope.getConteudo();
                    if (nome.isBlank()) {
                        conteudo = "erro,nome vazio";
                    } else {
                        if (!ranks.containsKey(nome)) {
                            ranks.put(nome, proximoRank);
                            proximoRank++;
                            System.out.println("[REFERENCIA] Servidor cadastrado: " + nome + " | rank " + ranks.get(nome));
                        }
                        ultimoHeartbeat.put(nome, System.currentTimeMillis());
                        conteudo = "sucesso," + ranks.get(nome);
                    }
                } else if (envelope.getAcao().equals("list")) {
                    List<String> itens = new ArrayList<>();
                    for (Map.Entry<String, Integer> e : ranks.entrySet()) {
                        itens.add(e.getKey() + ":" + e.getValue());
                    }
                    conteudo = "sucesso," + String.join(";", itens);
                } else if (envelope.getAcao().equals("heartbeat")) {
                    String nome = envelope.getConteudo();
                    if (ranks.containsKey(nome)) {
                        ultimoHeartbeat.put(nome, System.currentTimeMillis());
                        conteudo = "sucesso,ok";
                        System.out.println("[REFERENCIA] Heartbeat de " + nome);
                    } else {
                        conteudo = "erro,servidor nao cadastrado";
                    }
                } else {
                    conteudo = "erro,acao desconhecida";
                }

                relogio++;
                // o timestamp da resposta e a "hora correta" que os servidores usam pra sincronizar
                Envelope resposta = Envelope.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setAcao("resposta_" + envelope.getAcao())
                    .setConteudo(conteudo)
                    .setContador(relogio)
                    .build();
                socket.send(resposta.toByteArray(), 0);
            }
        }
    }
}
