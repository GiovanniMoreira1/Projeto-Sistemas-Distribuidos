package sd.projeto.referencia;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sd.projeto.protos.Mensagem;
import sd.projeto.protos.RespostaReferencia;
import sd.projeto.protos.ServidorInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Servico de referencia em Java (contrato comum: consultar_rank, listar_servidores, heartbeat).
 * O docker-compose do projeto unificado usa a referencia em Python; esta fica como alternativa.
 */
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
            int relogio = 0;

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

                Mensagem pedido = Mensagem.parseFrom(socket.recv(0));
                relogio = Math.max(relogio, pedido.getContadorMensagem());
                String nome = pedido.getConteudo();

                RespostaReferencia.Builder resposta = RespostaReferencia.newBuilder();
                switch (pedido.getAcao()) {
                    case "consultar_rank" -> {
                        if (nome.isBlank()) {
                            resposta.setStatus("400").setResposta("Nome vazio!");
                        } else {
                            if (!ranks.containsKey(nome)) {
                                ranks.put(nome, proximoRank++);
                                System.out.println("[REFERENCIA] Servidor cadastrado: " + nome + " | rank " + ranks.get(nome));
                            }
                            ultimoHeartbeat.put(nome, System.currentTimeMillis());
                            resposta.setStatus("200").setResposta("Servidor cadastrado com sucesso!").setRank(ranks.get(nome));
                        }
                    }
                    case "listar_servidores" -> {
                        resposta.setStatus("200").setResposta("Servidores disponíveis listados!");
                        for (Map.Entry<String, Integer> e : ranks.entrySet()) {
                            resposta.addServidores(ServidorInfo.newBuilder().setNome(e.getKey()).setRank(e.getValue()));
                        }
                    }
                    case "heartbeat" -> {
                        if (ranks.containsKey(nome)) {
                            ultimoHeartbeat.put(nome, System.currentTimeMillis());
                            resposta.setStatus("200").setResposta("Heartbeat recebido!").setRank(ranks.get(nome));
                            System.out.println("[REFERENCIA] Heartbeat de " + nome);
                        } else {
                            resposta.setStatus("404").setResposta("Servidor não cadastrado!");
                        }
                    }
                    default -> resposta.setStatus("400").setResposta("Ação desconhecida!");
                }

                // heartbeat nao devolve a hora: quem acerta o relogio e o coordenador (Parte 4)
                resposta.setContadorMensagem(++relogio);
                socket.send(resposta.build().toByteArray(), 0);
            }
        }
    }
}
