package sd.projeto.client;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sd.projeto.protos.Mensagem;
import sd.projeto.protos.Resposta;
import sd.projeto.protos.RespostaSub;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Client
{
    private static final int MIN_CANAIS = 5;
    private static final int MIN_INSCRICOES = 3;
    private static final int MENSAGENS_POR_RODADA = 10;
    private static final long INTERVALO_MS = 1000;

    private static final List<String> USUARIOS = List.of(
        "Felipe", "teste", "teste2", "teste3", "teste4", "teste5", "teste6", "teste7", "teste8", "teste9", "teste10");

    private static int relogio = 0;
    private static ZMQ.Socket requester;
    private static ZMQ.Socket sub;
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception
    {
        try (ZContext context = new ZContext()) {
            requester = context.createSocket(SocketType.REQ);
            requester.connect("tcp://broker:5559");
            sub = context.createSocket(SocketType.SUB);
            sub.connect("tcp://proxy:5558");
            System.out.println("[CLIENTE] Conectado no broker e no proxy...");

            List<String> candidatos = new ArrayList<>(USUARIOS.subList(1, USUARIOS.size()));
            Collections.shuffle(candidatos, random);
            candidatos.add(0, USUARIOS.get(0));

            String usuario = null;
            for (String candidato : candidatos) {
                Resposta r = requisitar("login", "", candidato);
                if (r.getStatus().equals("200")) {
                    usuario = candidato;
                    System.out.println("[" + usuario + "] " + r.getResposta());
                    break;
                }
                System.out.println("[" + candidato + "] Erro " + r.getStatus() + ": " + r.getResposta()
                    + " - repetindo o login");
            }
            if (usuario == null) {
                System.out.println("[CLIENTE] Nenhum usuario da lista funcionou.");
                System.exit(1);
            }

            Set<String> inscritos = new HashSet<>();
            while (true) {
                List<String> canais = requisitar("listar", "", "").getCanaisList();

                if (canais.size() < MIN_CANAIS) {
                    String novo = "canal" + (1000 + random.nextInt(9000));
                    Resposta r = requisitar("cadastrar", "", novo);
                    System.out.println("[" + usuario + "] criar " + novo + ": " + r.getStatus() + " " + r.getResposta());
                    canais = requisitar("listar", "", "").getCanaisList();
                }

                if (inscritos.size() < MIN_INSCRICOES) {
                    List<String> disponiveis = new ArrayList<>();
                    for (String c : canais) {
                        if (!inscritos.contains(c)) {
                            disponiveis.add(c);
                        }
                    }
                    if (!disponiveis.isEmpty()) {
                        String escolhido = disponiveis.get(random.nextInt(disponiveis.size()));
                        sub.subscribe((escolhido + "|").getBytes(StandardCharsets.UTF_8));
                        inscritos.add(escolhido);
                        System.out.println("[" + usuario + "] inscrito no " + escolhido);
                    }
                }

                if (canais.isEmpty()) {
                    esperar(INTERVALO_MS);
                    continue;
                }

                String canal = canais.get(random.nextInt(canais.size()));
                for (int i = 1; i <= MENSAGENS_POR_RODADA; i++) {
                    String texto = usuario + ": msg" + random.nextInt(100000) + " (" + i + "/" + MENSAGENS_POR_RODADA + ")";
                    Resposta r = requisitar("publicar", canal, texto);
                    if (!r.getStatus().equals("200")) {
                        System.out.println("[" + usuario + "] erro ao publicar em " + canal + ": "
                            + r.getStatus() + " " + r.getResposta());
                    }
                    esperar(INTERVALO_MS);
                }
            }
        }
    }

    private static Resposta requisitar(String acao, String canal, String conteudo) throws Exception
    {
        Mensagem mensagem = Mensagem.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setAcao(acao)
            .setCanal(canal)
            .setConteudo(conteudo)
            .setContadorMensagem(++relogio)
            .build();
        requester.send(mensagem.toByteArray(), 0);

        Resposta resposta = Resposta.parseFrom(requester.recv(0));
        relogio = Math.max(relogio, resposta.getContadorMensagem());
        return resposta;
    }

    private static void esperar(long ms) throws Exception
    {
        long fim = System.currentTimeMillis() + ms;
        long restante;
        while ((restante = fim - System.currentTimeMillis()) > 0) {
            sub.setReceiveTimeOut((int) restante);
            String topico = sub.recvStr();
            if (topico == null) {
                continue;
            }
            RespostaSub pub = RespostaSub.parseFrom(sub.recv(0));
            relogio = Math.max(relogio, pub.getContadorMensagem());
            System.out.println("[CLIENTE] Canal: " + pub.getCanal() + " | Mensagem: " + pub.getConteudo()
                + " | Envio: " + (long) pub.getTimestampEnvio()
                + " | Recebimento: " + System.currentTimeMillis()
                + " | Relogio: " + pub.getContadorMensagem());
        }
    }
}
