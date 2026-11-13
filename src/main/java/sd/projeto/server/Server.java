package sd.projeto.server;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import sd.projeto.protos.Envelope;
import java.util.List;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.net.InetAddress;

public class Server
{
    private static long relogio = 0;
    private static long diferencaRelogio = 0;

    public static void main(String[] args) throws Exception
    {
        try (ZContext context = new ZContext()) {
            
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5560");

            ZMQ.Socket pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");

            ZMQ.Socket referencia = context.createSocket(SocketType.REQ);
            referencia.connect("tcp://referencia:5556");

            String nome = InetAddress.getLocalHost().getHostName();
            Envelope respostaRank = enviarReferencia(referencia, "rank", nome);
            int rank = Integer.parseInt(respostaRank.getConteudo().split(",")[1]);
            System.out.println("[SERVER] " + nome + " cadastrado na referencia com rank " + rank);

            Envelope respostaLista = enviarReferencia(referencia, "list", "");
            System.out.println("[SERVER] Servidores na referencia: " + respostaLista.getConteudo());

            int mensagensRecebidas = 0;

            List<String> canais = new ArrayList<>();

            Path arquivoLogins = Paths.get("login.txt");
            Path arquivosCanais = Paths.get("canais.txt");
            Path arquivoPublicacoes = Paths.get("publicacoes.txt");
            Path arquivoMensagens = Paths.get("mensagens.txt");

            if (Files.exists(arquivosCanais)){
                String conteudoArquivo = Files.readString(arquivosCanais);
                if(!conteudoArquivo.isBlank()){
                    canais.addAll(Arrays.asList(conteudoArquivo.split(",")));
                }
            }

            while (!Thread.currentThread().isInterrupted()) {
                
                byte[] reply = socket.recv(0);

                
                Envelope envelope = Envelope.parseFrom(reply);
                relogio = Math.max(relogio, envelope.getContador());

                mensagensRecebidas++;
                if (mensagensRecebidas % 10 == 0) {
                    long antes = System.currentTimeMillis();
                    Envelope hb = enviarReferencia(referencia, "heartbeat", nome);
                    long ida_volta = System.currentTimeMillis() - antes;
                    if (hb.getConteudo().startsWith("sucesso")) {
                        // a hora da referencia ja esta velha quando chega, soma metade do tempo de ida e volta
                        diferencaRelogio = hb.getTimestamp() + ida_volta / 2 - System.currentTimeMillis();
                        System.out.println("[SERVER] Heartbeat ok, relogio ajustado em " + diferencaRelogio + " ms");
                    } else {
                        // referencia nao conhece mais esse servidor (removido por timeout), cadastra de novo
                        enviarReferencia(referencia, "rank", nome);
                    }
                }

                registrar(arquivoMensagens, "req," + envelope.getTimestamp() + "," + envelope.getAcao() + "," + envelope.getConteudo());
                System.out.println(
                    "[SERVER] Recebido: [" + envelope + "]"
                );

                String conteudo = "";
                if (envelope.getAcao().equals("login")) {
                    String user = envelope.getConteudo();
                    if (user.isBlank()) {
                        conteudo = "erro,nome de usuario vazio";
                    } else {
                        conteudo = "sucesso,bem-vindo " + user;
                        Files.writeString(arquivoLogins, envelope.getTimestamp() + "," + user
                            + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }   
                } else if (envelope.getAcao().equals("criar_canal")) {
                    String nomeCanal = envelope.getConteudo();
                    if (nomeCanal.isBlank() || canais.contains(nomeCanal)) {
                        conteudo = "erro,nome invalido ou canal ja criado";
                    } else {
                        conteudo = "sucesso,canal criado";
                        canais.add(nomeCanal);
                        Files.writeString(arquivosCanais, String.join(",", canais));
                    }
                } else if (envelope.getAcao().equals("listar_canais")){
                    conteudo = String.join(",", canais);
                } else if (envelope.getAcao().equals("publicar")) {
                    String[] partes = envelope.getConteudo().split(",", 2);
                    if (partes.length != 2 || partes[0].isBlank() || partes[1].isBlank()) {
                        conteudo = "erro,formato invalido";
                    } else {
                        relogio++;
                        Envelope publicacao = Envelope.newBuilder()
                            .setTimestamp(System.currentTimeMillis() + diferencaRelogio)
                            .setAcao("publicacao")
                            .setConteudo(envelope.getConteudo())
                            .setContador(relogio)
                            .build();

                        pub.send(partes[0] + "|", ZMQ.SNDMORE);
                        pub.send(publicacao.toByteArray(), 0);

                        registrar(arquivoPublicacoes, publicacao.getTimestamp() + "," + partes[0] + "," + partes[1]);
                        conteudo = "sucesso,mensagem publicada";
                    }
                }

                relogio++;
                Envelope envelope_asr = Envelope.newBuilder()
                    .setTimestamp(System.currentTimeMillis() + diferencaRelogio)
                    .setAcao("resposta_" + envelope.getAcao())
                    .setConteudo(conteudo)
                    .setContador(relogio)
                    .build();
                
                byte[] data_asr = envelope_asr.toByteArray();
                registrar(arquivoMensagens, "resp," + envelope_asr.getTimestamp() + "," + envelope_asr.getAcao() + "," + envelope_asr.getConteudo());
                socket.send(data_asr, 0);
            }
        }
    }

    private static Envelope enviarReferencia(ZMQ.Socket socket, String acao, String conteudo) throws Exception
    {
        relogio++;
        Envelope envelope = Envelope.newBuilder()
            .setTimestamp(System.currentTimeMillis() + diferencaRelogio)
            .setAcao(acao)
            .setConteudo(conteudo)
            .setContador(relogio)
            .build();
        socket.send(envelope.toByteArray(), 0);

        Envelope resposta = Envelope.parseFrom(socket.recv(0));
        relogio = Math.max(relogio, resposta.getContador());
        return resposta;
    }

    private static void registrar(Path arquivo, String linha) throws Exception
    {
        Files.writeString(arquivo, linha + System.lineSeparator(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}