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

public class Server
{
    public static void main(String[] args) throws Exception
    {
        try (ZContext context = new ZContext()) {
            
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5560");

            ZMQ.Socket pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");

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
                        Envelope publicacao = Envelope.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setAcao("publicacao")
                            .setConteudo(envelope.getConteudo())
                            .build();

                        pub.send(partes[0] + "|", ZMQ.SNDMORE);
                        pub.send(publicacao.toByteArray(), 0);

                        registrar(arquivoPublicacoes, publicacao.getTimestamp() + "," + partes[0] + "," + partes[1]);
                        conteudo = "sucesso,mensagem publicada";
                    }
                }

                Envelope envelope_asr = Envelope.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setAcao("resposta_" + envelope.getAcao())
                    .setConteudo(conteudo)
                    .build();
                
                byte[] data_asr = envelope_asr.toByteArray();
                registrar(arquivoMensagens, "resp," + envelope_asr.getTimestamp() + "," + envelope_asr.getAcao() + "," + envelope_asr.getConteudo());
                socket.send(data_asr, 0);
            }
        }
    }

    private static void registrar(Path arquivo, String linha) throws Exception
    {
        Files.writeString(arquivo, linha + System.lineSeparator(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}