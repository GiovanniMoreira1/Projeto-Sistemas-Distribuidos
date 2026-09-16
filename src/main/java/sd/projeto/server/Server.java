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

            List<String> canais = new ArrayList<>();

            Path arquivoLogins = Paths.get("login.txt");
            Path arquivosCanais = Paths.get("canais.txt");

            if (Files.exists(arquivosCanais)){
                String conteudoArquivo = Files.readString(arquivosCanais);
                if(!conteudoArquivo.isBlank()){
                    canais.addAll(Arrays.asList(conteudoArquivo.split(",")));
                }
            }

            while (!Thread.currentThread().isInterrupted()) {
                
                byte[] reply = socket.recv(0);

                
                Envelope envelope = Envelope.parseFrom(reply);
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
                }

                // Send a response
                Envelope envelope_asr = Envelope.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setAcao("resposta_" + envelope.getAcao())
                    .setConteudo(conteudo)
                    .build();
                
                byte[] data_asr = envelope_asr.toByteArray();
                socket.send(data_asr, 0);
            }
        }
    }
}