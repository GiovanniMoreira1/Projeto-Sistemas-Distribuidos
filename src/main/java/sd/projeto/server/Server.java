package sd.projeto.server;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Server
{
    private static final AtomicLong relogio = new AtomicLong(0);
    private static volatile long diferencaRelogio = 0;

    private static volatile String coordenador = "";
    private static volatile boolean anuncioRecebido = false;
    private static final AtomicBoolean emEleicao = new AtomicBoolean(false);

    private static ZContext context;
    private static ZMQ.Socket pub;
    private static String nome;
    private static int rank;

    public static void main(String[] args) throws Exception
    {
        try (ZContext ctx = new ZContext()) {
            context = ctx;

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5560");

            pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");

            ZMQ.Socket referencia = context.createSocket(SocketType.REQ);
            referencia.connect("tcp://referencia:5556");

            nome = InetAddress.getLocalHost().getHostName();
            Envelope respostaRank = enviarReferencia(referencia, "rank", nome);
            rank = Integer.parseInt(respostaRank.getConteudo().split(",")[1]);
            System.out.println("[SERVER] " + nome + " cadastrado na referencia com rank " + rank);

            Envelope respostaLista = enviarReferencia(referencia, "list", "");
            System.out.println("[SERVER] Servidores na referencia: " + respostaLista.getConteudo());

            ZMQ.Socket servidores = context.createSocket(SocketType.REP);
            servidores.bind("tcp://*:5561");
            Thread atendimento = new Thread(() -> atenderServidores(servidores));
            atendimento.setDaemon(true);
            atendimento.start();

            Thread anuncios = new Thread(() -> ouvirAnuncios());
            anuncios.setDaemon(true);
            anuncios.start();

            Thread.sleep(1000);
            iniciarEleicao();

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
                atualizarRelogio(envelope.getContador());

                mensagensRecebidas++;
                if (mensagensRecebidas % 10 == 0) {
                    Envelope hb = enviarReferencia(referencia, "heartbeat", nome);
                    if (!hb.getConteudo().startsWith("sucesso")) {
                        // referencia nao conhece mais esse servidor (removido por timeout), cadastra de novo
                        enviarReferencia(referencia, "rank", nome);
                    }
                }
                if (mensagensRecebidas % 15 == 0) {
                    sincronizarRelogio();
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
                        Envelope publicacao = montar("publicacao", envelope.getConteudo());

                        synchronized (pub) {
                            pub.send(partes[0] + "|", ZMQ.SNDMORE);
                            pub.send(publicacao.toByteArray(), 0);
                        }

                        registrar(arquivoPublicacoes, publicacao.getTimestamp() + "," + partes[0] + "," + partes[1]);
                        conteudo = "sucesso,mensagem publicada";
                    }
                }

                Envelope envelope_asr = montar("resposta_" + envelope.getAcao(), conteudo);

                byte[] data_asr = envelope_asr.toByteArray();
                registrar(arquivoMensagens, "resp," + envelope_asr.getTimestamp() + "," + envelope_asr.getAcao() + "," + envelope_asr.getConteudo());
                socket.send(data_asr, 0);
            }
        }
    }

    private static long agora()
    {
        return System.currentTimeMillis() + diferencaRelogio;
    }

    private static void atualizarRelogio(long recebido)
    {
        relogio.updateAndGet(atual -> Math.max(atual, recebido));
    }

    // soma 1 no relogio logico e monta o envelope pronto pra enviar
    private static Envelope montar(String acao, String conteudo)
    {
        return Envelope.newBuilder()
            .setTimestamp(agora())
            .setAcao(acao)
            .setConteudo(conteudo)
            .setContador(relogio.incrementAndGet())
            .build();
    }

    private static Envelope enviarReferencia(ZMQ.Socket socket, String acao, String conteudo) throws Exception
    {
        socket.send(montar(acao, conteudo).toByteArray(), 0);

        Envelope resposta = Envelope.parseFrom(socket.recv(0));
        atualizarRelogio(resposta.getContador());
        return resposta;
    }

    // pergunta pra outro processo com socket novo e espera no maximo 3s, devolve null se ninguem responder
    private static Envelope perguntar(String endereco, String acao, String conteudo)
    {
        ZMQ.Socket req = null;
        try {
            req = context.createSocket(SocketType.REQ);
            req.setReceiveTimeOut(3000);
            req.setLinger(0);
            req.connect(endereco);
            req.send(montar(acao, conteudo).toByteArray(), 0);

            byte[] bytes = req.recv(0);
            if (bytes == null) {
                return null;
            }
            Envelope resposta = Envelope.parseFrom(bytes);
            atualizarRelogio(resposta.getContador());
            return resposta;
        } catch (Exception e) {
            return null;
        } finally {
            if (req != null) {
                context.destroySocket(req);
            }
        }
    }

    private static void sincronizarRelogio()
    {
        String atual = coordenador;
        if (atual.isEmpty()) {
            iniciarEleicao();
            return;
        }
        if (atual.equals(nome)) {
            return;
        }

        long antes = System.currentTimeMillis();
        Envelope resposta = perguntar("tcp://" + atual + ":5561", "relogio", nome);
        long idaVolta = System.currentTimeMillis() - antes;

        if (resposta == null || !resposta.getConteudo().startsWith("sucesso")) {
            System.out.println("[SERVER] Coordenador " + atual + " indisponivel, pedindo eleicao");
            iniciarEleicao();
            return;
        }

        // a hora do coordenador ja esta velha quando chega, soma metade do tempo de ida e volta
        diferencaRelogio = resposta.getTimestamp() + idaVolta / 2 - System.currentTimeMillis();
        System.out.println("[SERVER] Relogio sincronizado com " + atual + ", diferenca de " + diferencaRelogio + " ms");
    }

    private static void iniciarEleicao()
    {
        if (!emEleicao.compareAndSet(false, true)) {
            return;
        }
        Thread t = new Thread(() -> {
            try {
                eleicao();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                emEleicao.set(false);
            }
        });
        t.setDaemon(true);
        t.start();
    }

    // so quem tem rank maior que o meu pode ganhar de mim
    private static void eleicao() throws Exception
    {
        while (true) {
            anuncioRecebido = false;
            System.out.println("[SERVER] " + nome + " iniciou uma eleicao");

            boolean alguemRespondeu = false;
            Envelope lista = perguntar("tcp://referencia:5556", "list", "");
            if (lista != null && lista.getConteudo().startsWith("sucesso")) {
                String itens = lista.getConteudo().split(",", 2)[1];
                if (!itens.isBlank()) {
                    for (String item : itens.split(";")) {
                        String[] partes = item.split(":");
                        if (Integer.parseInt(partes[1]) > rank) {
                            Envelope r = perguntar("tcp://" + partes[0] + ":5561", "eleicao", nome);
                            if (r != null && r.getConteudo().startsWith("sucesso")) {
                                alguemRespondeu = true;
                            }
                        }
                    }
                }
            }

            if (!alguemRespondeu) {
                coordenador = nome;
                diferencaRelogio = 0;
                anuncioRecebido = true;
                System.out.println("[SERVER] " + nome + " eleito coordenador");

                Envelope anuncio = montar("coordenador", nome);
                synchronized (pub) {
                    pub.send("servers|", ZMQ.SNDMORE);
                    pub.send(anuncio.toByteArray(), 0);
                }
                return;
            }

            System.out.println("[SERVER] Servidor de rank maior respondeu, aguardando o anuncio");
            Thread.sleep(10000);
            if (anuncioRecebido) {
                return;
            }
        }
    }

    // thread que responde os outros servidores (eleicao e relogio)
    private static void atenderServidores(ZMQ.Socket servidores)
    {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Envelope pedido = Envelope.parseFrom(servidores.recv(0));
                atualizarRelogio(pedido.getContador());

                String conteudo;
                boolean eleicao = false;
                if (pedido.getAcao().equals("eleicao")) {
                    conteudo = "sucesso,OK";
                    eleicao = true;
                } else if (pedido.getAcao().equals("relogio")) {
                    if (coordenador.equals(nome)) {
                        conteudo = "sucesso,hora correta";
                    } else {
                        conteudo = "erro,nao sou o coordenador";
                    }
                } else {
                    conteudo = "erro,acao desconhecida";
                }

                servidores.send(montar("resposta_" + pedido.getAcao(), conteudo).toByteArray(), 0);

                if (eleicao) {
                    iniciarEleicao();
                }
            } catch (ZMQException e) {
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // thread que escuta o topico servers, onde o coordenador eleito se anuncia
    private static void ouvirAnuncios()
    {
        ZMQ.Socket sub = context.createSocket(SocketType.SUB);
        sub.connect("tcp://proxy:5558");
        sub.subscribe("servers|");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                String topico = sub.recvStr();
                byte[] corpo = sub.recv(0);

                Envelope anuncio = Envelope.parseFrom(corpo);
                atualizarRelogio(anuncio.getContador());
                coordenador = anuncio.getConteudo();
                anuncioRecebido = true;
                System.out.println("[SERVER] Novo coordenador anunciado: " + coordenador);
            } catch (ZMQException e) {
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void registrar(Path arquivo, String linha) throws Exception
    {
        Files.writeString(arquivo, linha + System.lineSeparator(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
