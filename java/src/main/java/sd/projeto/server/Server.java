package sd.projeto.server;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import sd.projeto.protos.Mensagem;
import sd.projeto.protos.Resposta;
import sd.projeto.protos.RespostaReferencia;
import sd.projeto.protos.RespostaSub;
import sd.projeto.protos.ServidorInfo;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server
{
    private static final int PORTA_SERVIDORES = 5559;
    private static final int HEARTBEAT_A_CADA = 10;
    private static final int SINCRONIZAR_A_CADA = 15;
    private static final int TEMPO_LIMITE_MS = 3000;
    private static final int ESPERA_ANUNCIO_MS = 10000;
    private static final int ESPERA_ASSINATURA_MS = 1000;
    private static final String URL_REFERENCIA = "tcp://referencia:5556";

    private static final Object trava = new Object();
    private static int relogio = 0;
    private static double diferencaRelogio = 0;

    private static volatile int rank = 0;
    private static volatile String coordenador = "";
    private static volatile boolean anuncioRecebido = false;
    private static volatile boolean sincronizado = false;
    private static final AtomicBoolean emEleicao = new AtomicBoolean(false);

    private static ZContext context;
    private static ZMQ.Socket pub;
    private static String nome;

    private static final Gson gson = new Gson();
    private static List<String> usuarios = new ArrayList<>();

    private static final Object dados = new Object();
    private static final List<String> canais = new ArrayList<>();
    private static final List<Login> logins = new ArrayList<>();
    private static final List<Publicacao> publicacoes = new ArrayList<>();

    private static final Set<String> loginsVistos = new HashSet<>();
    private static final Set<String> publicacoesVistas = new HashSet<>();

    record Login(String usuario, double timestamp, String status) {
        String chave() { return usuario + "\u0000" + timestamp; }
    }

    record Publicacao(String canal, String conteudo, double timestamp) {
        String chave() { return canal + "\u0000" + conteudo + "\u0000" + timestamp; }
    }

    record EstadoServidor(List<String> canais, List<Login> logins, List<Publicacao> publicacoes) {}

    private static Path arquivoLogins;
    private static Path arquivoCanais;
    private static Path arquivoPublicacoes;
    private static Path arquivoMensagens;

    public static void main(String[] args) throws Exception
    {
        nome = System.getenv().getOrDefault("NOME_SERVIDOR", InetAddress.getLocalHost().getHostName());
        prepararArquivos();

        try (ZContext ctx = new ZContext()) {
            context = ctx;

            pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");

            ZMQ.Socket servidores = context.createSocket(SocketType.REP);
            servidores.bind("tcp://*:" + PORTA_SERVIDORES);
            iniciarThread(() -> atenderServidores(servidores));
            iniciarThread(Server::ouvirAnuncios);

            CountDownLatch replicacaoPronta = new CountDownLatch(1);
            iniciarThread(() -> ouvirReplicacao(replicacaoPronta));
            replicacaoPronta.await();

            Integer rankInicial;
            while ((rankInicial = tentarRank()) == null) {
                log("aguardando o servico de referencia...");
                Thread.sleep(1000);
            }
            rank = rankInicial;
            log("cadastrado na referencia com rank " + rank);

            Thread.sleep(ESPERA_ASSINATURA_MS);
            obterEstadoInicial();
            sincronizado = true;
            iniciarEleicao();
            atenderClientes();
        }
    }

    private static void prepararArquivos() throws Exception
    {
        Path diretorio = Paths.get(System.getenv().getOrDefault("DADOS_DIR", "dados"), nome);
        Files.createDirectories(diretorio);

        arquivoLogins = diretorio.resolve("login.txt");
        arquivoCanais = diretorio.resolve("canais.txt");
        arquivoPublicacoes = diretorio.resolve("publicacoes.txt");
        arquivoMensagens = diretorio.resolve("mensagens.txt");

        JsonObject seed = gson.fromJson(Files.readString(Paths.get("usuarios.json")), JsonObject.class);
        seed.getAsJsonArray("usuarios").forEach(u -> usuarios.add(u.getAsString()));

        if (Files.exists(arquivoCanais)) {
            String conteudo = Files.readString(arquivoCanais);
            if (!conteudo.isBlank()) {
                canais.addAll(Arrays.asList(conteudo.strip().split(",")));
            }
        }

        if (Files.exists(arquivoLogins)) {
            for (String linha : Files.readAllLines(arquivoLogins)) {
                String[] p = linha.split(",", 3);
                if (p.length == 3) {
                    Login l = new Login(p[1], Double.parseDouble(p[0]), p[2]);
                    logins.add(l);
                    loginsVistos.add(l.chave());
                }
            }
        }

        if (Files.exists(arquivoPublicacoes)) {
            for (String linha : Files.readAllLines(arquivoPublicacoes)) {
                String[] p = linha.split(",", 3);
                if (p.length == 3) {
                    Publicacao pub = new Publicacao(p[1], p[2], Double.parseDouble(p[0]));
                    publicacoes.add(pub);
                    publicacoesVistas.add(pub.chave());
                }
            }
        }
        log("dados em " + diretorio);
    }

    private static void registrarLogin(String usuario, double timestamp, String status)
    {
        synchronized (dados) {
            Login l = new Login(usuario, timestamp, status);
            if (!loginsVistos.add(l.chave())) {
                return;
            }
            logins.add(l);
            registrar(arquivoLogins, timestamp + "," + usuario + "," + status);
        }
    }

    private static boolean registrarCanal(String canal)
    {
        synchronized (dados) {
            if (canais.contains(canal)) {
                return false;
            }
            canais.add(canal);
            salvarCanais();
            return true;
        }
    }

    private static void registrarPublicacao(String canal, String conteudo, double timestamp)
    {
        synchronized (dados) {
            Publicacao pub = new Publicacao(canal, conteudo, timestamp);
            if (!publicacoesVistas.add(pub.chave())) {
                return;
            }
            publicacoes.add(pub);

            registrar(arquivoPublicacoes, timestamp + "," + canal + "," + conteudo.replace('\n', ' '));
        }
    }

    private static void registrar(Path arquivo, String linha)
    {
        try {
            Files.writeString(arquivo, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            log("erro ao gravar " + arquivo + ": " + e.getMessage());
        }
    }

    private static void salvarCanais()
    {
        try {
            Files.writeString(arquivoCanais, String.join(",", canais), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log("erro ao gravar canais: " + e.getMessage());
        }
    }

    private static double agora()
    {
        synchronized (trava) {
            return Math.floor(System.currentTimeMillis() + diferencaRelogio);
        }
    }

    private static int proximoContador()
    {
        synchronized (trava) {
            return ++relogio;
        }
    }

    private static void atualizarRelogio(int recebido)
    {
        synchronized (trava) {
            relogio = Math.max(relogio, recebido);
        }
    }

    private static Resposta criarResposta(String status, String texto)
    {
        return criarResposta(status, texto, List.of());
    }

    private static Resposta criarResposta(String status, String texto, List<String> lista)
    {
        return Resposta.newBuilder()
            .setStatus(status)
            .setResposta(texto)
            .addAllCanais(lista)
            .setContadorMensagem(proximoContador())
            .setTimestamp(agora())
            .build();
    }

    private static void publicarNoProxy(String topico, byte[] corpo)
    {
        synchronized (pub) {
            pub.send(topico, ZMQ.SNDMORE);
            pub.send(corpo, 0);
        }
    }

    private static byte[] requisitar(String url, String acao, String conteudo)
    {
        Mensagem mensagem = Mensagem.newBuilder()
            .setTimestamp(agora())
            .setAcao(acao)
            .setConteudo(conteudo)
            .setContadorMensagem(proximoContador())
            .build();

        ZMQ.Socket req = context.createSocket(SocketType.REQ);
        try {
            req.setSendTimeOut(TEMPO_LIMITE_MS);
            req.setReceiveTimeOut(TEMPO_LIMITE_MS);
            req.setLinger(0);
            req.connect(url);
            if (!req.send(mensagem.toByteArray(), 0)) {
                return null;
            }
            return req.recv(0);
        } catch (Exception e) {
            return null;
        } finally {
            context.destroySocket(req);
        }
    }

    private static Resposta requisitarServidor(String alvo, String acao, String conteudo)
    {
        byte[] dados = requisitar("tcp://" + alvo + ":" + PORTA_SERVIDORES, acao, conteudo);
        if (dados == null) {
            return null;
        }
        try {
            Resposta resposta = Resposta.parseFrom(dados);
            atualizarRelogio(resposta.getContadorMensagem());
            return resposta;
        } catch (Exception e) {
            return null;
        }
    }

    private static RespostaReferencia requisitarReferencia(String acao)
    {
        byte[] dados = requisitar(URL_REFERENCIA, acao, nome);
        if (dados == null) {
            return null;
        }
        try {
            RespostaReferencia resposta = RespostaReferencia.parseFrom(dados);
            atualizarRelogio(resposta.getContadorMensagem());
            return resposta;
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer tentarRank()
    {
        RespostaReferencia resposta = requisitarReferencia("consultar_rank");
        return resposta != null && resposta.getStatus().equals("200") ? resposta.getRank() : null;
    }

    private static void heartbeat()
    {
        RespostaReferencia resposta = requisitarReferencia("heartbeat");
        if (resposta == null) {
            log("heartbeat sem resposta da referencia");
        } else if (resposta.getStatus().equals("200")) {
            log("heartbeat confirmado");
        } else {
            Integer novo = tentarRank();
            if (novo != null) {
                rank = novo;
                log("recadastrado na referencia, rank " + novo);
            }
        }
    }

    private static void iniciarEleicao()
    {
        if (!emEleicao.compareAndSet(false, true)) {
            return;
        }
        anuncioRecebido = false;
        iniciarThread(() -> {
            boolean refazer = false;
            try {
                refazer = eleicao();
            } catch (Exception e) {
                log("erro na eleicao: " + e.getMessage());
            } finally {
                emEleicao.set(false);
            }
            if (refazer) {
                iniciarEleicao();
            }
        });
    }

    private static boolean eleicao() throws InterruptedException
    {
        log("iniciando eleicao...");
        int meuRank = rank;

        List<ServidorInfo> maiores = new ArrayList<>();
        RespostaReferencia lista = requisitarReferencia("listar_servidores");
        if (lista != null && lista.getStatus().equals("200")) {
            for (ServidorInfo s : lista.getServidoresList()) {
                if (s.getRank() > meuRank && !s.getNome().equals(nome)) {
                    maiores.add(s);
                }
            }
        }

        boolean alguemRespondeu = false;
        for (ServidorInfo alvo : maiores) {
            Resposta r = requisitarServidor(alvo.getNome(), "eleicao", nome);
            if (r != null && r.getResposta().equals("OK")) {
                alguemRespondeu = true;
            }
        }

        if (!alguemRespondeu) {
            tornarSeCoordenador();
            return false;
        }

        log("servidor de rank maior assumiu a eleicao; aguardando anuncio...");
        long fim = System.currentTimeMillis() + ESPERA_ANUNCIO_MS;
        while (!anuncioRecebido && System.currentTimeMillis() < fim) {
            Thread.sleep(200);
        }
        if (!anuncioRecebido) {
            log("anuncio nao chegou; refazendo eleicao");
            return true;
        }
        return false;
    }

    private static void tornarSeCoordenador() throws InterruptedException
    {
        coordenador = nome;
        synchronized (trava) {
            diferencaRelogio = 0;
        }
        anuncioRecebido = true;
        log("eleito coordenador (rank " + rank + ")");

        for (int i = 0; i < 2; i++) {
            RespostaSub anuncio = RespostaSub.newBuilder()
                .setTimestampEnvio(agora())
                .setCanal("servers")
                .setConteudo(nome)
                .setContadorMensagem(proximoContador())
                .build();
            publicarNoProxy("servers|", anuncio.toByteArray());
            Thread.sleep(1000);
        }
    }

    private static void ouvirAnuncios()
    {
        ZMQ.Socket sub = context.createSocket(SocketType.SUB);
        sub.connect("tcp://proxy:5558");
        sub.subscribe("servers|".getBytes(StandardCharsets.UTF_8));

        while (!Thread.currentThread().isInterrupted()) {
            try {
                sub.recvStr();
                RespostaSub anuncio = RespostaSub.parseFrom(sub.recv(0));
                atualizarRelogio(anuncio.getContadorMensagem());

                boolean mudou = !anuncio.getConteudo().equals(coordenador);
                coordenador = anuncio.getConteudo();
                anuncioRecebido = true;
                if (mudou) {
                    log("novo coordenador anunciado: " + coordenador);
                }
            } catch (ZMQException e) {
                break;
            } catch (Exception e) {
                log("erro ao processar anuncio: " + e.getMessage());
            }
        }
    }

    private static void atenderServidores(ZMQ.Socket servidores)
    {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] dados;
            try {
                dados = servidores.recv(0);
            } catch (ZMQException e) {
                break;
            }

            boolean iniciar = false;
            Resposta resposta;
            try {
                Mensagem pedido = Mensagem.parseFrom(dados);
                atualizarRelogio(pedido.getContadorMensagem());

                if (pedido.getAcao().equals("obter_estado") && !sincronizado) {
                    resposta = criarResposta("503", "Servidor ainda sincronizando");
                } else if (pedido.getAcao().equals("obter_estado")) {
                    resposta = criarResposta("200", exportarEstado());
                    log("estado enviado para " + pedido.getConteudo());
                } else if (pedido.getAcao().equals("eleicao")) {
                    resposta = criarResposta("200", "OK");
                    iniciar = true;
                } else if (pedido.getAcao().equals("sincronizar")) {
                    resposta = coordenador.equals(nome)
                        ? criarResposta("200", "Hora correta")
                        : criarResposta("400", "Nao sou o coordenador!");
                } else {
                    resposta = criarResposta("400", "Acao desconhecida!");
                }
            } catch (Exception e) {
                log("erro ao atender servidor: " + e.getMessage());
                resposta = criarResposta("500", "Erro interno no servidor!");
            }

            servidores.send(resposta.toByteArray(), 0);
            if (iniciar) {
                iniciarEleicao();
            }
        }
    }

    private static void sincronizarRelogio()
    {
        String alvo = coordenador;
        if (alvo.isEmpty()) {
            iniciarEleicao();
            return;
        }
        if (alvo.equals(nome)) {
            log("sou o coordenador: relogio mantido");
            return;
        }

        long t0 = System.currentTimeMillis();
        Resposta resposta = requisitarServidor(alvo, "sincronizar", nome);
        long t1 = System.currentTimeMillis();

        if (resposta == null || !resposta.getStatus().equals("200")) {
            log("coordenador " + alvo + " indisponivel; iniciando eleicao");
            iniciarEleicao();
            return;
        }

        double horaCoordenador = resposta.getTimestamp() + (t1 - t0) / 2.0;
        double diferenca;
        synchronized (trava) {
            diferencaRelogio = horaCoordenador - t1;
            diferenca = diferencaRelogio;
        }
        log(String.format("relogio sincronizado com %s (diferenca %+.0f ms)", alvo, diferenca));
    }

    private static Resposta tratarLogin(Mensagem req)
    {
        String usuario = req.getConteudo().strip();
        Resposta resposta = usuarios.contains(usuario)
            ? criarResposta("200", "Login realizado com sucesso!")
            : criarResposta("404", "Nome de usuario incorreto!");

        registrarLogin(usuario, req.getTimestamp(), resposta.getStatus());
        replicar("login", "", usuario, req.getTimestamp());
        return resposta;
    }

    private static Resposta tratarListar()
    {
        List<String> lista;
        synchronized (dados) {
            lista = new ArrayList<>(canais);
        }
        return criarResposta("200", "Canais listados", lista);
    }

    private static Resposta tratarCadastrar(Mensagem req)
    {
        String canal = req.getConteudo().strip();
        if (canal.isEmpty() || canal.length() > 50 || canal.contains("|") || canal.contains(",")) {
            return criarResposta("400", "Nome de canal invalido!");
        }
        if (!registrarCanal(canal)) {
            return criarResposta("404", "Canal já existe!");
        }

        replicar("cadastrar", "", canal, req.getTimestamp());
        return criarResposta("200", "Canal " + canal + " cadastrado com sucesso!");
    }

    private static Resposta tratarPublicar(Mensagem req)
    {
        String canal = req.getCanal().strip();
        if (canal.isEmpty() || req.getConteudo().isEmpty()) {
            return criarResposta("400", "Canal e mensagem sao obrigatorios!");
        }

        RespostaSub publicacao = RespostaSub.newBuilder()
            .setTimestampEnvio(agora())
            .setCanal(canal)
            .setConteudo(req.getConteudo())
            .setContadorMensagem(proximoContador())
            .build();
        publicarNoProxy(canal + "|", publicacao.toByteArray());

        registrarPublicacao(canal, req.getConteudo(), req.getTimestamp());
        replicar("publicar", canal, req.getConteudo(), req.getTimestamp());
        return criarResposta("200", "Mensagem publicada no canal " + canal + " com sucesso!");
    }

    private static final String TOPICO_REPLICACAO = "replicacao|";

    private static void replicar(String acao, String canal, String conteudo, double timestamp)
    {
        Mensagem operacao = Mensagem.newBuilder()
            .setTimestamp(timestamp)
            .setAcao(acao)
            .setCanal(canal)
            .setConteudo(conteudo)
            .setContadorMensagem(proximoContador())
            .build();
        publicarNoProxy(TOPICO_REPLICACAO + nome, operacao.toByteArray());
    }

    private static void aplicarOperacao(Mensagem operacao)
    {
        switch (operacao.getAcao()) {
            case "login" -> registrarLogin(operacao.getConteudo(), operacao.getTimestamp(),
                usuarios.contains(operacao.getConteudo()) ? "200" : "404");
            case "cadastrar" -> registrarCanal(operacao.getConteudo());
            case "publicar" -> registrarPublicacao(operacao.getCanal(), operacao.getConteudo(), operacao.getTimestamp());
            default -> { }
        }
    }

    private static void ouvirReplicacao(CountDownLatch pronto)
    {
        ZMQ.Socket sub = context.createSocket(SocketType.SUB);
        sub.connect("tcp://proxy:5558");
        sub.subscribe(TOPICO_REPLICACAO.getBytes(StandardCharsets.UTF_8));
        pronto.countDown();

        String proprio = TOPICO_REPLICACAO + nome;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String topico = sub.recvStr();
                byte[] corpo = sub.recv(0);
                if (topico.equals(proprio)) {
                    continue;
                }
                Mensagem operacao = Mensagem.parseFrom(corpo);
                atualizarRelogio(operacao.getContadorMensagem());
                aplicarOperacao(operacao);
                log("replicado de " + topico.substring(TOPICO_REPLICACAO.length()) + ": "
                    + operacao.getAcao() + " " + operacao.getCanal() + " " + operacao.getConteudo());
            } catch (ZMQException e) {
                break;
            } catch (Exception e) {
                log("erro ao aplicar replicacao: " + e.getMessage());
            }
        }
    }

    private static String exportarEstado()
    {
        synchronized (dados) {
            return gson.toJson(new EstadoServidor(canais, logins, publicacoes));
        }
    }

    private static void importarEstado(String texto, String origem)
    {
        EstadoServidor estado = gson.fromJson(texto, EstadoServidor.class);
        synchronized (dados) {
            if (estado.canais() != null) {
                estado.canais().forEach(Server::registrarCanal);
            }
            if (estado.logins() != null) {
                estado.logins().forEach(l -> registrarLogin(l.usuario(), l.timestamp(), l.status() == null ? "" : l.status()));
            }
            if (estado.publicacoes() != null) {
                estado.publicacoes().forEach(p -> registrarPublicacao(p.canal(), p.conteudo(), p.timestamp()));
            }
            log("estado recebido de " + origem + ": " + canais.size() + " canais, " + logins.size()
                + " logins, " + publicacoes.size() + " publicacoes");
        }
    }

    private static void obterEstadoInicial()
    {
        String atual = coordenador;
        List<String> candidatos = new ArrayList<>();
        if (!atual.isEmpty() && !atual.equals(nome)) {
            candidatos.add(atual);
        }
        RespostaReferencia lista = requisitarReferencia("listar_servidores");
        if (lista != null) {
            for (ServidorInfo s : lista.getServidoresList()) {
                if (!s.getNome().equals(nome) && !s.getNome().equals(atual)) {
                    candidatos.add(s.getNome());
                }
            }
        }

        for (String alvo : candidatos) {
            Resposta resposta = requisitarServidor(alvo, "obter_estado", nome);
            if (resposta != null && resposta.getStatus().equals("200")) {
                importarEstado(resposta.getResposta(), alvo);
                return;
            }
        }
        log("nenhum outro servidor respondeu: mantendo os dados locais");
    }

    private static void atenderClientes()
    {
        ZMQ.Socket socket = context.createSocket(SocketType.REP);
        socket.connect("tcp://broker:5560");

        int mensagensRecebidas = 0;
        while (!Thread.currentThread().isInterrupted()) {
            byte[] dados = socket.recv(0);

            Resposta resposta;
            try {
                Mensagem req = Mensagem.parseFrom(dados);
                atualizarRelogio(req.getContadorMensagem());
                log("recebido: " + req.getAcao() + " canal=" + req.getCanal()
                    + " conteudo=" + req.getConteudo() + " timestamp=" + (long) req.getTimestamp()
                    + " contador=" + req.getContadorMensagem());
                registrar(arquivoMensagens, "req," + req.getTimestamp() + "," + req.getContadorMensagem()
                    + "," + req.getAcao() + "," + req.getCanal() + "," + req.getConteudo());

                resposta = switch (req.getAcao()) {
                    case "login" -> tratarLogin(req);
                    case "listar" -> tratarListar();
                    case "cadastrar" -> tratarCadastrar(req);
                    case "publicar" -> tratarPublicar(req);
                    default -> criarResposta("400", "Acao desconhecida!");
                };
            } catch (Exception e) {
                log("erro ao processar mensagem: " + e.getMessage());
                resposta = criarResposta("500", "Erro interno no servidor!");
            }

            registrar(arquivoMensagens, "resp," + resposta.getTimestamp() + "," + resposta.getContadorMensagem()
                + "," + resposta.getStatus() + "," + resposta.getResposta());
            log("enviado: " + resposta.getStatus() + " " + resposta.getResposta()
                + " timestamp=" + (long) resposta.getTimestamp() + " contador=" + resposta.getContadorMensagem());
            socket.send(resposta.toByteArray(), 0);

            mensagensRecebidas++;
            if (mensagensRecebidas % HEARTBEAT_A_CADA == 0) {
                heartbeat();
            }
            if (mensagensRecebidas % SINCRONIZAR_A_CADA == 0) {
                sincronizarRelogio();
            }
        }
    }

    private static void iniciarThread(Runnable tarefa)
    {
        Thread t = new Thread(tarefa);
        t.setDaemon(true);
        t.start();
    }

    private static void log(String texto)
    {
        System.out.println("[" + nome + "] " + texto);
    }
}
