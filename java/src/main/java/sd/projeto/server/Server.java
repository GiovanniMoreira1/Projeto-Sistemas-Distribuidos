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

/**
 * Servidor Java. Segue o contrato comum do grupo (proto/client.proto, portas, acoes e topicos),
 * igual aos servidores das outras linguagens:
 *   clientes  -> broker (DEALER :5560) -> este servidor (REP)
 *   publicacoes -> proxy (XSUB :5557), topico "<canal>|"
 *   referencia (REP :5556): consultar_rank, listar_servidores, heartbeat
 *   outros servidores -> este servidor (REP :5559): eleicao, sincronizar
 *   anuncio do coordenador no topico "servers|"
 */
public class Server
{
    private static final int PORTA_SERVIDORES = 5559;
    private static final int HEARTBEAT_A_CADA = 10;     // mensagens de clientes (Parte 3)
    private static final int SINCRONIZAR_A_CADA = 15;   // mensagens de clientes (Parte 4)
    private static final int TEMPO_LIMITE_MS = 3000;
    private static final int ESPERA_ANUNCIO_MS = 10000;
    private static final String URL_REFERENCIA = "tcp://referencia:5556";

    // relogio logico e correcao do relogio fisico, usados por varias threads
    private static final Object trava = new Object();
    private static int relogio = 0;
    private static double diferencaRelogio = 0;

    private static volatile int rank = 0;
    private static volatile String coordenador = "";
    private static volatile boolean anuncioRecebido = false;
    private static final AtomicBoolean emEleicao = new AtomicBoolean(false);

    private static ZContext context;
    private static ZMQ.Socket pub;
    private static String nome;

    private static final Gson gson = new Gson();
    private static List<String> usuarios = new ArrayList<>();

    // dados persistidos; a partir da Parte 5 tambem sao alterados pela thread de replicacao,
    // por isso todo acesso e feito com synchronized (dados)
    private static final Object dados = new Object();
    private static final List<String> canais = new ArrayList<>();
    private static final List<Login> logins = new ArrayList<>();
    private static final List<Publicacao> publicacoes = new ArrayList<>();
    // chaves do que ja esta guardado: aplicar a mesma operacao duas vezes nao duplica o registro
    private static final Set<String> loginsVistos = new HashSet<>();
    private static final Set<String> publicacoesVistas = new HashSet<>();

    // formato dos registros na transferencia de estado (igual em todas as linguagens)
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

            // Parte 5: assina a replicacao antes de pedir o estado, para nao perder escritas no meio
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

            obterEstadoInicial();
            iniciarEleicao();
            atenderClientes();
        }
    }

    // ------------------------------------------------------------------ persistencia

    // cada servidor tem o seu proprio diretorio de dados: nada e compartilhado entre servidores
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
        // login.txt: timestamp,usuario,status
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
        // publicacoes.txt: timestamp,canal,mensagem (a mensagem pode ter virgula)
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

    // devolve false se o canal ja existia
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
            // a mensagem vai numa linha so: quebras de linha viram espaco
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

    // ------------------------------------------------------------------ relogios

    private static double agora()
    {
        synchronized (trava) {
            return Math.floor(System.currentTimeMillis() + diferencaRelogio);
        }
    }

    // regra 1 do relogio logico: incrementa antes de cada envio
    private static int proximoContador()
    {
        synchronized (trava) {
            return ++relogio;
        }
    }

    // regra 2: ao receber, fica com o maximo entre o local e o recebido
    private static void atualizarRelogio(int recebido)
    {
        synchronized (trava) {
            relogio = Math.max(relogio, recebido);
        }
    }

    // ------------------------------------------------------------------ comunicacao

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

    // REQ/REP com socket novo e limite de tempo; devolve null se o destino nao responder
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
            // servidor que caiu: o nome some do DNS e o connect/envio falha
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

    // ------------------------------------------------------------------ referencia (Parte 3)

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
            // a referencia removeu este servidor (ficou muito tempo sem heartbeat): cadastra de novo
            Integer novo = tentarRank();
            if (novo != null) {
                rank = novo;
                log("recadastrado na referencia, rank " + novo);
            }
        }
    }

    // ------------------------------------------------------------------ eleicao (Parte 4, valentao)

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

    // so quem tem rank maior que o meu pode ganhar de mim; devolve true se precisar refazer
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
            diferencaRelogio = 0;  // o coordenador e a referencia de hora
        }
        anuncioRecebido = true;
        log("eleito coordenador (rank " + rank + ")");

        // publica duas vezes para tolerar a perda da primeira por conexao recem-aberta
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

    // thread que escuta o topico servers, onde o coordenador eleito se anuncia
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

    // thread que responde os outros servidores (eleicao e relogio)
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

                if (pedido.getAcao().equals("obter_estado")) {  // Parte 5: transferencia de estado
                    resposta = criarResposta("200", exportarEstado());
                    log("estado enviado para " + pedido.getConteudo());
                } else if (pedido.getAcao().equals("eleicao")) {
                    resposta = criarResposta("200", "OK");
                    iniciar = true;  // quem recebe pedido de eleicao tambem participa
                } else if (pedido.getAcao().equals("sincronizar")) {
                    resposta = coordenador.equals(nome)
                        ? criarResposta("200", "Hora correta")  // timestamp = hora do coordenador
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

    // a cada 15 mensagens pede a hora ao coordenador; se ele nao responder, pede eleicao
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

        // a hora do coordenador ja esta velha quando chega: soma metade do tempo de ida e volta
        double horaCoordenador = resposta.getTimestamp() + (t1 - t0) / 2.0;
        double diferenca;
        synchronized (trava) {
            diferencaRelogio = horaCoordenador - t1;
            diferenca = diferencaRelogio;
        }
        log(String.format("relogio sincronizado com %s (diferenca %+.0f ms)", alvo, diferenca));
    }

    // ------------------------------------------------------------------ acoes dos clientes

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

    // ------------------------------------------------------------------ replicacao (Parte 5)
    // Replicacao ativa: cada escrita aceita de um cliente (login, cadastrar, publicar) e aplicada
    // localmente e difundida no topico "replicacao|<servidor de origem>"; todos os servidores assinam
    // "replicacao|" e aplicam as escritas dos outros. Quem entra (ou volta) pede o estado completo a
    // outro servidor com "obter_estado".

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

    // aplica uma escrita feita por outro servidor (sem responder cliente nem republicar no canal)
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
                    continue;  // eco da propria escrita: ja foi aplicada
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

    // junta o estado recebido com o local (sem duplicar)
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

    // pede o estado ao coordenador (se for outro servidor) ou ao proximo servidor da lista
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
                log("recebido: acao=" + req.getAcao() + " canal=" + req.getCanal()
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
                // o REP sempre precisa responder, senao trava
                log("erro ao processar mensagem: " + e.getMessage());
                resposta = criarResposta("500", "Erro interno no servidor!");
            }

            registrar(arquivoMensagens, "resp," + resposta.getTimestamp() + "," + resposta.getContadorMensagem()
                + "," + resposta.getStatus() + "," + resposta.getResposta());
            log("enviado: status=" + resposta.getStatus() + " resposta=" + resposta.getResposta()
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

    // ------------------------------------------------------------------ util

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
