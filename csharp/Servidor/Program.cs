using System.Text.Json;
using ChatCSharp.Gen;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

#region Configuracao

var nome = Environment.GetEnvironmentVariable("NOME_SERVIDOR") ?? Environment.MachineName;
const int portaServidores = 5559;
const int heartbeatACada = 10;
const int sincronizarACada = 15;
const string urlReferencia = "tcp://referencia:5556";
var tempoLimite = TimeSpan.FromSeconds(3);

#endregion

#region Dados

var diretorio = Path.Combine(Environment.GetEnvironmentVariable("DADOS_DIR") ?? "dados", nome);
Directory.CreateDirectory(diretorio);
var caminhoUsuarios = Path.Combine(diretorio, "usuarios.json");
var caminhoCanais = Path.Combine(diretorio, "canais.json");
var caminhoLogins = Path.Combine(diretorio, "login.json");
var caminhoMensagens = Path.Combine(diretorio, "mensagens.jsonl");
var caminhoPublicacoes = Path.Combine(diretorio, "publicacoes.jsonl");

if (!File.Exists(caminhoUsuarios)) File.Copy("usuarios.json", caminhoUsuarios);
var usuarios = JsonSerializer.Deserialize<UsuariosArquivo>(File.ReadAllText(caminhoUsuarios))!;
var canais = File.Exists(caminhoCanais)
    ? JsonSerializer.Deserialize<CanaisArquivo>(File.ReadAllText(caminhoCanais))!
    : new CanaisArquivo();
var logins = File.Exists(caminhoLogins)
    ? JsonSerializer.Deserialize<List<LoginEntrada>>(File.ReadAllText(caminhoLogins))!
    : [];
var publicacoes = File.Exists(caminhoPublicacoes)
    ? File.ReadLines(caminhoPublicacoes).Where(l => l.Trim() != "")
        .Select(l => JsonSerializer.Deserialize<PublicacaoEntrada>(l)!).ToList()
    : new List<PublicacaoEntrada>();

void AnexarLinha(string caminho, object registro)
{
    File.AppendAllText(caminho, JsonSerializer.Serialize(registro) + "\n");
}

// Parte 5: os dados tambem sao alterados pela thread de replicacao
var travaDados = new object();
// chaves do que ja esta guardado: aplicar a mesma operacao duas vezes nao duplica o registro
var loginsVistos = logins.Select(l => (l.Usuario, l.Timestamp)).ToHashSet();
var publicacoesVistas = publicacoes.Select(p => (p.Canal, p.Conteudo, p.Timestamp)).ToHashSet();

void RegistrarLogin(string usuario, double timestamp, string status)
{
    lock (travaDados)
    {
        if (!loginsVistos.Add((usuario, timestamp))) return;
        logins.Add(new LoginEntrada { Usuario = usuario, Timestamp = timestamp, Status = status });
        File.WriteAllText(caminhoLogins, JsonSerializer.Serialize(logins));
    }
}

bool RegistrarCanal(string canal)
{
    lock (travaDados)
    {
        if (canais.Canais.Contains(canal)) return false;
        canais.Canais.Add(canal);
        File.WriteAllText(caminhoCanais, JsonSerializer.Serialize(canais));
        return true;
    }
}

void RegistrarPublicacao(string canal, string conteudo, double timestamp)
{
    lock (travaDados)
    {
        if (!publicacoesVistas.Add((canal, conteudo, timestamp))) return;
        var registro = new PublicacaoEntrada { Canal = canal, Conteudo = conteudo, Timestamp = timestamp };
        publicacoes.Add(registro);
        AnexarLinha(caminhoPublicacoes, registro);
    }
}

#endregion

#region Estado compartilhado

var trava = new object();
var contador = 0;
double offsetMs = 0;
var rank = 0;
var coordenador = "";
var emEleicao = false;
var anuncioRecebido = new ManualResetEventSlim(false);
var mensagensRecebidas = 0;

var publicador = new PublisherSocket();
publicador.Connect("tcp://proxy:5557");

void Log(string texto) => Console.WriteLine($"[{nome}] {texto}");

#endregion

#region Relogios

double AgoraMs()
{
    lock (trava) return Math.Floor(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + offsetMs);
}

int ProximoContador()
{
    lock (trava) return ++contador;
}

void AtualizarContador(int recebido)
{
    lock (trava) contador = Math.Max(contador, recebido);
}

#endregion

#region Comunicacao

Resposta CriarResposta(string status, string texto, IEnumerable<string>? lista = null)
{
    var resposta = new Resposta
    {
        Status = status,
        Resposta_ = texto,
        ContadorMensagem = ProximoContador(),
        Timestamp = AgoraMs(),
    };
    if (lista != null) resposta.Canais.AddRange(lista);
    return resposta;
}

void Publicar(string topico, RespostaSub publicacao)
{
    lock (trava)
    {
        publicador.SendMoreFrame(topico).SendFrame(publicacao.ToByteArray());
    }
}

byte[]? Requisitar(string url, string acao, string conteudo = "")
{
    var mensagem = new Mensagem
    {
        Timestamp = AgoraMs(),
        Acao = acao,
        Conteudo = conteudo,
        ContadorMensagem = ProximoContador(),
    };

    using var requester = new RequestSocket();
    requester.Options.Linger = TimeSpan.Zero;
    try
    {
        // o Connect resolve o nome na hora e lanca excecao se o servidor caiu (nome sumiu do DNS)
        requester.Connect(url);
        if (!requester.TrySendFrame(tempoLimite, mensagem.ToByteArray())) return null;
        return requester.TryReceiveFrameBytes(tempoLimite, out var dados) ? dados : null;
    }
    catch (Exception erro)
    {
        Log($"sem resposta de {url}: {erro.Message}");
        return null;
    }
}

Resposta? RequisitarServidor(string alvo, string acao, string conteudo = "")
{
    var dados = Requisitar($"tcp://{alvo}:{portaServidores}", acao, conteudo);
    if (dados == null) return null;

    var resposta = Resposta.Parser.ParseFrom(dados);
    AtualizarContador(resposta.ContadorMensagem);
    return resposta;
}

RespostaReferencia? RequisitarReferencia(string acao)
{
    var dados = Requisitar(urlReferencia, acao, nome);
    if (dados == null) return null;

    var resposta = RespostaReferencia.Parser.ParseFrom(dados);
    AtualizarContador(resposta.ContadorMensagem);
    return resposta;
}

#endregion

#region Referencia

int? TentarRank()
{
    var resposta = RequisitarReferencia("consultar_rank");
    return resposta is { Status: "200" } ? resposta.Rank : null;
}

void Heartbeat()
{
    var resposta = RequisitarReferencia("heartbeat");
    if (resposta == null)
    {
        Log("heartbeat sem resposta da referencia");
    }
    else if (resposta.Status == "200")
    {
        Log("heartbeat confirmado");
    }
    else if (TentarRank() is int novo)
    {
        lock (trava) rank = novo;
        Log($"recadastrado na referencia, rank {novo}");
    }
}

#endregion

#region Eleicao

void IniciarEleicao()
{
    lock (trava)
    {
        if (emEleicao) return;
        emEleicao = true;
        anuncioRecebido.Reset();
    }
    new Thread(ExecutarEleicao) { IsBackground = true }.Start();
}

void ExecutarEleicao()
{
    var refazer = false;
    try
    {
        Log("iniciando eleicao...");
        int meuRank;
        lock (trava) meuRank = rank;

        var lista = RequisitarReferencia("listar_servidores");
        var maiores = lista?.Servidores.Where(s => s.Rank > meuRank && s.Nome != nome).ToList() ?? [];

        var algumRespondeu = false;
        foreach (var alvo in maiores)
        {
            if (RequisitarServidor(alvo.Nome, "eleicao", nome)?.Resposta_ == "OK") algumRespondeu = true;
        }

        if (!algumRespondeu)
        {
            TornarSeCoordenador();
        }
        else if (!anuncioRecebido.Wait(TimeSpan.FromSeconds(10)))
        {
            Log("anuncio nao chegou; refazendo eleicao");
            refazer = true;
        }
    }
    finally
    {
        lock (trava) emEleicao = false;
    }
    if (refazer) IniciarEleicao();
}

void TornarSeCoordenador()
{
    lock (trava)
    {
        coordenador = nome;
        offsetMs = 0;
    }
    anuncioRecebido.Set();
    Log($"eleito coordenador (rank {rank})");

    for (var i = 0; i < 2; i++)
    {
        var anuncio = new RespostaSub
        {
            TimestampEnvio = AgoraMs(),
            Canal = "servers",
            Conteudo = nome,
            ContadorMensagem = ProximoContador(),
        };
        Publicar("servers|", anuncio);
        Thread.Sleep(1000);
    }
}

void OuvirAnuncios()
{
    using var sub = new SubscriberSocket();
    sub.Connect("tcp://proxy:5558");
    sub.Subscribe("servers|");

    while (true)
    {
        sub.ReceiveFrameString();
        var anuncio = RespostaSub.Parser.ParseFrom(sub.ReceiveFrameBytes());
        AtualizarContador(anuncio.ContadorMensagem);

        bool mudou;
        lock (trava)
        {
            mudou = coordenador != anuncio.Conteudo;
            coordenador = anuncio.Conteudo;
        }
        anuncioRecebido.Set();
        if (mudou) Log($"novo coordenador anunciado: {anuncio.Conteudo}");
    }
}

#endregion

#region Replicacao (Parte 5)

// Replicacao ativa: cada escrita aceita de um cliente (login, cadastrar, publicar) e aplicada
// localmente e difundida no topico "replicacao|<servidor de origem>"; todos os servidores assinam
// "replicacao|" e aplicam as escritas dos outros. Quem entra (ou volta) pede o estado completo a
// outro servidor com "obter_estado".

const string topicoReplicacao = "replicacao|";

void Replicar(string acao, string canal, string conteudo, double timestamp)
{
    var operacao = new Mensagem
    {
        Timestamp = timestamp,
        Acao = acao,
        Canal = canal,
        Conteudo = conteudo,
        ContadorMensagem = ProximoContador(),
    };
    lock (trava)
    {
        publicador.SendMoreFrame(topicoReplicacao + nome).SendFrame(operacao.ToByteArray());
    }
}

void AplicarOperacao(Mensagem operacao)
{
    switch (operacao.Acao)
    {
        case "login":
            var status = usuarios.Usuarios.Contains(operacao.Conteudo) ? "200" : "404";
            RegistrarLogin(operacao.Conteudo, operacao.Timestamp, status);
            break;
        case "cadastrar":
            RegistrarCanal(operacao.Conteudo);
            break;
        case "publicar":
            RegistrarPublicacao(operacao.Canal, operacao.Conteudo, operacao.Timestamp);
            break;
    }
}

void OuvirReplicacao(ManualResetEventSlim pronto)
{
    using var sub = new SubscriberSocket();
    sub.Connect("tcp://proxy:5558");
    sub.Subscribe(topicoReplicacao);
    pronto.Set();

    while (true)
    {
        var topico = sub.ReceiveFrameString();
        var corpo = sub.ReceiveFrameBytes();
        if (topico == topicoReplicacao + nome) continue; // eco da propria escrita

        var operacao = Mensagem.Parser.ParseFrom(corpo);
        AtualizarContador(operacao.ContadorMensagem);
        AplicarOperacao(operacao);
        Log($"replicado de {topico[topicoReplicacao.Length..]}: {operacao.Acao} {operacao.Canal} {operacao.Conteudo}");
    }
}

string ExportarEstado()
{
    lock (travaDados)
    {
        return JsonSerializer.Serialize(new EstadoServidor
        {
            Canais = canais.Canais,
            Logins = logins,
            Publicacoes = publicacoes,
        });
    }
}

void ImportarEstado(string texto, string origem)
{
    var estado = JsonSerializer.Deserialize<EstadoServidor>(texto)!;
    lock (travaDados)
    {
        foreach (var canal in estado.Canais) RegistrarCanal(canal);
        foreach (var l in estado.Logins) RegistrarLogin(l.Usuario, l.Timestamp, l.Status);
        foreach (var p in estado.Publicacoes) RegistrarPublicacao(p.Canal, p.Conteudo, p.Timestamp);
        File.WriteAllLines(caminhoPublicacoes, publicacoes.Select(p => JsonSerializer.Serialize(p)));
        Log($"estado recebido de {origem}: {canais.Canais.Count} canais, {logins.Count} logins, {publicacoes.Count} publicacoes");
    }
}

// pede o estado ao coordenador (se for outro servidor) ou ao proximo servidor da lista
void ObterEstadoInicial()
{
    string atual;
    lock (trava) atual = coordenador;

    var candidatos = new List<string>();
    if (atual != "" && atual != nome) candidatos.Add(atual);
    var lista = RequisitarReferencia("listar_servidores");
    if (lista != null)
        candidatos.AddRange(lista.Servidores.Select(s => s.Nome).Where(n => n != nome && n != atual));

    foreach (var alvo in candidatos)
    {
        var resposta = RequisitarServidor(alvo, "obter_estado", nome);
        if (resposta is { Status: "200" })
        {
            ImportarEstado(resposta.Resposta_, alvo);
            return;
        }
    }
    Log("nenhum outro servidor respondeu: mantendo os dados locais");
}

#endregion

#region Servidor para servidor

void AtenderServidores()
{
    using var responder = new ResponseSocket();
    responder.Bind($"tcp://*:{portaServidores}");

    while (true)
    {
        var requisicao = Mensagem.Parser.ParseFrom(responder.ReceiveFrameBytes());
        AtualizarContador(requisicao.ContadorMensagem);

        var iniciar = false;
        Resposta resposta;
        if (requisicao.Acao == "obter_estado") // Parte 5: transferencia de estado
        {
            resposta = CriarResposta("200", ExportarEstado());
            Log($"estado enviado para {requisicao.Conteudo}");
        }
        else if (requisicao.Acao == "eleicao")
        {
            resposta = CriarResposta("200", "OK");
            iniciar = true;
        }
        else if (requisicao.Acao == "sincronizar")
        {
            bool souCoordenador;
            lock (trava) souCoordenador = coordenador == nome;
            resposta = souCoordenador
                ? CriarResposta("200", "Hora correta")
                : CriarResposta("400", "Nao sou o coordenador!");
        }
        else
        {
            resposta = CriarResposta("400", "Acao desconhecida!");
        }

        responder.SendFrame(resposta.ToByteArray());
        if (iniciar) IniciarEleicao();
    }
}

void SincronizarRelogio()
{
    string alvo;
    lock (trava) alvo = coordenador;

    if (alvo == "")
    {
        IniciarEleicao();
        return;
    }
    if (alvo == nome)
    {
        Log("sou o coordenador: relogio mantido");
        return;
    }

    var t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    var resposta = RequisitarServidor(alvo, "sincronizar");
    var t1 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    if (resposta is not { Status: "200" })
    {
        Log("coordenador indisponivel; iniciando eleicao");
        IniciarEleicao();
        return;
    }

    var horaCoordenador = resposta.Timestamp + (t1 - t0) / 2.0;
    lock (trava) offsetMs = horaCoordenador - t1;
    Log($"relogio sincronizado com {alvo} (offset {offsetMs:+0;-0;0} ms)");
}

#endregion

#region Acoes dos clientes

Resposta TratarLogin(Mensagem requisicao)
{
    var usuario = requisicao.Conteudo.Trim();
    var resposta = usuarios.Usuarios.Contains(usuario)
        ? CriarResposta("200", "Login realizado com sucesso!")
        : CriarResposta("404", "Nome de usuario incorreto!");

    RegistrarLogin(usuario, requisicao.Timestamp, resposta.Status);
    Replicar("login", "", usuario, requisicao.Timestamp);
    return resposta;
}

Resposta TratarListar()
{
    List<string> lista;
    lock (travaDados) lista = [.. canais.Canais];
    return CriarResposta("200", "Canais listados", lista);
}

Resposta TratarCadastrar(Mensagem requisicao)
{
    var canal = requisicao.Conteudo.Trim();
    if (canal == "" || canal.Length > 50 || canal.Contains('|'))
        return CriarResposta("400", "Nome de canal invalido!");
    if (!RegistrarCanal(canal))
        return CriarResposta("404", "Canal já existe!");

    Replicar("cadastrar", "", canal, requisicao.Timestamp);
    return CriarResposta("200", $"Canal {canal} cadastrado com sucesso!");
}

Resposta TratarPublicar(Mensagem requisicao)
{
    var canal = requisicao.Canal.Trim();
    if (canal == "" || requisicao.Conteudo == "")
        return CriarResposta("400", "Canal e mensagem sao obrigatorios!");

    var publicacao = new RespostaSub
    {
        TimestampEnvio = AgoraMs(),
        Canal = canal,
        Conteudo = requisicao.Conteudo,
        ContadorMensagem = ProximoContador(),
    };
    Publicar($"{canal}|", publicacao);

    RegistrarPublicacao(canal, requisicao.Conteudo, requisicao.Timestamp);
    Replicar("publicar", canal, requisicao.Conteudo, requisicao.Timestamp);
    return CriarResposta("200", $"Mensagem publicada no canal {canal} com sucesso!");
}

#endregion

#region Atendimento de clientes

void AtenderClientes()
{
    using var responder = new ResponseSocket();
    responder.Connect("tcp://broker:5560");

    while (true)
    {
        var requisicao = Mensagem.Parser.ParseFrom(responder.ReceiveFrameBytes());
        AtualizarContador(requisicao.ContadorMensagem);

        Resposta resposta;
        try
        {
            resposta = requisicao.Acao switch
            {
                "login" => TratarLogin(requisicao),
                "listar" => TratarListar(),
                "cadastrar" => TratarCadastrar(requisicao),
                "publicar" => TratarPublicar(requisicao),
                _ => CriarResposta("400", "Acao desconhecida!"),
            };

            AnexarLinha(caminhoMensagens, new
            {
                requisicao = new { requisicao.Acao, requisicao.Canal, requisicao.Conteudo, requisicao.Timestamp, requisicao.ContadorMensagem },
                resposta = new { resposta.Status, Texto = resposta.Resposta_, resposta.Timestamp, resposta.ContadorMensagem },
            });
        }
        catch (Exception erro)
        {
            Log($"erro ao processar mensagem: {erro.Message}");
            resposta = CriarResposta("500", "Erro interno no servidor!");
        }

        responder.SendFrame(resposta.ToByteArray());

        mensagensRecebidas++;
        if (mensagensRecebidas % heartbeatACada == 0) Heartbeat();
        if (mensagensRecebidas % sincronizarACada == 0) SincronizarRelogio();
    }
}

#endregion

#region Inicializacao

Log($"dados em {diretorio}");

new Thread(AtenderServidores) { IsBackground = true }.Start();
new Thread(OuvirAnuncios) { IsBackground = true }.Start();

// Parte 5: assina a replicacao antes de pedir o estado, para nao perder escritas no meio
var replicacaoPronta = new ManualResetEventSlim(false);
new Thread(() => OuvirReplicacao(replicacaoPronta)) { IsBackground = true }.Start();
replicacaoPronta.Wait();

int? rankInicial;
while ((rankInicial = TentarRank()) == null)
{
    Log("aguardando o servico de referencia...");
    Thread.Sleep(1000);
}
lock (trava) rank = rankInicial.Value;
Log($"rank {rank}");

ObterEstadoInicial();
IniciarEleicao();
AtenderClientes();

#endregion
