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

var diretorio = Path.Combine("dados", nome);
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

void AnexarLinha(string caminho, object registro)
{
    File.AppendAllText(caminho, JsonSerializer.Serialize(registro) + "\n");
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
    requester.Connect(url);
    if (!requester.TrySendFrame(tempoLimite, mensagem.ToByteArray())) return null;
    return requester.TryReceiveFrameBytes(tempoLimite, out var dados) ? dados : null;
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
        if (requisicao.Acao == "eleicao")
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

    logins.Add(new LoginEntrada { Usuario = usuario, Timestamp = requisicao.Timestamp, Status = resposta.Status });
    File.WriteAllText(caminhoLogins, JsonSerializer.Serialize(logins));
    return resposta;
}

Resposta TratarCadastrar(Mensagem requisicao)
{
    var canal = requisicao.Conteudo.Trim();
    if (canal == "" || canal.Length > 50 || canal.Contains('|'))
        return CriarResposta("400", "Nome de canal invalido!");
    if (canais.Canais.Contains(canal))
        return CriarResposta("404", "Canal já existe!");

    canais.Canais.Add(canal);
    File.WriteAllText(caminhoCanais, JsonSerializer.Serialize(canais));
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

    AnexarLinha(caminhoPublicacoes, new
    {
        canal,
        conteudo = publicacao.Conteudo,
        timestampEnvioCliente = requisicao.Timestamp,
        timestampPublicacao = publicacao.TimestampEnvio,
        contadorMensagem = publicacao.ContadorMensagem,
    });
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
                "listar" => CriarResposta("200", "Canais listados", canais.Canais),
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

int? rankInicial;
while ((rankInicial = TentarRank()) == null)
{
    Log("aguardando o servico de referencia...");
    Thread.Sleep(1000);
}
lock (trava) rank = rankInicial.Value;
Log($"rank {rank}");

IniciarEleicao();
AtenderClientes();

#endregion
