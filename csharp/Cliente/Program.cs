using ChatCSharp.Gen;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

#region Configuracao

const int minCanais = 5;
const int minInscricoes = 3;
const int mensagensPorRodada = 10;

string[] usuarios = ["usuario_inexistente", "teste", "teste2", "teste3", "teste4", "teste5", "teste6", "teste7", "teste8", "teste9", "teste10"];
string[] palavras = ["ola", "bom dia", "alguem ai?", "teste", "sistemas distribuidos", "zeromq", "protobuf", "csharp"];

var aleatorio = new Random();
var contador = 0;
var inscritos = new HashSet<string>();

#endregion

#region Sockets

using var requester = new RequestSocket();
requester.Connect("tcp://broker:5559");
using var subscriber = new SubscriberSocket();
subscriber.Connect("tcp://proxy:5558");
Console.WriteLine("Cliente conectado no broker e no proxy...");

#endregion

#region Comunicacao

Resposta Requisitar(string acao, string canal = "", string conteudo = "")
{
    var mensagem = new Mensagem
    {
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Acao = acao,
        Canal = canal,
        Conteudo = conteudo,
        ContadorMensagem = ++contador,
    };
    requester.SendFrame(mensagem.ToByteArray());
    var resposta = Resposta.Parser.ParseFrom(requester.ReceiveFrameBytes());
    contador = Math.Max(contador, resposta.ContadorMensagem);
    return resposta;
}

void Esperar()
{
    var fim = DateTime.UtcNow.AddSeconds(1);
    while (DateTime.UtcNow < fim)
    {
        if (subscriber.TryReceiveFrameString(fim - DateTime.UtcNow, out _))
        {
            MostrarPublicacao();
        }
    }
}

void MostrarPublicacao()
{
    var publicacao = RespostaSub.Parser.ParseFrom(subscriber.ReceiveFrameBytes());
    contador = Math.Max(contador, publicacao.ContadorMensagem);
    Console.WriteLine($"canal={publicacao.Canal} mensagem={publicacao.Conteudo} " +
        $"envio={publicacao.TimestampEnvio} recebimento={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()} " +
        $"contador={publicacao.ContadorMensagem}");
}

#endregion

#region Login

var candidatos = usuarios.Take(1).Concat(usuarios.Skip(1).OrderBy(_ => aleatorio.Next())).ToList();
string? usuario = null;
foreach (var candidato in candidatos)
{
    var resposta = Requisitar("login", conteudo: candidato);
    if (resposta.Status == "200")
    {
        usuario = candidato;
        Console.WriteLine($"[{usuario}] {resposta.Resposta_}");
        break;
    }
    Console.WriteLine($"[{candidato}] Erro {resposta.Status}: {resposta.Resposta_}");
}

if (usuario == null)
{
    Console.WriteLine("Nenhum usuario da lista funcionou.");
    return 1;
}

#endregion

#region Bot

while (true)
{
    var canais = Requisitar("listar").Canais.ToList();

    if (canais.Count < minCanais)
    {
        var novo = $"canal{aleatorio.Next(1000, 10000)}";
        var resposta = Requisitar("cadastrar", conteudo: novo);
        Console.WriteLine($"[{usuario}] criar {novo}: {resposta.Status} {resposta.Resposta_}");
        canais = Requisitar("listar").Canais.ToList();
    }

    if (inscritos.Count < minInscricoes)
    {
        var disponiveis = canais.Where(c => !inscritos.Contains(c)).ToList();
        if (disponiveis.Count > 0)
        {
            var escolhido = disponiveis[aleatorio.Next(disponiveis.Count)];
            subscriber.Subscribe($"{escolhido}|");
            inscritos.Add(escolhido);
            Console.WriteLine($"[{usuario}] inscrito no {escolhido}");
        }
    }

    if (canais.Count == 0)
    {
        Esperar();
        continue;
    }

    var canal = canais[aleatorio.Next(canais.Count)];
    for (var i = 1; i <= mensagensPorRodada; i++)
    {
        var texto = $"{usuario}: {palavras[aleatorio.Next(palavras.Length)]} ({i}/{mensagensPorRodada})";
        var resposta = Requisitar("publicar", canal, texto);
        if (resposta.Status != "200")
        {
            Console.WriteLine($"[{usuario}] erro ao publicar em {canal}: {resposta.Status} {resposta.Resposta_}");
        }
        Esperar();
    }
}

#endregion
