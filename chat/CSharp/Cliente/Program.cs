using ChatCSharp.Gen;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

string[] usuarios = ["usuario1", "usuario2", "usuario3", "teste", "usuario4", "usuario5"];

using var requester = new RequestSocket();
requester.Connect("tcp://broker:5559");
Console.WriteLine("Cliente conectado no broker...");

var indiceUsuario = 0;
var reply = Login(requester, usuarios[indiceUsuario]);

while (reply.Status != "200")
{
    Console.WriteLine("Nome de usuario incorreto!");

    indiceUsuario++;
    if (indiceUsuario >= usuarios.Length)
    {
        Console.WriteLine("Nenhum usuario da lista funcionou.");
        Environment.Exit(1);
    }

    reply = Login(requester, usuarios[indiceUsuario]);
}

var canaisResp = ListarCanais(requester);
Console.WriteLine("Canais disponiveis: " + canaisResp.Resposta_);

var teste2 = CadastrarCanal(requester, "canal14");
if (teste2.Status == "200")
{
    Console.WriteLine(teste2.Resposta_);
}
else
{
    Console.WriteLine("Erro:" + teste2.Resposta_);
}

Environment.Exit(0);

static Resposta Login(RequestSocket requester, string usuario)
{
    var payload = new Mensagem
    {
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Acao = "login",
        Conteudo = usuario,
    };
    return EnviarERecever(requester, payload);
}

static Resposta ListarCanais(RequestSocket requester)
{
    var payload = new Mensagem
    {
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Acao = "listar",
    };
    return EnviarERecever(requester, payload);
}

static Resposta CadastrarCanal(RequestSocket requester, string canal)
{
    var payload = new Mensagem
    {
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Acao = "cadastrar",
        Conteudo = canal,
    };
    return EnviarERecever(requester, payload);
}

static Resposta EnviarERecever(RequestSocket requester, Mensagem payload)
{
    requester.SendFrame(payload.ToByteArray());
    var respostaBytes = requester.ReceiveFrameBytes();
    return Resposta.Parser.ParseFrom(respostaBytes);
}
