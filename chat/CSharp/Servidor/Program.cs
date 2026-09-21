using System.Text.Json;
using ChatCSharp.Gen;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

const string caminhoUsuarios = "usuarios.json";
const string caminhoCanais = "canais.json";
const string caminhoLogins = "login.json";

var usuarios = JsonSerializer.Deserialize<UsuariosArquivo>(File.ReadAllText(caminhoUsuarios))!;
var canais = JsonSerializer.Deserialize<CanaisArquivo>(File.ReadAllText(caminhoCanais))!;
var logins = JsonSerializer.Deserialize<List<LoginEntrada>>(File.ReadAllText(caminhoLogins))!;

using var responder = new ResponseSocket();
responder.Connect("tcp://broker:5560");
Console.WriteLine("Servidor conectado no broker...");

while (true)
{
    byte[] requestBytes;
    try
    {
        requestBytes = responder.ReceiveFrameBytes();
    }
    catch (Exception)
    {
        break;
    }

    var requisicao = Mensagem.Parser.ParseFrom(requestBytes);

    Resposta resposta;

    switch (requisicao.Acao)
    {
        case "login":
        {
            var usuario = requisicao.Conteudo;
            var entrada = new LoginEntrada { Usuario = usuario, Timestamp = requisicao.Timestamp };

            if (usuarios.Usuarios.Contains(usuario))
            {
                resposta = new Resposta { Status = "200", Resposta_ = "Login realizado com sucesso!" };
                entrada.Status = "200";
            }
            else
            {
                resposta = new Resposta { Status = "404", Resposta_ = "Nome de usuario incorreto!" };
                entrada.Status = "404";
            }

            logins.Add(entrada);
            File.WriteAllText(caminhoLogins,JsonSerializer.Serialize(logins));
            break;
        }

        case "listar":
            resposta = new Resposta
            {
                Status = "200",
                Resposta_ = "Canais disponiveis: " + string.Join(", ", canais.Canais),
            };
            break;

        case "cadastrar":
        {
            var canal = requisicao.Conteudo;
            if (canais.Canais.Contains(canal))
            {
                resposta = new Resposta { Status = "404", Resposta_ = "Canal já existe!" };
            }
            else
            {
                canais.Canais.Add(canal);
                File.WriteAllText(caminhoCanais,JsonSerializer.Serialize(canais));
                resposta = new Resposta { Status = "200", Resposta_ = $"Canal {canal} cadastrado com sucesso!" };
            }
            break;
        }

        default:
            resposta = new Resposta { Status = "404", Resposta_ = "Acao desconhecida" };
            break;
    }

    responder.SendFrame(resposta.ToByteArray());
}
