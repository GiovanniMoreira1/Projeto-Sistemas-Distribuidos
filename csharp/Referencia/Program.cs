using ChatCSharp.Gen;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

#region Configuracao

const int tempoLimiteSegundos = 30;

var servidores = new Dictionary<string, (int Rank, DateTime Heartbeat)>();
var proximoRank = 1;
var contador = 0;

#endregion

#region Socket

using var responder = new ResponseSocket();
responder.Bind("tcp://*:5556");
Console.WriteLine("Referencia aguardando mensagens...");

#endregion

#region Funcoes

RespostaReferencia Criar(string status, string texto, int rank = 0)
{
    contador++;
    return new RespostaReferencia { Status = status, Resposta = texto, Rank = rank, ContadorMensagem = contador };
}

void RemoverInativos()
{
    var inativos = servidores
        .Where(s => (DateTime.UtcNow - s.Value.Heartbeat).TotalSeconds > tempoLimiteSegundos)
        .Select(s => s.Key)
        .ToList();

    foreach (var nome in inativos)
    {
        Console.WriteLine($"Servidor removido: {nome}");
        servidores.Remove(nome);
    }
}

#endregion

#region Atendimento

while (true)
{
    if (!responder.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out var dados))
    {
        RemoverInativos();
        continue;
    }

    RemoverInativos();

    RespostaReferencia resposta;
    try
    {
        var mensagem = Mensagem.Parser.ParseFrom(dados);
        contador = Math.Max(contador, mensagem.ContadorMensagem);
        var nome = mensagem.Conteudo;
        Console.WriteLine($"Acao recebida: {mensagem.Acao}");

        switch (mensagem.Acao)
        {
            case "consultar_rank":
                if (!servidores.ContainsKey(nome))
                {
                    servidores[nome] = (proximoRank++, DateTime.UtcNow);
                    Console.WriteLine($"Servidor cadastrado: {nome} | Rank: {servidores[nome].Rank}");
                }
                else
                {
                    servidores[nome] = (servidores[nome].Rank, DateTime.UtcNow);
                }
                resposta = Criar("200", "Servidor cadastrado com sucesso!", servidores[nome].Rank);
                break;

            case "listar_servidores":
                resposta = Criar("200", "Servidores disponiveis listados!");
                foreach (var (n, dadosServidor) in servidores)
                {
                    resposta.Servidores.Add(new ServidorInfo { Nome = n, Rank = dadosServidor.Rank });
                }
                break;

            case "heartbeat":
                if (servidores.ContainsKey(nome))
                {
                    servidores[nome] = (servidores[nome].Rank, DateTime.UtcNow);
                    resposta = Criar("200", "Heartbeat recebido!", servidores[nome].Rank);
                }
                else
                {
                    resposta = Criar("404", "Servidor nao cadastrado!");
                }
                break;

            default:
                resposta = Criar("400", "Acao desconhecida!");
                break;
        }
    }
    catch (Exception erro)
    {
        Console.WriteLine($"Erro na referencia: {erro.Message}");
        resposta = Criar("500", "Erro interno na referencia!");
    }

    responder.SendFrame(resposta.ToByteArray());
}

#endregion
