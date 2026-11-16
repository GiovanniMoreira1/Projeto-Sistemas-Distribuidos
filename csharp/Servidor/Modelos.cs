using System.Text.Json.Serialization;

class UsuariosArquivo
{
    [JsonPropertyName("usuarios")]
    public List<string> Usuarios { get; set; } = [];
}

class CanaisArquivo
{
    [JsonPropertyName("canais")]
    public List<string> Canais { get; set; } = [];
}

class PublicacaoEntrada
{
    [JsonPropertyName("canal")]
    public string Canal { get; set; } = string.Empty;

    [JsonPropertyName("conteudo")]
    public string Conteudo { get; set; } = string.Empty;

    // timestamp do envio pelo cliente (o formato antigo usava "timestampEnvioCliente")
    [JsonPropertyName("timestamp")]
    public double Timestamp { get; set; }

    [JsonPropertyName("timestampEnvioCliente")]
    public double TimestampAntigo { set => Timestamp = value; }
}

// estado completo trocado na transferencia de estado entre servidores (Parte 5)
class EstadoServidor
{
    [JsonPropertyName("canais")]
    public List<string> Canais { get; set; } = [];

    [JsonPropertyName("logins")]
    public List<LoginEntrada> Logins { get; set; } = [];

    [JsonPropertyName("publicacoes")]
    public List<PublicacaoEntrada> Publicacoes { get; set; } = [];
}

class LoginEntrada
{
    [JsonPropertyName("usuario")]
    public string Usuario { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public double Timestamp { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;
}
