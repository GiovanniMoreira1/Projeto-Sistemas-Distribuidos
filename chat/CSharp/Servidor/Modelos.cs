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

class LoginEntrada
{
    [JsonPropertyName("usuario")]
    public string Usuario { get; set; } = "";

    [JsonPropertyName("timestamp")]
    public float Timestamp { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "";
}
