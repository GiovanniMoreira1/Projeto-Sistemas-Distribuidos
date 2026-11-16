use std::collections::HashSet;
use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chat::agora_ms;
use chat::proto::{Mensagem, Resposta, RespostaReferencia, RespostaSub};
use prost::Message as _;
use serde::{Deserialize, Serialize};

const PORTA_SERVIDORES: u16 = 5559;
const TEMPO_LIMITE_MS: i32 = 3000;

#[derive(Serialize, Deserialize)]
struct UsuariosData {
    usuarios: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct CanaisData {
    canais: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct LoginRegistro {
    usuario: String,
    timestamp: f64,
    #[serde(default)]
    status: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct PublicacaoRegistro {
    canal: String,
    conteudo: String,
    #[serde(default)]
    timestamp: f64,
}

/// Estado completo trocado na transferencia de estado entre servidores (Parte 5).
#[derive(Serialize, Deserialize, Default)]
struct EstadoServidor {
    #[serde(default)]
    canais: Vec<String>,
    #[serde(default)]
    logins: Vec<LoginRegistro>,
    #[serde(default)]
    publicacoes: Vec<PublicacaoRegistro>,
}

/// Dados persistidos do servidor. Ficam num Mutex porque, a partir da Parte 5, tambem sao
/// alterados pela thread de replicacao e lidos por quem pede a transferencia de estado.
struct Dados {
    arquivos: Arquivos,
    usuarios: UsuariosData,
    canais: CanaisData,
    logins: Vec<LoginRegistro>,
    publicacoes: Vec<PublicacaoRegistro>,
    // chaves do que ja esta guardado: aplicar a mesma operacao duas vezes nao duplica o registro
    logins_vistos: HashSet<(String, u64)>,
    publicacoes_vistas: HashSet<(String, String, u64)>,
}

impl Dados {
    fn carregar(arquivos: Arquivos) -> Dados {
        let usuarios: UsuariosData = ler_json("usuarios.json", UsuariosData { usuarios: Vec::new() });
        let canais: CanaisData = ler_json(&arquivos.canais, CanaisData { canais: Vec::new() });
        let logins: Vec<LoginRegistro> = ler_json(&arquivos.logins, Vec::new());
        let publicacoes: Vec<PublicacaoRegistro> = ler_json(&arquivos.publicacoes, Vec::new());
        let logins_vistos = logins.iter().map(|l| (l.usuario.clone(), l.timestamp.to_bits())).collect();
        let publicacoes_vistas = publicacoes
            .iter()
            .map(|p| (p.canal.clone(), p.conteudo.clone(), p.timestamp.to_bits()))
            .collect();
        Dados { arquivos, usuarios, canais, logins, publicacoes, logins_vistos, publicacoes_vistas }
    }

    fn status_login(&self, usuario: &str) -> &'static str {
        if self.usuarios.usuarios.iter().any(|u| u == usuario) { "200" } else { "404" }
    }

    fn registrar_login(&mut self, usuario: &str, timestamp: f64, status: &str) {
        if !self.logins_vistos.insert((usuario.to_string(), timestamp.to_bits())) {
            return;
        }
        self.logins.push(LoginRegistro {
            usuario: usuario.to_string(),
            timestamp,
            status: status.to_string(),
        });
        salvar_json(&self.arquivos.logins, &self.logins);
    }

    /// Devolve false se o canal ja existia.
    fn registrar_canal(&mut self, canal: &str) -> bool {
        if self.canais.canais.iter().any(|c| c == canal) {
            return false;
        }
        self.canais.canais.push(canal.to_string());
        salvar_json(&self.arquivos.canais, &self.canais);
        true
    }

    fn registrar_publicacao(&mut self, canal: &str, conteudo: &str, timestamp: f64) {
        let chave = (canal.to_string(), conteudo.to_string(), timestamp.to_bits());
        if !self.publicacoes_vistas.insert(chave) {
            return;
        }
        self.publicacoes.push(PublicacaoRegistro {
            canal: canal.to_string(),
            conteudo: conteudo.to_string(),
            timestamp,
        });
        salvar_json(&self.arquivos.publicacoes, &self.publicacoes);
    }

    fn exportar(&self) -> String {
        let estado = EstadoServidor {
            canais: self.canais.canais.clone(),
            logins: self.logins.clone(),
            publicacoes: self.publicacoes.clone(),
        };
        serde_json::to_string(&estado).unwrap_or_default()
    }

    /// Junta o estado recebido com o local (sem duplicar).
    fn importar(&mut self, texto: &str) -> Result<(), serde_json::Error> {
        let estado: EstadoServidor = serde_json::from_str(texto)?;
        for canal in &estado.canais {
            self.registrar_canal(canal);
        }
        for l in &estado.logins {
            self.registrar_login(&l.usuario, l.timestamp, &l.status);
        }
        for p in &estado.publicacoes {
            self.registrar_publicacao(&p.canal, &p.conteudo, p.timestamp);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MensagemRegistro {
    usuario: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    acao: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    conteudo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    canal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contador_mensagem: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    canais: Option<Vec<String>>,
}

struct Estado {
    nome_servidor: String,
    rank_servidor: i32,
    contador_mensagens: i32,
    offset_relogio: f64,
    servidor_eleito: String,
    em_eleicao: bool,
    anuncio_recebido: bool,
}

impl Estado {
    fn timestamp_ajustado(&self) -> f64 {
        agora_ms() + self.offset_relogio
    }

    fn proximo_contador(&mut self) -> i32 {
        self.contador_mensagens += 1;
        self.contador_mensagens
    }

    fn atualizar_contador(&mut self, recebido: i32) {
        self.contador_mensagens = self.contador_mensagens.max(recebido);
    }

    fn criar_resposta(&mut self, status: &str, resposta: &str, canais: Vec<String>) -> Resposta {
        let contador = self.proximo_contador();
        Resposta {
            status: status.to_string(),
            resposta: resposta.to_string(),
            canais,
            contador_mensagem: contador,
            timestamp: self.timestamp_ajustado(),
        }
    }
}

/// Le um json do disco; se o arquivo ainda nao existir (servidor novo), usa o valor padrao.
fn ler_json<T: for<'de> Deserialize<'de>>(caminho: &str, padrao: T) -> T {
    match fs::read_to_string(caminho) {
        Ok(conteudo) => serde_json::from_str(&conteudo).expect("falha ao parsear json"),
        Err(_) => padrao,
    }
}

fn salvar_json<T: Serialize>(caminho: &str, dados: &T) {
    let texto = serde_json::to_string(dados).expect("falha ao serializar json");
    fs::write(caminho, texto).expect("falha ao salvar arquivo json");
}

/// Caminhos dos arquivos deste servidor: cada servidor tem o seu proprio diretorio
/// (DADOS_DIR/NOME_SERVIDOR), nada e compartilhado entre servidores.
struct Arquivos {
    canais: String,
    logins: String,
    publicacoes: String,
    mensagens: String,
}

impl Arquivos {
    fn new(nome_servidor: &str) -> Arquivos {
        let base = std::env::var("DADOS_DIR").unwrap_or_else(|_| "dados".to_string());
        let dir = std::path::Path::new(&base).join(nome_servidor);
        fs::create_dir_all(&dir).expect("falha ao criar diretorio de dados");
        let caminho = |nome: &str| dir.join(nome).to_string_lossy().into_owned();
        Arquivos {
            canais: caminho("canais.json"),
            logins: caminho("login.json"),
            publicacoes: caminho("publicacoes.json"),
            mensagens: caminho("mensagens.json"),
        }
    }
}

fn requisitar<R: prost::Message + Default>(
    ctx: &zmq::Context,
    url: &str,
    payload: &Mensagem,
) -> Option<R> {
    let socket = ctx.socket(zmq::REQ).ok()?;
    socket.set_sndtimeo(TEMPO_LIMITE_MS).ok()?;
    socket.set_rcvtimeo(TEMPO_LIMITE_MS).ok()?;
    socket.set_linger(0).ok()?;
    socket.connect(url).ok()?;

    socket.send(payload.encode_to_vec(), 0).ok()?;
    let reply = socket.recv_bytes(0).ok()?;
    R::decode(reply.as_slice()).ok()
}

fn consultar_rank(
    referencia: &zmq::Socket,
    estado: &Arc<Mutex<Estado>>,
) -> Result<i32, String> {
    let payload = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        Mensagem {
            timestamp: st.timestamp_ajustado(),
            acao: "consultar_rank".to_string(),
            canal: String::new(),
            conteudo: st.nome_servidor.clone(),
            contador_mensagem: contador,
        }
    };

    referencia
        .send(payload.encode_to_vec(), 0)
        .map_err(|e| e.to_string())?;
    let reply = referencia.recv_bytes(0).map_err(|e| e.to_string())?;
    let objeto = RespostaReferencia::decode(reply.as_slice()).map_err(|e| e.to_string())?;

    estado.lock().unwrap().atualizar_contador(objeto.contador_mensagem);

    if objeto.status != "200" {
        return Err(objeto.resposta);
    }

    Ok(objeto.rank)
}

fn listar_servidores(
    ctx: &zmq::Context,
    estado: &Arc<Mutex<Estado>>,
) -> Vec<chat::proto::ServidorInfo> {
    let payload = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        Mensagem {
            timestamp: st.timestamp_ajustado(),
            acao: "listar_servidores".to_string(),
            canal: String::new(),
            conteudo: st.nome_servidor.clone(),
            contador_mensagem: contador,
        }
    };

    match requisitar::<RespostaReferencia>(ctx, "tcp://referencia:5556", &payload) {
        Some(objeto) => {
            estado.lock().unwrap().atualizar_contador(objeto.contador_mensagem);
            if objeto.status == "200" {
                objeto.servidores
            } else {
                Vec::new()
            }
        }
        None => Vec::new(),
    }
}

fn anunciar_coordenador(
    pub_sock: &Arc<Mutex<zmq::Socket>>,
    estado: &Arc<Mutex<Estado>>,
) {
    let payload_sub = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        RespostaSub {
            timestamp_envio: st.timestamp_ajustado(),
            canal: "servers".to_string(),
            conteudo: st.nome_servidor.clone(),
            timestamp_recebimento: 0.0,
            contador_mensagem: contador,
        }
    };

    let sock = pub_sock.lock().unwrap();
    if let Err(err) = sock.send_multipart([b"servers|".as_slice(), &payload_sub.encode_to_vec()], 0) {
        eprintln!("Erro ao anunciar coordenador: {err}");
    }
}

fn iniciar_eleicao(
    ctx: zmq::Context,
    pub_sock: Arc<Mutex<zmq::Socket>>,
    estado: Arc<Mutex<Estado>>,
) {
    let (nome_servidor, rank_servidor) = {
        let mut st = estado.lock().unwrap();
        if st.em_eleicao {
            return;
        }
        st.em_eleicao = true;
        st.anuncio_recebido = false;
        (st.nome_servidor.clone(), st.rank_servidor)
    };

    println!("Iniciando eleição...");

    let servidores = listar_servidores(&ctx, &estado);
    let maiores: Vec<_> = servidores
        .into_iter()
        .filter(|s| s.rank > rank_servidor && s.nome != nome_servidor)
        .collect();

    let mut algum_respondeu = false;
    for alvo in maiores {
        let payload = {
            let mut st = estado.lock().unwrap();
            let contador = st.proximo_contador();
            Mensagem {
                timestamp: st.timestamp_ajustado(),
                acao: "eleicao".to_string(),
                canal: String::new(),
                conteudo: nome_servidor.clone(),
                contador_mensagem: contador,
            }
        };

        let url = format!("tcp://{}:{PORTA_SERVIDORES}", alvo.nome);
        if let Some(objeto) = requisitar::<Resposta>(&ctx, &url, &payload) {
            estado.lock().unwrap().atualizar_contador(objeto.contador_mensagem);
            if objeto.resposta == "OK" {
                algum_respondeu = true;
            }
        }
    }

    if algum_respondeu {
        println!("Servidor de rank maior assumiu a eleição. Aguardando anúncio...");
        estado.lock().unwrap().em_eleicao = false;

        let ctx_timer = ctx.clone();
        let pub_timer = Arc::clone(&pub_sock);
        let estado_timer = Arc::clone(&estado);
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(10));
            let precisa_reiniciar = !estado_timer.lock().unwrap().anuncio_recebido;
            if precisa_reiniciar {
                iniciar_eleicao(ctx_timer, pub_timer, estado_timer);
            }
        });
    } else {
        {
            let mut st = estado.lock().unwrap();
            st.servidor_eleito = nome_servidor.clone();
            st.offset_relogio = 0.0;
            st.anuncio_recebido = true;
            st.em_eleicao = false;
            println!("Servidor eleito como coordenador: {}", st.servidor_eleito);
        }
        anunciar_coordenador(&pub_sock, &estado);
    }
}

fn ouvir_anuncios(sub: zmq::Socket, estado: Arc<Mutex<Estado>>) {
    loop {
        match sub.recv_multipart(0) {
            Ok(frames) => {
                if frames.len() < 2 {
                    continue;
                }
                match RespostaSub::decode(frames[1].as_slice()) {
                    Ok(objeto) => {
                        let mut st = estado.lock().unwrap();
                        st.atualizar_contador(objeto.contador_mensagem);
                        st.servidor_eleito = objeto.conteudo;
                        st.anuncio_recebido = true;
                        println!("Novo coordenador anunciado: {}", st.servidor_eleito);
                    }
                    Err(err) => println!("Erro ao processar anúncio: {err}"),
                }
            }
            Err(err) => {
                println!("Erro ao ouvir anúncios: {err}");
                break;
            }
        }
    }
}

fn atender_servidores(
    ctx: zmq::Context,
    socket: zmq::Socket,
    pub_sock: Arc<Mutex<zmq::Socket>>,
    estado: Arc<Mutex<Estado>>,
    dados: Arc<Mutex<Dados>>,
) {
    loop {
        let request = match socket.recv_bytes(0) {
            Ok(req) => req,
            Err(err) => {
                println!("Erro ao receber de servidor: {err}");
                continue;
            }
        };

        let payload = match Mensagem::decode(request.as_slice()) {
            Ok(objeto) => {
                // Parte 5: transferencia de estado (exportado antes de travar o Estado)
                let estado_exportado = if objeto.acao == "obter_estado" {
                    Some(dados.lock().unwrap().exportar())
                } else {
                    None
                };

                let mut iniciar = false;
                let resp = {
                    let mut st = estado.lock().unwrap();
                    st.atualizar_contador(objeto.contador_mensagem);

                    match objeto.acao.as_str() {
                        "obter_estado" => {
                            println!("Estado enviado para {}", objeto.conteudo);
                            st.criar_resposta("200", &estado_exportado.unwrap_or_default(), Vec::new())
                        }
                        "eleicao" => {
                            iniciar = true;
                            st.criar_resposta("200", "OK", Vec::new())
                        }
                        "sincronizar" => {
                            if st.servidor_eleito == st.nome_servidor {
                                st.criar_resposta("200", "Hora correta", Vec::new())
                            } else {
                                st.criar_resposta("400", "Não sou o coordenador!", Vec::new())
                            }
                        }
                        _ => st.criar_resposta("400", "Ação desconhecida!", Vec::new()),
                    }
                };

                let _ = socket.send(resp.encode_to_vec(), 0);

                if iniciar {
                    let ctx_c = ctx.clone();
                    let pub_c = Arc::clone(&pub_sock);
                    let st_c = Arc::clone(&estado);
                    thread::spawn(move || iniciar_eleicao(ctx_c, pub_c, st_c));
                }
                continue;
            }
            Err(err) => {
                println!("Erro ao atender servidor: {err}");
                estado
                    .lock()
                    .unwrap()
                    .criar_resposta("500", "Erro interno no servidor!", Vec::new())
            }
        };

        let _ = socket.send(payload.encode_to_vec(), 0);
    }
}

fn sincronizar_com_servidor_eleito(
    ctx: &zmq::Context,
    pub_sock: &Arc<Mutex<zmq::Socket>>,
    estado: &Arc<Mutex<Estado>>,
) {
    let (servidor_eleito, nome_servidor, payload) = {
        let mut st = estado.lock().unwrap();
        if st.servidor_eleito.is_empty() {
            drop(st);
            let ctx_c = ctx.clone();
            let pub_c = Arc::clone(pub_sock);
            let st_c = Arc::clone(estado);
            thread::spawn(move || iniciar_eleicao(ctx_c, pub_c, st_c));
            return;
        }
        if st.servidor_eleito == st.nome_servidor {
            println!("Este servidor é o coordenador. Relógio mantido.");
            return;
        }
        let contador = st.proximo_contador();
        let payload = Mensagem {
            timestamp: st.timestamp_ajustado(),
            acao: "sincronizar".to_string(),
            canal: String::new(),
            conteudo: st.nome_servidor.clone(),
            contador_mensagem: contador,
        };
        (st.servidor_eleito.clone(), st.nome_servidor.clone(), payload)
    };

    let t0 = agora_ms();
    let url = format!("tcp://{servidor_eleito}:{PORTA_SERVIDORES}");
    let resposta = requisitar::<Resposta>(ctx, &url, &payload);
    let t1 = agora_ms();

    match resposta {
        Some(objeto) if objeto.status == "200" => {
            let mut st = estado.lock().unwrap();
            st.atualizar_contador(objeto.contador_mensagem);
            let hora_coordenador = objeto.timestamp + (t1 - t0) / 2.0;
            st.offset_relogio = hora_coordenador - t1;
            println!(
                "Relógio sincronizado com {servidor_eleito} ({nome_servidor}): {}",
                agora_ms() + st.offset_relogio
            );
        }
        _ => {
            println!("Coordenador indisponível. Iniciando eleição...");
            let ctx_c = ctx.clone();
            let pub_c = Arc::clone(pub_sock);
            let st_c = Arc::clone(estado);
            thread::spawn(move || iniciar_eleicao(ctx_c, pub_c, st_c));
        }
    }
}

fn heartbeat(referencia: &zmq::Socket, estado: &Arc<Mutex<Estado>>) {
    let payload = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        Mensagem {
            timestamp: st.timestamp_ajustado(),
            acao: "heartbeat".to_string(),
            canal: String::new(),
            conteudo: st.nome_servidor.clone(),
            contador_mensagem: contador,
        }
    };

    if let Err(err) = referencia.send(payload.encode_to_vec(), 0) {
        println!("Erro ao enviar heartbeat: {err}");
        return;
    }

    if let Ok(reply) = referencia.recv_bytes(0) {
        if let Ok(objeto) = RespostaReferencia::decode(reply.as_slice()) {
            estado.lock().unwrap().atualizar_contador(objeto.contador_mensagem);
            if objeto.status == "200" {
                println!("Heartbeat confirmado.");
            } else {
                // a referencia removeu este servidor (ficou muito tempo sem heartbeat): cadastra de novo
                println!("Erro no heartbeat: {}", objeto.resposta);
                if let Ok(rank) = consultar_rank(referencia, estado) {
                    estado.lock().unwrap().rank_servidor = rank;
                    println!("Recadastrado na referência, rank {rank}");
                }
            }
        }
    }
}

/// Publica uma vez no canal (topico "<canal>|"); o bot e quem manda 10 mensagens por rodada.
fn publicar(
    pub_sock: &Arc<Mutex<zmq::Socket>>,
    estado: &Arc<Mutex<Estado>>,
    dados: &Arc<Mutex<Dados>>,
    req: &Mensagem,
) -> zmq::Result<()> {
    let payload_sub = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        RespostaSub {
            timestamp_envio: st.timestamp_ajustado(),
            canal: req.canal.clone(),
            conteudo: req.conteudo.clone(),
            timestamp_recebimento: 0.0,
            contador_mensagem: contador,
        }
    };

    let topico = format!("{}|", req.canal);
    pub_sock
        .lock()
        .unwrap()
        .send_multipart([topico.as_bytes(), &payload_sub.encode_to_vec()], 0)?;

    dados
        .lock()
        .unwrap()
        .registrar_publicacao(&req.canal, &req.conteudo, req.timestamp);
    replicar(pub_sock, estado, "publicar", &req.canal, &req.conteudo, req.timestamp);

    println!("Mensagem publicada: {}", req.conteudo);
    Ok(())
}

// ---------------------------------------------------------------- Parte 5: replicacao
// Replicacao ativa: cada escrita aceita de um cliente (login, cadastrar, publicar) e aplicada
// localmente e difundida no topico "replicacao|<servidor de origem>"; todos os servidores assinam
// "replicacao|" e aplicam as escritas dos outros. Quem entra (ou volta) pede o estado completo a
// outro servidor com "obter_estado".

const TOPICO_REPLICACAO: &str = "replicacao|";

fn replicar(
    pub_sock: &Arc<Mutex<zmq::Socket>>,
    estado: &Arc<Mutex<Estado>>,
    acao: &str,
    canal: &str,
    conteudo: &str,
    timestamp: f64,
) {
    let (topico, operacao) = {
        let mut st = estado.lock().unwrap();
        let contador = st.proximo_contador();
        (
            format!("{TOPICO_REPLICACAO}{}", st.nome_servidor),
            Mensagem {
                timestamp,
                acao: acao.to_string(),
                canal: canal.to_string(),
                conteudo: conteudo.to_string(),
                contador_mensagem: contador,
            },
        )
    };

    let sock = pub_sock.lock().unwrap();
    if let Err(err) = sock.send_multipart([topico.as_bytes(), &operacao.encode_to_vec()], 0) {
        println!("Erro ao replicar: {err}");
    }
}

/// Aplica uma escrita feita por outro servidor (sem responder cliente nem republicar no canal).
fn aplicar_operacao(dados: &Arc<Mutex<Dados>>, operacao: &Mensagem) {
    let mut d = dados.lock().unwrap();
    match operacao.acao.as_str() {
        "login" => {
            let status = d.status_login(&operacao.conteudo);
            d.registrar_login(&operacao.conteudo, operacao.timestamp, status);
        }
        "cadastrar" => {
            d.registrar_canal(&operacao.conteudo);
        }
        "publicar" => d.registrar_publicacao(&operacao.canal, &operacao.conteudo, operacao.timestamp),
        _ => {}
    }
}

fn ouvir_replicacao(sub: zmq::Socket, estado: Arc<Mutex<Estado>>, dados: Arc<Mutex<Dados>>) {
    let proprio = format!("{TOPICO_REPLICACAO}{}", estado.lock().unwrap().nome_servidor);
    loop {
        let frames = match sub.recv_multipart(0) {
            Ok(frames) if frames.len() >= 2 => frames,
            Ok(_) => continue,
            Err(err) => {
                println!("Erro ao ouvir replicação: {err}");
                break;
            }
        };
        let topico = String::from_utf8_lossy(&frames[0]).into_owned();
        if topico == proprio {
            continue; // eco da propria escrita: ja foi aplicada
        }
        match Mensagem::decode(frames[1].as_slice()) {
            Ok(operacao) => {
                estado.lock().unwrap().atualizar_contador(operacao.contador_mensagem);
                aplicar_operacao(&dados, &operacao);
                println!(
                    "Replicado de {}: {} {} {}",
                    &topico[TOPICO_REPLICACAO.len()..],
                    operacao.acao,
                    operacao.canal,
                    operacao.conteudo
                );
            }
            Err(err) => println!("Erro ao decodificar replicação: {err}"),
        }
    }
}

/// Pede o estado ao coordenador (se for outro servidor) ou ao proximo servidor da lista.
fn obter_estado_inicial(ctx: &zmq::Context, estado: &Arc<Mutex<Estado>>, dados: &Arc<Mutex<Dados>>) {
    let (nome, atual) = {
        let st = estado.lock().unwrap();
        (st.nome_servidor.clone(), st.servidor_eleito.clone())
    };

    let mut candidatos = Vec::new();
    if !atual.is_empty() && atual != nome {
        candidatos.push(atual.clone());
    }
    for s in listar_servidores(ctx, estado) {
        if s.nome != nome && s.nome != atual {
            candidatos.push(s.nome);
        }
    }

    for alvo in candidatos {
        let payload = {
            let mut st = estado.lock().unwrap();
            let contador = st.proximo_contador();
            Mensagem {
                timestamp: st.timestamp_ajustado(),
                acao: "obter_estado".to_string(),
                canal: String::new(),
                conteudo: nome.clone(),
                contador_mensagem: contador,
            }
        };
        let url = format!("tcp://{alvo}:{PORTA_SERVIDORES}");
        if let Some(resposta) = requisitar::<Resposta>(ctx, &url, &payload) {
            estado.lock().unwrap().atualizar_contador(resposta.contador_mensagem);
            if resposta.status != "200" {
                continue;
            }
            let mut d = dados.lock().unwrap();
            match d.importar(&resposta.resposta) {
                Ok(()) => {
                    println!(
                        "Estado recebido de {alvo}: {} canais, {} logins, {} publicacoes",
                        d.canais.canais.len(),
                        d.logins.len(),
                        d.publicacoes.len()
                    );
                    return;
                }
                Err(err) => println!("Estado inválido recebido de {alvo}: {err}"),
            }
        }
    }
    println!("Nenhum outro servidor respondeu: mantendo os dados locais");
}

fn main() -> zmq::Result<()> {
    // Nome do servidor = nome do servico no docker-compose: os outros servidores usam esse
    // nome como endereco (tcp://<nome>:5559) para pedir eleicao e sincronizacao.
    let nome_servidor =
        std::env::var("NOME_SERVIDOR").unwrap_or_else(|_| "servidorRust".to_string());
    let arquivos = Arquivos::new(&nome_servidor);
    let caminho_mensagens = arquivos.mensagens.clone();
    let mut mensagens: Vec<MensagemRegistro> = ler_json(&caminho_mensagens, Vec::new());
    let dados = Arc::new(Mutex::new(Dados::carregar(arquivos)));

    let ctx = zmq::Context::new();
    let responder = ctx.socket(zmq::REP)?;
    let resposta_servidores = ctx.socket(zmq::REP)?;
    let pub_sock = Arc::new(Mutex::new(ctx.socket(zmq::PUB)?));
    let sub = ctx.socket(zmq::SUB)?;
    let sub_replicacao = ctx.socket(zmq::SUB)?;
    let referencia = ctx.socket(zmq::REQ)?;

    pub_sock.lock().unwrap().connect("tcp://proxy:5557")?;
    sub.connect("tcp://proxy:5558")?;
    sub.set_subscribe(b"servers")?;
    // Parte 5: assina a replicacao antes de pedir o estado, para nao perder escritas no meio
    sub_replicacao.connect("tcp://proxy:5558")?;
    sub_replicacao.set_subscribe(TOPICO_REPLICACAO.as_bytes())?;
    responder.connect("tcp://broker:5560")?;
    resposta_servidores.bind(&format!("tcp://*:{PORTA_SERVIDORES}"))?;
    referencia.connect("tcp://referencia:5556")?;

    println!("Servidor conectado ao broker...");
    println!("Servidor conectado ao proxy...");
    println!("Servidor conectado à referência...");

    let estado = Arc::new(Mutex::new(Estado {
        nome_servidor,
        rank_servidor: 0,
        contador_mensagens: 0,
        offset_relogio: 0.0,
        servidor_eleito: String::new(),
        em_eleicao: false,
        anuncio_recebido: false,
    }));

    let rank = consultar_rank(&referencia, &estado).expect("falha ao consultar rank");
    estado.lock().unwrap().rank_servidor = rank;
    println!("Rank do servidor: {rank}");

    let estado_sub = Arc::clone(&estado);
    thread::spawn(move || ouvir_anuncios(sub, estado_sub));

    let estado_rep = Arc::clone(&estado);
    let dados_rep = Arc::clone(&dados);
    thread::spawn(move || ouvir_replicacao(sub_replicacao, estado_rep, dados_rep));

    let ctx_srv = ctx.clone();
    let pub_srv = Arc::clone(&pub_sock);
    let estado_srv = Arc::clone(&estado);
    let dados_srv = Arc::clone(&dados);
    thread::spawn(move || {
        atender_servidores(ctx_srv, resposta_servidores, pub_srv, estado_srv, dados_srv)
    });

    obter_estado_inicial(&ctx, &estado, &dados);

    let ctx_el = ctx.clone();
    let pub_el = Arc::clone(&pub_sock);
    let estado_el = Arc::clone(&estado);
    thread::spawn(move || iniciar_eleicao(ctx_el, pub_el, estado_el));

    let mut mensagens_recebidas: u64 = 0;

    loop {
        let request = responder.recv_bytes(0)?;
        let req = match Mensagem::decode(request.as_slice()) {
            Ok(m) => m,
            Err(err) => {
                println!("Erro ao processar mensagem: {err}");
                let payload = estado.lock().unwrap().criar_resposta(
                    "500",
                    "Erro interno no servidor!",
                    Vec::new(),
                );
                responder.send(payload.encode_to_vec(), 0)?;
                continue;
            }
        };

        estado.lock().unwrap().atualizar_contador(req.contador_mensagem);
        mensagens_recebidas += 1;

        if mensagens_recebidas % 10 == 0 {
            heartbeat(&referencia, &estado);
        }

        if mensagens_recebidas % 15 == 0 {
            sincronizar_com_servidor_eleito(&ctx, &pub_sock, &estado);
        }

        mensagens.push(MensagemRegistro {
            usuario: "cliente".to_string(),
            acao: Some(req.acao.clone()),
            status: None,
            conteudo: req.conteudo.clone(),
            canal: Some(req.canal.clone()),
            timestamp: Some(req.timestamp),
            contador_mensagem: Some(req.contador_mensagem),
            canais: None,
        });
        salvar_json(&caminho_mensagens, &mensagens);

        match req.acao.as_str() {
            "login" => {
                let status = {
                    let mut d = dados.lock().unwrap();
                    let status = d.status_login(&req.conteudo);
                    d.registrar_login(&req.conteudo, req.timestamp, status);
                    status
                };
                let texto = if status == "200" {
                    "Login realizado com sucesso!"
                } else {
                    "Nome de usuario incorreto!"
                };
                replicar(&pub_sock, &estado, "login", "", &req.conteudo, req.timestamp);

                let payload = estado
                    .lock()
                    .unwrap()
                    .criar_resposta(status, texto, Vec::new());
                responder.send(payload.encode_to_vec(), 0)?;

                mensagens.push(MensagemRegistro {
                    usuario: "servidor".to_string(),
                    acao: None,
                    status: Some(status.to_string()),
                    conteudo: texto.to_string(),
                    canal: None,
                    timestamp: None,
                    contador_mensagem: None,
                    canais: None,
                });
                salvar_json(&caminho_mensagens, &mensagens);
            }
            "listar" => {
                let lista = dados.lock().unwrap().canais.canais.clone();
                let payload = estado.lock().unwrap().criar_resposta(
                    "200",
                    "Canais listados",
                    lista.clone(),
                );
                responder.send(payload.encode_to_vec(), 0)?;

                mensagens.push(MensagemRegistro {
                    usuario: "servidor".to_string(),
                    acao: None,
                    status: Some("200".to_string()),
                    conteudo: "Canais listados".to_string(),
                    canal: None,
                    timestamp: None,
                    contador_mensagem: None,
                    canais: Some(lista),
                });
                salvar_json(&caminho_mensagens, &mensagens);
            }
            "cadastrar" => {
                let criado = dados.lock().unwrap().registrar_canal(&req.conteudo);
                let (status, texto) = if criado {
                    replicar(&pub_sock, &estado, "cadastrar", "", &req.conteudo, req.timestamp);
                    (
                        "200",
                        format!("Canal {} cadastrado com sucesso!", req.conteudo),
                    )
                } else {
                    ("404", "Canal já existe!".to_string())
                };

                let payload = estado
                    .lock()
                    .unwrap()
                    .criar_resposta(status, &texto, Vec::new());
                responder.send(payload.encode_to_vec(), 0)?;

                mensagens.push(MensagemRegistro {
                    usuario: "servidor".to_string(),
                    acao: None,
                    status: Some(status.to_string()),
                    conteudo: texto,
                    canal: None,
                    timestamp: None,
                    contador_mensagem: None,
                    canais: None,
                });
                salvar_json(&caminho_mensagens, &mensagens);
            }
            "publicar" => {
                let (status, texto) = match publicar(
                    &pub_sock,
                    &estado,
                    &dados,
                    &req,
                ) {
                    Ok(()) => (
                        "200",
                        format!("Mensagem publicada no canal {} com sucesso!", req.canal),
                    ),
                    Err(err) => {
                        println!("Erro ao publicar: {err}");
                        ("500", "Erro ao publicar mensagem!".to_string())
                    }
                };

                let payload = estado
                    .lock()
                    .unwrap()
                    .criar_resposta(status, &texto, Vec::new());
                responder.send(payload.encode_to_vec(), 0)?;

                mensagens.push(MensagemRegistro {
                    usuario: "servidor".to_string(),
                    acao: None,
                    status: Some(status.to_string()),
                    conteudo: texto,
                    canal: None,
                    timestamp: None,
                    contador_mensagem: None,
                    canais: None,
                });
                salvar_json(&caminho_mensagens, &mensagens);
            }
            _ => {
                let payload = estado.lock().unwrap().criar_resposta(
                    "400",
                    "Ação desconhecida!",
                    Vec::new(),
                );
                responder.send(payload.encode_to_vec(), 0)?;
            }
        }
    }
}
