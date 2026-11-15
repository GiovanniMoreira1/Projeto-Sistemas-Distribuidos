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

#[derive(Serialize, Deserialize)]
struct LoginRegistro {
    usuario: String,
    timestamp: f64,
    status: String,
}

#[derive(Serialize, Deserialize)]
struct PublicacaoRegistro {
    conteudo: String,
    canal: String,
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

fn ler_json<T: for<'de> Deserialize<'de>>(caminho: &str) -> T {
    let conteudo = fs::read_to_string(caminho).expect("falha ao ler arquivo json");
    serde_json::from_str(&conteudo).expect("falha ao parsear json")
}

fn salvar_json<T: Serialize>(caminho: &str, dados: &T) {
    let texto = serde_json::to_string(dados).expect("falha ao serializar json");
    fs::write(caminho, texto).expect("falha ao salvar arquivo json");
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
                let mut iniciar = false;
                let resp = {
                    let mut st = estado.lock().unwrap();
                    st.atualizar_contador(objeto.contador_mensagem);

                    match objeto.acao.as_str() {
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
                println!("Erro no heartbeat: {}", objeto.resposta);
            }
        }
    }
}

fn publicar(
    pub_sock: &Arc<Mutex<zmq::Socket>>,
    estado: &Arc<Mutex<Estado>>,
    canal: &str,
    conteudo: &str,
    publicacoes: &mut Vec<PublicacaoRegistro>,
) -> zmq::Result<()> {
    for _ in 1..=10 {
        let payload_sub = {
            let mut st = estado.lock().unwrap();
            let contador = st.proximo_contador();
            RespostaSub {
                timestamp_envio: st.timestamp_ajustado(),
                canal: canal.to_string(),
                conteudo: conteudo.to_string(),
                timestamp_recebimento: 0.0,
                contador_mensagem: contador,
            }
        };

        let topico = format!("{canal}|");
        pub_sock
            .lock()
            .unwrap()
            .send_multipart([topico.as_bytes(), &payload_sub.encode_to_vec()], 0)?;

        publicacoes.push(PublicacaoRegistro {
            conteudo: conteudo.to_string(),
            canal: canal.to_string(),
        });
        salvar_json("publicacoes.json", publicacoes);

        println!("Mensagem publicada: {conteudo}");
        thread::sleep(Duration::from_millis(100));
    }

    Ok(())
}

fn main() -> zmq::Result<()> {
    let usuarios: UsuariosData = ler_json("usuarios.json");
    let mut canais: CanaisData = ler_json("canais.json");
    let mut logins: Vec<LoginRegistro> = ler_json("login.json");
    let mut publicacoes: Vec<PublicacaoRegistro> = ler_json("publicacoes.json");
    let mut mensagens: Vec<MensagemRegistro> = ler_json("mensagens.json");

    let nome_servidor =
        std::env::var("NOME_SERVIDOR").unwrap_or_else(|_| "servidorRust".to_string());

    let ctx = zmq::Context::new();
    let responder = ctx.socket(zmq::REP)?;
    let resposta_servidores = ctx.socket(zmq::REP)?;
    let pub_sock = Arc::new(Mutex::new(ctx.socket(zmq::PUB)?));
    let sub = ctx.socket(zmq::SUB)?;
    let referencia = ctx.socket(zmq::REQ)?;

    pub_sock.lock().unwrap().connect("tcp://proxy:5557")?;
    sub.connect("tcp://proxy:5558")?;
    sub.set_subscribe(b"servers")?;
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

    let ctx_srv = ctx.clone();
    let pub_srv = Arc::clone(&pub_sock);
    let estado_srv = Arc::clone(&estado);
    thread::spawn(move || atender_servidores(ctx_srv, resposta_servidores, pub_srv, estado_srv));

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
        salvar_json("mensagens.json", &mensagens);

        match req.acao.as_str() {
            "login" => {
                let (status, texto) = if usuarios.usuarios.contains(&req.conteudo) {
                    ("200", "Login realizado com sucesso!")
                } else {
                    ("404", "Nome de usuario incorreto!")
                };

                logins.push(LoginRegistro {
                    usuario: req.conteudo.clone(),
                    timestamp: req.timestamp,
                    status: status.to_string(),
                });
                salvar_json("login.json", &logins);

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
                salvar_json("mensagens.json", &mensagens);
            }
            "listar" => {
                let lista = canais.canais.clone();
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
                salvar_json("mensagens.json", &mensagens);
            }
            "cadastrar" => {
                let (status, texto) = if !canais.canais.contains(&req.conteudo) {
                    canais.canais.push(req.conteudo.clone());
                    salvar_json("canais.json", &canais);
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
                salvar_json("mensagens.json", &mensagens);
            }
            "publicar" => {
                let (status, texto) = match publicar(
                    &pub_sock,
                    &estado,
                    &req.canal,
                    &req.conteudo,
                    &mut publicacoes,
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
                salvar_json("mensagens.json", &mensagens);
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
