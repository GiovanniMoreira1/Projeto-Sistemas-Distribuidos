use std::collections::HashSet;
use std::fmt::Display;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chat::agora_ms;
use chat::proto::{Mensagem, Resposta, RespostaReferencia, RespostaSub};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::json;

const PORTA_SERVIDORES: u16 = 5559;
const URL_REFERENCIA: &str = "tcp://referencia:5556";
const HEARTBEAT_A_CADA: u64 = 10;
const SINCRONIZAR_A_CADA: u64 = 15;
const TEMPO_LIMITE_MS: i32 = 3000;
const ESPERA_ANUNCIO: Duration = Duration::from_secs(10);
const ESPERA_ASSINATURA: Duration = Duration::from_secs(1);
const TOPICO_REPLICACAO: &str = "replicacao|";

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

#[derive(Serialize, Deserialize, Default)]
struct EstadoServidor {
    #[serde(default)]
    canais: Vec<String>,
    #[serde(default)]
    logins: Vec<LoginRegistro>,
    #[serde(default)]
    publicacoes: Vec<PublicacaoRegistro>,
}

fn ler_json<T: for<'de> Deserialize<'de>>(caminho: &str, padrao: T) -> T {
    match fs::read_to_string(caminho) {
        Ok(conteudo) => serde_json::from_str(&conteudo).expect("falha ao ler json"),
        Err(_) => padrao,
    }
}

fn salvar_json<T: Serialize>(caminho: &str, dados: &T) {
    let texto = serde_json::to_string(dados).expect("falha ao serializar json");
    fs::write(caminho, texto).expect("falha ao gravar json");
}

struct Dados {
    caminho_canais: String,
    caminho_logins: String,
    caminho_publicacoes: String,
    usuarios: Vec<String>,
    canais: CanaisData,
    logins: Vec<LoginRegistro>,
    publicacoes: Vec<PublicacaoRegistro>,
    logins_vistos: HashSet<(String, u64)>,
    publicacoes_vistas: HashSet<(String, String, u64)>,
}

impl Dados {
    fn carregar(diretorio: &Path) -> Dados {
        let caminho = |nome: &str| diretorio.join(nome).to_string_lossy().into_owned();
        let usuarios: UsuariosData = ler_json("usuarios.json", UsuariosData { usuarios: Vec::new() });
        let canais = ler_json(&caminho("canais.json"), CanaisData { canais: Vec::new() });
        let logins: Vec<LoginRegistro> = ler_json(&caminho("login.json"), Vec::new());
        let publicacoes: Vec<PublicacaoRegistro> = ler_json(&caminho("publicacoes.json"), Vec::new());
        let logins_vistos = logins.iter().map(|l| (l.usuario.clone(), l.timestamp.to_bits())).collect();
        let publicacoes_vistas = publicacoes
            .iter()
            .map(|p| (p.canal.clone(), p.conteudo.clone(), p.timestamp.to_bits()))
            .collect();
        Dados {
            caminho_canais: caminho("canais.json"),
            caminho_logins: caminho("login.json"),
            caminho_publicacoes: caminho("publicacoes.json"),
            usuarios: usuarios.usuarios,
            canais,
            logins,
            publicacoes,
            logins_vistos,
            publicacoes_vistas,
        }
    }

    fn status_login(&self, usuario: &str) -> &'static str {
        if self.usuarios.iter().any(|u| u == usuario) { "200" } else { "404" }
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
        salvar_json(&self.caminho_logins, &self.logins);
    }

    fn registrar_canal(&mut self, canal: &str) -> bool {
        if self.canais.canais.iter().any(|c| c == canal) {
            return false;
        }
        self.canais.canais.push(canal.to_string());
        salvar_json(&self.caminho_canais, &self.canais);
        true
    }

    fn registrar_publicacao(&mut self, canal: &str, conteudo: &str, timestamp: f64) {
        if !self.publicacoes_vistas.insert((canal.to_string(), conteudo.to_string(), timestamp.to_bits())) {
            return;
        }
        self.publicacoes.push(PublicacaoRegistro {
            canal: canal.to_string(),
            conteudo: conteudo.to_string(),
            timestamp,
        });
        salvar_json(&self.caminho_publicacoes, &self.publicacoes);
    }

    fn exportar(&self) -> String {
        let estado = EstadoServidor {
            canais: self.canais.canais.clone(),
            logins: self.logins.clone(),
            publicacoes: self.publicacoes.clone(),
        };
        serde_json::to_string(&estado).unwrap_or_default()
    }

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

#[derive(Default)]
struct Estado {
    contador: i32,
    offset_ms: f64,
    rank: i32,
    coordenador: String,
    em_eleicao: bool,
    anuncio_recebido: bool,
    sincronizado: bool,
}

#[derive(Clone)]
struct Servidor {
    nome: String,
    ctx: zmq::Context,
    publicador: Arc<Mutex<zmq::Socket>>,
    estado: Arc<Mutex<Estado>>,
    dados: Arc<Mutex<Dados>>,
}

impl Servidor {
    fn log(&self, texto: impl Display) {
        println!("[{}] {texto}", self.nome);
    }

    fn agora(&self) -> f64 {
        (agora_ms() + self.estado.lock().unwrap().offset_ms).floor()
    }

    fn proximo_contador(&self) -> i32 {
        let mut st = self.estado.lock().unwrap();
        st.contador += 1;
        st.contador
    }

    fn atualizar_contador(&self, recebido: i32) {
        let mut st = self.estado.lock().unwrap();
        st.contador = st.contador.max(recebido);
    }

    fn coordenador(&self) -> String {
        self.estado.lock().unwrap().coordenador.clone()
    }

    fn criar_resposta(&self, status: &str, texto: &str, canais: Vec<String>) -> Resposta {
        Resposta {
            status: status.to_string(),
            resposta: texto.to_string(),
            canais,
            contador_mensagem: self.proximo_contador(),
            timestamp: self.agora(),
        }
    }

    fn publicar_no_proxy(&self, topico: &str, corpo: Vec<u8>) {
        let sock = self.publicador.lock().unwrap();
        if let Err(erro) = sock.send_multipart([topico.as_bytes(), corpo.as_slice()], 0) {
            self.log(format!("erro ao publicar em {topico}: {erro}"));
        }
    }

    fn requisitar(&self, url: &str, acao: &str) -> Option<Vec<u8>> {
        let mensagem = Mensagem {
            timestamp: self.agora(),
            acao: acao.to_string(),
            canal: String::new(),
            conteudo: self.nome.clone(),
            contador_mensagem: self.proximo_contador(),
        };
        let socket = self.ctx.socket(zmq::REQ).ok()?;
        socket.set_sndtimeo(TEMPO_LIMITE_MS).ok()?;
        socket.set_rcvtimeo(TEMPO_LIMITE_MS).ok()?;
        socket.set_linger(0).ok()?;
        socket.connect(url).ok()?;
        socket.send(mensagem.encode_to_vec(), 0).ok()?;
        socket.recv_bytes(0).ok()
    }

    fn requisitar_servidor(&self, alvo: &str, acao: &str) -> Option<Resposta> {
        let bytes = self.requisitar(&format!("tcp://{alvo}:{PORTA_SERVIDORES}"), acao)?;
        let resposta = Resposta::decode(bytes.as_slice()).ok()?;
        self.atualizar_contador(resposta.contador_mensagem);
        Some(resposta)
    }

    fn requisitar_referencia(&self, acao: &str) -> Option<RespostaReferencia> {
        let bytes = self.requisitar(URL_REFERENCIA, acao)?;
        let resposta = RespostaReferencia::decode(bytes.as_slice()).ok()?;
        self.atualizar_contador(resposta.contador_mensagem);
        Some(resposta)
    }

    fn tentar_rank(&self) -> Option<i32> {
        self.requisitar_referencia("consultar_rank")
            .filter(|r| r.status == "200")
            .map(|r| r.rank)
    }

    fn heartbeat(&self) {
        match self.requisitar_referencia("heartbeat") {
            None => self.log("heartbeat sem resposta da referência"),
            Some(r) if r.status == "200" => self.log("heartbeat confirmado"),
            Some(_) => {
                if let Some(novo) = self.tentar_rank() {
                    self.estado.lock().unwrap().rank = novo;
                    self.log(format!("recadastrado na referência, rank {novo}"));
                }
            }
        }
    }

    fn iniciar_eleicao(&self) {
        {
            let mut st = self.estado.lock().unwrap();
            if st.em_eleicao {
                return;
            }
            st.em_eleicao = true;
            st.anuncio_recebido = false;
        }
        let servidor = self.clone();
        thread::spawn(move || {
            let refazer = servidor.executar_eleicao();
            servidor.estado.lock().unwrap().em_eleicao = false;
            if refazer {
                servidor.iniciar_eleicao();
            }
        });
    }

    fn executar_eleicao(&self) -> bool {
        self.log("iniciando eleição...");
        let meu_rank = self.estado.lock().unwrap().rank;
        let maiores: Vec<String> = self
            .requisitar_referencia("listar_servidores")
            .map(|r| r.servidores)
            .unwrap_or_default()
            .into_iter()
            .filter(|s| s.rank > meu_rank && s.nome != self.nome)
            .map(|s| s.nome)
            .collect();

        let mut algum_respondeu = false;
        for alvo in &maiores {
            if self.requisitar_servidor(alvo, "eleicao").is_some_and(|r| r.resposta == "OK") {
                algum_respondeu = true;
            }
        }

        if !algum_respondeu {
            self.tornar_se_coordenador();
            return false;
        }

        self.log("servidor de rank maior assumiu a eleição; aguardando anúncio...");
        let inicio = Instant::now();
        while inicio.elapsed() < ESPERA_ANUNCIO {
            if self.estado.lock().unwrap().anuncio_recebido {
                return false;
            }
            thread::sleep(Duration::from_millis(200));
        }
        self.log("anúncio não chegou; refazendo eleição");
        true
    }

    fn tornar_se_coordenador(&self) {
        let rank = {
            let mut st = self.estado.lock().unwrap();
            st.coordenador = self.nome.clone();
            st.offset_ms = 0.0;
            st.anuncio_recebido = true;
            st.rank
        };
        self.log(format!("eleito coordenador (rank {rank})"));

        for _ in 0..2 {
            let anuncio = RespostaSub {
                timestamp_envio: self.agora(),
                canal: "servers".to_string(),
                conteudo: self.nome.clone(),
                timestamp_recebimento: 0.0,
                contador_mensagem: self.proximo_contador(),
            };
            self.publicar_no_proxy("servers|", anuncio.encode_to_vec());
            thread::sleep(Duration::from_secs(1));
        }
    }

    fn ouvir_anuncios(&self, sub: zmq::Socket) {
        while let Ok(frames) = sub.recv_multipart(0) {
            let Some(Ok(anuncio)) = frames.get(1).map(|f| RespostaSub::decode(f.as_slice())) else {
                continue;
            };
            self.atualizar_contador(anuncio.contador_mensagem);
            let mudou = {
                let mut st = self.estado.lock().unwrap();
                let mudou = st.coordenador != anuncio.conteudo;
                st.coordenador = anuncio.conteudo.clone();
                st.anuncio_recebido = true;
                mudou
            };
            if mudou {
                self.log(format!("novo coordenador anunciado: {}", anuncio.conteudo));
            }
        }
    }

    fn atender_servidores(&self, socket: zmq::Socket) {
        while let Ok(bytes) = socket.recv_bytes(0) {
            let mut iniciar = false;
            let resposta = match Mensagem::decode(bytes.as_slice()) {
                Ok(pedido) => {
                    self.atualizar_contador(pedido.contador_mensagem);
                    match pedido.acao.as_str() {
                        "obter_estado" if !self.estado.lock().unwrap().sincronizado => {
                            self.criar_resposta("503", "Servidor ainda sincronizando", Vec::new())
                        }
                        "obter_estado" => {
                            let estado = self.dados.lock().unwrap().exportar();
                            self.log(format!("estado enviado para {}", pedido.conteudo));
                            self.criar_resposta("200", &estado, Vec::new())
                        }
                        "eleicao" => {
                            iniciar = true;
                            self.criar_resposta("200", "OK", Vec::new())
                        }
                        "sincronizar" if self.coordenador() == self.nome => {
                            self.criar_resposta("200", "Hora correta", Vec::new())
                        }
                        "sincronizar" => self.criar_resposta("400", "Não sou o coordenador!", Vec::new()),
                        _ => self.criar_resposta("400", "Ação desconhecida!", Vec::new()),
                    }
                }
                Err(erro) => {
                    self.log(format!("erro ao atender servidor: {erro}"));
                    self.criar_resposta("500", "Erro interno no servidor!", Vec::new())
                }
            };
            let _ = socket.send(resposta.encode_to_vec(), 0);
            if iniciar {
                self.iniciar_eleicao();
            }
        }
    }

    fn sincronizar_relogio(&self) {
        let alvo = self.coordenador();
        if alvo.is_empty() {
            self.iniciar_eleicao();
            return;
        }
        if alvo == self.nome {
            self.log("sou o coordenador: relógio mantido");
            return;
        }

        let t0 = agora_ms();
        let resposta = self.requisitar_servidor(&alvo, "sincronizar");
        let t1 = agora_ms();

        match resposta {
            Some(r) if r.status == "200" => {
                let offset = r.timestamp + (t1 - t0) / 2.0 - t1;
                self.estado.lock().unwrap().offset_ms = offset;
                self.log(format!("relógio sincronizado com {alvo} (offset {offset:+.0} ms)"));
            }
            _ => {
                self.log("coordenador indisponível; iniciando eleição");
                self.iniciar_eleicao();
            }
        }
    }

    fn replicar(&self, acao: &str, canal: &str, conteudo: &str, timestamp: f64) {
        let operacao = Mensagem {
            timestamp,
            acao: acao.to_string(),
            canal: canal.to_string(),
            conteudo: conteudo.to_string(),
            contador_mensagem: self.proximo_contador(),
        };
        self.publicar_no_proxy(&format!("{TOPICO_REPLICACAO}{}", self.nome), operacao.encode_to_vec());
    }

    fn aplicar_operacao(&self, operacao: &Mensagem) {
        let mut d = self.dados.lock().unwrap();
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

    fn ouvir_replicacao(&self, sub: zmq::Socket) {
        let proprio = format!("{TOPICO_REPLICACAO}{}", self.nome);
        while let Ok(frames) = sub.recv_multipart(0) {
            if frames.len() < 2 {
                continue;
            }
            let topico = String::from_utf8_lossy(&frames[0]).into_owned();
            if topico == proprio {
                continue;
            }
            match Mensagem::decode(frames[1].as_slice()) {
                Ok(operacao) => {
                    self.atualizar_contador(operacao.contador_mensagem);
                    self.aplicar_operacao(&operacao);
                    self.log(format!(
                        "replicado de {}: {} {} {}",
                        &topico[TOPICO_REPLICACAO.len()..],
                        operacao.acao,
                        operacao.canal,
                        operacao.conteudo
                    ));
                }
                Err(erro) => self.log(format!("erro ao ler replicação: {erro}")),
            }
        }
    }

    fn obter_estado_inicial(&self) {
        let atual = self.coordenador();
        let mut candidatos = Vec::new();
        if !atual.is_empty() && atual != self.nome {
            candidatos.push(atual.clone());
        }
        if let Some(lista) = self.requisitar_referencia("listar_servidores") {
            for s in lista.servidores {
                if s.nome != self.nome && s.nome != atual {
                    candidatos.push(s.nome);
                }
            }
        }

        for alvo in candidatos {
            let Some(resposta) = self.requisitar_servidor(&alvo, "obter_estado") else {
                continue;
            };
            if resposta.status != "200" {
                continue;
            }
            let mut d = self.dados.lock().unwrap();
            match d.importar(&resposta.resposta) {
                Ok(()) => {
                    let resumo = format!(
                        "estado recebido de {alvo}: {} canais, {} logins, {} publicações",
                        d.canais.canais.len(),
                        d.logins.len(),
                        d.publicacoes.len()
                    );
                    drop(d);
                    self.log(resumo);
                    return;
                }
                Err(erro) => self.log(format!("estado inválido recebido de {alvo}: {erro}")),
            }
        }
        self.log("nenhum outro servidor respondeu: mantendo os dados locais");
    }

    fn tratar_login(&self, req: &Mensagem) -> Resposta {
        let usuario = req.conteudo.trim();
        let status = {
            let mut d = self.dados.lock().unwrap();
            let status = d.status_login(usuario);
            d.registrar_login(usuario, req.timestamp, status);
            status
        };
        self.replicar("login", "", usuario, req.timestamp);
        if status == "200" {
            self.criar_resposta("200", "Login realizado com sucesso!", Vec::new())
        } else {
            self.criar_resposta("404", "Nome de usuario incorreto!", Vec::new())
        }
    }

    fn tratar_listar(&self) -> Resposta {
        let lista = self.dados.lock().unwrap().canais.canais.clone();
        self.criar_resposta("200", "Canais listados", lista)
    }

    fn tratar_cadastrar(&self, req: &Mensagem) -> Resposta {
        let canal = req.conteudo.trim();
        if canal.is_empty() || canal.len() > 50 || canal.contains('|') {
            return self.criar_resposta("400", "Nome de canal invalido!", Vec::new());
        }
        if !self.dados.lock().unwrap().registrar_canal(canal) {
            return self.criar_resposta("404", "Canal já existe!", Vec::new());
        }
        self.replicar("cadastrar", "", canal, req.timestamp);
        self.criar_resposta("200", &format!("Canal {canal} cadastrado com sucesso!"), Vec::new())
    }

    fn tratar_publicar(&self, req: &Mensagem) -> Resposta {
        let canal = req.canal.trim();
        if canal.is_empty() || req.conteudo.is_empty() {
            return self.criar_resposta("400", "Canal e mensagem sao obrigatorios!", Vec::new());
        }

        let publicacao = RespostaSub {
            timestamp_envio: self.agora(),
            canal: canal.to_string(),
            conteudo: req.conteudo.clone(),
            timestamp_recebimento: 0.0,
            contador_mensagem: self.proximo_contador(),
        };
        self.publicar_no_proxy(&format!("{canal}|"), publicacao.encode_to_vec());

        self.dados.lock().unwrap().registrar_publicacao(canal, &req.conteudo, req.timestamp);
        self.replicar("publicar", canal, &req.conteudo, req.timestamp);
        self.criar_resposta("200", &format!("Mensagem publicada no canal {canal} com sucesso!"), Vec::new())
    }

    fn atender_clientes(&self, socket: zmq::Socket, caminho_mensagens: &str) -> zmq::Result<()> {
        let mut mensagens: Vec<serde_json::Value> = ler_json(caminho_mensagens, Vec::new());
        let mut recebidas: u64 = 0;

        loop {
            let bytes = socket.recv_bytes(0)?;
            let resposta = match Mensagem::decode(bytes.as_slice()) {
                Ok(req) => {
                    self.atualizar_contador(req.contador_mensagem);
                    self.log(format!(
                        "recebido: {} canal={} conteudo={} timestamp={} contador={}",
                        req.acao, req.canal, req.conteudo, req.timestamp, req.contador_mensagem
                    ));
                    mensagens.push(json!({
                        "usuario": "cliente",
                        "acao": req.acao,
                        "canal": req.canal,
                        "conteudo": req.conteudo,
                        "timestamp": req.timestamp,
                        "contadorMensagem": req.contador_mensagem,
                    }));
                    match req.acao.as_str() {
                        "login" => self.tratar_login(&req),
                        "listar" => self.tratar_listar(),
                        "cadastrar" => self.tratar_cadastrar(&req),
                        "publicar" => self.tratar_publicar(&req),
                        _ => self.criar_resposta("400", "Ação desconhecida!", Vec::new()),
                    }
                }
                Err(erro) => {
                    self.log(format!("erro ao processar mensagem: {erro}"));
                    self.criar_resposta("500", "Erro interno no servidor!", Vec::new())
                }
            };

            self.log(format!(
                "enviado: {} {} timestamp={} contador={}",
                resposta.status, resposta.resposta, resposta.timestamp, resposta.contador_mensagem
            ));
            socket.send(resposta.encode_to_vec(), 0)?;

            mensagens.push(json!({
                "usuario": "servidor",
                "status": resposta.status,
                "resposta": resposta.resposta,
                "canais": resposta.canais,
                "timestamp": resposta.timestamp,
                "contadorMensagem": resposta.contador_mensagem,
            }));
            salvar_json(caminho_mensagens, &mensagens);

            recebidas += 1;
            if recebidas % HEARTBEAT_A_CADA == 0 {
                self.heartbeat();
            }
            if recebidas % SINCRONIZAR_A_CADA == 0 {
                self.sincronizar_relogio();
            }
        }
    }
}

fn main() -> zmq::Result<()> {
    let nome = std::env::var("NOME_SERVIDOR").unwrap_or_else(|_| "servidorRust".to_string());
    let base = std::env::var("DADOS_DIR").unwrap_or_else(|_| "dados".to_string());
    let diretorio = Path::new(&base).join(&nome);
    fs::create_dir_all(&diretorio).expect("falha ao criar diretorio de dados");
    let caminho_mensagens = diretorio.join("mensagens.json").to_string_lossy().into_owned();

    let ctx = zmq::Context::new();
    let publicador = ctx.socket(zmq::PUB)?;
    publicador.connect("tcp://proxy:5557")?;

    let servidor = Servidor {
        nome,
        ctx: ctx.clone(),
        publicador: Arc::new(Mutex::new(publicador)),
        estado: Arc::new(Mutex::new(Estado::default())),
        dados: Arc::new(Mutex::new(Dados::carregar(&diretorio))),
    };
    servidor.log(format!("dados em {}", diretorio.display()));

    let sub_anuncios = ctx.socket(zmq::SUB)?;
    sub_anuncios.connect("tcp://proxy:5558")?;
    sub_anuncios.set_subscribe(b"servers|")?;
    let s = servidor.clone();
    thread::spawn(move || s.ouvir_anuncios(sub_anuncios));

    let sub_replicacao = ctx.socket(zmq::SUB)?;
    sub_replicacao.connect("tcp://proxy:5558")?;
    sub_replicacao.set_subscribe(TOPICO_REPLICACAO.as_bytes())?;
    let s = servidor.clone();
    thread::spawn(move || s.ouvir_replicacao(sub_replicacao));

    let socket_servidores = ctx.socket(zmq::REP)?;
    socket_servidores.bind(&format!("tcp://*:{PORTA_SERVIDORES}"))?;
    let s = servidor.clone();
    thread::spawn(move || s.atender_servidores(socket_servidores));

    let rank = loop {
        if let Some(rank) = servidor.tentar_rank() {
            break rank;
        }
        servidor.log("aguardando o serviço de referência...");
        thread::sleep(Duration::from_secs(1));
    };
    servidor.estado.lock().unwrap().rank = rank;
    servidor.log(format!("rank {rank}"));

    thread::sleep(ESPERA_ASSINATURA);
    servidor.obter_estado_inicial();
    servidor.estado.lock().unwrap().sincronizado = true;
    servidor.iniciar_eleicao();

    let socket_clientes = ctx.socket(zmq::REP)?;
    socket_clientes.connect("tcp://broker:5560")?;
    servidor.atender_clientes(socket_clientes, &caminho_mensagens)
}
