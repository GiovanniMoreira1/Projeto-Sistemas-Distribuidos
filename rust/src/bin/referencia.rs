use std::collections::HashMap;
use std::time::{Duration, Instant};

use chat::proto::{Mensagem, RespostaReferencia, ServidorInfo};
use prost::Message as _;

const TEMPO_LIMITE: Duration = Duration::from_secs(30);

struct DadosServidor {
    rank: i32,
    ultimo_heartbeat: Instant,
}

struct EstadoReferencia {
    servidores: HashMap<String, DadosServidor>,
    proximo_rank: i32,
    contador_logico: i32,
}

impl EstadoReferencia {
    fn new() -> Self {
        Self {
            servidores: HashMap::new(),
            proximo_rank: 1,
            contador_logico: 0,
        }
    }

    fn remover_inativos(&mut self) {
        let agora = Instant::now();
        let inativos: Vec<String> = self
            .servidores
            .iter()
            .filter(|(_, dados)| agora.duration_since(dados.ultimo_heartbeat) > TEMPO_LIMITE)
            .map(|(nome, _)| nome.clone())
            .collect();

        for nome in inativos {
            println!("Servidor removido: {nome}");
            self.servidores.remove(&nome);
        }
    }

    fn criar_resposta(&mut self, status: &str, resposta: &str, rank: i32) -> RespostaReferencia {
        self.contador_logico += 1;
        RespostaReferencia {
            status: status.to_string(),
            resposta: resposta.to_string(),
            rank,
            servidores: Vec::new(),
            contador_mensagem: self.contador_logico,
            servidor_eleito: String::new(),
        }
    }

    fn listar_servidores(&mut self) -> RespostaReferencia {
        self.remover_inativos();
        let mut resposta = self.criar_resposta("200", "Servidores disponíveis listados!", 0);
        resposta.servidores = self
            .servidores
            .iter()
            .map(|(nome, dados)| ServidorInfo {
                nome: nome.clone(),
                rank: dados.rank,
            })
            .collect();

        let resumo: Vec<(&str, i32)> = resposta
            .servidores
            .iter()
            .map(|s| (s.nome.as_str(), s.rank))
            .collect();
        println!("Lista de servidores enviada: {resumo:?}");

        resposta
    }
}

fn main() -> zmq::Result<()> {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP)?;
    socket.bind("tcp://*:5556")?;

    let mut estado = EstadoReferencia::new();
    println!("Referência aguardando mensagens...");

    loop {
        let mut items = [socket.as_poll_item(zmq::POLLIN)];
        zmq::poll(&mut items, 1000)?;
        estado.remover_inativos();

        if !items[0].is_readable() {
            continue;
        }

        let dados = socket.recv_bytes(0)?;
        let resposta = match Mensagem::decode(dados.as_slice()) {
            Ok(mensagem) => {
                estado.contador_logico = estado.contador_logico.max(mensagem.contador_mensagem);
                println!("Ação recebida: {}", mensagem.acao);

                match mensagem.acao.as_str() {
                    "consultar_rank" => {
                        let nome = mensagem.conteudo;
                        let rank = if let Some(dados_srv) = estado.servidores.get_mut(&nome) {
                            dados_srv.ultimo_heartbeat = Instant::now();
                            dados_srv.rank
                        } else {
                            let rank = estado.proximo_rank;
                            estado.proximo_rank += 1;
                            estado.servidores.insert(
                                nome.clone(),
                                DadosServidor {
                                    rank,
                                    ultimo_heartbeat: Instant::now(),
                                },
                            );
                            println!("Servidor cadastrado: {nome} | Rank: {rank}");
                            rank
                        };

                        estado.criar_resposta("200", "Servidor cadastrado com sucesso!", rank)
                    }
                    "listar_servidores" => estado.listar_servidores(),
                    "heartbeat" => {
                        let nome = mensagem.conteudo;
                        if let Some(dados_srv) = estado.servidores.get_mut(&nome) {
                            dados_srv.ultimo_heartbeat = Instant::now();
                            let rank = dados_srv.rank;
                            println!("Heartbeat recebido de {nome}");
                            estado.criar_resposta("200", "Heartbeat recebido!", rank)
                        } else {
                            estado.criar_resposta("404", "Servidor não cadastrado!", 0)
                        }
                    }
                    _ => estado.criar_resposta("400", "Ação desconhecida!", 0),
                }
            }
            Err(err) => {
                eprintln!("Erro na referência: {err}");
                estado.criar_resposta("500", "Erro interno na referência!", 0)
            }
        };

        socket.send(resposta.encode_to_vec(), 0)?;
    }
}
