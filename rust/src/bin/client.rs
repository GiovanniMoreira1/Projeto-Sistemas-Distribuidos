use std::collections::HashSet;
use std::process;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chat::agora_ms;
use chat::proto::{Mensagem, Resposta, RespostaSub};
use prost::Message as _;
use rand::seq::SliceRandom;
use rand::Rng;

const MIN_CANAIS: usize = 5;
const MIN_INSCRICOES: usize = 3;
const MENSAGENS_POR_RODADA: u32 = 10;
const INTERVALO: Duration = Duration::from_secs(1);

fn enviar_requisicao(
    requester: &zmq::Socket,
    contador: &AtomicI32,
    acao: &str,
    canal: &str,
    conteudo: &str,
) -> zmq::Result<Resposta> {
    let seq = contador.fetch_add(1, Ordering::SeqCst) + 1;
    let payload = Mensagem {
        timestamp: agora_ms(),
        acao: acao.to_string(),
        canal: canal.to_string(),
        conteudo: conteudo.to_string(),
        contador_mensagem: seq,
    };

    requester.send(payload.encode_to_vec(), 0)?;
    let reply = requester.recv_bytes(0)?;
    let resposta = Resposta::decode(reply.as_slice()).expect("falha ao decodificar Resposta");

    contador.fetch_max(resposta.contador_mensagem, Ordering::SeqCst);
    Ok(resposta)
}

fn receber_publicacoes(subscriber: zmq::Socket, inscricoes: Receiver<String>, contador: Arc<AtomicI32>) {
    loop {
        while let Ok(canal) = inscricoes.try_recv() {
            let topico = format!("{canal}|");
            if let Err(err) = subscriber.set_subscribe(topico.as_bytes()) {
                eprintln!("Erro ao inscrever no {canal}: {err}");
            }
        }

        match subscriber.poll(zmq::POLLIN, 200) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(err) => {
                eprintln!("Erro ao aguardar publicação: {err}");
                break;
            }
        }

        match subscriber.recv_multipart(0) {
            Ok(frames) => {
                if frames.len() < 2 {
                    continue;
                }
                match RespostaSub::decode(frames[1].as_slice()) {
                    Ok(dados) => {
                        contador.fetch_max(dados.contador_mensagem, Ordering::SeqCst);
                        println!(
                            "{{ canal: {:?}, mensagem: {:?}, timestampEnvio: {}, timestampRecebimento: {}, contadorMensagem: {} }}",
                            dados.canal,
                            dados.conteudo,
                            dados.timestamp_envio,
                            agora_ms(),
                            dados.contador_mensagem
                        );
                    }
                    Err(err) => eprintln!("Erro ao decodificar publicação: {err}"),
                }
            }
            Err(err) => {
                eprintln!("Erro ao receber publicação: {err}");
                break;
            }
        }
    }
}

fn main() -> zmq::Result<()> {
    let usuarios = [
        "usuario1", "usuario2", "usuario3", "teste", "usuario4", "usuario5",
    ];

    let ctx = zmq::Context::new();
    let requester = ctx.socket(zmq::REQ)?;
    let subscriber = ctx.socket(zmq::SUB)?;

    subscriber.connect("tcp://proxy:5558")?;
    requester.connect("tcp://broker:5559")?;
    println!("Cliente conectado no broker...");

    let contador = Arc::new(AtomicI32::new(0));
    let mut indice_usuario = 0;

    let mut reply = enviar_requisicao(&requester, &contador, "login", "", usuarios[indice_usuario])?;
    while reply.status != "200" {
        println!("Nome de usuario incorreto!");
        indice_usuario += 1;
        if indice_usuario >= usuarios.len() {
            println!("Nenhum usuario da lista funcionou.");
            process::exit(1);
        }
        reply = enviar_requisicao(&requester, &contador, "login", "", usuarios[indice_usuario])?;
    }

    let usuario = usuarios[indice_usuario];
    println!("[{usuario}] {}", reply.resposta);

    let (tx_inscricao, rx_inscricao) = mpsc::channel::<String>();
    let contador_sub = Arc::clone(&contador);
    thread::spawn(move || receber_publicacoes(subscriber, rx_inscricao, contador_sub));

    let mut rng = rand::thread_rng();
    let mut inscritos: HashSet<String> = HashSet::new();

    loop {
        let mut canais = enviar_requisicao(&requester, &contador, "listar", "", "")?.canais;

        if canais.len() < MIN_CANAIS {
            let novo = format!("canal{}", rng.gen_range(1000..10000));
            let r = enviar_requisicao(&requester, &contador, "cadastrar", "", &novo)?;
            println!("[{usuario}] criar {novo}: {} {}", r.status, r.resposta);
            canais = enviar_requisicao(&requester, &contador, "listar", "", "")?.canais;
        }

        if inscritos.len() < MIN_INSCRICOES {
            let disponiveis: Vec<&String> = canais.iter().filter(|c| !inscritos.contains(*c)).collect();
            if let Some(escolhido) = disponiveis.choose(&mut rng) {
                inscritos.insert((*escolhido).clone());
                let _ = tx_inscricao.send((*escolhido).clone());
                println!("[{usuario}] inscrito no {escolhido}");
            }
        }

        let Some(canal) = canais.choose(&mut rng).cloned() else {
            thread::sleep(INTERVALO);
            continue;
        };

        for i in 1..=MENSAGENS_POR_RODADA {
            let msg = format!("{usuario}: mensagem de teste para o canal {canal} ({i}/{MENSAGENS_POR_RODADA})");
            let r = enviar_requisicao(&requester, &contador, "publicar", &canal, &msg)?;
            if r.status != "200" {
                println!("[{usuario}] erro ao publicar em {canal}: {} {}", r.status, r.resposta);
            }
            thread::sleep(INTERVALO);
        }
    }
}
