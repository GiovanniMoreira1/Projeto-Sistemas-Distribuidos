use std::process;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread;

use chat::agora_ms;
use chat::proto::{Mensagem, Resposta, RespostaSub};
use prost::Message as _;
use rand::seq::SliceRandom;

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

fn inscrever_canal(canal: &str, subscriber: &zmq::Socket) -> zmq::Result<()> {
    let topico = format!("{canal}|");
    subscriber.set_subscribe(topico.as_bytes())?;
    println!("cliente inscrito no {canal}");
    Ok(())
}

fn receber_publicacoes(subscriber: zmq::Socket, contador: Arc<AtomicI32>) {
    loop {
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

    let mut canais_resp = enviar_requisicao(&requester, &contador, "listar", "", "")?;
    if canais_resp.canais.len() < 5 {
        println!(
            "Erro: quantidade de canais insuficiente. É necessário ter pelo menos 5 canais cadastrados."
        );
        let novo_canal = format!("canal{}", canais_resp.canais.len() + 2);
        enviar_requisicao(&requester, &contador, "cadastrar", "", &novo_canal)?;
        canais_resp = enviar_requisicao(&requester, &contador, "listar", "", "")?;
    }

    let canais = canais_resp.canais;
    println!("Canais disponiveis: {}", canais.join(", "));

    let mut rng = rand::thread_rng();
    let escolhidos: Vec<&String> = canais.choose_multiple(&mut rng, 3.min(canais.len())).collect();
    for canal in escolhidos {
        inscrever_canal(canal, &subscriber)?;
    }

    let contador_sub = Arc::clone(&contador);
    thread::spawn(move || receber_publicacoes(subscriber, contador_sub));

    loop {
        if let Some(aleatorio) = canais.choose(&mut rng) {
            let msg = format!("Mensagem de teste para o canal {aleatorio}");
            enviar_requisicao(&requester, &contador, "publicar", aleatorio, &msg)?;
        }
    }
}
