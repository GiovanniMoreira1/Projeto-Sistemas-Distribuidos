fn main() -> zmq::Result<()> {
    let ctx = zmq::Context::new();

    let mut xsub = ctx.socket(zmq::XSUB)?;
    let mut xpub = ctx.socket(zmq::XPUB)?;

    xsub.bind("tcp://*:5557")?;
    xpub.bind("tcp://*:5558")?;

    println!("Proxy Pub/Sub rodando (XSUB 5557 / XPUB 5558)");

    zmq::proxy(&mut xsub, &mut xpub)
}
