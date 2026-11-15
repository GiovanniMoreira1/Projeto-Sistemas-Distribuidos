fn main() -> zmq::Result<()> {
    let ctx = zmq::Context::new();

    let mut client = ctx.socket(zmq::ROUTER)?;
    let mut server = ctx.socket(zmq::DEALER)?;

    client.bind("tcp://*:5559")?;
    server.bind("tcp://*:5560")?;

    zmq::proxy(&mut client, &mut server)
}
