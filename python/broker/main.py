import zmq

contexto = zmq.Context()

clientes = contexto.socket(zmq.ROUTER)
servidores = contexto.socket(zmq.DEALER)

clientes.bind("tcp://*:5559")
servidores.bind("tcp://*:5560")

print("Broker escutando em 5559 (clientes) e 5560 (servidores)...", flush=True)

zmq.proxy(clientes, servidores)
