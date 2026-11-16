import zmq

contexto = zmq.Context()

xsub = contexto.socket(zmq.XSUB)
xpub = contexto.socket(zmq.XPUB)

xsub.bind("tcp://*:5557")
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub rodando (XSUB 5557 / XPUB 5558)", flush=True)

zmq.proxy(xsub, xpub)
