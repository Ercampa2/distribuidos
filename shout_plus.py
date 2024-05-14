#!/usr/bin/env python3

# Implementacao shout plus
# (a ser usado como template p/ outros
# algoritmos distribuidos)

from sys import argv
from pika import BlockingConnection
from enum import Enum

if len(argv) < 2:
    print(f'USO: {argv[0]} <id> [<v1> <v2> ...]')
    exit(1)

idx = argv[1]  # identificador do componente
Nx = argv[2:]  # vizinhos

Estado = Enum('Estado', 'INICIADOR OCIOSO ATIVO OK')
estado = Estado.OCIOSO     # estado inicial

print("idx =", idx)
print("Nx =", Nx)

pai = None
raiz = False
Nt = []
cont = 0

# entrega a mensagem
def recebendo(msg, origem, canal):
    global estado
    global pai
    global cont
    global Nt
    global Nx

    print(f'mensagem recebida: "{msg}"')
    if estado == Estado.OCIOSO:
        if msg == "Q":
            pai = origem
            Nt.append(origem)
            envia("Sim", [origem], canal)
            cont = 1
            if cont == len(Nx):
                estado = Estado.OK
                print(Nt)
            else:
                Nx2 = Nx[:]
                Nx2.remove(origem)
                envia("Q", Nx2, canal)
                estado = Estado.ATIVO

    elif estado == Estado.ATIVO:
        if msg == "Q": 
            cont += 1
            if cont == len(Nx):
                estado = Estado.OK
                print(Nt)
        elif msg == "Sim":
            Nt.append(origem)
            cont += 1
            if cont == len(Nx):
                estado = Estado.OK
                print(Nt)



# envia msg a destinatarios (lista)
def envia(msg, dests, canal):
    m = idx + ":" + msg   # adiciona origem
    for d in dests:
        canal.basic_publish(exchange='',
                            routing_key=d,
                            body=m)

# quando recebe mensagem externa (do starter)
def espontaneamente(msg, canal):
    print("sou iniciador!")
    global estado
    global raiz
    estado = Estado.INICIADOR
    raiz = True
    estado = Estado.ATIVO
    envia("Q", Nx, canal)   # envia msg para todos os vizinhos

conexao = BlockingConnection()
canal = conexao.channel()

canal.queue_declare(queue=idx, auto_delete=True)
for v in Nx:
    canal.queue_declare(queue=v, auto_delete=True)

# callback interno do AMQP
def callback(canal, metodo, props, corpo):
    m = corpo.decode().split(":")
    if len(m) < 2:
        print("mensagem sem origem!")
    else:
        origem = m[0]
        msg = m[1]
        if origem == "STARTER":
            print('espontaneo')
            espontaneamente(msg, canal)
        else:
            recebendo(msg, origem, canal)

canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)
try:
    print(f"{idx} aguardando mensagens")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()

