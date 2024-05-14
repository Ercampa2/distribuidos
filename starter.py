#!/usr/bin/env python3

# Starter
# envia mensagem inicial ("espontanea") para 1 ou
# mais componentes
# Prof. Luiz Lima Jr.

from pika import BlockingConnection
from sys import argv

if len(argv)<3:
    print(f'Uso: {argv[0]} <msg> <d1> [<d2> ...]')
    exit(1)

msg = argv[1]
dests = argv[2:]

conexao = BlockingConnection()
canal = conexao.channel()

for f in dests:
    canal.queue_declare(queue=f, auto_delete=True)

def envia(msg, dests, canal):
    m = "STARTER:" + msg
    for d in dests:
        canal.basic_publish(exchange="",
                            routing_key=d,
                            body=m)
envia(msg, dests, canal)

print(f'mensagem "{msg}" enviada!')
conexao.close()
