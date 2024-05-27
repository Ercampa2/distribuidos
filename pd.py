#!/usr/bin/python3

from sys import argv
from pika import BlockingConnection
from enum import Enum

if len(argv) < 2:
    print(f'USO: {argv[0]} <id> [<v1> <v2> ...]')
    exit(1)

idx = argv[1]
Nx = argv[2:]

Estado = Enum('Estado', "INCIADOR OCIOSO VISITADO OK")
estado = Estado.OCIOSO

print('idx = ', idx)
print('Nx = ', Nx)

entrada = None
iniciador = False
Nv = []

def recebendo(msg, origem, canal):
    global iniciador
    global estado
    global Nv
    global entrada

    if Estado == "OCIOSO":
        entrada = origem
        Nv = Nx[:]
        Nv.remove(origem)
        iniciador = "false"
        visita(canal)
    elif Estado == "VISITADO":
        if msg == "T":
            Nv.remove(origem)
            envia("B",origem, canal)
        elif msg == "R":
            visita(canal)
        elif msg == "B":
            visita(canal)

def envia(msg, dest, canal):
    m = f'{idx}:{msg}'   # adiciona origem                                                         
    print(m)
    canal.basic_publish(exchange='', routing_key=dest, body=m)

def visita(canal):
    global Nv
    global estado
    if len(Nv) > 0:
        estado = Estado.VISITADO
        envia("T", Nv[0], canal)
    else: 
        estado = Estado.OK
        if not iniciador:
            envia("R", entrada, canal)
            print(f"Fim do {idx}")
        else: 
            print("Fim de execução")

def espontaneamente(msg, canal):
    global Nv
    global iniciador
    Nv = Nx[:]
    iniciador = True
    visita(canal)



conexao = BlockingConnection()                                                                      
canal = conexao.channel()                                                                           

canal.queue_declare(queue=idx, auto_delete=True)                                                    
for v in Nx:                                                                                        
    canal.queue_declare(queue=v, auto_delete=True)                                                  

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
