import socket
import json
import struct
import threading
from collections import Counter
import numpy as np

# Lire les adresses des machines à partir du fichier machines.txt
with open('machines.txt', 'r') as file:
    machines = [line.strip() for line in file.readlines()]

tab_fin_phase_1 = [False]*len(machines)
tab_fin_phase_2 = [False]*len(machines)
tab_fin_phase_3 = [False]*len(machines)
tab_fin_phase_4 = [False]*len(machines)
tab_fin_phase_5 = [False]*len(machines)
tab_fin_phase_6 = [False]*len(machines)
tab_fin_phase_7 = [False]*len(machines)
tab_fin_phase_8 = [False]*len(machines)



machines_json = json.dumps(machines)

with open("example.txt", "r") as file:
    text = file.read()

text_to_messages = text.split()

num_machines = len(machines)
connexions = {}
result_dict = {}
etat = [0]


# Créer les connexions à toutes les machines
for machine in machines:
    try:
        # Créer un socket TCP/IP
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Se connecter à la machine
        client_socket.connect((machine, 3462))
        
        # Stocker la connexion
        connexions[machine] = client_socket
        print(f"Connexion établie avec {machine}")
    except Exception as e:
        print(f"Erreur lors de la connexion à {machine}: {e}")

def envoyer_message(client_socket, message):
    # Convertir le message en bytes
    message_bytes = message.encode('utf-8')
    # Envoyer la taille du message en utilisant send
    taille_message = struct.pack('!I', len(message_bytes))

    total_envoye = 0
    while total_envoye < len(taille_message):
        envoye = client_socket.send(taille_message[total_envoye:])
        if envoye == 0:
            raise RuntimeError("La connexion a été fermée")
        total_envoye += envoye
    # Envoyer le message
    client_socket.sendall(message_bytes)

def envoyer_messages():
    #Envoyer la liste des machines à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, machines_json)
            print(f"Envoyé la liste des machines à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

    split_size = 5
    # Envoyer les messages spécifiques de manière cyclique
    index = 0
    for split_index in range (0,len(text_to_messages),split_size):
        machine_index = index % len(machines)
        machine = machines[machine_index]
        try:
            client_socket = connexions[machine]
            split_end = split_index + split_size
            split_messages = json.dumps(text_to_messages[split_index : split_end])
            envoyer_message(client_socket, split_messages)
            #print(f"Envoyé '{split_messages}' à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")
        index +=1
    # Envoyer le message de fin de phase à chaque machine
    for machine, client_socket in connexions.items():
        try:
            envoyer_message(client_socket, "FIN PHASE 1")
            print(f"Envoyé 'FIN PHASE 1' à {machine}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machine}: {e}")

def recevoir_exactement(client_socket, n):
    data = b''
    while len(data) < n:
        packet = client_socket.recv(n - len(data))
        if not packet:
            raise ConnectionError("Connexion fermée par le client")
        data += packet
    return data

def recevoir_message(client_socket):
    # Recevoir la taille du message
    taille_message = struct.unpack('!I', recevoir_exactement(client_socket, 4))[0]
    # Recevoir le message en utilisant la taille
    data = recevoir_exactement(client_socket, taille_message)
    return data.decode('utf-8')

def envoyer_counters_quantiles(counters , quantiles):
    machines = list(connexions.keys())  
    for i, counter in enumerate(counters):
        try:
            counter_json = json.dumps([quantiles , counter])
            client_socket = connexions[machines[i % len(machines)]]  
            envoyer_message(client_socket, counter_json)
        except Exception as e:
            print(f"Erreur lors de l'envoi à {machines[i % len(machines)]}: {e}")


def extract_quantiles(counter_map, num_machines):
    frequencies = list(counter_map.values())
    quantiles = np.quantile(frequencies, np.linspace(1/num_machines, 1-1/num_machines, num_machines-1))
    print("quantiles", quantiles)
    return quantiles.tolist()

def divide_into_counters(counter_map , num_machines):

    sorted_items = sorted(counter_map.items(), key=lambda x: x[1], reverse=True)

    partitions = [Counter() for _ in range(num_machines)]
    key_counts = [0] * num_machines  


    for key, value in sorted_items:
        min_index = key_counts.index(min(key_counts))
        partitions[min_index][key] = value
        key_counts[min_index] += 1

    print('Capacity partition',len(partitions[0]))
    return partitions

def recevoir_messages():
    Count_result = Counter()  
    while True:
        for machine, client_socket in connexions.items():
            try:
                message_reçu = recevoir_message(client_socket)
                if message_reçu == "OK FIN PHASE 1":
                    etat[0] = 1
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_1[machines.index(machine)] = True
                    if all(tab_fin_phase_1):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 2")
                            print(f"Envoyé 'GO PHASE 2' à {machine}")
                
                elif message_reçu == "OK FIN PHASE 2":
                    etat[0] = 2
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_2[machines.index(machine)] = True
                    if all(tab_fin_phase_2):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 3")
                            print(f"Envoyé 'GO PHASE 3' à {machine}")

                elif message_reçu == "OK FIN PHASE 3":
                    etat[0] = 3
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_3[machines.index(machine)] = True
                    if all(tab_fin_phase_3):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 4")
                            print(f"Envoyé 'GO PHASE 4' à {machine}")

                elif message_reçu == "OK FIN PHASE 4":
                    etat[0] = 4
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_4[machines.index(machine)] = True
                    if all(tab_fin_phase_4):
                        quantiles =extract_quantiles(Count_result , num_machines)
                        count_partitions = divide_into_counters(Count_result , num_machines)
                        envoyer_counters_quantiles(count_partitions , quantiles)
                        print(f"Envoyé count partition à {machine}")

                elif message_reçu == "OK FIN PHASE 5":
                    etat[0] = 5
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_5[machines.index(machine)] = True
                    if all(tab_fin_phase_5):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 6")
                            print(f"Envoyé 'GO PHASE 6' à {machine}")
                       
                elif message_reçu == "OK FIN PHASE 6":
                    etat[0] = 6
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_6[machines.index(machine)] = True
                    if all(tab_fin_phase_6):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 7")
                            print(f"Envoyé 'GO PHASE 7' à {machine}")

                elif message_reçu == "OK FIN PHASE 7":
                    etat[0] = 7
                    print(f"Reçu '{message_reçu}' de {machine}")
                    tab_fin_phase_7[machines.index(machine)] = True
                    if all(tab_fin_phase_7):
                        for machine, client_socket in connexions.items():
                            envoyer_message(client_socket, "GO PHASE 8")
                            print(f"Envoyé 'GO PHASE 8' à {machine}")
                            etat[0] = 8

                elif etat[0] == 8 :
                    result_dict[machine] = json.loads(message_reçu)

                    print(machine , result_dict)
                    tab_fin_phase_8[machines.index(machine)] = True
                    if all(tab_fin_phase_8):
                        final_result = result_dict[machines[0]] 
                        for machine in machines[1:]:  
                            final_result |= result_dict[machine]
                        print("FINAL RESULT", final_result)
                else : 
                    Count_result+=json.loads(message_reçu)

            except Exception as e:
                print(f"Erreur lors de la réception de {machine}: {e}")

    

# Créer et démarrer les threads pour envoyer et recevoir les messages
thread_envoi = threading.Thread(target=envoyer_messages)
thread_reception = threading.Thread(target=recevoir_messages)

thread_envoi.start()
thread_reception.start()

# Attendre que les threads se terminent
thread_envoi.join()
thread_reception.join()
