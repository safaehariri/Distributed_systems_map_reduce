import socket
import json
import struct
import threading
import os
import time
from collections import Counter , OrderedDict
import statistics
# Obtenir le nom de la machine
nom_machine = socket.gethostname()
PORT = 3462
PORT2 = 3469
print(f"'{nom_machine}' : Bonjour, je suis la machine {nom_machine}")

# Créer un socket TCP/IP
serveur_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


# Lier le socket à l'adresse et au port avec un maximum de 5 tentatives
for tentative in range(5):
    try:
        serveur_socket.bind(('0.0.0.0', PORT))
        print(f"'{nom_machine}' : Le socket est lié au port {PORT} après {tentative + 1} tentative(s).")
        break
    except OSError:
        if tentative < 4:
            # Si le port est déjà utilisé, libérer le port en utilisant la commande kill
            print(f"'{nom_machine}' : Le port {PORT} est déjà utilisé. Tentative de libération du port ({tentative + 1}/5)...")
            # Afficher avec print le PID du processus qui utilise le port
            pid = os.popen(f'lsof -t -i:{PORT}').read().strip()
            print(f"'{nom_machine}' : PID du processus qui utilise le port {PORT} : {pid}")
            if pid:
                # Libérer le port et afficher le résultat de kill
                os.system(f'kill -9 {pid}')
                print(f"'{nom_machine}' : Tentative de tuer le processus {pid}.")
            else:
                print(f"'{nom_machine}' : Aucun processus n'utilise le port {PORT}.")
            time.sleep(5)
        else:
            raise Exception(f"'{nom_machine}' : Impossible de lier le socket au port {PORT} après 5 tentatives.")

# Créer un socket TCP/IP
serveur_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Lier le socket à l'adresse et au port avec un maximum de 5 tentatives
for tentative in range(5):
    try:
        serveur_socket2.bind(('0.0.0.0', PORT2))
        print(f"'{nom_machine}' : Le socket est lié au port {PORT2} après {tentative + 1} tentative(s).")
        break
    except OSError:
        if tentative < 4:
            # Si le port est déjà utilisé, libérer le port en utilisant la commande kill
            print(f"'{nom_machine}' : Le port {PORT2} est déjà utilisé. Tentative de libération du port ({tentative + 1}/5)...")
            # Afficher avec print le PID du processus qui utilise le port
            pid = os.popen(f'lsof -t -i:{PORT2}').read().strip()
            print(f"'{nom_machine}' : PID du processus qui utilise le port {PORT2} : {pid}")
            if pid:
                # Libérer le port et afficher le résultat de kill
                os.system(f'kill -9 {pid}')
                print(f"'{nom_machine}' : Tentative de tuer le processus {pid}.")
            else:
                print(f"'{nom_machine}' : Aucun processus n'utilise le port {PORT2}.")
            time.sleep(5)
        else:
            raise Exception(f"'{nom_machine}' : Impossible de lier le socket au port {PORT2} après 5 tentatives.")


# Écouter les connexions entrantes
serveur_socket.listen(5)
print(f"'{nom_machine}' : PHASE 1 Le serveur écoute sur le port {PORT}...")

serveur_socket2.listen(5)
print(f"'{nom_machine}' : PHASE 2 Le serveur écoute sur le port {PORT2}...")

connexions = {}
connexions_phase_2 = {}

def recevoir_exactement(client_socket, n):
    data = b''
    while len(data) < n:
        packet = client_socket.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def recevoir_message(client_socket):
    # Recevoir la taille du message
    taille_message_bytes = recevoir_exactement(client_socket, 4)
    if taille_message_bytes is None:
        print("Connexion fermée lors de la réception de la taille du message.")
        return None

    taille_message = struct.unpack('!I', taille_message_bytes)[0]

    # Recevoir le message en utilisant la taille
    data = recevoir_exactement(client_socket, taille_message)
    if data is None:
        print("Connexion fermée lors de la réception du message.")
        return None
    data =  data.decode('utf-8')
    return data

def envoyer_message(client_socket, message):
    # Convertir le message en bytes
    message_bytes = message.encode('utf-8')
    # Envoyer la taille du message
    client_socket.sendall(struct.pack('!I', len(message_bytes)))
    # Envoyer le message
    client_socket.sendall(message_bytes)

def gerer_connexion(client_socket, adresse_client):
    print(f"'{nom_machine}' : Connexion acceptée de {adresse_client}")

    # Ajouter la connexion au dictionnaire
    connexions[adresse_client] = client_socket

    nb_message=0
    mots=[]
    machines_reçues=[]
    shuffle_words = []
    etat=[1]
    capacity=[1]
    partition = {}
    quantiles = []
    while etat[0]!=9:

        message_reçu = recevoir_message(client_socket)
        if etat[0]==1 and nb_message==0:
            machines_reçues = json.loads(message_reçu)
            nb_message+=1
            continue
        if etat[0]==1 and nb_message>0 and message_reçu != "FIN PHASE 1":
            mots += json.loads(message_reçu)
            continue
        if message_reçu == "FIN PHASE 1":
            etat[0]=2
            thread_accepter_phase2 = threading.Thread(target=accepter_connexion_phase2 , args =(shuffle_words,etat , partition , capacity , machines_reçues, quantiles))
            thread_accepter_phase2.start()
            #envoyer "OK FIN PHASE 1"
            envoyer_message(client_socket, "OK FIN PHASE 1")
            # Créer les connexions à toutes les machines
            for machine in machines_reçues:
                try:
                    # Créer un socket TCP/IP
                    client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                    # Se connecter à la machine
                    client_socket2.connect((machine, PORT2))

                    #vérifier si la connexion est établie
                    
                    # Stocker la connexion
                    connexions_phase_2[machine] = client_socket2
                    print(f"{nom_machine}:Connexion établie avec {machine}")
                except Exception as e:
                    print(f"Erreur lors de la connexion à {machine}: {e}")
            continue
        if message_reçu == "GO PHASE 2":
            for mot in mots:
                machine_number = len(mot)%len(machines_reçues)
                print(f"{nom_machine}: Envoi de {mot} à {machines_reçues[machine_number]}")
                envoyer_message(connexions_phase_2[machines_reçues[machine_number]], mot)
            print(f"{nom_machine}: Envoi de OK FIN PHASE 2 à {adresse_client}")
            envoyer_message(client_socket, "OK FIN PHASE 2")
            print(f"{nom_machine}: OK FIN PHASE 2 envoyé")
            #continue
        if message_reçu == "GO PHASE 3":
            etat[0] = 3
            print(f'{nom_machine} received GO PHASE 3')
            shuffle_words_counted = gerer_phase_3(shuffle_words)
            envoyer_message(client_socket, "OK FIN PHASE 3")

        if message_reçu == "GO PHASE 4":
            etat[0] = 4
            print(f'{nom_machine} received GO PHASE 4')
            gerer_phase_4(client_socket,shuffle_words_counted)
            envoyer_message(client_socket, "OK FIN PHASE 4")
            etat[0] = 5

        if  etat[0] == 5 and message_reçu[:2]!='GO':
            print(f"MESSAGE RECEIVED INSIDE 5 to {nom_machine} : {message_reçu}")
            message_reçu = json.loads(message_reçu)
            quantiles.extend(message_reçu[0])
            capacity[0] = len(message_reçu[1].copy())
            print("CAPACITY IS ", capacity)
            capacity= capacity[0]
            partition.update(message_reçu[1])
            print(f"capacity of {nom_machine} is {capacity}")
            envoyer_message(client_socket, "OK FIN PHASE 5")
            print(f"Partition of Machine {nom_machine} is {partition}")

        if  message_reçu == 'GO PHASE 6':
            actual_capacity = len(partition)
            items_to_remove = []
            etat[0] = 6
            for mot in list(partition.keys()):
                
                count = partition[mot]
                if count <= quantiles[0]:
                    target_machine = machines_reçues[0]
                    print(f'quantile{quantiles[0]} machine {machines_reçues[0]}')
                elif count > quantiles[-1]:
                    target_machine = machines_reçues[-1]
                    print(f'quantile{quantiles[0]} machine {machines_reçues[0]}')

                else:
                    for i in range(1, len(quantiles)):
                        if quantiles[i-1] < count <= quantiles[i]:
                            target_machine = machines_reçues[i]
                            print(f'quantile{quantiles[0]} machine {machines_reçues[0]}')

                            break 

                if nom_machine != target_machine:
                    print(f"{nom_machine} sends to {target_machine} : {mot}:{count}")
                    envoyer_message(connexions_phase_2[target_machine], json.dumps({mot: count}))
                    items_to_remove.append(mot)

            
            envoyer_message(client_socket, "OK FIN PHASE 6")
            print(f"{nom_machine}: OK FIN PHASE 6 envoyé")
            

        if  message_reçu == 'GO PHASE 7':
            etat[0] = 7
            for item in items_to_remove:
                if item in partition:
                    del partition[item]
            actual_capacity = len(partition)
            print(f'capacity of {machine} is {capacity}')
            
            while actual_capacity > capacity :

                excess_count = actual_capacity - capacity
                print(f"Capacité excédentaire : {excess_count}")

                # Sélectionner les éléments excédentaires
                items_to_remove = list(partition.items())[:excess_count]

                for mot, count in items_to_remove:
                    current_index = machines_reçues.index(nom_machine)

                    if (current_index + 1 < len(machines_reçues) and 
                        quantiles[current_index] == quantiles[current_index + 1]):

                        target_machine = machines_reçues[current_index + 1]
                        print(f"{nom_machine} envoie {mot}:{count} à {target_machine} pour gérer la surcharge.")
                        envoyer_message(connexions_phase_2[target_machine], json.dumps({mot: count}))
                        del partition[mot]
                    else:
                        print(f"{nom_machine} conserve {mot}:{count}.")
                        continue
                    

                # Mettre à jour la capacité actuelle
                actual_capacity = len(partition)

            envoyer_message(client_socket, "OK FIN PHASE 7")
                    
        if  message_reçu == 'GO PHASE 8':
            etat[0] = 8
            print(f"FINAL parititon after second shuffle  {nom_machine} is {len(partition)}")
            ordered_dict = OrderedDict(sorted(partition.items(), key=lambda item: item[1]))
            envoyer_message(client_socket,json.dumps(ordered_dict))
            print((f'ORDER dict of {nom_machine} is  sent {ordered_dict}'))
            

def gerer_phase_3(shuffle_words):
    print(f"'PHASE 3 {nom_machine}'")
    
    shuffle_words_counted = Counter(shuffle_words)

    print(f"'PHASE  {nom_machine} Words Counted : {shuffle_words_counted}")
    return shuffle_words_counted

def gerer_phase_4(client_socket , shuffle_words_counted):
    print(f"'PHASE 4 {nom_machine}'")
    envoyer_message(client_socket,json.dumps(shuffle_words_counted))
    print(f"'PHASE 4 {nom_machine} wors counts envoyé {shuffle_words_counted}")

def gerer_phase_2(client_socket, adresse_client , shuffle_words , etat , partition , capacity ,machines_reçues , quantiles):
    print(f"'PHASE 2 {nom_machine}' : Gérer phase 2 pour {adresse_client}")
    redistribution_needed = False  # Indicateur pour démarrer une redistribution active

    while True:
        message_reçu = recevoir_message(client_socket)
        if etat[0] == 2:
            shuffle_words.append(message_reçu)
            print(f"'PHASE 2 {nom_machine} words: {shuffle_words}")
        elif etat[0] == 7 and message_reçu[:2]!="GO":
            partition.update(json.loads(message_reçu))
            print(f"'second shuffle {nom_machine}' : second partition: {partition}")
            redistribution_needed = True
            while redistribution_needed:
                actual_capacity = len(partition)

                if actual_capacity <= capacity[0] or nom_machine==machines_reçues[-1]:
                    redistribution_needed = False  # Arrêter si la capacité est respectée
                    break

                excess_count = actual_capacity - capacity[0]
                print(f"Capacité excédentaire : {excess_count}")

                items_to_remove = list(partition.items())[:excess_count]
                for mot, count in items_to_remove:
                    current_index = machines_reçues.index(nom_machine)

                    if (current_index + 1 < len(machines_reçues) and 
                        quantiles[current_index] == quantiles[current_index + 1]): 

                        target_machine = machines_reçues[current_index + 1]
                        print(f"{nom_machine} envoie {mot}:{count} à {target_machine} pour gérer la surcharge.")
                        envoyer_message(connexions_phase_2[target_machine], json.dumps({mot: count}))
                        del partition[mot]
                    else:
                        print(f"{nom_machine} conserve {mot}:{count}.")
                        continue
                    
                
                actual_capacity = len(partition)


        else:
           partition.update(json.loads(message_reçu))

def accepter_connexion_phase1():
    # Accepter une nouvelle connexion
    client_socket, adresse_client = serveur_socket.accept()
    # Créer un thread pour gérer la connexion
    thread_connexion = threading.Thread(target=gerer_connexion, args=(client_socket, adresse_client))
    thread_connexion.start()

def accepter_connexion_phase2(shuffle_words , etat , partition , capacity , machine_reçues , quantiles ):
    while True:
        # Accepter une nouvelle connexion
        print(f"'PHASE 2 {nom_machine}' : En attente de connexion...")
        client_socket2, adresse_client = serveur_socket2.accept()
        print(f"'PHASE 2 {nom_machine}' : Connexion acceptée de {adresse_client}")
        # Créer un thread pour gérer la connexion
        thread_connexion = threading.Thread(target=gerer_phase_2, args=(client_socket2, adresse_client , shuffle_words, etat , partition , capacity , machine_reçues , quantiles))
        thread_connexion.start()


# Créer et démarrer le thread pour accepter les connexions
thread_accepter = threading.Thread(target=accepter_connexion_phase1)
thread_accepter.start()

# Attendre que les threads se terminent (ce qui n'arrivera probablement jamais)
thread_accepter.join()
