import socket
import threading


class MyChatServer:
    listaMessaggi = []
    listaTopic = []
    indiceTopic = 0
    countMessaggi = -1
    registerDictionary = {}

    def __init__(self, dict, indirizzo, porta):
        self.__dict = dict
        self.__indirizzo = indirizzo
        self.__porta = porta
        self.__lock = threading.Lock()

    def start(self):
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((self.__indirizzo, self.__porta))
        serverSocket.listen(10)
        print "Server started..."
        print "Using this dictionary: {}".format(self.__dict)
        while 1:
            print "Waiting for connections.."
            connectionScoket, addr = serverSocket.accept()
            print "New connection from {}".format(addr)
            t = threading.Thread(target=self.client_handler, args=(connectionScoket, self.__lock,))
            t.start()

    def client_handler(self, connectionScoket, lock):
        stringa_ricevuta = ""
        userMandato = 0
        nomeUtente = ""
        AUTENTICATO = 1
        UTENTE = 0
        while 1:
            try:
                stringa_ricevuta += connectionScoket.recv(2048)
            except:
                return
            if not stringa_ricevuta: break
            stringa_comandi, resto = self.parse_batch(stringa_ricevuta)
            stringa_ricevuta = resto
            for comando in stringa_comandi:
                funzione = comando.split()[0]
                if not userMandato and UTENTE != AUTENTICATO and funzione != "USER":
                    connectionScoket.send("KO\r\n")
                elif UTENTE != AUTENTICATO and funzione == "USER" and nomeUtente == "":
                    connectionScoket.send("OK\r\n")
                    userMandato = 1
                    nomeUtente = " ".join(comando.split()[1:])
                elif UTENTE != AUTENTICATO and funzione == "PASS" and userMandato:
                    password = " ".join(comando.split()[1:])
                    if nomeUtente in self.__dict and self.__dict.get(nomeUtente) == password:
                        connectionScoket.send("OK\r\n")
                        UTENTE = AUTENTICATO
                    else:
                        connectionScoket.send("KO\r\n")
                        userMandato = 0
                        nomeUtente = ""
                elif UTENTE == AUTENTICATO:
                    try:
                        lock.acquire()
                        funzione = comando.split()[0]
                        if funzione in self.comandi:
                            try:
                                ret = self.comandi[funzione](self, comando, nomeUser=nomeUtente)  # tab a destra
                            except Exception:
                                connectionScoket.send("KO\r\n")  # 1 tab a destra
                            connectionScoket.send(ret)
                        else:
                            connectionScoket.send("KO\r\n")
                    finally:
                        lock.release()
                else:
                    connectionScoket.send("KO\r\n")

    def new_topic(self, stringaComando, nomeUser=None):
        nomeTopic = " ".join(stringaComando.split()[1:])
        if not nomeTopic:
            return "KO\r\n"
        topic = Topic(nomeTopic)
        self.listaTopic.append(topic)
        return "OK {}\r\n".format(len(self.listaTopic) - 1)

    def topic_list(self, stringaComando, nomeUser=None):
        if stringaComando != "TOPICS\r\n":
            return "KO\r\n"
        del stringaComando
        testa = "TOPIC_LIST\r\n"
        if len(self.listaTopic) > 0:
            for topic in self.listaTopic:
                testa += "{} {}\r\n".format(self.listaTopic.index(topic), topic.nome_topic())
        testa += "\r\n"
        return testa

    def messaggio(self, stringaComando, nomeUser=None):
        if len(stringaComando.split()) <= 3:
            return "KO\r\n"
        try:
            lista_topic = map(int, stringaComando.split("\r\n")[0].split()[1:])
        except:
            return "KO\r\n"
        testo_messaggio = " ".join(("".join(stringaComando.split("\r\n.\r\n\r\n"))).split()[1 + (len(lista_topic)):])
        for topic in lista_topic:
            try:
                self.listaTopic[topic].append(testo_messaggio, self.countMessaggi)
            except Exception:
                return "KO\r\n"
        self.countMessaggi += 1
        self.listaMessaggi.append((self.countMessaggi, testo_messaggio, lista_topic, [], -1))
        return "OK {}\r\n".format(self.countMessaggi)

    def lista_messaggi(self, stringaComando, nomeUser=None):
        try:
            mid = stringaComando.split()[1]
        except:
            return "KO\r\n"
        tlist = []
        if len(stringaComando.split()) > 2:
            try:
                tlist = map(int, stringaComando.split()[2:])
            except:
                return "KO\r\n"
        if len(tlist) != 0:
            for topic in tlist:
                if topic < 0:
                    return "KO\r\n"
                if topic >= len(self.listaTopic):
                    return "KO\r\n"
        testa = "MESSAGES\r\n"
        for message in self.listaMessaggi:
            if message[0] >= int(mid):
                if len(tlist) == 0:
                    y = " ".join(map(str, message[2]))
                    testa += "{} {}\r\n".format(message[0], y)
                else:
                    for topic in tlist:
                        if topic in message[2]:
                            y = " ".join(map(str, message[2]))
                            if testa.find("{} {}\r\n".format(message[0], y)) == -1:
                                testa += "{} {}\r\n".format(message[0], y)
        return "{}\r\n".format(testa)

    def trova_messaggio(self, stringaComando, nomeUser=None):
        mid = int(stringaComando.split()[1])
        testa = "MESSAGE {}\r\n".format(mid)
        try:
            messaggioTrovato = self.listaMessaggi[mid]
        except Exception:
            return "KO\r\n"
        tids = " ".join(map(str, messaggioTrovato[2]))
        testa += "TOPICS {}\r\n".format(tids)
        testa += "{}\r\n.\r\n\r\n".format(messaggioTrovato[1])
        return testa

    def risposta(self, stringaComando, nomeUser=None):
        mid = stringaComando.split()[1]
        if not mid.isdigit():
            return "KO\r\n"
        mid = int(mid)
        testo_messaggio = stringaComando.split("\r\n")[1]
        if not testo_messaggio:
            return "KO\r\n"
        try:
            padre = self.listaMessaggi[mid]
        except Exception as e:
            return "KO\r\n"
        lista_topic = padre[2]
        self.countMessaggi += 1
        padre[3].append(self.countMessaggi)
        self.listaMessaggi.append((self.countMessaggi, testo_messaggio, lista_topic, [], mid))
        return "OK {}\r\n".format(self.countMessaggi)

    def conversazione(self, stringaComando, nomeUser=None):
        mid = int(stringaComando.split()[1])
        messaggio = self.listaMessaggi[mid]
        topics_messaggio = " ".join(map(str, messaggio[2]))
        numeroPadre = messaggio[4]
        testa = []
        testa.append("{} {}\r\n".format(mid, topics_messaggio))
        while numeroPadre != -1:
            messaggio = self.listaMessaggi[numeroPadre]
            topics_padre = " ".join(map(str, messaggio[2]))
            testa.append("{} {}\r\n".format(numeroPadre, topics_padre))
            numeroPadre = messaggio[4]
        testa = testa[::-1]
        mid = int(stringaComando.split()[1])
        messaggio = self.listaMessaggi[mid]
        lista_figli = messaggio[3]
        for figlio in lista_figli:
            self.recursion_child(figlio, messaggio, testa)
        ret = "MESSAGES\r\n"
        ret += "".join(testa)
        ret += "\r\n"
        return ret

    def recursion_child(self, figlio, messaggio, testa):
        lista_figli = self.listaMessaggi[figlio][3]
        topics_messaggio = " ".join(map(str, messaggio[2]))
        testa.append("{} {}\r\n".format(figlio, topics_messaggio))
        for ele in lista_figli:
            if ele != 0:
                self.recursion_child(ele, messaggio, testa)

    def register(self, stringaComando, nomeUser):
        if len(stringaComando.split()) != 3:
            return "KO\r\n"
        host = stringaComando.split()[1]
        port = stringaComando.split()[2]
        for ele in self.registerDictionary:
            if ele != nomeUser and self.registerDictionary.get(ele) == (host, port):
                return "KO\r\n"
        if nomeUser is self.registerDictionary:
            del self.registerDictionary[nomeUser]
        for ele in self.registerDictionary:
            if ele != nomeUser and self.registerDictionary.get(ele) == (host, port):
                return "KO\r\n"
        self.registerDictionary.update({nomeUser: (host, port)})
        return "OK\r\n"

    def unregister(self, stringa, nomeUser):
        if self.registerDictionary.get(nomeUser) == None:
            return "KO\r\n"
        del self.registerDictionary[nomeUser]
        return "OK\r\n"

    def parse_batch(self, stringa):
        lista_comandi = []
        while stringa.find("\r\n") != -1:
            indexM = stringa.find("\r\n.\r\n\r\n")
            if stringa.startswith("MESSAGE ") and stringa.startswith("REPLY ") and stringa.find("\r\n.\r\n\r\n") == -1:
                break
            index = stringa.find("\r\n")
            if indexM == index:
                lista_comandi[-1] += stringa[:indexM + 7]
                stringa = stringa[index + 7:]
            else:
                lista_comandi.append(stringa[:index + 2])
                stringa = stringa[index + 2:]
        return lista_comandi, stringa

    comandi = {'NEW': new_topic, 'TOPICS': topic_list, 'MESSAGE': messaggio, 'LIST': lista_messaggi,
               'GET': trova_messaggio, 'REPLY': risposta, 'CONV': conversazione, 'REGISTER': register,
               'UNREGISTER': unregister}


class Topic:
    messageList = []
    nome = ""

    def __init__(self, nome):
        self.nome = nome

    def append(self, msg, id):
        self.messageList.append((msg, id))

    def nome_topic(self):
        return self.nome

    def lista_messaggi(self):
        return self.messageList