# PiChatServer

ealizzare la classe Python chiamata MyChatServer che implementa un server che consenta ad un gruppo di utenti di scambiarsi messaggi. 

La classe deve fornire:

    un costruttore che ricevere come parametri (nell'ordine):

        un dizionario che associa al nome degli utenti la corrispondente password (entrambi di tipo stringa);
        indirizzo di ascolto della socket del server;
        porta di ascolto della socket del server.

    un metodo start(), senza parametri, che avvia il server.


I client contattano il server in sessioni successive ognuna delle quali ha il seguente schema:

    si connettono al server;
    inviano i comandi di autenticazione;
    interagiscono con il server usando il protocollo descritto;
    chiudono la connessione. 

N.B. Non è previsto alcun messaggio di chiusura, la connessione si chiude chiudendo la socket.



Il server dovrà implementare le seguenti funzionalità elencate in ordine di priorità.


1. Messaggio di Login

Connesso al server, ogni client deve per prima cosa effettuare l'autenticazione. Questa viene effettuata inviando per prima cosa il messaggio:

USER <username>\r\n 

E successivamente il messaggio:

PASS <password>\r\n

Il server risponderà sempre al primo messaggio con: 

OK\r\n

mentre al secondo potrà rispondere con 

OK\r\n 

se l'autenticazione è andata a buon fine (password corretta) o con il messaggio

KO\r\n

se l'utente non è riconosciuto o se la password inserita non è corretta.

Fino a quando l'utente non ha completato la procedura di autenticazione, il server risponderà ad ogni messaggio con: 

KO\r\n



N.B. Prima di ricevere il messaggio USER, il server risponde ad ogni messaggio con KO ignorando il messaggio ricevuto. Dopo aver ricevuto il messaggio USER il server ignora tutti i messaggi diversi da PASS rispondendo con KO. Ricevuto il messaggio PASS, se la password è corretta il server entra nella fase successiva; se la password è errata la procedura di autenticazione riprende dall'inizio (il server si aspetta un nuovo messaggio USER).

Terminata la fase di autenticazione, il server risponderà ai messaggi USER e PASS (così come ai messaggi sintatticamente errati) sempre con un KO.


2. Gestione degli topic

Ad ogni messaggio inviato nella chat è associato un topic. I seguenti messaggi possono 

essere inviati dal client al server per gestire i topic nella chat

NEW <topic name>\r\n

TOPICS\r\n

Nel primo caso il server risponderà con il messaggio:

OK <tid>\r\n

dove <tid> è un intero che identifica il topic nel server (il primo topic avrà indice 0, il secondo 1, etc...)

Quando viene ricevuto il messaggio TOPICS, invece, il server risponderà con un messaggio della forma:

TOPIC_LIST\r\n

<tid_0> <topic name_0>\r\n

...

<tid_n> <topic name_n>\r\n

\r\n

N.B. Se non ci sono registrati topic nel server, la risposta sarà:

TOPIC_LIST\r\n

\r\n


3. Invio di messaggi

Per inviare un messaggio alla chat viene utilizzato il seguente messaggio:

MESSAGE <tid_1> <tid_2>...<tid_n>\r\n

<testo messaggio>\r\n

.\r\n

\r\n

Nel messaggio <tid_i> rappresentano gli indici dei topic di riferimento del particolare 

messaggio (ad ogni messaggio deve essere associato almeno un topic),


N.B. Il messaggio rappresentato con la stringa <testo messaggio> sarà quindi terminato dalla sequenza:

\r\n

.\r\n

\r\n


Il server risponderà con

OK <mid>\r\n

Dove <mid> è un intero progressivo che viene associato ad ogni messaggio ricevuto dal server.

Se almeno uno dei topic non esiste o il corpo del messaggio è vuoto allora il server risponderà con il messaggio:

KO\r\n

 in questo caso il messaggio verrà ignorato senza essere registrato in nessun topic.


4. Recupero messaggi:

Per recuperare la lista dei messaggi memorizzati sul server, il client invierà il messaggio:

LIST <mid> tlist\r\n

dove <mid> è un intero, mentre tlist è una lista (anche vuota) di topics.  

A questo messaggio il server risponderà con l'elenco dei messaggi i cui id sono maggiori o uguali a mid disponibili sul server per i topic selezionati (tutti i topic se la lista è vuota). 

MESSAGES\r\n

<mid_1> <tid_1_1> ... <tid_1_n_1>\r\n

...

<mid_k> <tid_k_1> ... <tid_k_n_k>\r\n

\r\n

Dove <mid_i> è l'id del mesaggio, mentre <tid_k_1> ... <tid_k_n_i> sono i topic associati  al messaggio.

 Se uno dei topic richiesti non esiste, allora viene restituito il messaggio

KO\r\n

Per recuperare il testo di un messaggio specifico il client dovrà inviare il messaggio:

GET <mid>\r\n

A questo comando il server risponderà con:

MESSAGE <mid>\r\n

TOPICS <tid_1> ... <tid_n>\r\n 

<testo_messaggio>\r\n

.\r\n

\r\n

Se il messaggio <mid> non esiste, allora il server risponde con il messaggio:

KO\r\n

5. Gestione batch dei messaggi

Il server è in grado di gestire i messaggi in batch. Questo significa che il client può inviare più messaggi senza attendere la risposta da parte del server. In questo caso il server leggerà un messaggio alla volta inviando una sequenza di risposte.


6. Gestione di più utenti contemporaneamente.

Il server è in grado di gestire più utenti contemporaneamente.


7. Gestione delle risposte

Ogni utente può rispondere ad un determinato messaggio inviando il messaggio:

REPLY <mid>\r\n

<testo_messaggio>\r\n

.\r\n

\r\n

Il server risponderà con 

OK <mid_2>\r\n

se il messaggio <mid> è l'id di un messaggio esistente, con

KO\r\n 

altrimenti.

Il server assegnerà al messaggio ricevuto da una reply gli stessi topic del messaggio sorgente.

Il cliente potrà recuperare la lista dei messaggi in una "conversazione" inviando  il comando:

CONV <mid>\r\n

Il server risponderà con il messaggio: 

MESSAGES\r\n

<mid_1> <tid_1_1> ... <tid_1_n_1>\r\n

...

<mid_k> <tid_k_1> ... <tid_k_n_k>\r\n

\r\n

contenente tutti i messaggi, elencati per ordine crescente di id, che hanno preceduto  <mid> e tutti quelli che lo hanno seguito. 


8. Modalità push del server.

Le funzionalità elencate fino ad ora consentono al server di funzionare in modalità pull. Ossia un client contatta il server per "ottenere" la lista dei messaggi. 

Un client può inviare al server il messaggio

REGISTER <host> <port>\r\n

Per registrare un particolare indirizzo (<host>:<port>) quale destinatario di messaggi.

Se <host> <port> non è utilizzato da un altro utente il server risponderà con :

OK\r\n

altrimenti, se la coppia <host> <port> è già utilizzata da un altro utente allora verrà inviato il messaggio:

KO\r\n

Se l'utente è già registrato, il server rimpiazzerà il vecchio indirizzo con il nuovo.

Inviando il messaggio

UNREGISTER\r\n

il server rimuoverà l'attuale associazione utente/indirizzo inviando successivamente il messaggio 

OK\r\n

Se non esiste alcuna registrazione per l'utente corrente, verrà inviato il messaggio

KO\r\n

9. Subscribe/Unsubscribe

Un  client, dopo aver effettuato la REGISTER, può inviare i seguenti comandi:

SUBSCRIBE <tid_1> ... <tid_n>\r\n

UNSUBSCRIBE <tid_1> ... <tid_n>\r\n

Se questi messaggi non sono preceduti (anche in sessioni diverse) da una REGISTER, il server risponde con il messaggio:

KO\r\n

lo stesso messaggio verrà inviato se uno o più topic non esistono sul server.

Altrimenti il server risponderà con il messaggio 

OK\r\n

Dopo aver effettuato la sottoscrizione per determinato <tid> il server, ricevuto un

messaggio per uno dei topic sottoscritti, contatterà ogni utente registrato al 

corrispondente indirizzo <host>:<port> inviando il messaggio:

MESSAGE <mid>\r\n

TOPICS <tid_1> ... <tid_n>\r\n 

<testo_messaggio>\r\n

.\r\n

\r\n

Il server chiuderà la connessione dopo aver inviato il messaggio.

Dopo aver effettuato la sottoscrizione per alcuni topic, il server evidenzierà con il carattere '*' quelli sottoscritti. Il messaggio di risposta al comando TOPICS diventerà, quindi:

TOPIC_LIST\r\n

(S)<tid_0> <topic name_0>\r\n

...

(S)<tid_n> <topic name_n>\r\n

\r\n

Dove (S) è *, se l'utente è registrato per il topic corrispondente, la stringa vuota altrimenti.

Se ad esempio un utente è registrato per i topic 1, 2 e 5, la risposta a TOPICS  sarà:

TOPIC_LIST\r\n

0 subject 0\r\n

*1 subject 1\r\n

*2 subject 2\r\n

3 subject 3\r\n

4 subject 4\r\n

*5 subject 5\r\n

6 subject 6\r\n

\r\n

N.B. I valori "subject i" sono puramente esemplificativi.

Quando il server riceve da un client il messaggio UNREGISTER, tutti i topic sottoscritti vengono automaticamente rimossi dal server. 


10. Digest.

Un utente può limitare il numero comunicazioni da parte del server inviando il comando:

DIGEST <k>\r\n

Dove <k> è un intero. Il server risponderà con il messaggio 

OK\r\n

Da quel momento in poi quando saranno collezionati k messaggi, il server contatterà il client all'indirizzo di registrazione inviando il messaggio:

MESSAGE <mid_1>\r\n

TOPICS <tid_1> ... <tid_n>\r\n 

<testo_messaggio>\r\n

\r\n

MESSAGE <mid_1>\r\n

TOPICS <tid_1> ... <tid_n>\r\n 

<testo_messaggio>\r\n

...

MESSAGE <mid_1>\r\n

TOPICS <tid_1> ... <tid_n>\r\n 

<testo_messaggio>\r\n

\r\n

.\r\n

\r\n 
