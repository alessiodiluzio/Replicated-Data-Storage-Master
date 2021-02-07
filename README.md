# Replicated Data Storage

Lo scopo del progetto contenuto in questa repository è lo sviluppo di un sistema
di Data Storage replicato, il cui deployment è stato eseguito con gli strumenti offerti da Amazon Web Services, in particolar modo EC2.
Il linguaggio scelto per lo sviluppo è Java.

## Architettura del sistema

Il sistema di Data Storage Replicato consta di tre tipologie di nodi:

* Master: gestisce il mapping tra nome di un file e posizione delle repliche. E’ responsabile del coordinamento degli altri nodi del sistema occupandosi di allocare e de-allocare istanze in modo elastico per rispondere alle variazioni del carico sul sistema.
* DataNode: memorizza file e loro contenuto.
* Cloudlet: nodo periferico del sistema, pensato per posizionarsi ai bordi della rete in vicinanza delle sorgenti di dati (Device Client, Sensori...). E’ il front- end del sistema essendo il nodo di interfaccia con il sistema per le operazioni supportate.

Il sistema ha un’architettura multi-Master in cui ogni singolo Master conosce e può contattare tutti gli altri e gestisce un sottoinsieme di DataNode e Cloudlet.
Il numero di Master, DataNode e Cloudlet del sistema all’avvio, è pienamente configurabile.

In questa repository è contenuto il codice dei nodi Master e Data Node


### Master

Il Master riceve periodicamente segnali di vita e statistiche sull’utilizzo delle risorse dai propri nodi DataNode e Cloudlet. Sulla base di queste può decidere di allocare nuovi nodi o di rimuoverne, per rispondere rispettivamente a situazioni di sovrautilizzazione o sottoutilizzazione delle risorse monitorate (CPU e RAM). Viene rilevato anche il crash di un nodo DataNode o CloudLet a seguito del quale è
attivata una procedura di sostituzione comprensiva di un fase di recovery di eventuali dati perduti.
Il Master provvede al bilanciamento del carico spostando i file più acceduti o più grandi da DataNode sovraccarichi verso altri meno utilizzati o in assenza di questi verso nuovi nodi. 

Il Master gestisce una tabella di mapping tra il nome di un file e la posizione di una replica se questa è su un DataNode da lui gestito, può comunque recuperare informazioni sul posizionamento dei file comunicando con gli altri Master.

### Data Node

Il contenuto dei file è memorizzato nelle istanze di DataNode, gli aggiornamenti avvengono solamente attraverso operazioni di append e sono propagati da una replica alla successiva in modo non bloccante e asincrono rispetto all’operazione di scrittura.


## Dettagli implementativi 


Il Sistema è distribuito su istanze di AWS EC2.
Il Sistema è implementato in linguaggio Java, la comunicazione tra i nodi del sistema avviene sfruttando la tecnologia Java RMI, la gestione delle dipendenze è affidata al tool Maven.
La comunicazione RMI avviene usando gli indirizzi IP pubblici delle istanze EC2.
Le tabelle locali presenti in ogni nodo sono salvate in database relazionali Apache Derby embedded in memory.


