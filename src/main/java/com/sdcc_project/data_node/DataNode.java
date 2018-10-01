package com.sdcc_project.data_node;

import com.sdcc_project.config.Config;
import com.sdcc_project.dao.DataNodeDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.exception.DataNodeException;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;
import com.sdcc_project.util.FileManager;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.sdcc_project.exception.FileNotFoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.rmi.registry.LocateRegistry.createRegistry;

public class DataNode extends UnicastRemoteObject implements StorageInterface {

    private static Registry registry;
    private static DataNodeDAO dataNodeDAO;
    private static File file;
    private static int registryPort;
    private static final Logger LOGGER = Logger.getLogger( DataNode.class.getName() );
    private static boolean condition = true;
    private static final Object dataNodeLock = new Object();
    private static int masterAddress;
    private static long milliseconds;
    private static String completeName;

    private DataNode() throws RemoteException {
        super();
    }

    public static void main(String args[]) {

        if(args.length != 2){
            System.out.println("Usage: DataNode <dataNode_address> <master_address>");
            System.exit(1);
        }

        Date time = new Date();
        milliseconds = time.getTime();
        final int REGISTRYPORT = Integer.parseInt(args[0]);
        masterAddress = Integer.parseInt(args[1]);
        registryPort = REGISTRYPORT;
        String registryHost = Config.registryHost;
        String serviceName = Config.dataNodeServiceName;

        try {
            dataNodeDAO = DataNodeDAO.getInstance(registryPort);
        }
        catch (DataNodeException e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE,e.getMessage() + " DATA NODE SHUTDOWN");
            System.exit(0);
        }
        // Creazione del file di Logging.
        file = new File(Integer.toString(registryPort) + ".txt");

        try {
            completeName = "//" + registryHost + ":" + REGISTRYPORT + "/" + serviceName;
            DataNode dataNode = new DataNode();

            // Connessione dell'istanza con l'RMI Registry.
            registry = createRegistry(REGISTRYPORT);
            registry.rebind(completeName, dataNode);
            //System.out.println("DataNode Bound " + REGISTRYPORT);
            writeOutput("DataNode lanciato sulla porta: " + REGISTRYPORT + " - Porta del Master: " + masterAddress);
        }
        catch (Exception e) {
            e.printStackTrace();
            writeOutput(e.getMessage());
            LOGGER.log(Level.SEVERE,"Impossible to bind to master");
            System.exit(0);
        }

        statisticThread.start();
        saveDBThread.start();
        lifeThread.start();
    }

    @Override
    public ArrayList<ArrayList<String>> getDatabaseData() {

        ArrayList<ArrayList<String>> filesInfo = new ArrayList<>();

        try {
            filesInfo = dataNodeDAO.getAllFilesInformation(Integer.toString(registryPort));
        }
        catch (DataNodeException e) {
            e.printStackTrace();
        }

        return filesInfo;
    }


    /**
     * Lettura di un file da memoria.
     *
     * @param fileName nome del file
     * @return array di byte che rappresenta il file
     */
    @Override
    public byte[] read(String fileName) throws FileNotFoundException, DataNodeException {

        File file = new File(fileName);
        Path path = Paths.get(file.getPath());
        String encodedBase64File ;
        synchronized (dataNodeLock) {
            encodedBase64File = dataNodeDAO.getFileString(fileName,true);
        }
        byte[] fileBytes = null;
        //Nel DB è salvata la stringa in base64 che rappresenta il file.
        FileManager.convertStringToFile(encodedBase64File,fileName);
        try {
            //Converto il file in array di byte
            fileBytes = Files.readAllBytes(path);
            System.out.println("Lunghezza filebytes " +fileBytes.length);
        }
        catch (IOException e) {
            e.printStackTrace();
            writeOutput(e.getMessage());
        }
        finally {
            try {
                // Cancello il file temporaneo e chiudo la connessione.

                Files.delete(path);
            }
            catch (IOException e) {
                e.printStackTrace();
                writeOutput(e.getMessage());
            }
        }
        // Restituisco al client l'array di byte.
        return fileBytes;
    }

    /**
     * Scrive dati su un file in memoria
     *
     * @param data dati da scrivere
     * @param fileName nome del file da aggiornare
     * @return il nome file aggiornato
     */
    @Override
    public String write(String data,String fileName, ArrayList<String> dataNodePorts, int version,String oldPort) throws DataNodeException, FileNotFoundException {

        String base64String ="";
        String dataString = "";

        synchronized (dataNodeLock){
            try {
                base64String = dataNodeDAO.getFileString(fileName, false);
            }
            catch (FileNotFoundException e){
                LOGGER.log(Level.WARNING,"NEW FILE");

            }
        }

        if(base64String != null){
            //Converto il la stringa base64 in file.
            dataString = FileManager.decodeString(base64String);
        }

        //Codifico il file aggiornato in base64 e aggiorno il DB
        dataString = dataString +data+"\n";
        String updatedBase64File = FileManager.encodeString(dataString);
        long fileSize = FileManager.getStringMemorySize(updatedBase64File);
        dataNodePorts.remove(Integer.toString(registryPort));

        return forwardWrite(updatedBase64File,fileName,dataNodePorts,version,fileSize,oldPort);
    }

    /**
     * Comunica al Master il salvataggio di un file sul DataNode e:
     *
     *  - Nel caso di prima scrittura, comunica al Master la posizione (porta del DataNode) del file.
     *  - Nel caso di scritture successive, comunica al Master di aggiornate le informazioni di posizione del file.
     *
     * @param fileName Nome del file che è stato salvato.
     */
    private void sendCompletedWrite(String fileName, int version, String oldPort) {

        String completeName = "//" + Config.registryHost + ":" + masterAddress + "/" + Config.masterServiceName;
        MasterInterface master;
        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, masterAddress);
            master = (MasterInterface) registry.lookup(completeName);

            master.writeAck(fileName, Integer.toString(registryPort), version, oldPort);
        }
        catch (RemoteException | NotBoundException e) {

            // 2° tentativo di contattare il master:
            writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + "- Waiting...");
            LOGGER.log(Level.WARNING,"IMPOSSIBLE TO CONTACT Master " + masterAddress + "- Waiting...");
            try {
                // Attende TOT secondi:
                Date date = new Date();
                long timeInMillis = date.getTime();
                while (true) {
                    Date now = new Date();
                    if (now.getTime() - timeInMillis > 2000) {
                        break;
                    }
                }
                completeName = "//" + Config.registryHost + ":" + masterAddress + "/" + Config.masterServiceName;
                registry = LocateRegistry.getRegistry(Config.registryHost, masterAddress);
                master = (MasterInterface) registry.lookup(completeName);
                master.writeAck(fileName, Integer.toString(registryPort), version, oldPort);
            }
            catch (RemoteException | NotBoundException e2){
                e.printStackTrace();
                writeOutput("SEVERE: IMPOSSIBLE TO ACK Master " + masterAddress);
                LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO ACK Master " + masterAddress);
            }
        }
    }

    /**
     * Sposta un file verso un altro DataNode.
     *
     * @param fileName nome del file da postare
     * @param newServerPort indirizzo del destinatario
     * @param version versione del file
     */
    @Override
    public void moveFile(String fileName, String newServerPort, int version,String oldPort) throws FileNotFoundException,DataNodeException {
        String completeName = "//" + Config.registryHost + ":" + newServerPort + "/" + Config.dataNodeServiceName;
        // Look up the remote object by name in the server host's registry
        String base64;
        synchronized (dataNodeLock){
            base64 = dataNodeDAO.getFileString(fileName,false);
        }
        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(newServerPort));
            System.out.println(completeName);
            StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
            ArrayList<String> ports = new ArrayList<>();
            ports.add(newServerPort);
            dataNode.write(base64, fileName,ports, version,oldPort);
            writeOutput("Spostato "+fileName+" a "+newServerPort + " da " +registryPort);
            synchronized (dataNodeLock) {
                dataNodeDAO.deleteFile(fileName);
                writeOutput("Cancello " + fileName);
            }
        }
        catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
            writeOutput("Impossible to bind to DataNode");
            LOGGER.log(Level.OFF,"Impossible to bind to DataNode");
            throw new DataNodeException("Impossible to bind to DataNode "+newServerPort);
        }

    }

    private static void sendLifeSignal() {

        String completeName = "//" + Config.registryHost + ":" + masterAddress + "/" + Config.masterServiceName;

        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, masterAddress);
            MasterInterface master = (MasterInterface) registry.lookup(completeName);

            master.lifeSignal(Integer.toString(registryPort));
        }
        catch (RemoteException | NotBoundException e) {

            e.printStackTrace();
            writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Life Signal NOT Sent");
            LOGGER.log(Level.WARNING,"IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Life Signal NOT Sent");
            // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia il segnale.
        }
    }

    public boolean lifeSignal() {
        return true;
    }

    /**
     * Il Thread invia periodicamente un segnale al Master per segnalare che il DataNode è ancora attivo.
     *
     */
    private static Thread lifeThread = new Thread("LifeThread"){

        @Override
        public void run() {
            while(condition){
                sendLifeSignal();
                try {
                    sleep(2000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    /**
     * Invocata quando deve essere inoltrato l'aggiornamento di un file dalla replica primarie alle secondarie.
     *
     * @param data I dati da scrivere.
     * @param fileName File aggiornato.
     * @param dataNodePorts Porte dei dataNode che contengono le repliche secondarie.
     */
    @Override
    public String forwardWrite(String data, String fileName, ArrayList<String> dataNodePorts, int version, long fileSize,String oldPort) throws FileNotFoundException, DataNodeException {
        //writeOutput("forwardWrite "+fileName + " "+ registryPort);
        String message;
        synchronized (dataNodeLock) {
            dataNodeDAO.setFileString(fileName, data, version);
            sendCompletedWrite(fileName,version,oldPort);
            message = dataNodeDAO.getFileString(fileName,false);
        }
        Thread th = new Thread("forward-Thread") {

            @Override
            public void run() {
                if (dataNodePorts.isEmpty())
                    return;
                String nextDataNode = dataNodePorts.get(0);
                dataNodePorts.remove(nextDataNode);
                String completeName = "//" + Config.registryHost + ":" + nextDataNode + "/" + Config.dataNodeServiceName;
                // Look up the remote object by name in the server host's registry
                try {
                    registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(nextDataNode));
                    StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
                    dataNode.forwardWrite(data, fileName, dataNodePorts,version, fileSize,null);
                }
                catch (RemoteException | NotBoundException | FileNotFoundException | DataNodeException e) {
                    e.printStackTrace();
                    writeOutput(e.getMessage());
                    LOGGER.log(Level.SEVERE,e.getMessage());
                }
            }
        };
        th.start();

        return message;
    }

    /**
     * Funzione per attendere la terminazione del Thread per l'invio delle statistiche al Master e del Thread per il
     *  salvataggio del DB su disco.
     *
     */
    @Override
    public void killSignal() {
        condition = false;
        try {
            registry.unbind(completeName);
            UnicastRemoteObject.unexportObject(this, true);
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
         {
            try {
                statisticThread.join();
                saveDBThread.join();
                lifeThread.join();

            }
            catch (InterruptedException e) {
                e.printStackTrace();
                writeOutput(e.getMessage());
            }
        }
        System.out.println("SHUTDOWN");
        System.exit(1);
    }

    /**
     * Funzione per fare il logging dell'output di ogni DataNode.
     *
     * @param message Output del DataNode da appendere nel file.
     */
    @SuppressWarnings("all")
    private static void writeOutput(String message){

        try(BufferedWriter bw = new BufferedWriter(new FileWriter(file,true))) {
            message+="\n";
            bw.append(message);
            bw.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cambia l'indirizzo del Master di riferimento del DataNode.
     *
     * @param newMasterAddress Indirizzo del nuovo Master.
     */
    @Override
    public void changeMasterAddress(int newMasterAddress) {

        writeOutput("\nCambiato indirizzo del Master! Old: " + masterAddress + " - New: " + newMasterAddress + "\n");
        masterAddress = newMasterAddress;
    }

    /**
     *  Metodo per verificare che se le statistiche sono vuote.
     *
     */
    @Override
    public boolean isEmpty() {

        synchronized (dataNodeLock) {
            DataNodeStatistic statistic = dataNodeDAO.getDataNodeStatistic();
            if(statistic.getFileInfos().isEmpty()){
                return true; // DataNode vuoto.
            }
        }

        return false; // DataNode non vuoto.
    }

    /**
     * Metodo per terminare il DataNode da remoto.
     *
     */
    @Override
    public void terminate() {
        try {
            registry.unbind(completeName);
            UnicastRemoteObject.unexportObject(this, true);
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        writeOutput("Termination...");
        System.exit(0);
    }

    /**
     * Il Thread si occupa di inviare periodicamente al Master le statistiche del DataNode, quali:
     *
     *  - Numero di richieste ricevute.
     *  - Dimensione dei file memorizzati (in totale).
     *  - Numero di richieste di un file (per ogni singolo file).
     *  - Dimensione di un file (per ogni singolo file).
     *
     */
    private static Thread statisticThread = new Thread("statisticThread"){

        @Override
        public void run() {
            while (condition){
                String completeName = "//" + Config.registryHost + ":" + masterAddress + "/" + Config.masterServiceName;
                // Look up the remote object by name in the server host's registry
                try {
                    registry = LocateRegistry.getRegistry(Config.registryHost, masterAddress);
                    MasterInterface master = (MasterInterface) registry.lookup(completeName);

                    synchronized (dataNodeLock) {
                        DataNodeStatistic statistic = dataNodeDAO.getDataNodeStatistic();
                        writeOutput(statistic.toString());
                        // Inserisce il valore del timer nelle statistiche. Lo azzera se le statistiche NON sono vuote:
                        Date time = new Date();
                        if(statistic.getFileInfos().isEmpty()){
                            statistic.setMilliseconds_timer(time.getTime() - milliseconds);
                        }
                        else {
                            milliseconds = time.getTime();
                            statistic.setMilliseconds_timer(0);
                        }

                        statistic.orderStatistics();
                        // Invia le statistiche al Master:
                        master.setStatistic(statistic);
                    }
                }
                catch (RemoteException | NotBoundException e) {

                    e.printStackTrace();
                    writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Statistics NOT Sent");
                    LOGGER.log(Level.WARNING,"IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Statistics NOT Sent");

                    continue; // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia le statistiche.
                }

                try {
                    synchronized (dataNodeLock){
                        dataNodeDAO.resetStatistic();
                    }
                    sleep(Config.STATISTIC_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    writeOutput(e.getMessage());
                }
            }
        }
    };

    /**
     * Il Thread si occupa di salvare periodicamente le informazioni del DB in-memory del DataNode
     *  nel DB persistente su disco.
     *
     */
    private static Thread saveDBThread = new Thread("saveDBThread"){

        @Override
        public void run() {
            while (condition){
                synchronized (dataNodeLock) {
                    dataNodeDAO.saveDB();
                }
                try {
                    sleep(Config.SAVE_DB_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    writeOutput(e.getMessage());
                }
            }
        }
    };


}
