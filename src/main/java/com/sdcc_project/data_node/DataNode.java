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
import com.sdcc_project.util.Util;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import static java.rmi.registry.LocateRegistry.createRegistry;

public class DataNode extends UnicastRemoteObject implements StorageInterface {

    private static Registry registry;
    private static DataNodeDAO dataNodeDAO;
    private static File file;
    private static String address;
    private static boolean condition = true;
    private static final Object dataNodeLock = new Object();
    private static String masterAddress;
    private static long milliseconds;
    private static String completeName;

    private DataNode() throws RemoteException {
        super();
    }

    public static void main(String args[]) {

        if(args.length != 1){
            System.out.println("Usage: DataNode <master_address>");
            System.exit(1);
        }

        Date time = new Date();
        milliseconds = time.getTime();
        final int REGISTRY_PORT = Config.port;
        masterAddress = args[0];
        address = Util.getLocalIPAddress();
        String registryHost = Config.registryHost;
        String serviceName = Config.dataNodeServiceName;

        try {
            dataNodeDAO = DataNodeDAO.getInstance(address);
        }
        catch (DataNodeException e) {
            writeOutput("SEVERE: DataNode SHUTDOWN - " + e.getMessage());
            System.exit(0);
        }
        // Creazione del file di Logging:
        file = new File(Config.DATANODE_FILE_LOGGING_NAME + ".txt");

        try {
            completeName = "//" + registryHost + ":" + REGISTRY_PORT + "/" + serviceName;
            DataNode dataNode = new DataNode();

            // Connessione dell'istanza con l'RMI Registry.
            registry = createRegistry(REGISTRY_PORT);
            registry.rebind(completeName, dataNode);
            writeOutput("DataNode lanciato all'indirizzo: " + address + " - Indirizzo del Master: " + masterAddress);
        }
        catch (Exception e) {
            writeOutput("SEVERE: Impossible to bind to master - " + e.getMessage());
            System.exit(0);
        }

        statisticThread.start();
        saveDBThread.start();
        lifeThread.start();
    }

    private static Remote registryLookup(String registryHost, String Address, String serviceName) throws NotBoundException, RemoteException {

        String completeName = "//" + registryHost + ":" + Address + "/" + serviceName;

        registry = LocateRegistry.getRegistry(registryHost, Integer.parseInt(Address));
        return registry.lookup(completeName);
    }

    @Override
    public ArrayList<ArrayList<String>> getDatabaseData() {

        ArrayList<ArrayList<String>> filesInfo = new ArrayList<>();

        try {
            filesInfo = dataNodeDAO.getAllFilesInformation(address);
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
            writeOutput(e.getMessage());
        }
        finally {
            try {
                // Cancello il file temporaneo e chiudo la connessione.

                Files.delete(path);
            }
            catch (IOException e) {
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
    public String write(String data,String fileName, ArrayList<String> dataNodeAddresses, int version, String oldAddress) throws DataNodeException, FileNotFoundException {

        String base64String ="";
        String dataString = "";

        synchronized (dataNodeLock){
            try {
                base64String = dataNodeDAO.getFileString(fileName, false);
            }
            catch (FileNotFoundException e){
                writeOutput("WARNING: New File");
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
        dataNodeAddresses.remove(address);

        return forwardWrite(updatedBase64File, fileName, dataNodeAddresses, version, fileSize, oldAddress);
    }

    /**
     * Comunica al Master il salvataggio di un file sul DataNode e:
     *
     *  - Nel caso di prima scrittura, comunica al Master la posizione (indirizzo del DataNode) del file.
     *  - Nel caso di scritture successive, comunica al Master di aggiornate le informazioni di posizione del file.
     *
     * @param fileName Nome del file che è stato salvato.
     */
    private void sendCompletedWrite(String fileName, int version, String oldAddress) {

        MasterInterface master;
        try {
            master = (MasterInterface) registryLookup(Config.registryHost, masterAddress, Config.masterServiceName);
            master.writeAck(fileName, address, version, oldAddress);
        }
        catch (RemoteException | NotBoundException e) {

            // 2° tentativo di contattare il master:
            writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + "- Waiting...");
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
                master = (MasterInterface) registryLookup(Config.registryHost, masterAddress, Config.masterServiceName);
                master.writeAck(fileName, address, version, oldAddress);
            }
            catch (RemoteException | NotBoundException e2){
                writeOutput("SEVERE: IMPOSSIBLE TO ACK Master " + masterAddress);
            }
        }
    }

    /**
     * Sposta un file verso un altro DataNode.
     *
     * @param fileName nome del file da postare
     * @param newServerAddress indirizzo del destinatario
     * @param version versione del file
     */
    @Override
    public void moveFile(String fileName, String newServerAddress, int version, String oldAddress) throws FileNotFoundException,DataNodeException {

        String base64;
        synchronized (dataNodeLock){
            base64 = dataNodeDAO.getFileString(fileName,false);
        }
        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(Config.registryHost, newServerAddress, Config.dataNodeServiceName);
            ArrayList<String> addresses = new ArrayList<>();
            addresses.add(newServerAddress);
            dataNode.write(base64, fileName, addresses, version, oldAddress);

            writeOutput("Spostato " + fileName + " da " + address + " a " + newServerAddress);
            synchronized (dataNodeLock) {
                dataNodeDAO.deleteFile(fileName);
                writeOutput("Cancello " + fileName);
            }
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput("Impossible to bind to DataNode");
            throw new DataNodeException("Impossible to bind to DataNode " + newServerAddress);
        }
    }

    /**
     * Invia al Master un segnale per fargli sapere che è ancora attivo.
     *
     */
    private static void sendLifeSignal() {

        try {
            MasterInterface master = (MasterInterface) registryLookup(Config.registryHost, masterAddress, Config.masterServiceName);
            master.lifeSignal(address);
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Life Signal NOT Sent");
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
                    writeOutput(e.getMessage());
                }
            }
        }
    };

    /**
     * Invocata quando deve essere inoltrato l'aggiornamento di un file dalla replica primarie alle secondarie.
     *
     * @param data I dati da scrivere.
     * @param fileName File aggiornato.
     * @param dataNodeAddresses Indirizzi dei dataNode che contengono le repliche secondarie.
     */
    @Override
    public String forwardWrite(String data, String fileName, ArrayList<String> dataNodeAddresses, int version, long fileSize, String oldAddress) throws FileNotFoundException, DataNodeException {

        String message;
        synchronized (dataNodeLock) {
            dataNodeDAO.setFileString(fileName, data, version);
            sendCompletedWrite(fileName, version, oldAddress);
            message = dataNodeDAO.getFileString(fileName,false);
        }
        Thread th = new Thread("forward-Thread") {

            @Override
            public void run() {
                if (dataNodeAddresses.isEmpty())
                    return;
                String nextDataNode = dataNodeAddresses.get(0);
                dataNodeAddresses.remove(nextDataNode);
                try {
                    StorageInterface dataNode = (StorageInterface) registryLookup(Config.registryHost, nextDataNode, Config.dataNodeServiceName);
                    dataNode.forwardWrite(data, fileName, dataNodeAddresses, version, fileSize,null);
                }
                catch (RemoteException | NotBoundException | FileNotFoundException | DataNodeException e) {
                    writeOutput(e.getMessage());
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
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput(e.getMessage());
        }
         {
            try {
                statisticThread.join();
                saveDBThread.join();
                lifeThread.join();
            }
            catch (InterruptedException e) {
                writeOutput(e.getMessage());
            }
        }
        writeOutput("SHUTDOWN");
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
            writeOutput(e.getMessage());
        }
    }

    /**
     * Cambia l'indirizzo del Master di riferimento del DataNode.
     *
     * @param newMasterAddress Indirizzo del nuovo Master.
     */
    @Override
    public void changeMasterAddress(String newMasterAddress) {

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
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput(e.getMessage());
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
                try {
                    MasterInterface master = (MasterInterface) registryLookup(Config.registryHost, masterAddress, Config.masterServiceName);

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
                    writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Statistics NOT Sent");
                    continue; // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia le statistiche.
                }

                try {
                    synchronized (dataNodeLock){
                        dataNodeDAO.resetStatistic();
                    }
                    sleep(Config.STATISTIC_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
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
                    writeOutput(e.getMessage());
                }
            }
        }
    };


}
