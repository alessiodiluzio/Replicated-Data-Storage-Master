package com.sdcc_project.data_node;

import com.amazonaws.util.EC2MetadataUtils;
import com.sdcc_project.config.Config;
import com.sdcc_project.dao.DataNodeDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.exception.*;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.monitor.Monitor;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;
import com.sdcc_project.util.FileManager;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
    private static String instanceID;
    private static boolean condition = true;
    private static final Object dataNodeLock = new Object();
    private static String masterAddress;
    private static long milliseconds;
    private static String completeName;
    private static Monitor monitor;

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
        monitor = Monitor.getInstance();
        masterAddress = args[0];
        address = Util.getLocalIPAddress();
        instanceID = EC2MetadataUtils.getInstanceId(); // TODO: Verificare che funzioni.
        writeOutput("MIO INSTANCE ID " + instanceID);

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
            completeName = "//" + address + ":" + Config.port + "/" + serviceName;
            //writeOutput(completeName);
            DataNode dataNode = new DataNode();

            // Connessione dell'istanza con l'RMI Registry.
            registry = createRegistry(Config.port);
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

    private static Remote registryLookup(String Address, String serviceName) throws NotBoundException, RemoteException {

        String completeName = "//" + Address + ":" + Config.port + "/" + serviceName;
        //writeOutput("Contatto "+Address +" " +Config.port + " per "+completeName);
        registry = LocateRegistry.getRegistry(Address, Config.port);
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

    @Override
    public String getInstanceID() {

        return instanceID;
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
     * Scrittura/Aggiornamento di un file sul DataNode e creazione/aggiornamento delle altre repliche.
     *
     * @param filename Nome del file da scrivere/aggiornare.
     * @param data Contenuto del file da aggiornare.
     * @param replication_factory Numero di repliche da scrivere/aggiornare.
     *
     * @return Successo/Fallimento.
     */
    @Override
    public boolean write(String filename, String data, int updated_version, int replication_factory) throws DataNodeException {

        int new_replication_factory;
        String old_base64;
        String updated_base64;

        synchronized (dataNodeLock){
            try {
                old_base64 = dataNodeDAO.getFileString(filename, false);
                String old_data = FileManager.decodeString(old_base64);
                String updated_data = old_data + data + "\n";
                updated_base64 = FileManager.encodeString(updated_data);
            }
            catch (FileNotFoundException e){
                updated_base64 = FileManager.encodeString(data);
            }
        }

        try {
            synchronized (dataNodeLock) {
                dataNodeDAO.setFileString(filename, updated_base64, updated_version); // Inserisce/Aggiorna il file.
                writeOutput("Inserito File: " + filename + " - Versione: " + updated_version);
            }
        }
        catch (DataNodeException e) {
            writeOutput(e.getMessage());
            return false;
        }

        sendCompletedWrite(filename, updated_version); // Comunica al Master la scrittura del file.

        new_replication_factory = replication_factory - 1; // Decrementa il numero di repliche mancanti da creare/aggiornare.

        // Crea/Aggiorna le altre repliche del file:
        if(new_replication_factory > 0) {

            writeOutput("Cerco Altre Repliche da Creare/Aggiornare");

            Thread thread = new Thread("createReplicaThread") {
                @Override
                public void run() {

                    MasterInterface master;
                    String replica_address = null;

                    try {
                        master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
                        replica_address = master.getDataNodeAddressForReplication(filename, updated_version);
                    }
                    catch (RemoteException | NotBoundException | ImpossibleToFindDataNodeForReplication e) {
                        writeOutput("SEVERE:" + e.getMessage() + " - File" + filename);
                    }
                    if (replica_address != null) {
                        try {
                            writeOutput("Invio Creazione/Aggiornamento di una Replica del File: " + filename + " - Sul DataNode: " + replica_address);
                            StorageInterface dataNode = (StorageInterface) registryLookup(replica_address, Config.dataNodeServiceName);
                            dataNode.write(filename, data, updated_version, new_replication_factory);
                            writeOutput("Replica del File " + filename + " - Creata/Aggiornata sul DataNode: " + replica_address);
                        }
                        catch (RemoteException | NotBoundException | DataNodeException e) {
                            writeOutput("SEVERE: Impossible to Contact DataNode for Replication " + replica_address);
                        }
                    }
                }
            };
            thread.start();
        }

        return true;
    }

    /**
     * Scrive i dati di un file spostato da un altro DataNode dello STESSO MASTER.
     *
     * @param base64 Dati da scrivere.
     * @param fileName Nome del file da scrivere.
     * @return il nome file scritto.
     * */
    @Override
    public boolean writeMovedFile(String base64, String fileName, int version) {

        try {
            synchronized (dataNodeLock) {
                // Scrive il file:
                dataNodeDAO.setFileString(fileName, base64, version);
                writeOutput("Inserito File: " + fileName + " - Versione: " + version);
                // Comunica al Master lo spostamento del file:
                sendCompletedWrite(fileName, version);
            }
        }
        catch (Exception e) {
            writeOutput(e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Comunica al PROPRIO MASTER il salvataggio di un file sul DataNode.
     *
     * @param fileName Nome del file che è stato salvato.
     */
    private boolean sendCompletedWrite(String fileName, int version) {

        MasterInterface master;

        try {
            master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
            master.writeAck(fileName, address, version);
            writeOutput("Inviato ACK al Master: " + masterAddress + " - File: " + fileName + " - Versione: " + version);
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
                master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
                master.writeAck(fileName, address, version);
            }
            catch (RemoteException | NotBoundException e2){
                writeOutput("SEVERE: IMPOSSIBLE TO ACK Master " + masterAddress);
                return false;
            }
        }

        return true;
    }

    /**
     * Sposta un file su un altro DataNode dello STESSO MASTER.
     *
     * @param fileName nome del file da postare.
     * @param version versione del file.
     */
    @Override
    public void moveFile(String fileName, String new_dataNodeAddress, int version) throws FileNotFoundException, DataNodeException, ImpossibleToMoveFileException {

        String base64;
        boolean result;

        synchronized (dataNodeLock){
            base64 = dataNodeDAO.getFileString(fileName,false);
        }
        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(new_dataNodeAddress, Config.dataNodeServiceName);
            result = dataNode.writeMovedFile(base64, fileName, version);

            if(result) {
                writeOutput("Spostato File: " + fileName + " - Da " + address  + " a " + new_dataNodeAddress);
                synchronized (dataNodeLock) {
                    dataNodeDAO.deleteFile(fileName);
                    writeOutput("Cancellato il file: " + fileName);
                }
            }
            else {
                writeOutput("Impossible to move file to DataNode " + new_dataNodeAddress);
                throw new ImpossibleToMoveFileException("Impossible to move file to DataNode " + new_dataNodeAddress);
            }
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput("Impossible to bind to DataNode " + new_dataNodeAddress);
            throw new DataNodeException("Impossible to bind to DataNode " + new_dataNodeAddress);
        }
    }

    /**
     * Contatta un altro DataNode per passargli un file di cui deve essere ricreata una replica.
     *
     * @param filename Nome del file che deve essere copiato sull'altro DataNdde.
     * @param replaced_dataNode Indirizzo del DataNode su cui si deve copiare il file.
     * @throws ImpossibleToCopyFileOnDataNode ...
     */
    @Override
    public void copyFileOnAnotherDataNode(String filename, String replaced_dataNode) throws ImpossibleToCopyFileOnDataNode {

        String base64;
        String data;
        int version;
        boolean result;

        try {
            synchronized (dataNodeLock){
                base64 = dataNodeDAO.getFileString(filename,false);
                version = dataNodeDAO.getFileVersion(filename);
            }
            data = FileManager.decodeString(base64);

            Date date = new Date();
            long timeInMillis = date.getTime();
            while (!getLifeSignal(replaced_dataNode)) {
                Date now = new Date();
                if (now.getTime() - timeInMillis > Config.MAX_TIME_WAITING_FOR_INSTANCE_RUNNING) {
                    writeOutput("SEVERE: Impossible to Copy File on: " + replaced_dataNode + " - DataNode Not Active");
                    throw new ImpossibleToCopyFileOnDataNode("Impossible to Copy File on: " + replaced_dataNode + " - DataNode Not Active");
                }
            }

            StorageInterface dataNode = (StorageInterface) registryLookup(replaced_dataNode, Config.dataNodeServiceName);
            result = dataNode.write(filename, data, version, 1);
        }
        catch (RemoteException | NotBoundException e) {
            throw new ImpossibleToCopyFileOnDataNode("Impossible to Copy File on: " + replaced_dataNode + " - DataNode Not Active");
        }
        catch (FileNotFoundException e) {
            throw new ImpossibleToCopyFileOnDataNode("Impossible to Copy File on: " + replaced_dataNode + " - File Not Found");
        }
        catch (DataNodeException e) {
            throw new ImpossibleToCopyFileOnDataNode("Impossible to Copy File on: " + replaced_dataNode + " - " + e.getMessage());
        }

        if(!result) {
            throw new ImpossibleToCopyFileOnDataNode("Impossible to Copy File on: " + replaced_dataNode + " - Can't Write File");
        }

        writeOutput("File: " + filename + " Copied on DataNode: " + replaced_dataNode + " with Success");
    }

    private boolean getLifeSignal(String dataNode_address) {

        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(dataNode_address, Config.dataNodeServiceName);
            return dataNode.lifeSignal();
        }
        catch (NotBoundException | RemoteException e) {
            return false;
        }
    }

    /**
     * Invia al Master un segnale per fargli sapere che è ancora attivo.
     *
     */
    private static void sendLifeSignal() {

        try {
            MasterInterface master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
            master.lifeSignal(address);
        }
        catch (RemoteException | NotBoundException e) {
            writeOutput("WARNING: IMPOSSIBLE TO CONTACT Master " + masterAddress + " - Life Signal NOT Sent");
            // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia il segnale.
        }
    }

    @Override
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
                    MasterInterface master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);

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
                        statistic.setOverCpuUsage(monitor.isOverCpuUsage());
                        statistic.setRamUsage(monitor.isOverRamUsage());
                        // Invia le statistiche al Master:
                        master.setStatistic(statistic);
                    }
                }
                catch (RemoteException e) {
                    writeOutput("WARNING: Impossible to contact Master " + masterAddress);
                    continue; // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia le statistiche.
                }catch (NotBoundException e){
                    writeOutput("NOT BOUND EXCEPTION\n" + e.getMessage());
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
