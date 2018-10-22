package com.sdcc_project.data_node;

import com.amazonaws.util.EC2MetadataUtils;
import com.sdcc_project.config.Config;
import com.sdcc_project.dao.DataNodeDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.exception.*;
import com.sdcc_project.monitor.Monitor;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;
import com.sdcc_project.util.FileManager;
import com.sdcc_project.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import static java.rmi.registry.LocateRegistry.createRegistry;

/**
 * Nodo del sistema che si occupa di :
 *
 * <ul>
 *     <li>Memorizzare il contenuto di un file e eventuali aggiornamenti</li>
 *     <li>Restituire il contenuto del file se richiesto</li>
 * </ul>
 *
 * Ogni DataNode è gestito da un Master dal sistema
 *
 */
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

    /**
     * Viene avviato il nodo,pubblica l'interfaccia sul registro RMI e lanciati i Thread di Gestione
     * @param args Indirizzo del Master di competenza.
     */
    public static void main(String args[]) {

        if(args.length != 1){
            System.out.println("Usage: DataNode <master_address>");
            System.exit(1);
        }

        milliseconds = Util.getTimeInMillies();
        monitor = Monitor.getInstance();
        masterAddress = args[0];
        address = Util.getPublicIPAddress();
        instanceID = EC2MetadataUtils.getInstanceId();
        System.setProperty("java.rmi.server.hostname",address);

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
        writeOutput("MIO INSTANCE ID " + instanceID);

        try {
            completeName = "//" + address + ":" + Config.port + "/" + serviceName;
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
        //Avvio del Thread che monitora l'utilizzo del risorse HW dell'istanza in cui è eseguito il nodo
        monitor.startThread();
        //Thread che invia al master le statistiche del nodo (File Contenuti,Peso di ogni file,Richieste per ogni file...
        statisticThread.start();
        //Thread che invia un segnale di vita al master
        lifeThread.start();
    }

    private static Remote registryLookup(String Address, String serviceName) throws NotBoundException, RemoteException {

        String completeName = "//" + Address + ":" + Config.port + "/" + serviceName;
        //writeOutput("Contatto "+Address +" " +Config.port + " per "+completeName);
        registry = LocateRegistry.getRegistry(Address, Config.port);
        return registry.lookup(completeName);
    }

    /**
     * Servizio RMI
     * @return I nomi di tutti i file contenuti nel DB del DataNode
     */
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
     * Servizio RMI
     * @return InstanceID dell'istanza EC2 in cui è eseguito il nodo
     */
    @Override
    public String getInstanceID() {

        return instanceID;
    }

    /**
     * Servizio RMI
     * Cancella un file dal DataNode
     * @param filename nome del file da cancellare
     * @return riuscita dell'operazione
     */
    @Override
    public boolean delete(String filename) {
        try {
            dataNodeDAO.deleteFile(filename);
        } catch (DataNodeException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Servizio RMI
     * Avvia la sequenza di spegnimento del Nodo (usata in caso di sottoutilizzo delle risorse)
     * Il DataNode invia le proprie repliche ad altri DataNode e viene spento.
     * @param aliveDataNode DataNode a cui propagare i file contenuti in questo nodo.
     */
    @Override
    public void shutDown(ArrayList<String> aliveDataNode)  {
        boolean ok = true;
        try {
            writeOutput("SEQUENZA DI SHUTDOWN");
            condition = false;
            try {
                lifeThread.join();
                statisticThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ArrayList<ArrayList<String>> allFiles = dataNodeDAO.getAllFilesInformation(address);
            writeOutput(allFiles.toString());
            int fileToEachDataNode =  allFiles.size()/aliveDataNode.size();
            if(fileToEachDataNode<allFiles.size()){
                fileToEachDataNode = allFiles.size();
            }
            for(String address : aliveDataNode){
                if(allFiles.isEmpty()) break;
                for(int i = 0;i<fileToEachDataNode;i++){
                    ArrayList<String> fileToMove = allFiles.get(0);
                    try {
                        writeOutput(fileToMove.toString());
                        moveFile(fileToMove.get(0),address,Integer.parseInt(fileToMove.get(2)));
                        allFiles.remove(0);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                        writeOutput(e.getMessage());

                    } catch (ImpossibleToMoveFileException e) {
                        e.printStackTrace();
                        writeOutput(e.getMessage());
                        ok = false;
                    }
                }
                if(fileToEachDataNode<allFiles.size())
                    fileToEachDataNode = allFiles.size();
                else if(allFiles.size()-fileToEachDataNode<fileToEachDataNode){
                    fileToEachDataNode = allFiles.size();
                }
            }
            if(ok){
                Thread shutDown = new Thread("shutDown"){
                    @Override
                    public void run() {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            MasterInterface masterInterface = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
                            masterInterface.shutdownDataNodeSignal(address);
                        } catch (RemoteException | NotBoundException e) {
                            e.printStackTrace();
                        }
                    }
                };
                shutDown.start();

            }
        } catch (DataNodeException e) {
            e.printStackTrace();
            writeOutput(e.getMessage());
        }
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
                writeOutput("Inserito File: " + filename + "- Data: "+data+" - Versione: " + updated_version);
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
    private void sendCompletedWrite(String fileName, int version) {

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

                long timeInMillis = Util.getTimeInMillies();
                while (true) {

                    if (Util.getTimeInMillies() - timeInMillis > 2000) {
                        break;
                    }
                }
                master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);
                master.writeAck(fileName, address, version);
            }
            catch (RemoteException | NotBoundException e2){
                writeOutput("SEVERE: IMPOSSIBLE TO ACK Master " + masterAddress);
            }
        }

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


            long timeInMillis = Util.getTimeInMillies();
            while (!getLifeSignal(replaced_dataNode)) {

                if (Util.getTimeInMillies() - timeInMillis > Config.SYSTEM_STARTUP_TYME) {
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

    /**
     * Verifica lo stato di attività di un altro DataNode del sistema
     * @param dataNode_address indirizzo del DataNode da monitorare
     * @return true se il DataNode è attivo False altrimenti
     */
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

    /**
     * Servizio RMI
     * Ping del Master per verificare che il DataNode è ancora attivo
     * @return true
     */
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
                    sleep(Config.LIFE_THREAD_SLEEP_TIME);
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
     *   <ul>
     *      <li>Numero di richieste ricevute</li>
     *      <li>Dimensione dei file memorizzati (in totale).</li>
     *      <li>Numero di richieste di un file (per ogni singolo file).</li>
     *      <li>Dimensione di un file (per ogni singolo file).</li>
     *  </ul>
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

                        if(statistic.getFileInfos().isEmpty()){
                            statistic.setMilliseconds_timer(Util.getTimeInMillies() - milliseconds);
                        }
                        else {
                            milliseconds = Util.getTimeInMillies();
                            statistic.setMilliseconds_timer(0);
                        }

                        statistic.orderStatistics();
                        statistic.setOverCpuUsage(monitor.isOverCpuUsage());
                        statistic.setRamUsage(monitor.isOverRamUsage());
                        statistic.setUnderUsage(monitor.isUnderUsage());
                        // Invia le statistiche al Master:
                        master.setStatistic(statistic);
                    }
                }
                catch (RemoteException e) {
                    writeOutput("WARNING: Impossible to contact Master " + masterAddress);
                     // Se non riesce a contattare il Master, semplicemente a questo giro non gli invia le statistiche.
                }catch (NotBoundException e){
                    writeOutput("NOT BOUND EXCEPTION\n" + e.getMessage());
                }

                try {
                    sleep(Config.STATISTIC_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    writeOutput(e.getMessage());
                }
            }
        }
    };



}
