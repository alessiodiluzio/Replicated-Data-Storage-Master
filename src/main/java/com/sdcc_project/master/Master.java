package com.sdcc_project.master;

import com.amazonaws.util.EC2MetadataUtils;
import com.sdcc_project.aws_managing.EC2InstanceFactory;
import com.sdcc_project.aws_managing.S3Upload;
import com.sdcc_project.config.Config;
import com.sdcc_project.dao.MasterDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.entity.FileInfo;
import com.sdcc_project.entity.FileLocation;
import com.sdcc_project.exception.*;
import com.sdcc_project.monitor.Monitor;
import com.sdcc_project.monitor.State;
import com.sdcc_project.service_interface.CloudletInterface;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;
import java.io.*;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.util.NodeType;
import com.sdcc_project.util.Util;
import sun.rmi.runtime.Log;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.rmi.registry.LocateRegistry.createRegistry;

public class Master extends UnicastRemoteObject implements MasterInterface {

    //Addresses
    private static String mainMasterAddress;
    private static String address;
    private static ArrayList<String> dataNodeAddresses = new ArrayList<>();
    private static ArrayList<String> masterAddresses = new ArrayList<>();
    private static HashMap<String,String> dataNodeInstanceIDMap = new HashMap<>();
    private static HashMap<String,String> cloudletInstanceIDMap = new HashMap();

    //Controller
    private static EC2InstanceFactory ec2InstanceFactory;
    private static MasterDAO masterDAO;
    private static S3Upload s3Upload;
    private static Monitor monitor;

    //DataNodeInformations
    private static HashMap<String,DataNodeStatistic> dataNodesStatisticMap = new HashMap<>();
    private static HashMap<String,Long> lifeSignalMap = new HashMap<>();

    //CloudletInformations
    private static ArrayList<String> cloudletAddress = new ArrayList<>();
    private static HashMap<String,Long> cloudletLifeSignalMap = new HashMap<>();
    private static ArrayList<String> usableCloudlet = new ArrayList<>();

    // Lock:
    private static final Object statisticLock = new Object();
    private static final Object dataNodeAddressesLock = new Object();
    private static final Object lifeSignalMapLock = new Object();
    private static final Object cloudletLifeSignalMapLock = new Object();

    //Util
    private static boolean exit = false;
    private static final Logger LOGGER = Logger.getLogger(Master.class.getName());
    private static File file;

    private static String shadowMasterAddress;
    private static String shadowMasterInstanceID;

    private static boolean firstShadow = false;
    private static boolean splittingStartup = false;

    private Master() throws RemoteException {
        super();
    }

    public static void main(String args[]){

        if(args.length < 1){
            System.out.println("Usage: Master System_Startup");
            System.exit(1);
        }

        Thread waitThread = new Thread("Wait Thread"){
            @Override
            public void run() {
                try {
                    writeOutput("INIZIO THREAD DI STARTUP");
                    System.out.println("INIZIO DI THREAD DI STARTUP");
                    sleep(Config.SYSTEM_STARTUP_TYME);
                    writeOutput("FINE THREAD DI STARTUP");
                    System.out.println("FINE THREAD DI STARTUP");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        monitor = Monitor.getInstance();
        ec2InstanceFactory = EC2InstanceFactory.getInstance();
        s3Upload = S3Upload.getInstance();
        System.out.println("Mia istanza : "+ EC2MetadataUtils.getInstanceId());
        switch (args[0]){
            case "System_Startup":
                try {
                    masterConfiguration("Main");

                    // Creazione degli altri Master:
                    for(int i = 0; i < Config.REPLICATION_FACTORY-1; i++) {
                        createMasterInstance("Main");
                    }
                    System.out.print("Master Addresses: " + masterAddresses + ", " + address + "(io)\t");

                    // Creazione dei propri DataNode:
                    for(int j = 0; j < Config.NUMBER_OF_DATANODES; j++) {
                        createDataNodeInstance();
                    }
                    for (int i = 0;i<Config.CLOUDLET_NUMBER;i++){
                        createCloudLetInstance();
                    }
                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");
                }
                catch (MasterException e) {
                    e.printStackTrace();
                    LOGGER.log(Level.SEVERE,"MASTER SHUTDOWN " + e.getMessage());
                    System.exit(1);
                }
                catch (RemoteException e){
                    e.printStackTrace();
                }
                catch (ImpossibleToCreateMasterInstance e) {
                    e.printStackTrace();
                    LOGGER.log(Level.SEVERE, e.getMessage());
                }
                waitThread.start();
                try {
                    waitThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;

            case "Main":
                if (args.length != 2) {
                    System.out.println("Usage: Master Main <System_Startup_Address>");
                    System.exit(1);
                }
                try {
                    masterConfiguration("Main");

                    // Prende dal primo Master gli indirizzi dei Master:
                    String startupMasterAddress = args[1];
                    MasterInterface master = (MasterInterface) registryLookup(startupMasterAddress, Config.masterServiceName);
                    masterAddresses.addAll(master.getMasterAddresses());
                    masterAddresses.remove(address);
                    masterAddresses.add(startupMasterAddress);

                    writeOutput("Master Addresses: " + masterAddresses + ", " + address + "(io)\t");

                    // Creazione dei DataNode:
                    for (int i = 0; i < Config.NUMBER_OF_DATANODES; i++) {
                        createDataNodeInstance();
                    }
                    for (int i = 0;i<Config.CLOUDLET_NUMBER;i++){
                        createCloudLetInstance();
                    }
                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");
                }
                catch (MasterException e) {
                    writeOutput("MAIN MASTER SHUTDOWN: " + e.getMessage());
                    System.exit(1);
                }
                catch (ImpossibleToCreateMasterInstance e) {
                    writeOutput("SEVERE: " + e.getMessage());
                }
                catch (RemoteException | NotBoundException e){
                    writeOutput(e.getMessage());
                }
                waitThread.start();
                try {
                    waitThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;

            case "Shadow":
                if (args.length != 2) {
                    System.out.println("Usage: Master Shadow <main_master_address>");
                    System.exit(1);
                }
                try {
                    mainMasterAddress = args[1];
                    masterConfiguration("Shadow");
                    shadowThread.start();

                    // Quando lo Shadow Thread termina vuol dire che il Master principale è caduto.
                    // Lo Shadow Master deve quindi prendere il suo posto.
                    shadowThread.join();
                    firstShadow = true;
                }
                catch (Exception e) {
                    writeOutput("SHADOW MASTER SHUTDOWN: " + e.getMessage());
                    System.exit(1);
                }

                writeOutput("DataNode Addresses:\n" + dataNodeAddresses);
                writeOutput("Cloudlet Address:\n"+cloudletAddress);
                try {
                    for (String dataNodeAddress : dataNodeAddresses) {
                        StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);
                        // Informa il DataNode del cambio di indirizzo del Master:
                        writeOutput("Invio il nuovo indirizzo del Master " + address + " al DataNode " + dataNodeAddress);
                        dataNode.changeMasterAddress(address);
                        ArrayList<ArrayList<String>> db_data;
                        // Prende le informazioni dal DataNode:
                        db_data = dataNode.getDatabaseData();
                        for (ArrayList<String> file_info : db_data) {
                            // Inserisce le informazioni nel suo DB:
                            masterDAO.insertOrUpdateSingleFilePositionAndVersion(file_info.get(0), file_info.get(1), Integer.parseInt(file_info.get(2)));
                        }
                    }
                    for(String cloudletAddr : cloudletAddress){
                        CloudletInterface cloudlet = (CloudletInterface) registryLookup(cloudletAddr,Config.cloudLetServiceName);
                        writeOutput("Invio il nuovo indirizzo del Master "+address+" alla cloudlet " +cloudletAddr);
                        cloudlet.newMasterAddress(address);
                    }
                    //writeOutput("DB Data:\n" + masterDAO.getAllData());
                }
                catch (Exception e) {
                    writeOutput(e.getMessage());
                }

                writeOutput("Master Addresses:\n" + masterAddresses + ", " + address + "(io)\t");
                // Contatta gli altri Master per farsi conoscere:
                try {
                    for (String masterAddress : masterAddresses) {

                        MasterInterface master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);

                        // Rimuove l'indirizzo del Main Master e aggiunge l'indirizzo dello Shadow Master:
                        master.updateMasterAddresses(address, mainMasterAddress);
                    }

                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");
                }
                catch (NotBoundException | RemoteException e) {
                    writeOutput(e.getMessage());
                }
                catch (ImpossibleToCreateMasterInstance e){
                    writeOutput("SEVERE: " + e.getMessage());
                }

                break;

            case "Splitting":
                try {
                    masterConfiguration("Splitting");

                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");
                }
                catch (MasterException e) {
                    writeOutput("SPLITTING MASTER SHUTDOWN: " + e.getMessage());
                    System.exit(1);
                }
                catch (ImpossibleToCreateMasterInstance e){
                    writeOutput("SEVERE: " + e.getMessage());
                }
                catch (RemoteException e){
                    writeOutput(e.getMessage());
                }

                break;
        }

        shadowLifeThread.start();
        monitor.startThread();
        balancingThread.start();
        lifeThread.start();
        cloudletLifeThread.start();
        publishCloudletAddress.start();

    }



    /**
     * Setta le informazioni base del Master.
     *
     * @param masterType Tipologia di Master: Main, Shadow, Splitting.
     * @throws MasterException ...
     * @throws RemoteException ...
     */
    private static void masterConfiguration(String masterType) throws MasterException, RemoteException {

        masterDAO = MasterDAO.getInstance(Config.MASTER_DATABASE_NAME);
        address = Util.getLocalIPAddress();
        String serviceName = Config.masterServiceName;
        String completeName = "//" + address + ":" + Config.port + "/" + serviceName;
        //System.out.println(completeName);

        Master master = new Master();
        Registry registry = createRegistry(Config.port);
        registry.rebind(completeName, master);

        System.out.println(masterType + " Master Bound");
        file = new File(Config.MASTER_FILE_LOGGING_NAME + ".txt");
        writeOutput(masterType + " Master lanciato all'indirizzo: " + address);
    }

    /**
     * Setta la connessione RMI.
     *
     * @param Address L'indirizzo IP dell'host con cui instaurare la connessione IP
     * @param serviceName Nome del servizio di cui effettuare il lookup
     * @return interfaccia dell'oggetto remoto trovato
     * @throws NotBoundException ...
     * @throws RemoteException ...
     */
    private static Remote registryLookup(String Address, String serviceName) throws NotBoundException, RemoteException {

        String completeName = "//" + Address + ":" + Config.port + "/" + serviceName;

        Registry registry = LocateRegistry.getRegistry(Address, Config.port);
        return registry.lookup(completeName);
    }

    /**
     * Funzione per fare il logging dell'output di ogni DataNode.
     *
     * @param message Output del DataNode da appendere nel file
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
     * Metodo che restituisce tutti gli indirizzi dei DataNode gestiti dal Master.
     *
     * @return Lista di indirizzi di DataNode.
     */
    @Override
    public ArrayList<String> getDataNodeAddresses() {

        synchronized (dataNodeAddressesLock) {

            return dataNodeAddresses;
        }
    }

    @Override
    public ArrayList<String> getMasterAddresses() {

        return masterAddresses;
    }

    @Override
    public ArrayList<String> getCloudletAddresses(){
        return cloudletAddress;
    }

    /**
     * Metodo che aggiorna la lista di indirizzi dei Master.
     *
     * @param newMasterAddress Indirizzo di un nuovo Master da aggiungere.
     * @param oldMasterAddress Indirizzo di un vecchio Master da rimuovere.
     */
    @Override
    public void updateMasterAddresses(String newMasterAddress, String oldMasterAddress) {

        if(oldMasterAddress != null){
            masterAddresses.remove(oldMasterAddress);
        }

        if(!masterAddresses.contains(newMasterAddress)) {
            masterAddresses.add(newMasterAddress);
        }

        writeOutput("Master Addresses Updated! New List: " + masterAddresses + ", " + address + "(io)\t");
        System.out.print("Master Addresses Updated! New List: " + masterAddresses + ", " + address + "(io)\t");
    }


    /**
     * Crea un DataNode avviandolo su una nuova istanza di Amazon EC2.
     *
     * @return l'indirizzo IP del nuovo DataNode
     */
    private static String createDataNodeInstance(){

        String arguments = address;
        ArrayList<String> newInstanceInfo =  ec2InstanceFactory.createEC2Instance(NodeType.DataNode,arguments);
        String newDataNodeIP = newInstanceInfo.get(1);
        dataNodeInstanceIDMap.put(newDataNodeIP,newInstanceInfo.get(0));
        dataNodeAddresses.add(newDataNodeIP);

        writeOutput("Nuovo DataNode lanciato all'indirizzo: " + newDataNodeIP + " - Indirizzo del Master: " + address);
        System.out.println("Nuovo DataNode lanciato all'indirizzo: " + newDataNodeIP + " - Indirizzo del Master: " + address);
        Date now = new Date();


        return newDataNodeIP;
    }


    private static String createCloudLetInstance() {
        String arguments = address;
        ArrayList<String> newInstanceInfo=ec2InstanceFactory.createEC2Instance(NodeType.CloudLet,arguments);
        String newCloudLetIP = newInstanceInfo.get(1);
        cloudletInstanceIDMap.put(newCloudLetIP,newInstanceInfo.get(0));
        cloudletAddress.add(newCloudLetIP);
        System.out.println("Launched cloudlet at "+newCloudLetIP+"\n\n");
        writeOutput("Launched cloudlet at "+newCloudLetIP+"\n\n");
        return newCloudLetIP;
    }

    /**
     * Crea un master avviandolo su una nuova istanza di Amazon EC2
     *
     * @param nodeType tipo di master da avviare
     * @return L'indirizzo ip del nuovo master
     * @throws ImpossibleToCreateMasterInstance ...
     */
    private static String createMasterInstance(String nodeType) throws ImpossibleToCreateMasterInstance {

        String arguments;
        String localIPAddress = Util.getLocalIPAddress();

        switch (nodeType) {
            case "Shadow":

                if (localIPAddress == null) {
                    throw new ImpossibleToCreateMasterInstance("Impossibile to get Master's Local IP Address");
                }
                arguments = "\"Shadow" + " " + localIPAddress + "\"";
                break;
            case "Main":
                arguments = "\"Main" + " " + localIPAddress + "\"";
                break;
            default:  // "Splitting"
                arguments = nodeType;
                break;
        }
        ArrayList<String> newMasterInfo = ec2InstanceFactory.createEC2Instance(NodeType.Master, arguments);
        String newMasterIP = newMasterInfo.get(1);
        if(nodeType.equals("Shadow")) {
            shadowMasterAddress = newMasterIP;
            shadowMasterInstanceID = newMasterInfo.get(0);
        }
        if(nodeType.equals("Main")){
            masterAddresses.add(newMasterIP);
        }

        writeOutput("\nNuovo Master -" + nodeType + "- lanciato all'indirizzo: " + newMasterIP+"\n\n");
        System.out.print("\nNuovo Master -" + nodeType + "- lanciato all'indirizzo: " + newMasterIP+"\n\n");

        return newMasterIP;
    }

    public FileLocation getMostUpdatedFileLocation(String filename,String operation) throws FileNotFoundException {
        ArrayList<String> toContactMaster = new ArrayList<>(masterAddresses) ;
        toContactMaster.add(address);
        FileLocation result = new FileLocation();
        ArrayList<FileLocation> fileLocations = new ArrayList<>();
        for(String addr : toContactMaster){
            try {
                MasterInterface masterInterface = (MasterInterface) registryLookup(addr,Config.masterServiceName);
                FileLocation fl = masterInterface.checkFile(filename,operation);
                if(fl.getFilePositions().get(0)!=null) fileLocations.add(fl);
            } catch (FileNotFoundException  e) {
                LOGGER.log(Level.WARNING,e.getMessage());

            } catch (MasterException | RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
        }
        if(fileLocations.isEmpty()){
            if(operation.equals("W")){
                String dataNode = findReplicaPosition(filename,1);
                result.setResult(true);
                result.setFileVersion(1);
                ArrayList<String> fPos = new ArrayList<>();
                fPos.add(dataNode);
                result.setFilePositions(fPos);
            }
            else {
                throw new FileNotFoundException("FILE NOT IN THE SYSTEM");
            }
        }
        else {
            int latestReplica = 0;
            for(FileLocation  fLoc: fileLocations){
                if(fLoc.getFileVersion()>latestReplica){
                    latestReplica = fLoc.getFileVersion();
                    result = fLoc;
                }
            }
        }
        return result;
    }

    @Override
    public boolean ping() {
        return true;
    }


    /**
     * Funzone che ricerca il DataNode in cui è contenuto un file richiesto dal Client.
     *
     * @param fileName Nome del file richiesto dal Client.
     * @return Indirizzo del DataNode responsabile del file.
     */
    @Override
    public FileLocation checkFile(String fileName, String operation) throws MasterException,FileNotFoundException {

        FileLocation cl = new FileLocation();
        if (operation.equals("R")) {

            String fileAddress = masterDAO.getPrimaryReplicaFilePosition(fileName);
            ArrayList<String> result = new ArrayList<>();
            result.add(fileAddress);
            cl.setResult(true);
            cl.setFilePositions(result);
            int latestVersion = masterDAO.getFileVersion(fileName,fileAddress);
            cl.setFileVersion(latestVersion);
            return cl;
        }
        if (operation.equals("W")) {

                String filePosition ;
                ArrayList<String> positions = new ArrayList<>();
                filePosition = masterDAO.getFilePosition(fileName);
                cl.setResult(true);
                int latestVersion = masterDAO.getFileVersion(fileName, filePosition);
                cl.setFileVersion(latestVersion + 1);
                positions.add(filePosition);
                cl.setFilePositions(positions);

        }

        return cl;
    }


    /**
     * Notifica di scrittura di un file su un DataNode.
     *
     * @param filename Nome del file di cui si deve aggiornare la versione.
     * @param dataNode_address nuova Posizione del file.
     * @param version versione del file aggiornato.
     */
    @Override
    public void writeAck(String filename, String dataNode_address, int version){

        boolean find;

        synchronized (dataNodeAddressesLock){
            find = dataNodeAddresses.contains(dataNode_address);
        }

        try {
            if (find) {
                masterDAO.insertOrUpdateSingleFilePositionAndVersion(filename, dataNode_address, version);
            }
            else {
                masterDAO.deleteFilePosition(filename, dataNode_address);
            }
        }
        catch (MasterException e) {
            writeOutput(e.getMessage());
            LOGGER.log(Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Il Master chiede agli altri Masters che conosce la posizione di una replica del file da creare/aggiornare.
     *
     * @param filename Nome del file di cui si vuole cercare la posizione di una replica da creare/aggiornare.
     * @param version Versione del file.
     * @return Indirizzo di un DataNode su un altro Master su cui poter creare/aggiornare la replica del file.
     * @throws ImpossibleToFindDataNodeForReplication ...
     */
    @Override
    public String getDataNodeAddressForReplication(String filename, int version) throws ImpossibleToFindDataNodeForReplication {

        MasterInterface master;
        String replica_address = null;
        ArrayList<String> random_masterAddresses = new ArrayList<>(masterAddresses);
        Collections.shuffle(random_masterAddresses);

        for (String master_address : random_masterAddresses) {

            try {
                writeOutput("Cerco una Possibile Replica sul Master: " + master_address);
                System.out.println("Cerco una Possibile Replica sul Master: " + master_address);
                master = (MasterInterface) registryLookup(master_address, Config.masterServiceName);
                replica_address = master.findReplicaPosition(filename, version);

                if (replica_address != null) {
                    break; // Trovato il DataNode su cui scrivere una replica.
                }
            } catch (RemoteException | NotBoundException e) {
                writeOutput("WARNING: Impossible to Contact Master " + master_address);
                System.out.println("WARNING: Impossible to Contact Master " + master_address);
            }
        }

        if(replica_address == null) {
            throw new ImpossibleToFindDataNodeForReplication("Impossible to Find DataNode for Replication");
        }

        writeOutput("Trovato DataNode: " + replica_address + " - Su cui Creare/Aggiornare una Replica del File: " + filename);
        System.out.println("Trovato DataNode: " + replica_address + " - Su cui Creare/Aggiornare una Replica del File: " + filename);

        return replica_address;
    }

    /**
     * Il Master cerca tra i suoi DataNodes uno su cui:
     *
     *  - Poter scrivere una replica del file, se non ha ancora quel file su nessun DataNodes.
     *  o
     *  - Dover aggiornare la replica del file che possiede.
     *
     * @param filename Nome del file di cui si cerca la replica.
     * @param version Versione del file.
     * @return Indirizzo di un proprio DataNode su cui scrivere/aggiornare una replica del file.
     */
    @Override
    public String findReplicaPosition(String filename, int version) {

        String replica_address = null;

        // Creazione di una replica:
        if(version == 1) {
            try {
                // Controlla se ha già una replica presente:
                replica_address = masterDAO.getFilePosition(filename);

                if(replica_address == null) {
                // Prende un DataNode su cui replicare, con il RoundRobin:
                    replica_address = getRandomAliveDataNode();
                    writeOutput("Trovata una Nuova Posizione di Replicazione: " + replica_address + " - Per il File: " + filename);
                }
                else {
                    writeOutput("Già Presente una Replica: " + replica_address + " - Del il File: " + filename);
                    return null; // Possiede già una replica del file.
                }
            }
            catch (MasterException e) {
                writeOutput(e.getMessage());
                System.out.println(e.getMessage());
            }

        }
        // Verifico la presenza della replica da aggiornare:
        else {
            try {
                // Controlla se ha una replica del file:
                replica_address = masterDAO.getFilePosition(filename);

                if(replica_address != null) {
                    int fileVersion = masterDAO.getFileVersion(filename, replica_address);
                    if(fileVersion >= version) {
                        return null; // Replica già aggiornata.
                    }
                    writeOutput("Trovata una Replica da Aggiornare: " + replica_address + " - Del il File: " + filename);
                }
                else {
                    writeOutput("Nessuna Replica da Aggiornare Trovata - Per il File: " + filename);
                    return null; // Non ha nessuna replica di quel file.
                }
            }
            catch (MasterException e) {
                writeOutput(e.getMessage());
                System.out.println(e.getMessage());
            }
        }

        return replica_address;
    }

    /**
     * Restituisce l'indirizzo di un DataNode in cui è presente il file specificato.
     *  Altrimenti NULL.
     *
     * @param filename Nome del file di cui cercare la posizione.
     * @return Indirizzo del DataNode in cui è presente il file, o NULL.
     */
    @Override
    public String getDataNodeWithFile(String filename) {

        String dataNode_address = null;

        try {
            dataNode_address = masterDAO.getFilePosition(filename);
        }
        catch (MasterException e) {
            writeOutput("SEVERE: " + e.getMessage());
            LOGGER.log(Level.SEVERE, e.getMessage());
        }

        return dataNode_address;
    }

    /**
     *  Inserisce in un oggetto 'dataNodesStatistic' le informazioni sulle statistiche del DataNode.
     *
     */
    @Override
    public void setStatistic(DataNodeStatistic dataNodeStatistic) {

        boolean find;

        // Verifica che l'indirizzo compare nell'elenco di DataNode gestito da questo Master:
        synchronized (dataNodeAddressesLock){
            find = dataNodeAddresses.contains(dataNodeStatistic.getDataNodeAddress());
        }
        if(find){
            synchronized (statisticLock) {
                dataNodesStatisticMap.put(dataNodeStatistic.getDataNodeAddress(), dataNodeStatistic);
            }
        }
    }

    /**
     * Inserisce/Aggiorna il segnale di vita di un DataNode.
     *
     * @param dataNodeAddress Indirizzo del DataNode che ha mandato il sengale.
     */
    @Override
    public void lifeSignal(String dataNodeAddress) {

        boolean find;

        // Verifica che l'indirizzo compare nell'elenco di DataNode gestito da questo Master:
        synchronized (dataNodeAddressesLock){
           find = dataNodeAddress.contains(dataNodeAddress);
        }


        if(find){
            Date now = new Date();
            long timeInMillis = now.getTime();

            synchronized (lifeSignalMapLock) {
                lifeSignalMap.remove(dataNodeAddress);
                lifeSignalMap.put(dataNodeAddress, timeInMillis);
            }
        }
    }

    /**
     * Inserisce/Aggiorna il segnale di vita di una Cloudlet.
     *
     * @param cloudletAddr Indirizzo del DataNode che ha mandato il sengale.
     * @param state Stato della Cloudlet.
     */
    @Override
    public void cloudletLifeSignal(String cloudletAddr, State state) {

        boolean find = cloudletAddress.contains(cloudletAddr);
        System.out.println("CloudLetLifeSignal "+cloudletAddr+ " stato "+state);
        // Verifica che l'indirizzo compare nell'elenco di DataNode gestito da questo Master:
        if(state.equals(State.BUSY)){
            usableCloudlet.remove(cloudletAddr);
            System.out.println("CloudLet "+cloudletAddr+ " BUSY ");
        }
        else if(state.equals(State.NORMAL)){
            if(!usableCloudlet.contains(cloudletAddr))
                usableCloudlet.add(cloudletAddr);
        }

        if(find){
            Date now = new Date();
            long timeInMillis = now.getTime();

            synchronized (cloudletLifeSignalMapLock) {
                cloudletLifeSignalMap.remove(cloudletAddr);
                cloudletLifeSignalMap.put(cloudletAddr, timeInMillis);
            }
        }
    }

    /**
     * Algoritmo Round - Robin per la distribuzione delle "prime richieste" di scrittura
     *
     * @return un array di indirizzi estratte tramite round robin di dimensione pari al coefficente di replicazione(interno)
     */
    private String getRandomAliveDataNode(){
        ArrayList<String> aliveDataNode = new ArrayList<>();
        HashMap<String,Long> localSignalMap ;
        synchronized (lifeSignalMapLock){
            localSignalMap = new HashMap<>(lifeSignalMap);
        }

        for (Map.Entry<String, Long> entry : localSignalMap.entrySet()) {
            Date now = new Date();
            long timeInMillies = now.getTime();
            if (timeInMillies - entry.getValue() < Config.MAX_TIME_NOT_RESPONDING_DATANODE) {
                aliveDataNode.add(entry.getKey());
            }
        }
        Collections.shuffle(aliveDataNode);
        if(aliveDataNode.isEmpty())
            return null;
        return aliveDataNode.get(0);
    }

    /**
     * Avvia l'operazione di bilanciamento di un file, inviandolo al DataNode destinatario.
     *
     * @param filename FIle spostare e reletive informazioni
     * @param old_address indirizzo di provenienza
     * @param new_serverAddress Destinazione del file
     */
    private static void balanceFile(String filename, String old_address, String new_serverAddress) throws MasterException, FileNotFoundException, DataNodeException, NotBalancableFile, AlreadyMovedFileException, ImpossibleToMoveFileException {

        writeOutput("Tento di mandare " + filename + " in " + new_serverAddress + " da " + old_address);
        System.out.println("Tento di mandare " + filename + " in " + new_serverAddress + " da " + old_address);

        if (masterDAO.serverContainsFile(filename, new_serverAddress)) {
            throw new NotBalancableFile("A replica of this file " + filename + " is already in the server " + new_serverAddress);
        }
        if(!masterDAO.serverContainsFile(filename, old_address)){
            throw new AlreadyMovedFileException("File " + filename + " is not in the server " + old_address + " anymore.");
        }

        sendFileToDataNode(filename, new_serverAddress, old_address);

        System.out.println("Spostato " + filename + " da " + old_address + " in " + new_serverAddress);
        writeOutput("Spostato " + filename + " da " + old_address + " in " + new_serverAddress);
    }

    /**
     * Ordina ad un DataNode di spostare un file ad un altro DataNode dello STESSO MASTER.
     *
     * @param fileName Nome del file da spostare
     * @param new_serverAddress Indirizzo di destinazione.
     * @param old_serverAddress Indirizzo mittente.
     */
    private static void sendFileToDataNode(String fileName, String new_serverAddress, String old_serverAddress) throws MasterException, FileNotFoundException, DataNodeException, ImpossibleToMoveFileException {

        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(old_serverAddress, Config.dataNodeServiceName);
            dataNode.moveFile(fileName, new_serverAddress, masterDAO.getFileVersion(fileName, old_serverAddress));
        }
        catch (NotBoundException | IOException e) {
            e.printStackTrace();
            throw new MasterException("Error in bind to DataNode on Address " + old_serverAddress);
        }
    }

    private static boolean getLifeSignal(String dataNodeAddress){

        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);
            return dataNode.lifeSignal();
        }
        catch (NotBoundException | IOException e) {
            return false;
        }
    }

    /**
     * Recupera i file che erano sul DataNode crashato, contattanto gli altri Master e richiedento la posizione di una
     *  replica di ciascun file da recuperare.
     *
     * @param address Indirizzo del DataNode crashato.
     */
    private static void handleDataNodeCrash(String address){

        ArrayList<String> files_to_recover ;
        MasterInterface master;

        // Verifica nuovamente che il DataNode sia caduto:
        if(getLifeSignal(address)) {
            return;
        }



        synchronized (dataNodeAddressesLock){
            dataNodeAddresses.remove(address);
        }
        synchronized (statisticLock){
            dataNodesStatisticMap.remove(address);
        }
        synchronized (lifeSignalMapLock) {
            lifeSignalMap.remove(address);
        }
        // Crea un DataNode su cui ripristinare i file del DataNode crashato:
        String replaced_dataNode = createDataNodeInstance();
        ec2InstanceFactory.terminateEC2Instance(dataNodeInstanceIDMap.get(address));
        dataNodeInstanceIDMap.remove(address);

        // Cerca una replica di ciascun file da recuperare:
        try {
            files_to_recover = masterDAO.getServerFiles(address);

            for(String filename : files_to_recover) {

                String file_position = null;
                masterDAO.deleteFilePosition(filename, address);

                // Contatta gli altri Master per cercare un DataNode con il file da recuperare:
                for(String master_address : masterAddresses) {

                    try {
                        // Contatta un altro Master:
                        master = (MasterInterface) registryLookup(master_address, Config.masterServiceName);
                        file_position = master.getDataNodeWithFile(filename);

                        if (file_position != null) {
                            break; // Trovato il DataNode che possiede il file da recuperare.
                        }
                    }
                    catch (RemoteException | NotBoundException e) {
                        writeOutput("WARNING: Impossible to Contact Master " + master_address);
                        System.out.println("WARNING: Impossible to Contact Master " + master_address);
                    }
                }

                // Non ha trovato nessun DataNode da cui recuperare il file:
                if(file_position == null) {
                    writeOutput("SEVERE: Impossible to Recover File: " + filename);
                    LOGGER.log(Level.SEVERE, "Impossible to Recover File: " + filename);
                    continue;
                }

                writeOutput("Trovato DataNode: " + file_position + " - Da cui Recuperare il File: " + filename);
                System.out.println("Trovato DataNode: " + file_position + " - Da cui Recuperare il File: " + filename);

                // Contatta il DataNode che possiede il file da recuperare:
                try {
                    StorageInterface dataNode = (StorageInterface) registryLookup(file_position, Config.dataNodeServiceName);
                    dataNode.copyFileOnAnotherDataNode(filename, replaced_dataNode);
                }
                catch (NotBoundException | RemoteException e) {
                    writeOutput("SEVERE: Impossible to Contact DataNode " + file_position);
                    LOGGER.log(Level.SEVERE,"Impossible to Contact DataNode " + file_position);
                    continue;
                }
                catch (ImpossibleToCopyFileOnDataNode e) {
                    writeOutput("SEVERE: " + e.getMessage());
                    LOGGER.log(Level.SEVERE, e.getMessage());
                    continue;
                }

                writeOutput("Recuperato il File: " + filename + " - Dal DataNode: " + file_position);
                System.out.println("Recuperato il File: " + filename + " - Dal DataNode: " + file_position);
            }
        }
        catch (MasterException e) {
            e.printStackTrace();
            writeOutput(e.getMessage());
        }
        catch (FileNotFoundException e) {
            writeOutput("WARNING: " + e.getMessage());
            LOGGER.log(Level.WARNING, e.getMessage());
        }
    }

    private static Thread shadowLifeThread = new Thread("ShadowLifeThread"){
        @Override
        public void run() {
            while (!exit) {
                if(firstShadow){
                    try {
                        sleep(Config.SYSTEM_STARTUP_TYME);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    firstShadow= false;

                }
                MasterInterface masterInterface;
                try {
                    Date date = new Date();
                    masterInterface = (MasterInterface) registryLookup(shadowMasterAddress, Config.masterServiceName);
                    masterInterface.ping();
                } catch (NotBoundException | RemoteException e) {
                    try {
                        System.out.println("MORTO LO SHADOW LO UCCIDO E NE CREO UN ALTRO");
                        ec2InstanceFactory.terminateEC2Instance(shadowMasterInstanceID);
                        createMasterInstance("Shadow");
                        try {
                            sleep(Config.SYSTEM_STARTUP_TYME);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } catch (ImpossibleToCreateMasterInstance impossibleToCreateMasterInstance) {
                        impossibleToCreateMasterInstance.printStackTrace();
                    }
                }
            }
        }
    };

    /**
     *  Il Thread si occupa di verificare che tutti i DataNode siano ancora attivi.
     *
     */
    private static Thread lifeThread =  new Thread("lifeThread"){

        @Override
        public void run() {

            while (!exit) {
                    HashMap<String, Long> localSignalMap;
                    synchronized (lifeSignalMapLock) {
                        localSignalMap = new HashMap<>(lifeSignalMap);
                    }

                    for (Map.Entry<String, Long> entry : localSignalMap.entrySet()) {

                        Date now = new Date();
                        long timeInMillies = now.getTime();
                        if (timeInMillies - entry.getValue() > Config.MAX_TIME_NOT_RESPONDING_DATANODE) {
                            writeOutput("DATANODE " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()));
                            LOGGER.log(Level.INFO, "DATANODE " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()));
                            handleDataNodeCrash(entry.getKey());
                        }
                    }

                try{
                        sleep(Config.LIFE_THREAD_SLEEP_TIME);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        }
    };


    private static Thread cloudletLifeThread =  new Thread("CloudletLifeThread"){

        @Override
        public void run() {

            while (!exit) {
                HashMap<String, Long> localSignalMap;
                synchronized (cloudletLifeSignalMapLock) {
                    localSignalMap = new HashMap<>(cloudletLifeSignalMap);
                }

                for (Map.Entry<String, Long> entry : localSignalMap.entrySet()) {
                    Date now = new Date();
                    long timeInMillies = now.getTime();
                    if (timeInMillies - entry.getValue() > Config.MAX_TIME_NOT_RESPONDING_CLOUDLET) {
                        writeOutput("Cloudlet " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()));
                        LOGGER.log(Level.INFO, "Cloudlet " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()));
                        createCloudLetInstance();
                        cloudletLifeSignalMap.remove(entry.getKey());
                        cloudletAddress.remove(entry.getKey());
                        ec2InstanceFactory.terminateEC2Instance(cloudletInstanceIDMap.get(entry.getKey()));
                        cloudletInstanceIDMap.remove(entry.getKey());
                    }
                }
                int diff = Config.CLOUDLET_NUMBER - usableCloudlet.size();
                for(int i = 0;i<diff;i++){
                    createCloudLetInstance();
                }
                try{
                    sleep(Config.LIFE_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    /**
     * Il Thread si occupa di fare il bilanciamento dei file sui DataNode in base a:
     *
     *  - Dimensione dei file sui singoli DataNode.
     *  - Numero di richieste ai singoli DataNode.
     *
     */
    private static Thread balancingThread = new Thread("balancingThread") {

        //Variabili globali del Thread

        //Mappa delle statistiche dei Server
        HashMap<String,DataNodeStatistic> localStatisticMap = new HashMap<>();
        //Statistiche dei Server dopo il bilanciamento
        ArrayList<DataNodeStatistic> statisticAfterBalancing = new ArrayList<>();



        @Override
        public void run() {

            while (!exit) {

                // Uccido i DataNode che sono vuoti da un lungo periodo di tempo:
                try {
                    deleteEmptyDataNode();
                }
                catch (ImpossibleToTerminateDatanodeException e) {
                    writeOutput("SEVERE: IMPOSSIBLE TO TERMINATE DataNode " + e.getMessage());
                    LOGGER.log(Level.SEVERE,"SEVERE: IMPOSSIBLE TO TERMINATE DataNode " + e.getMessage());
                }
                catch (ImpossibleToTerminateEC2InstanceException e) {
                    writeOutput("SEVERE: IMPOSSIBLE TO TERMINATE EC2 Instance " + e.getMessage());
                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO TERMINATE EC2 Instance " + e.getMessage());
                }

                synchronized (statisticLock){
                    //Ad ogni ciclo la mappa delle statistiche è azzerata e popolata con le statistiche aggiornate
                    localStatisticMap.clear();
                    localStatisticMap.putAll(dataNodesStatisticMap);
                }
                //Azzero le statistiche usate da precedenti cicli.
                statisticAfterBalancing.clear();

                //Cerco i file da spostare tra i server sovraccarichi
                ArrayList<FileInfo> fileToBeMoved = findFileToBeMoved();

                statisticAfterBalancing.sort(DataNodeStatistic.getCrescentCompBySize());
                //System.out.println("____________CICLO-FIRST_____________");
                for(DataNodeStatistic dns : statisticAfterBalancing){
                    System.out.println(dns);
                    writeOutput(dns.toString());
                }
                System.out.println("\n");
                //System.out.println("File to be moved " + fileToBeMoved);
                fileToBeMoved.sort(FileInfo.getCompBySize());
                //Se la ricerca precedente ha trovato dei file da spostare provo a posizionarli negli altri server attivi
                if(!fileToBeMoved.isEmpty()) {
                    ArrayList<FileInfo> fileToNewServer = balanceLoadBetweenExistingDataNodes(fileToBeMoved);
                    fileToNewServer.sort(FileInfo.getCompBySize());
                    /*
                    System.out.println("____________CICLO-SECOND_____________");
                    for (DataNodeStatistic dns : statisticAfterBalancing)
                        System.out.println(dns);*/
                    System.out.println("File to new server " + fileToNewServer);
                    writeOutput("File to new server "+fileToNewServer);
                    //Se ho ancora file da riposizionare creo nuovi DataNode a cui mandarli.
                    if(!fileToNewServer.isEmpty()) {
                        /*
                        System.out.println("____________CICLO-THIRD_____________");*/
                        ArrayList<FileInfo> unbalancedFiles = balanceToNewDataNode(fileToNewServer);
                        unbalancedFiles.sort(FileInfo.getCompBySize());
                        //System.out.println("File impossible to balance " + unbalancedFiles);
                    }
                }
                synchronized (statisticLock){
                    //Azzero le statische per ricominciare a lavorare su quelle piu aggiornate.
                    dataNodesStatisticMap.clear();
                }
                try{
                    sleep(Config.BALANCING_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Crea un nuovo Master se necessario:
                splitMaster();

                writeOutput("DataNode Ancora Gestiti: " + dataNodeAddresses);
                System.out.println("DataNode Ancora Gestiti: " + dataNodeAddresses);
            }
        }

        /**
         * Cerco tra i DataNode sovraccarichi file da spostare
         *
         * @return elenco di file da spostare
         */
        private ArrayList<FileInfo> findFileToBeMoved(){

            ArrayList<FileInfo> buffer = new ArrayList<>();
            //Ciclo sulle statistiche di ogni server
            for(Map.Entry<String,DataNodeStatistic> serverStat : localStatisticMap.entrySet()){
                DataNodeStatistic stats = serverStat.getValue();
                stats.orderStatistics();
                ArrayList<FileInfo> dataNodeFiles ;
                if(stats.isOverCpuUsage())
                {
                    dataNodeFiles = new ArrayList<>(stats.getFilePerRequest());
                    System.out.println(stats.getDataNodeAddress() + " soglia superata cpu ");
                    writeOutput(stats.getDataNodeAddress() + " soglia superata cpu ");
                }

                else if(stats.isRamUsage()) {
                    dataNodeFiles = new ArrayList<>(stats.getFilePerSize());
                    System.out.println(stats.getDataNodeAddress() + " soglia superata ram ");
                    writeOutput(stats.getDataNodeAddress() + " soglia superata ram ");
                }
                else {
                    statisticAfterBalancing.add(stats);
                    continue;
                }
                if(dataNodeFiles.isEmpty()) return buffer;
                int nToMove = dataNodeFiles.size()/4;
                if(nToMove==0) nToMove = 1;
                for(int i = 0;i<nToMove;i++){
                    buffer.add(dataNodeFiles.get(i));
                    stats.remove(dataNodeFiles.get(i).getFileName());
                }
                statisticAfterBalancing.add(stats);
            }
            return buffer;
        }

        /**
         * Bilanciamento tra i server già attivi.
         *
         * @param buffer elenco dei file da bilanciare
         * @return elenco di file che non è stato possibile bilanciare tra i nodi già attivi
         */
        private ArrayList<FileInfo> balanceLoadBetweenExistingDataNodes(ArrayList<FileInfo> buffer){

            ArrayList<FileInfo> bufferRebalanced = new ArrayList<>(buffer);
            ArrayList<DataNodeStatistic> tmpStatistic = new ArrayList<>();
            //Ciclo sulle statische dei Server aggiornate dalla prima passata.
            ArrayList<DataNodeStatistic> accettableDataNode = new ArrayList<>();
            for(DataNodeStatistic serverStat : statisticAfterBalancing){
                if(!serverStat.isOverCpuUsage() && !serverStat.isRamUsage())
                    accettableDataNode.add(serverStat);
            }
            if(accettableDataNode.size()==0)
                return bufferRebalanced;
            int fileToEachDataNode = buffer.size()/accettableDataNode.size();
            //int remainingFile = buffer.size();
            for(DataNodeStatistic serverStat : accettableDataNode){
                //Per ogni file nel buffer
                if(bufferRebalanced.size()-fileToEachDataNode<fileToEachDataNode || bufferRebalanced.size()-fileToEachDataNode<0)
                    fileToEachDataNode = buffer.size();
                for(int i = 0;i<fileToEachDataNode;i++){
                    //Se l'aggiunto del file a un server non comporta il superamento delle soglie effettuo lo spostamento
                    FileInfo file = bufferRebalanced.get(0);
                    try {
                        Date time = new Date();
                        long timeInMillis = time.getTime();
                        while(!getLifeSignal(serverStat.getDataNodeAddress())){
                            Date now = new Date();
                            if(now.getTime()-timeInMillis>1000) {
                                LOGGER.log(Level.SEVERE, "IMPOSSIBLE TO CONTACT SERVER " + serverStat.getDataNodeAddress());
                                break;
                            }
                        }
                        balanceFile(file.getFileName(), file.getDataNodeOwner(), serverStat.getDataNodeAddress());
                        //Il file viene rimosso dal buffer e aggiunto alle statistiche del server a cui è stato inviato
                        bufferRebalanced.remove(file);
                        file.setFileRequests(Integer.toUnsignedLong(0));
                        file.setDataNodeOwner(serverStat.getDataNodeAddress());
                        serverStat.insert(file);

                    }catch (MasterException | FileNotFoundException | DataNodeException | ImpossibleToMoveFileException e){
                        LOGGER.log(Level.SEVERE,e.getMessage());
                    } catch (NotBalancableFile notBalancableFile) {
                        LOGGER.log(Level.WARNING,notBalancableFile.getMessage());
                    } catch (AlreadyMovedFileException e) {
                        LOGGER.log(Level.WARNING,e.getMessage());
                        bufferRebalanced.remove(file);
                    }
                }
                tmpStatistic.add(serverStat);
                buffer.clear();
                buffer.addAll(bufferRebalanced);
            }
            //Aggiorno le statistiche globali dopo questo secondo ciclo
            statisticAfterBalancing.clear();
            statisticAfterBalancing.addAll(tmpStatistic);
            //Restituisco il buffer con i file che non sono riuscito a ricollocare
            return bufferRebalanced;
        }

        /**
         * Crea nuovi datanode a cui inviare file da ricollocare
         *
         * @param buffer Array di file da ricollocare.
         * @return Ritorna il buffer.
         */
        private ArrayList<FileInfo> balanceToNewDataNode(ArrayList<FileInfo> buffer) {

            ArrayList<FileInfo> temp= new ArrayList<>(buffer);
            temp.sort(FileInfo.getCompByRequests());
            String newServerAddress = createDataNodeInstance();
            Date time = new Date();
            long timeInMillis = time.getTime();
            while(!getLifeSignal(newServerAddress)){
                Date now = new Date();
                if(now.getTime()-timeInMillis>300000) {
                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT SERVER "+newServerAddress);
                    break;
                }
            }
            ArrayList<FileInfo> tmpBuffer = new ArrayList<>(buffer);
            System.out.println("!! " + buffer);
            for (FileInfo file : buffer) {
                //Se il server può accettare un file lo invio
                try {

                    balanceFile(file.getFileName(), file.getDataNodeOwner(), newServerAddress);
                    //Aggiorno carico del nuovo server
                    System.out.println("Bilanciato " + file);
                    tmpBuffer.remove(file);
                    //Altrimenti i file è inserito tra quelli non bilanciati.

                } catch (MasterException | ImpossibleToMoveFileException masterException) {
                    LOGGER.log(Level.SEVERE, masterException.getMessage() + " ME");
                } catch (NotBalancableFile notBalancableFile) {
                    LOGGER.log(Level.WARNING, notBalancableFile.getMessage() + " NBFE");
                } catch (FileNotFoundException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage() + " FNTE");
                } catch (DataNodeException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage() + " DNE");
                } catch (AlreadyMovedFileException e) {
                    LOGGER.log(Level.WARNING, e.getMessage());
                    tmpBuffer.remove(file);
                }
            }

            buffer.clear();
            buffer.addAll(tmpBuffer);

            //Ritorna i file non ricollocabili
            return buffer;
        }

        /**
         *  Uccide i DataNode che sono vuoti da un lungo periodo di tempo.
         *
         */
        private void deleteEmptyDataNode() throws ImpossibleToTerminateDatanodeException, ImpossibleToTerminateEC2InstanceException {

            HashMap<String, DataNodeStatistic> localStatsMap;

            synchronized (statisticLock) {
                localStatsMap = new HashMap<>(dataNodesStatisticMap);
            }

            for (Map.Entry<String, DataNodeStatistic> serverStat : localStatsMap.entrySet()) {

                DataNodeStatistic stats = serverStat.getValue();

                if(stats.getFileInfos().isEmpty() && stats.getMilliseconds_timer() > Config.MAX_TIME_EMPTY_DATANODE) {

                    writeOutput("Empty DataNode: " + stats.getDataNodeAddress() + " - Timer: " + stats.getMilliseconds_timer());
                    System.out.println("Empty DataNode: " + stats.getDataNodeAddress() + " - Timer: " + stats.getMilliseconds_timer());
                    // Rimuove dalla lista di indirizzi globale l'indirizzo del DataNode:
                    synchronized (dataNodeAddressesLock) {
                        dataNodeAddresses.remove(stats.getDataNodeAddress());
                    }
                    // Contatta il DataNode per verificare che le statistiche sia ancora vuote:
                    boolean isEmpty = false;
                    try {
                        StorageInterface dataNode = (StorageInterface) registryLookup(stats.getDataNodeAddress(), Config.dataNodeServiceName);
                        isEmpty = dataNode.isEmpty();
                    }
                    catch (NotBoundException | IOException e) {
                        writeOutput("SEVERE: IMPOSSIBLE TO CONTACT DataNode "+ stats.getDataNodeAddress());
                        LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT DataNode "+ stats.getDataNodeAddress());
                    }
                    if(isEmpty){
                        String dataNode_instanceID = null;
                        try {
                            StorageInterface dataNode = (StorageInterface) registryLookup(stats.getDataNodeAddress(), Config.dataNodeServiceName);
                            dataNode_instanceID = dataNode.getInstanceID();
                            dataNode.terminate(); // Il DataNode viene terminato.
                        }
                        catch (NotBoundException | RemoteException e){

                            // DataNode terminato correttamente:
                            if(e.getCause().toString().equals("java.io.EOFException")){
                                writeOutput("INFO: EMPTY DATANODE " + stats.getDataNodeAddress() + " TERMINATED WITH SUCCESS!");
                                LOGGER.log(Level.INFO,"EMPTY DATANODE " + stats.getDataNodeAddress() + " TERMINATED WITH SUCCESS!");

                                // Terminazione dell'instanza del DataNode ucciso:
                                boolean success = ec2InstanceFactory.terminateEC2Instance(dataNode_instanceID);
                                if(!success){
                                    throw new ImpossibleToTerminateEC2InstanceException(dataNode_instanceID);
                                }

                                writeOutput("INFO: EC2 INSTANCE " + dataNode_instanceID + " TERMINATED WITH SUCCESS!");
                                LOGGER.log(Level.INFO,"EC2 INSTANCE " + dataNode_instanceID + " TERMINATED WITH SUCCESS!");
                            }
                            // DataNode NON terminato correttamente:
                            else {
                                // Rinserisce l'indirizzo del DataNode in quelle globali:
                                synchronized (dataNodeAddressesLock) {
                                    dataNodeAddresses.add(stats.getDataNodeAddress());
                                }
                                throw new ImpossibleToTerminateDatanodeException(stats.getDataNodeAddress());
                            }
                        }
                        // Rimuove le statistiche di quel DataNode:
                        synchronized (statisticLock) {
                            dataNodesStatisticMap.remove(stats.getDataNodeAddress());
                        }
                        synchronized (lifeSignalMapLock) {
                            lifeSignalMap.remove(stats.getDataNodeAddress());
                        }
                    }
                    // DataNode non vuoto:
                    else {
                        // Rinserisce l'indirizzo del DataNode in quelli globali:
                        synchronized (dataNodeAddressesLock) {
                            dataNodeAddresses.add(stats.getDataNodeAddress());
                        }
                        // Il DataNode non viene terminato.
                    }
                }
            }
        }


        /**
         * Controlla se è stato superato il limite massimo di DataNode gestibili dal Master, ed se necessario crea un nuovo Master
         *  passandogli il controllo di una parte di DataNode.
         *
         */
        private void splitMaster() {

            int dataNodes_number;

            synchronized (dataNodeAddressesLock) {
                dataNodes_number = dataNodeAddresses.size();
            }

            if(monitor.isOverRamUsage() || monitor.isOverCpuUsage()) {

                // Crea il nuovo Master:
                String newMasterAddress = "";
                try {
                    newMasterAddress = createMasterInstance("Splitting");
                }
                catch (ImpossibleToCreateMasterInstance e) {
                    writeOutput(e.getMessage());
                    e.printStackTrace();
                }

                // Prende metà degli indirizzi dei DataNode:
                int dataNode_to_move = dataNodes_number / 2;
                int cloudlet_to_move = cloudletAddress.size() / 2;
                ArrayList<String> dataNode_addresses = new ArrayList<>();
                ArrayList<String> cloudlet_addresses = new ArrayList<>();
                int index;

                while(!cloudletAddress.isEmpty() && cloudlet_to_move>0){
                    index = (int) (Math.random() * (cloudletAddress.size()));
                    cloudlet_addresses.add(cloudletAddress.get(index));
                    cloudletAddress.remove(index);
                    cloudlet_to_move--;
                }
                synchronized (dataNodeAddressesLock) {
                    while(!dataNodeAddresses.isEmpty() && dataNode_to_move > 0) {

                        // Numero random tra [ 0 , dataNodeAddresses.size() - 1 ] :
                        index = (int) (Math.random() * (dataNodeAddresses.size()));

                        dataNode_addresses.add(dataNodeAddresses.get(index));
                        dataNodeAddresses.remove(index);

                        dataNode_to_move--;
                    }
                }
                // Rimuove gli indirizzi dalla StatisticMap e dalla LifeMap:
                synchronized (statisticLock){
                    for (String addr : dataNode_addresses) {
                        dataNodesStatisticMap.remove(addr);
                    }
                }
                synchronized (lifeSignalMapLock) {
                    for (String addr : dataNode_addresses) {
                        lifeSignalMap.remove(addr);
                    }
                }
                for(String addr : cloudlet_addresses){
                    cloudletLifeSignalMap.remove(addr);
                }

                try {
                    Date date = new Date();
                    long timeInMillis = date.getTime();
                    // Invia al nuovo Master gli indirizzi dei DataNode che deve gestire:
                    while (!contactNewMaster(newMasterAddress, dataNode_addresses,cloudlet_addresses)) {
                        Date now = new Date();
                        if (now.getTime() - timeInMillis > Config.MAX_TIME_WAITING_FOR_INSTANCE_RUNNING) { // Aspetta che il Master sia attivo.
                            writeOutput("SEVERE: IMPOSSIBLE TO CONTACT New Master " + newMasterAddress);
                            LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT New Master " + newMasterAddress);
                            return;
                        }
                    }

                    // Rimuove dal DB tutte le informazioni relative ai DataNode traferito al nuovo Master:
                    for(String address : dataNode_addresses){
                        masterDAO.deleteAllAddress(address);
                    }

                    writeOutput("DataNode Spostati: " + dataNode_addresses);
                    System.out.println("DataNode Spostati: " + dataNode_addresses);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        private boolean contactNewMaster(String newMasterAddress, ArrayList<String> dataNode_addresses,ArrayList<String> cloudlet_address) {

            try {
                MasterInterface master = (MasterInterface) registryLookup(newMasterAddress, Config.masterServiceName);

                ArrayList<String> master_addresses = new ArrayList<>(masterAddresses);
                master_addresses.add(address);

                master.dataNodesToManage_AND_listOfMasters(dataNode_addresses, master_addresses,cloudlet_address);
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }
    };

    /**
     * Metodo che:
     *
     *  - Riceve una lista di indirizzi di DataNode ed esegue tutte le operazini per prendere in gestione quei DataNode.
     *
     *  - Riceve una lista di indirizzi di Master e li contatta per segnalargli la presenza del nuovo Master.
     *
     * @param dataNode_addresses Indirizzi dei DataNode che il nuovo Master deve gestire.
     */
    @Override
    public void dataNodesToManage_AND_listOfMasters(ArrayList<String> dataNode_addresses, ArrayList<String> master_addresses,ArrayList<String> cloudlet_addresses) {

        // Contatta i DataNode che deve prendersi in gestione:
        try {
            for(String cloudletAddress : cloudlet_addresses){
                addCloudlet(cloudletAddress);
            }
            writeOutput("CLOUDLET ADDRESSES\n"+cloudlet_addresses);
            for (String dataNodeAddress : dataNode_addresses) {

                StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);

                ArrayList<ArrayList<String>> db_data;
                // Richiede al DataNode la lista di file che possiede:
                db_data = dataNode.getDatabaseData();
                for(ArrayList<String> file_info : db_data){
                    // Inserisce le informazioni nel suo DB:
                    masterDAO.insertOrUpdateSingleFilePositionAndVersion(file_info.get(0), file_info.get(1), Integer.parseInt(file_info.get(2)));
                }

                // Iserisce l'indirizzo del DataNode in quelli globali:
                synchronized (dataNodeAddressesLock) {
                    dataNodeAddresses.add(dataNodeAddress);
                }

                // Informa il DataNode del cambio di indirizzo del Master:
                dataNode.changeMasterAddress(address);
            }
            if(dataNode_addresses.isEmpty())
                createDataNodeInstance();
            if(cloudlet_addresses.isEmpty()) {
                createCloudLetInstance();
                splittingStartup = true;
            }
            writeOutput("DATANODE ADDRESSES\n"+dataNodeAddresses);
        }
        catch (Exception e) {
            writeOutput(e.getMessage());
        }

        // Contatta gli altri Master per farsi conoscere:
        try {
            for (String masterAddress : master_addresses) {

                MasterInterface master = (MasterInterface) registryLookup(masterAddress, Config.masterServiceName);

                master.updateMasterAddresses(address, null);

                // Aggiunge alla lista questo Master:
                masterAddresses.add(masterAddress);
            }
        }
        catch (Exception e) {
            writeOutput(e.getMessage());
        }

        writeOutput("Master Addresses: " + masterAddresses + ", " + address + "(io)\t");
    }


    @Override
    public String getMinorLatencyCloudlet(String sourceIP) {
        System.out.println("Ricerco latenza minore da "+sourceIP);
        HashMap<String,Double> globalLatency = new HashMap<>();
        ArrayList<Thread> thArray = new ArrayList<>();
        ArrayList<String> totalAddr = new ArrayList<>(masterAddresses);
        totalAddr.add(address);
        for(String masterAdd : totalAddr){
            Thread latencyThread = new Thread("LatencyThread"){
                @Override
                public void run() {
                    try {
                        System.out.println("Latenza per Master "+masterAdd);
                        MasterInterface masterInterface = (MasterInterface) registryLookup(masterAdd,Config.masterServiceName);
                        ArrayList<String> myResult = masterInterface.getMinorLatencyLocalCloudlet(sourceIP);
                        String cloudletAddr = myResult.get(0);
                        Double cloudletLat = Double.parseDouble(myResult.get(1));
                        System.out.println("Latenza da "+cloudletAddr+ " " + cloudletLat);
                        if(!cloudletAddr.equals(""))
                            globalLatency.put(cloudletAddr,cloudletLat);
                    } catch (NotBoundException | RemoteException e) {
                        e.printStackTrace();
                    }

                }
            };
            thArray.add(latencyThread);
            latencyThread.start();

        }
        for(Thread th : thArray){
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return Util.extractMinFromMap(globalLatency).get(0);

    }


    public ArrayList<String> getMinorLatencyLocalCloudlet(String sourceIP) {
        HashMap<String,Double> latencyMap = new HashMap<>();
        ArrayList<Thread> thArray = new ArrayList<>();
        for(String cloudletAddr : usableCloudlet){
            Thread latencyThread = new Thread("LatencyThread"){
                @Override
                public void run() {
                    try {
                        CloudletInterface cloudletInterface =(CloudletInterface) registryLookup(cloudletAddr,Config.cloudLetServiceName);
                        double latency = cloudletInterface.getLatency(sourceIP);
                        System.out.println("Latency from "+cloudletAddr + " "+latency);
                        latencyMap.put(cloudletAddr,latency);
                    } catch (NotBoundException | RemoteException e) {
                        e.printStackTrace();
                    }

                }
            };
            thArray.add(latencyThread);
            latencyThread.start();
        }
        for(Thread th : thArray){
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return Util.extractMinFromMap(latencyMap);
    }

    @Override
    public boolean addCloudlet(String ipAddress) {
        if(!cloudletAddress.contains(ipAddress)) {
            cloudletAddress.add(ipAddress);
            usableCloudlet.add(ipAddress);
        }
        System.out.println("Aggiungo "+ipAddress + " a "+cloudletAddress);
        return true;
    }

    private static Thread publishCloudletAddress = new Thread("PublishCloudletAddress"){
        @Override
        public void run() {
            if(splittingStartup){
                try {
                    sleep(Config.SYSTEM_STARTUP_TYME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                splittingStartup = false;
            }
            while (!exit) {
                String fileName = Util.getLocalIPAddress() + ".txt";
                File file = new File(fileName);
                try {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(file));
                    for (String caddr : usableCloudlet) {
                        String line = caddr.concat("|");
                        bw.append(line);
                    }
                    bw.flush();
                    bw.close();
                    s3Upload.uploadFile(fileName);
                    Files.delete(Paths.get(fileName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    };

    /**
     *  Thread che contatta il Master Principale per verificare che sia attivo.
     *
     */
    private static Thread shadowThread =  new Thread("shadowThread"){

        @Override
        public void run() {

            while (!exit) {
                try {
                    Date date = new Date();
                    long timeInMillis = date.getTime();
                    while (!contactMainMaster()) {
                        Date now = new Date();
                        if (now.getTime() - timeInMillis > 1000) {
                            writeOutput("INFO: IMPOSSIBLE TO CONTACT Main Master " + mainMasterAddress);
                            return; // Il Thread termina.
                        }
                    }

                    sleep(Config.SHADOW_THREAD_SLEEP_TIME);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        private boolean contactMainMaster() {

            ArrayList<String> temp_dataNodes;
            ArrayList<String> temp_masters;
            ArrayList<String> temp_cloudlets;

            try {
                MasterInterface master = (MasterInterface) registryLookup(mainMasterAddress, Config.masterServiceName);

                // Prende la lista di DataNode:
                temp_dataNodes = master.getDataNodeAddresses();
                // Prende la lista di Master:
                temp_masters = master.getMasterAddresses();
                // Prende la lista delle Cloudlet :
                temp_cloudlets = master.getCloudletAddresses();

            }
            catch (Exception e) {
                return false;
            }
            dataNodeAddresses.clear();
            masterAddresses.clear();
            cloudletAddress.clear();

            dataNodeAddresses.addAll(temp_dataNodes);
            masterAddresses.addAll(temp_masters);
            cloudletAddress.addAll(temp_cloudlets);

            return true;
        }
    };

}
