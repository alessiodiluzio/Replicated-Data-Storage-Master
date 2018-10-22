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
import com.sdcc_project.util.SystemProperties;
import com.sdcc_project.util.Util;

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

/**
 * Nodo del sistema che si occupa di :
 * <ul>
 *     <li>Gestione ciclo di vita DataNode</li>
 *     <li>Mappaggio tra nome di un file e Repliche</li>
 *     <li>Comunicazione con altri Master</li>
 *     <li>Risposta alle richieste di una Cloudlet</li>
 *  </ul>
 *  Tipi di Master :
 *  <ul>
 *      <li>System_Startup --> Avvio di tutto il sistema crea gli altri Master,I propri DataNode le proprie CLoudlet
 *      e il proprio ShadowMaster</li>
 *      <li>Main --> Crea i propri DataNode,CLoudlet e ShadowMaster</li>
 *      <li>Splitting --> Creato per bilanciare il carico su un altro Master da cui riceve una parte di Cloudlet e DataNode</li>
 *      <li>Shadow --> Master "ombra" che controlla lo stato di vita un altro master e lo sostituisce in caso di fallimento</li>
 *  </ul>
 *
 */
public class Master extends UnicastRemoteObject implements MasterInterface {

    //Addresses
    private static String mainMasterAddress;
    private static String address;
    private static ArrayList<String> dataNodeAddresses = new ArrayList<>();
    private static ArrayList<String> masterAddresses = new ArrayList<>();
    private static HashMap<String,String> dataNodeInstanceIDMap = new HashMap<>();
    private static HashMap<String,String> cloudletInstanceIDMap = new HashMap<>();

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
    private static final Object startupMapLock = new Object();

    //Util
    private static boolean exit = false;
    private static final Logger LOGGER = Logger.getLogger(Master.class.getName());
    private static File file;
    private static SystemProperties systemProperties;


    //Shadow Master informations
    private static String shadowMasterAddress;
    private static String shadowMasterInstanceID;
    private static HashMap<String,Long> startupMap = new HashMap<>();

    private static boolean splitDone = false;


    private Master() throws RemoteException {
        super();
    }

    /**
     * Avvio del sistema,registrazione dell'interfaccia RMI e avvio thread di gestione
     * @param args Tipo del Master da avviare.
     */
    public static void main(String args[]){

        if(args.length < 1){
            System.out.println("Usage: mvn exec:java@Master -Dexec.args=System_Startup");
            System.exit(1);
        }
        systemProperties = SystemProperties.getInstance();
        monitor = Monitor.getInstance();
        ec2InstanceFactory = EC2InstanceFactory.getInstance();
        s3Upload = S3Upload.getInstance();
        System.out.println("Mia istanza : "+ EC2MetadataUtils.getInstanceId());
        System.setProperty("java.rmi.server.hostname", Objects.requireNonNull(Util.getPublicIPAddress()));
        switch (args[0]){

            case "System_Startup":
                try {
                    masterConfiguration("Main");

                    // Creazione degli altri Master:
                    int masterToCreate = systemProperties.getStart_number_of_master();
                    if(masterToCreate<systemProperties.getReplication_factory())
                        masterToCreate = systemProperties.getReplication_factory();
                    for(int i = 0; i < masterToCreate-1; i++) {
                        createMasterInstance("Main");
                    }
                    System.out.print("Master Addresses: " + masterAddresses + ", " + address + "(io)\n");

                    // Creazione dei propri DataNode:
                    for(int j = 0; j < systemProperties.getStart_number_of_data_node_for_master(); j++) {
                        createDataNodeInstance();
                    }
                    for (int i = 0;i< systemProperties.getStart_number_of_cloudlet_for_master();i++){
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

                    Util.writeOutput("Master Addresses: " + masterAddresses + ", " + address + "(io)\n",file);

                    // Creazione dei DataNode:
                    for (int i = 0; i < systemProperties.getStart_number_of_data_node_for_master(); i++) {
                        createDataNodeInstance();
                    }
                    for (int i = 0;i<systemProperties.getStart_number_of_cloudlet_for_master();i++){
                        createCloudLetInstance();
                    }
                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");
                }
                catch (MasterException e) {
                    Util.writeOutput("MAIN MASTER SHUTDOWN: " + e.getMessage(),file);
                    System.exit(1);
                }
                catch (ImpossibleToCreateMasterInstance e) {
                    Util.writeOutput("SEVERE: " + e.getMessage(),file);
                }
                catch (RemoteException | NotBoundException e){
                    Util.writeOutput(e.getMessage(),file);
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
                    for(String cAddr : usableCloudlet){
                        startupMap.put(cAddr,Util.getTimeInMillies());
                    }
                    Util.writeOutput("Usable Cloudlet fine shadow thread "+usableCloudlet,file);
                }
                catch (Exception e) {
                    Util.writeOutput("SHADOW MASTER SHUTDOWN: " + e.getMessage(),file);
                    System.exit(1);
                }

                Util.writeOutput("DataNode Addresses:\n" + dataNodeAddresses,file);
                Util.writeOutput("Cloudlet Address:\n"+cloudletAddress + " " +usableCloudlet,file);
                try {
                    for (String dataNodeAddress : dataNodeAddresses) {
                        synchronized (startupMapLock){
                            startupMap.put(dataNodeAddress,Util.getTimeInMillies());
                        }
                        StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);
                        // Informa il DataNode del cambio di indirizzo del Master:
                        Util.writeOutput("Invio il nuovo indirizzo del Master " + address + " al DataNode " + dataNodeAddress,file);
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
                        synchronized (startupMapLock){
                            startupMap.put(cloudletAddr,Util.getTimeInMillies());
                        }
                        CloudletInterface cloudlet = (CloudletInterface) registryLookup(cloudletAddr,Config.cloudLetServiceName);
                        Util.writeOutput("Invio il nuovo indirizzo del Master "+address+" alla cloudlet " +cloudletAddr,file);
                        cloudlet.newMasterAddress(address);
                        synchronized (cloudletLifeSignalMapLock) {
                            cloudletLifeSignalMap.put(cloudletAddr, Util.getTimeInMillies());
                        }
                    }
                    //writeOutput("DB Data:\n" + masterDAO.getAllData());
                }
                catch (Exception e) {
                    Util.writeOutput(e.getMessage(),file);
                }

                Util.writeOutput("Master Addresses:\n" + masterAddresses + ", " + address + "(io)\n",file);
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
                    Util.writeOutput(e.getMessage(),file);
                }
                catch (ImpossibleToCreateMasterInstance e){
                    Util.writeOutput("SEVERE: " + e.getMessage(),file);
                }

                break;

            case "Splitting":
                try {
                    masterConfiguration("Splitting");



                    Thread splitWaitThread = new Thread("SplitWaitThread"){
                        @Override
                        public void run() {
                            int slippedTime = 0;
                            while (!splitDone){
                                if(slippedTime>Config.SYSTEM_STARTUP_TYME){
                                    System.out.println("ABORT SPLITTING");
                                    String instanceID = EC2MetadataUtils.getInstanceId();
                                    ec2InstanceFactory.terminateEC2Instance(instanceID);
                                }
                                Util.writeOutput("WAITING FOR SPLITTING COMPLETION",file);
                                try {
                                    sleep(5000);
                                    slippedTime += 5000;
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    };
                    splitWaitThread.start();
                    try {
                        splitWaitThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // Creazione dello Shadow Master:
                    createMasterInstance("Shadow");

                }
                catch (MasterException e) {
                    Util.writeOutput("SPLITTING MASTER SHUTDOWN: " + e.getMessage(),file);
                    System.exit(1);
                }
                catch (ImpossibleToCreateMasterInstance e){
                    Util.writeOutput("SEVERE: " + e.getMessage(),file);
                }
                catch (RemoteException e){
                    Util.writeOutput(e.getMessage(),file);
                }

                break;

            default:
                System.out.println("Usage: mvn exec:java@Master -Dexec.args=System_Startup");
                System.exit(1);
        }
        for(Map.Entry<String,Long> entry : startupMap.entrySet()) {
            System.out.println("IP " + entry.getKey() + " ORA LANCIO " + entry.getValue());
        }
        //Monitora lo stato di salute del master shadow
        shadowLifeThread.start();
        //Monitora lo stato di utilizzo delle risorse HW dell'istanza in cui è eseguito il nodo
        monitor.startThread();
        //Thread che si occupa di bilanciare il carico del sistema
        balancingThread.start();
        //Monitora lo stato di attività dei DataNode gestiti
        lifeThread.start();
        //Monitora lo stato di attività delle cloudlet gestite
        cloudletLifeThread.start();
        //Thread che pubblica su S3 le cloudlet attive del sistema.
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
        address = Util.getPublicIPAddress();
        String serviceName = Config.masterServiceName;
        String completeName = "//" + address + ":" + Config.port + "/" + serviceName;
        //System.out.println(completeName);

        Master master = new Master();
        Registry registry = createRegistry(Config.port);
        registry.rebind(completeName, master);

        System.out.println(masterType + " Master Bound");
        file = new File(Config.MASTER_FILE_LOGGING_NAME + ".txt");
        Util.writeOutput(masterType + " Master lanciato all'indirizzo: " + address,file);
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
     * Metodo che restituisce tutti gli indirizzi dei DataNode gestiti dal Master.
     *
     * @return Lista di indirizzi di DataNode.
     */
    @Override
    public ArrayList<String> getDataNodeAddresses() {

        synchronized (startupMapLock) {
            if (startupMap.containsKey(shadowMasterAddress)) {
                startupMap.remove(shadowMasterAddress);
                System.out.println("FINE STARTUP SHADOW getDataNodeAddresses");
            }
        }


        synchronized (dataNodeAddressesLock) {

            return dataNodeAddresses;
        }
    }

    /**
     * Servizio RMI
     * @return gli indirizzi dei Master conosciuti da questo master
     */
    @Override
    public ArrayList<String> getMasterAddresses() {
        return masterAddresses;
    }

    /**
     * Servizio RMI
     * @return indirizzi delle cloudlet gestite dal Master
     */
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

        Util.writeOutput("Master Addresses Updated! New List: " + masterAddresses + ", " + address + "(io)\n",file);
        System.out.print("Master Addresses Updated! New List: " + masterAddresses + ", " + address + "(io)\n");
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
        Util.writeOutput("Nuovo DataNode lanciato all'indirizzo: " + newDataNodeIP + " - Indirizzo del Master: " + address,file);
        System.out.println("Nuovo DataNode lanciato all'indirizzo: " + newDataNodeIP + " - Indirizzo del Master: " + address);
        synchronized (startupMapLock) {
            startupMap.put(newDataNodeIP, Util.getTimeInMillies());
        }
        return newDataNodeIP;
    }


    /**
     * Crea un instanza EC2 e ci avvia una cloudlet
     */
    private static void createCloudLetInstance() {
        String arguments = address+","+systemProperties.getReplication_factory();
        ArrayList<String> newInstanceInfo=ec2InstanceFactory.createEC2Instance(NodeType.CloudLet,arguments);
        String newCloudLetIP = newInstanceInfo.get(1);
        cloudletInstanceIDMap.put(newCloudLetIP,newInstanceInfo.get(0));
        cloudletAddress.add(newCloudLetIP);
        System.out.println("Launched cloudlet at "+newCloudLetIP+"\n\n");
        Util.writeOutput("Launched cloudlet at "+newCloudLetIP+"\n\n",file);
        synchronized (startupMapLock) {
            startupMap.put(newCloudLetIP,Util.getTimeInMillies());
            Util.writeOutput("CloudletIP "+newCloudLetIP + " "+startupMap.get(newCloudLetIP),file);

        }
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
        String localIPAddress = Util.getPublicIPAddress();

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
            synchronized (startupMapLock) {
                startupMap.put(shadowMasterAddress, Util.getTimeInMillies());
            }
        }
        if(nodeType.equals("Main")){
            masterAddresses.add(newMasterIP);
        }

        Util.writeOutput("\nNuovo Master -" + nodeType + "- lanciato all'indirizzo: " + newMasterIP+"\n\n",file);
        System.out.print("\nNuovo Master -" + nodeType + "- lanciato all'indirizzo: " + newMasterIP+"\n\n");

        return newMasterIP;
    }

    /**
     * Restituisce la posizione della replica più aggiornata di un file
     *
     * @param filename nome del file da cercare
     * @param operation tipo di operazione (lettura scrittura)
     * @return la posizione trovata
     * @throws FileNotFoundException ...
     */
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

    /**
     * Servizio RMI ping per verificare stato di attività
     */
    @Override
    public void ping() {

    }

    /**
     * Servizio RMI
     * Cancella un file da TUTTO il sistema
     * @param filename nome del file da cancellare
     * @return riuscita dell'operazione
     */
    @Override
    public boolean delete(String filename) {
        deleteFromMaster(filename);
        for(String masterAddr : masterAddresses){
            try {
                MasterInterface masterInterface = (MasterInterface) registryLookup(masterAddr,Config.masterServiceName);
                masterInterface.deleteFromMaster(filename);
            } catch (NotBoundException | RemoteException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Servizio RMI
     * Cancella un file dalla tabella di un master (e dai DataNode da lui gestiti)
     * @param filename nome del file da cancellare
     */
    @Override
    public void deleteFromMaster(String filename) {
        try {
            String dataNode = masterDAO.getFilePosition(filename);
            masterDAO.deleteFilePosition(filename);
            if(dataNode!=null){
                StorageInterface storageInterface = (StorageInterface) registryLookup(dataNode,Config.dataNodeServiceName);
                storageInterface.delete(filename);
            }
        } catch (MasterException | RemoteException | NotBoundException e) {
            e.printStackTrace();

        }

    }

    /**
     * Servizio RMI
     * Una cloudlet comunica di essere in attesa di spegnimento e il Master ne termina l istanza EC2
     * @param address indirizzo della cloudlet che è in attesa di spegnimento
     */
    @Override
    public void shutdownCloudletSignal(String address)  {
        System.out.println("Cloudlet "+address+ "CANCELLATA");
        synchronized (cloudletLifeSignalMapLock){
            cloudletLifeSignalMap.remove(address);
        }
        usableCloudlet.remove(address);
        cloudletAddress.remove(address);
        System.out.println("Uccisione Cloudlet " + ec2InstanceFactory.terminateEC2Instance(cloudletInstanceIDMap.get(address)));
    }

    /**
     * Servizio RMI
     * Un DataNode comunica di essere in attesa di spegnimento e il Master ne termina l istanza EC2
     * @param address indirizzo del DataNode che è in attesa di spegnimento
     */
    @Override
    public void shutdownDataNodeSignal(String address)  {
        System.out.println("DATA NODE "+address+" CANCELLATO ");
        ec2InstanceFactory.terminateEC2Instance(dataNodeInstanceIDMap.get(address));
        synchronized (dataNodeAddressesLock){
            dataNodeAddresses.remove(address);
        }
        synchronized (statisticLock){
            dataNodesStatisticMap.remove(address);
        }
        synchronized (lifeSignalMapLock){
            lifeSignalMap.remove(address);
        }
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
            Util.writeOutput(e.getMessage(),file);
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
                Util.writeOutput("Cerco una Possibile Replica sul Master: " + master_address,file);
                //System.out.println("Cerco una Possibile Replica sul Master: " + master_address);
                master = (MasterInterface) registryLookup(master_address, Config.masterServiceName);
                replica_address = master.findReplicaPosition(filename, version);

                if (replica_address != null) {
                    break; // Trovato il DataNode su cui scrivere una replica.
                }
            } catch (RemoteException | NotBoundException e) {
                Util.writeOutput("WARNING: Impossible to Contact Master " + master_address,file);
                ///System.out.println("WARNING: Impossible to Contact Master " + master_address);
            }
        }

        if(replica_address == null) {
            throw new ImpossibleToFindDataNodeForReplication("Impossible to Find DataNode for Replication");
        }

        Util.writeOutput("Trovato DataNode: " + replica_address + " - Su cui Creare/Aggiornare una Replica del File: " + filename,file);
       // System.out.println("Trovato DataNode: " + replica_address + " - Su cui Creare/Aggiornare una Replica del File: " + filename);

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
                    Util.writeOutput("Trovata una Nuova Posizione di Replicazione: " + replica_address + " - Per il File: " + filename,file);
                }
                else {
                    Util.writeOutput("Già Presente una Replica: " + replica_address + " - Del il File: " + filename,file);
                    return null; // Possiede già una replica del file.
                }
            }
            catch (MasterException e) {
                Util.writeOutput(e.getMessage(),file);
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
                    Util.writeOutput("Trovata una Replica da Aggiornare: " + replica_address + " - Del il File: " + filename,file);
                }
                else {
                    Util.writeOutput("Nessuna Replica da Aggiornare Trovata - Per il File: " + filename,file);
                    return null; // Non ha nessuna replica di quel file.
                }
            }
            catch (MasterException e) {
                Util.writeOutput(e.getMessage(),file);
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
            Util.writeOutput("SEVERE: " + e.getMessage(),file);
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
           find = dataNodeAddresses.contains(dataNodeAddress);
        }
        synchronized(startupMapLock) {
            startupMap.remove(dataNodeAddress);
        }
        if(find){
            synchronized (lifeSignalMapLock) {
                lifeSignalMap.remove(dataNodeAddress);
                lifeSignalMap.put(dataNodeAddress, Util.getTimeInMillies());
                //System.out.println("Inserisco "+dataNodeAddress + " in lifesingalmap");
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
        synchronized (startupMapLock) {
            startupMap.remove(cloudletAddr);
        }
        System.out.println("CloudLetLifeSignal "+cloudletAddr+ " stato "+state);
        Util.writeOutput("CloudLetLifeSignal "+cloudletAddr+ " stato "+state,file);
        // Verifica che l'indirizzo compare nell'elenco di DataNode gestito da questo Master:
        if(state.equals(State.BUSY)){
            usableCloudlet.remove(cloudletAddr);
            System.out.println("CloudLet "+cloudletAddr+ " BUSY ");
            Util.writeOutput("CloudLet "+cloudletAddr+ " BUSY ",file);
        }
        if(state.equals(State.FREE) && usableCloudlet.size()>systemProperties.getStart_number_of_cloudlet_for_master()){
            System.out.println("CANCELLO CLOUDLET");
            Util.writeOutput("CANCELLO CLOUDLET",file);
            handleCloudletShutdown(cloudletAddr);
            return;
        }
        else if(state.equals(State.NORMAL) || state.equals(State.FREE)){
            if(!usableCloudlet.contains(cloudletAddr))
                usableCloudlet.add(cloudletAddr);
        }
        if(find){
            synchronized (cloudletLifeSignalMapLock) {
                cloudletLifeSignalMap.remove(cloudletAddr);
                cloudletLifeSignalMap.put(cloudletAddr, Util.getTimeInMillies());
            }
        }
        System.out.println("Usable cloudlet "+usableCloudlet);
        Util.writeOutput("Usable cloudlet "+usableCloudlet+ " " +cloudletAddress,file);
    }

    private void handleCloudletShutdown(String cloudletAddr) {
        try {
            CloudletInterface cloudletInterface = (CloudletInterface) registryLookup(cloudletAddr,Config.cloudLetServiceName);
            cloudletInterface.shutdownSignal();
        } catch (NotBoundException | RemoteException e) {
            e.printStackTrace();
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
            if (Util.getTimeInMillies() - entry.getValue() < Config.MAX_TIME_NOT_RESPONDING_DATANODE) {
                aliveDataNode.add(entry.getKey());
            }
        }
        //System.out.println("Alive Data Node "+aliveDataNode);
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

        Util.writeOutput("Tento di mandare " + filename + " in " + new_serverAddress + " da " + old_address,file);
        //System.out.println("Tento di mandare " + filename + " in " + new_serverAddress + " da " + old_address);

        if (masterDAO.serverContainsFile(filename, new_serverAddress)) {
            throw new NotBalancableFile("A replica of this file " + filename + " is already in the server " + new_serverAddress);
        }
        if(!masterDAO.serverContainsFile(filename, old_address)){
            throw new AlreadyMovedFileException("File " + filename + " is not in the server " + old_address + " anymore.");
        }

        sendFileToDataNode(filename, new_serverAddress, old_address);

        //System.out.println("Spostato " + filename + " da " + old_address + " in " + new_serverAddress);
        Util.writeOutput("Spostato " + filename + " da " + old_address + " in " + new_serverAddress,file);
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

    /**
     * Verifica che un DataNode è ancora attivo "Pingandolo"
     * @param dataNodeAddress DataNode da pingare
     * @return riuscita del ping
     */
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
            System.out.println("SEGNALE DI VITA");
            return;
        }
        System.out.println("HANDLE DATA NODE CRASH");

        synchronized (startupMapLock) {
            startupMap.remove(address);
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
                        Util.writeOutput("WARNING: Impossible to Contact Master " + master_address,file);
                        System.out.println("WARNING: Impossible to Contact Master " + master_address);
                    }
                }

                // Non ha trovato nessun DataNode da cui recuperare il file:
                if(file_position == null) {
                    Util.writeOutput("SEVERE: Impossible to Recover File: " + filename,file);
                    LOGGER.log(Level.SEVERE, "Impossible to Recover File: " + filename);
                    continue;
                }

                Util.writeOutput("Trovato DataNode: " + file_position + " - Da cui Recuperare il File: " + filename,file);
                System.out.println("Trovato DataNode: " + file_position + " - Da cui Recuperare il File: " + filename);

                // Contatta il DataNode che possiede il file da recuperare:
                try {
                    StorageInterface dataNode = (StorageInterface) registryLookup(file_position, Config.dataNodeServiceName);
                    dataNode.copyFileOnAnotherDataNode(filename, replaced_dataNode);
                }
                catch (NotBoundException | RemoteException e) {
                    Util.writeOutput("SEVERE: Impossible to Contact DataNode " + file_position,file);
                    LOGGER.log(Level.SEVERE,"Impossible to Contact DataNode " + file_position);
                    continue;
                }
                catch (ImpossibleToCopyFileOnDataNode e) {
                    Util.writeOutput("SEVERE: " + e.getMessage(),file);
                    LOGGER.log(Level.SEVERE, e.getMessage());
                    continue;
                }

                Util.writeOutput("Recuperato il File: " + filename + " - Dal DataNode: " + file_position,file);
                System.out.println("Recuperato il File: " + filename + " - Dal DataNode: " + file_position);
            }
        }
        catch (MasterException e) {
            e.printStackTrace();
            Util.writeOutput(e.getMessage(),file);
        }
        catch (FileNotFoundException e) {
            Util.writeOutput("WARNING: " + e.getMessage(),file);
            LOGGER.log(Level.WARNING, e.getMessage());
        }
    }

    /**
     * Monitora lo stato di vita di uno ShadowMaster ,creandone uno nuovo in caso di presunto crash
     */
    private static Thread shadowLifeThread = new Thread("ShadowLifeThread"){
        @Override
        public void run() {
            HashMap<String,Long> localStartupMap;
            while (!exit) {
                synchronized (startupMapLock){
                    localStartupMap = new HashMap<>(startupMap);
                }
                while(localStartupMap.containsKey(shadowMasterAddress)){
                    System.out.println("STARTUP SHADOW");
                    if(Util.getTimeInMillies()-localStartupMap.get(shadowMasterAddress)>Config.SYSTEM_STARTUP_TYME){
                        System.out.println("FINE STARTUP SHADOW");
                        break;
                    }
                    try {
                        sleep(30000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (startupMapLock){
                        localStartupMap = new HashMap<>(startupMap);
                    }
                }
                try {

                    MasterInterface masterInterface = (MasterInterface) registryLookup(shadowMasterAddress, Config.masterServiceName);
                    masterInterface.ping();


                } catch (NotBoundException | RemoteException e) {
                    System.out.println("SHADOW MASTER DEAD");
                    try {
                        ec2InstanceFactory.terminateEC2Instance(shadowMasterInstanceID);
                        createMasterInstance("Shadow");
                    } catch (ImpossibleToCreateMasterInstance impossibleToCreateMasterInstance) {
                        impossibleToCreateMasterInstance.printStackTrace();
                    }
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
                        long timeInMillies = Util.getTimeInMillies();
                        System.out.println("Signal Map : " +entry.getKey()+ " "+entry.getValue());
                        if (timeInMillies - entry.getValue() > Config.MAX_TIME_NOT_RESPONDING_DATANODE) {
                            Util.writeOutput("DATANODE " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()),file);
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


    /**
     * Verifica lo stato di attività delle cloudlet,creandone di nuove in caso di numero esiguo
     */
    private static Thread cloudletLifeThread =  new Thread("CloudletLifeThread"){

        @Override
        public void run() {
            while (!exit) {
                HashMap<String, Long> localSignalMap;
                synchronized (cloudletLifeSignalMapLock) {
                    localSignalMap = new HashMap<>(cloudletLifeSignalMap);
                }

                for (Map.Entry<String, Long> entry : localSignalMap.entrySet()) {
                    long timeInMillies = Util.getTimeInMillies();
                    if (timeInMillies - entry.getValue() > Config.MAX_TIME_NOT_RESPONDING_CLOUDLET) {
                        Util.writeOutput("Cloudlet " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()),file);
                        LOGGER.log(Level.INFO, "Cloudlet " + entry.getKey() + " NOT RESPONDING SINCE " + (timeInMillies - entry.getValue()));
                        createCloudLetInstance();
                        usableCloudlet.remove(entry.getKey());
                        cloudletLifeSignalMap.remove(entry.getKey());
                        cloudletAddress.remove(entry.getKey());
                        ec2InstanceFactory.terminateEC2Instance(cloudletInstanceIDMap.get(entry.getKey()));
                        cloudletInstanceIDMap.remove(entry.getKey());
                        synchronized (startupMapLock) {
                            startupMap.remove(entry.getKey());
                        }
                    }
                }
                HashMap<String,Long> localStartupMap;
                synchronized (startupMapLock){
                    localStartupMap = new HashMap<>(startupMap);
                }
                int launchedCloudlet = 0;
                System.out.println("Dimensione mappa startup " + localStartupMap.size());
                Util.writeOutput("Dimensione mappa startup" + localStartupMap.size(),file);
                for(Map.Entry<String,Long> entry : localStartupMap.entrySet()){
                    if(cloudletAddress.contains(entry.getKey())) {
                        System.out.println("La mappa di startup contiene la cloudlet "+entry.getKey());
                        Util.writeOutput("La mappa di startup contiene la cloudlet "+entry.getKey(),file);
                        if (Util.getTimeInMillies() - entry.getValue() < Config.SYSTEM_STARTUP_TYME) {
                            System.out.println("La cloudlet "+entry.getKey()+" è in fase di startup");
                            launchedCloudlet++;
                        }
                        else {
                            Util.writeOutput("La mappa di startup contiene la cloudlet scaduta"+entry.getKey()+" "+(Util.getTimeInMillies()-entry.getValue()),file);
                            System.out.println("La mappa di startup contiene mappa scaduta "+entry.getKey()+" "+(Util.getTimeInMillies()-entry.getValue()));
                            cloudletLifeSignalMap.remove(entry.getKey());
                            cloudletAddress.remove(entry.getKey());
                            ec2InstanceFactory.terminateEC2Instance(cloudletInstanceIDMap.get(entry.getKey()));
                            cloudletInstanceIDMap.remove(entry.getKey());
                            usableCloudlet.remove(entry.getKey());
                        }
                    }
                }
                if(usableCloudlet.size()==0 && launchedCloudlet == 0) {
                    System.out.println("Nessuna cloudlet attiva");
                    Util.writeOutput("Nessuna cloudlet attiva",file);
                    createCloudLetInstance();
                }
                else {
                    Util.writeOutput("Non cè bisogno di lanciare una cloudlet",file);
                    System.out.println("Non c'è bisogno di lanciare una cloudlet");
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
     *    <ul>
     *    <li>Dimensione dei file sui singoli DataNode.</li>
     *    <li>Numero di richieste ai singoli DataNode.</li>
     *    </ul>
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
                deleteEmptyDataNode();

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
                    Util.writeOutput(dns.toString(),file);
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
                    Util.writeOutput("File to new server "+fileToNewServer,file);
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
                searchUnderUsageDataNode();
                Util.writeOutput("DataNode Ancora Gestiti: " + dataNodeAddresses,file);
                System.out.println("DataNode Ancora Gestiti: " + dataNodeAddresses);

            }
        }

        /**
         * Ricerca DataNode sottoutilizzati , un DataNode sottoutilizzato è cancellato
         * se e solo se il Master gestisce più DataNode di quelli indicati in file di config
         */
        private void searchUnderUsageDataNode(){
            if(dataNodeAddresses.size()<= systemProperties.getStart_number_of_data_node_for_master())
                return;
            String toShutdownDataNode = null;
            ArrayList<String> aliveDataNode = new ArrayList<>();
            for(DataNodeStatistic dns : statisticAfterBalancing){
                if(dns.isUnderUsage()){
                    toShutdownDataNode = dns.getDataNodeAddress();
                    System.out.println("UNDER USED DATA NODE "+toShutdownDataNode);
                    break;
                }
            }
            if(toShutdownDataNode==null)
                return;
            synchronized (dataNodeAddressesLock){
                dataNodeAddresses.remove(toShutdownDataNode);
            }
            synchronized (lifeSignalMapLock){
                lifeSignalMap.remove(toShutdownDataNode);
                for (Map.Entry<String, Long> entry : lifeSignalMap.entrySet()) {
                    if (Util.getTimeInMillies() - entry.getValue() < Config.MAX_TIME_NOT_RESPONDING_DATANODE) {
                        aliveDataNode.add(entry.getKey());
                    }
                }
            }
            synchronized (statisticLock){
                dataNodesStatisticMap.remove(toShutdownDataNode);
            }
            try {
                System.out.println(aliveDataNode+ " mandati a data node da spegnere 1"+toShutdownDataNode);
                StorageInterface storageInterface = (StorageInterface) registryLookup(toShutdownDataNode,Config.dataNodeServiceName);
                storageInterface.shutDown(aliveDataNode);
                System.out.println(aliveDataNode+ " mandati a data node da spegnere 2"+toShutdownDataNode);
                masterDAO.deleteAllAddress(toShutdownDataNode);
            } catch (NotBoundException | RemoteException | MasterException e) {
                e.printStackTrace();
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
                    Util.writeOutput(stats.getDataNodeAddress() + " soglia superata cpu ",file);
                }

                else if(stats.isRamUsage()) {
                    dataNodeFiles = new ArrayList<>(stats.getFilePerSize());
                    System.out.println(stats.getDataNodeAddress() + " soglia superata ram ");
                    Util.writeOutput(stats.getDataNodeAddress() + " soglia superata ram ",file);
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
                        long timeInMillis = Util.getTimeInMillies();
                        while(!getLifeSignal(serverStat.getDataNodeAddress())){
                            if(Util.getTimeInMillies()-timeInMillis>10000) {
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
            long timeInMillis =Util.getTimeInMillies();
            while(!getLifeSignal(newServerAddress)){
                if(Util.getTimeInMillies()-timeInMillis>300000) {
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
        private void deleteEmptyDataNode()  {

            HashMap<String, DataNodeStatistic> localStatsMap;

            synchronized (statisticLock) {
                localStatsMap = new HashMap<>(dataNodesStatisticMap);
            }

            for (Map.Entry<String, DataNodeStatistic> serverStat : localStatsMap.entrySet()) {

                DataNodeStatistic stats = serverStat.getValue();

                if(stats.getFileInfos().isEmpty() && stats.getMilliseconds_timer() > Config.MAX_TIME_EMPTY_DATANODE) {

                    Util.writeOutput("Empty DataNode: " + stats.getDataNodeAddress() + " - Timer: " + stats.getMilliseconds_timer(),file);
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
                        Util.writeOutput("SEVERE: IMPOSSIBLE TO CONTACT DataNode "+ stats.getDataNodeAddress(),file);
                        LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT DataNode "+ stats.getDataNodeAddress());
                    }
                    if(isEmpty){
                        //String dataNode_instanceID = null;
                        ec2InstanceFactory.terminateEC2Instance(dataNodeInstanceIDMap.get(stats.getDataNodeAddress()));
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
                    Util.writeOutput(e.getMessage(),file);
                    e.printStackTrace();
                }

                // Prende metà degli indirizzi dei DataNode:
                int dataNode_to_move = dataNodes_number / 2;
                int cloudlet_to_move = cloudletAddress.size() / 2;
                System.out.println("number of Cloudlet to move " +cloudlet_to_move);
                ArrayList<String> dataNode_addresses = new ArrayList<>();
                ArrayList<String> cloudlet_addresses = new ArrayList<>();
                int index;

                while(!cloudletAddress.isEmpty() && cloudlet_to_move>0){
                    index = (int) (Math.random() * (cloudletAddress.size()));
                    cloudlet_addresses.add(cloudletAddress.get(index));
                    System.out.println("Cloudlet to move "+cloudlet_addresses);
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
                        System.out.println("LFMAP "+lifeSignalMap.get(addr));
                    }
                }
                for(String addr : cloudlet_addresses){
                    cloudletLifeSignalMap.remove(addr);
                }

                try {
                    long timeInMillis = Util.getTimeInMillies();
                    // Invia al nuovo Master gli indirizzi dei DataNode che deve gestire:
                    while (!contactNewMaster(newMasterAddress, dataNode_addresses,cloudlet_addresses)) {
                        if (Util.getTimeInMillies() - timeInMillis > Config.SYSTEM_STARTUP_TYME) { // Aspetta che il Master sia attivo.
                            Util.writeOutput("SEVERE: IMPOSSIBLE TO CONTACT New Master " + newMasterAddress,file);
                            LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT New Master " + newMasterAddress);
                            return;
                        }
                    }

                    // Rimuove dal DB tutte le informazioni relative ai DataNode traferito al nuovo Master:
                    for(String address : dataNode_addresses){
                        masterDAO.deleteAllAddress(address);
                    }

                    Util.writeOutput("DataNode Spostati: " + dataNode_addresses,file);
                    System.out.println("DataNode Spostati: " + dataNode_addresses);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        /**
         * Dopo che il Master ne ha creato uno con cui condividere il carico lo inizializza e gli invia parte
         * delle proprie istanze gestite .
         *
         * @param newMasterAddress Indirizzi degli altri master del sistema
         * @param dataNode_addresses DataNode che gestirà il nuovo Master
         * @param cloudlet_address cloudlet che gestirà il nuovo Master
         * @return riuscita dell'operazione
         */
        private boolean contactNewMaster(String newMasterAddress, ArrayList<String> dataNode_addresses,ArrayList<String> cloudlet_address) {

            try {
                MasterInterface master = (MasterInterface) registryLookup(newMasterAddress, Config.masterServiceName);

                ArrayList<String> master_addresses = new ArrayList<>(masterAddresses);
                master_addresses.add(address);

                master.initializationInformations(dataNode_addresses, master_addresses,cloudlet_address);
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }
    };

    /**
     * Servizio RMI
     * Metodo che:
     *
     *    <ul>
     *    <li>Riceve una lista di indirizzi di DataNode ed esegue tutte le operazini per prendere in gestione quei DataNode.</li>
     *    <li>Riceve una lista di indirizzi di Master e li contatta per segnalargli la presenza del nuovo Master.</li>
     *    <ul>
     * @param dataNode_addresses Indirizzi dei DataNode che il nuovo Master deve gestire.
     */
    @Override
    public void initializationInformations(ArrayList<String> dataNode_addresses, ArrayList<String> master_addresses,ArrayList<String> cloudlet_addresses) {

        // Contatta i DataNode che deve prendersi in gestione:
        try {
            Util.writeOutput("Nuove cloudlet "+cloudlet_addresses,file);
            for(String cloudletAddr : cloudlet_addresses){
                addCloudlet(cloudletAddr);
                CloudletInterface cloudletInterface = (CloudletInterface) registryLookup(cloudletAddr,Config.cloudLetServiceName);
                cloudletInterface.newMasterAddress(address);
                Util.writeOutput("Comunico il nuovo indirizzo alla cloudlet "+cloudletAddr,file);
            }
            Util.writeOutput("CLOUDLET ADDRESSES\n"+usableCloudlet,file);
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
            splitDone = true;
            Util.writeOutput("DATANODE ADDRESSES\n"+dataNodeAddresses,file);
        }
        catch (Exception e) {
            Util.writeOutput(e.getMessage(),file);
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
            Util.writeOutput(e.getMessage(),file);
        }

        Util.writeOutput("Master Addresses: " + masterAddresses + ", " + address + "(io)\n",file);
    }

    /**
     * Servizio RMI
     * Calcola la CloudLet più "vicina" a un indirizzo IP
     * (Usato per cercare la cloudlet piu vicina a un dispositivo esterno)
     * cercandola tra quelle di tutti i master noti.
     *
     * @param sourceIP Indirizzo del Dispositivo che richiede la cloudlet piu vicina
     * @return restituisce l'indirizzo della cloudlet trovata
     */
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


    /**
     * Servizio RMI
     * Cerca la cloudlet piu vicina a un indirizzo IP tra quelle gestite dal Master in esecuzione
     * @param sourceIP indirizzo IP del dispositivo richiedente
     * @return Indirizzo Ip della cloudlet trovata e latenza rispetto all'indirizzo passato come parametro.
     */
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

    /**
     * Servizio RMI
     * Aggiunge una cloudlet a quelle gestite
     * @param ipAddress indirizzo ip della cloudlet gestita
     */
    @Override
    public void addCloudlet(String ipAddress) {
        if(!cloudletAddress.contains(ipAddress)) {
            cloudletAddress.add(ipAddress);
            usableCloudlet.add(ipAddress);
            synchronized (cloudletLifeSignalMapLock){
                cloudletLifeSignalMap.put(ipAddress,Util.getTimeInMillies());
            }
            synchronized (startupMapLock){
                startupMap.put(ipAddress,Util.getTimeInMillies());
            }
        }
        Util.writeOutput("Aggiungo "+ipAddress + " a "+cloudletAddress,file);
        System.out.println("Aggiungo "+ipAddress + " a "+cloudletAddress);
    }


    /**
     * Thread che pubblica su Amazon S3 gli indirizzi IP delle cloudlet attive gesite dal Master
     */
    private static Thread publishCloudletAddress = new Thread("PublishCloudletAddress"){
        @Override
        public void run() {
            while (!exit) {
                String fileName = Util.getPublicIPAddress() + ".txt";
                File file = new File(fileName);
                try {
                    if(!usableCloudlet.isEmpty()) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
                        for (String caddr : usableCloudlet) {
                            String line = caddr.concat("|");
                            bw.append(line);
                        }
                        bw.flush();
                        bw.close();
                        s3Upload.uploadFile(fileName);
                        Files.delete(Paths.get(fileName));
                    }
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
                    long timeInMillis = Util.getTimeInMillies();
                    while (!contactMainMaster()) {
                        if (Util.getTimeInMillies() - timeInMillis > 10000) {
                            Util.writeOutput("INFO: IMPOSSIBLE TO CONTACT Main Master " + mainMasterAddress,file);
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


        /**
         * Contatta il Master "Monitorato" da una Shadow e si prende gli indirizzi di DataNode,Masters e CloudLet da lui gestiti
         * @return
         */
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
            usableCloudlet.clear();

            dataNodeAddresses.addAll(temp_dataNodes);
            masterAddresses.addAll(temp_masters);
            cloudletAddress.addAll(temp_cloudlets);
            usableCloudlet.addAll(temp_cloudlets);

            Util.writeOutput("Usable cloudlet " +usableCloudlet,file);


            return true;
        }
    };

}
