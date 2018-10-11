package com.sdcc_project.master;

import com.sdcc_project.aws_managing.EC2InstanceFactory;
import com.sdcc_project.config.Config;
import com.sdcc_project.dao.MasterDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.entity.FileInfo;
import com.sdcc_project.entity.FileLocation;
import com.sdcc_project.exception.*;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;
import java.io.*;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.util.NodeType;
import com.sdcc_project.util.Util;
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

    //Controller
    private static EC2InstanceFactory ec2InstanceFactory;
    private static MasterDAO masterDAO;

    //DataNodeInformations
    private static HashMap<String,DataNodeStatistic> dataNodesStatisticMap = new HashMap<>();
    private static HashMap<String,Long> lifeSignalMap = new HashMap<>();

    // Lock:
    private static final Object statisticLock = new Object();
    private static final Object dataNodeAddressesLock = new Object();
    private static final Object lifeSignalMapLock = new Object();

    //Util
    private static String lastChosenServer = null;
    private static boolean exit = false;
    private static final Logger LOGGER = Logger.getLogger(Master.class.getName());
    private static File file;

    private Master() throws RemoteException {
        super();
    }

    public static void main(String args[]){
        if(args.length < 1){
            System.out.println("Usage: Master Main || Master Shadow <main_master_address> || Master Splitting");
            System.exit(1);
        }
        ec2InstanceFactory = EC2InstanceFactory.getInstance();
        switch (args[0]){
            case "Main":
                try {
                    masterConfiguration("Main");
                } catch (MasterException e) {
                    e.printStackTrace();
                    LOGGER.log(Level.SEVERE, e.getMessage() + "MASTER SHUTDOWN");
                    System.exit(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // Creazione dei DataNode:
                for (int i = 0; i < Config.DATANODE_NUMBER; i++) {
                    createDataNodeInstance();
                }
                try {
                    createMasterInstance("Shadow");
                } catch (MasterException e) {
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
                } catch (Exception e) {
                    writeOutput("SHADOW MASTER SHUTDOWN: " + e.getMessage());
                    System.exit(1);
                }
                writeOutput("DataNode Addresses:\n" + dataNodeAddresses);
                System.out.println("DataNode Addresses:\n" + dataNodeAddresses);
                try {
                    for (String dataNodeAddress : dataNodeAddresses) {
                        StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);
                        // Informa il DataNode del cambio di indirizzo del Master:
                        dataNode.changeMasterAddress(address);
                        ArrayList<ArrayList<String>> db_data;
                        // Prende le informazioni dal DataNode:
                        db_data = dataNode.getDatabaseData();
                        for (ArrayList<String> file_info : db_data) {
                            // Inserisce le informazioni nel suo DB:
                            masterDAO.insertOrUpdateSingleFilePositionAndVersion(file_info.get(0), file_info.get(1), file_info.get(1), Integer.parseInt(file_info.get(2)));
                        }
                    }
                    //System.out.println("DB Data:\n" + masterDAO.getAllData());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    createMasterInstance("Shadow");
                } catch (MasterException e) {
                    e.printStackTrace();
                }
                break;
            case "Splitting":
                try {
                    masterConfiguration("Splitting");
                } catch (Exception e) {
                    writeOutput("MASTER SHUTDOWN: " + e.getMessage());
                    System.exit(1);
                }

                try {
                    createMasterInstance("Shadow");
                } catch (MasterException e) {
                    e.printStackTrace();
                }
                break;

        }
        balancingThread.start();
        lifeThread.start();
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
        address= Util.getLocalIPAddress();
        String serviceName = Config.masterServiceName;
        String completeName = "//" + address + ":" + Config.port + "/" + serviceName;
        System.out.println(completeName);
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
     * @param registryHost L'indirizzo IP dell'host con cui instaurare la connessione IP
     * @param serviceName Nome del servizio di cui effettuare il lookup
     * @return interfaccia dell'oggetto remoto trovato
     * @throws NotBoundException ...
     * @throws RemoteException ...
     */
    private static Remote registryLookup(String registryHost, String serviceName) throws NotBoundException, RemoteException {

        String completeName = "//" + registryHost + ":" + Config.port + "/" + serviceName;

        Registry registry = LocateRegistry.getRegistry(registryHost, Config.port);
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

    @Override
    public ArrayList<String> getDataNodeAddresses() {

        synchronized (dataNodeAddressesLock) {

            return dataNodeAddresses;
        }
    }

    /*
    private static void sendKillSignal(String dataNode_address){

        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(dataNode_address, Config.dataNodeServiceName);
            dataNode.killSignal();
        }
        catch (NotBoundException | IOException e) {
            LOGGER.log(Level.WARNING,dataNode_address + " shutdowned");
        }
    }*/

    /**
     * Crea un DataNode avviandolo su una nuova istanza di Amazon EC2
     *
     * @return l'indirizzo IP del nuovo DataNode
     */
    private static String createDataNodeInstance(){
        String arguments =address;
        String newDataNodeIP=  ec2InstanceFactory.createEC2Instance(NodeType.DataNode,arguments);
        dataNodeAddresses.add(newDataNodeIP);
        writeOutput("Nuovo DataNode lanciato All'indirizzo: " + newDataNodeIP + " - Indirizzo del Master: " + address);
        Date now = new Date();
        lifeSignalMap.put(newDataNodeIP, now.getTime());
        return newDataNodeIP;
    }

    /**
     * Crea un master avviandolo su una nuova istanza di Amazon EC2
     *
     * @param nodeType tipo di master da avviare
     * @return L'indirizzo ip del nuovo master
     * @throws MasterException ...
     */
    private static String createMasterInstance(String nodeType) throws MasterException {

        String arguments;
        if(nodeType.equals("Shadow")) {
            String localIPAddress = Util.getLocalIPAddress();
            if(localIPAddress==null) {
                throw new MasterException("Impossibile to find Master Address");
            }
            arguments = "\"Shadow\" " + "\"" + localIPAddress + "\"";
        }
        else
            arguments = "Splitting";
        return ec2InstanceFactory.createEC2Instance(NodeType.Master,arguments);
    }


    /**
     * Funzone che ricerca il DataNode in cui è contenuto un file richiesto dal Client.
     *
     * @param fileName Nome del file richiesto dal Client.
     * @return Indirizzo del DataNode responsabile del file.
     */
    @Override
    public FileLocation checkFile(String fileName, String operation) throws MasterException, FileNotFoundException {

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
                ArrayList<String> filePosition = new ArrayList<>();
                try {
                    filePosition=masterDAO.getFilePosition(fileName);
                } catch (MasterException e) {
                    LOGGER.log(Level.WARNING, "NEW FILE");
                }
                cl.setResult(true);
                if (!filePosition.isEmpty()) {

                    int latestVersion = masterDAO.getFileVersion(fileName, filePosition.get(0));
                    cl.setFileVersion(latestVersion + 1);
                    cl.setFilePositions(filePosition);

                } else {
                    filePosition = roundRobinDistribution();
                    if (filePosition.isEmpty()) {
                        cl.setResult(false);
                    }
                    cl.setFileVersion(1);
                    cl.setFilePositions(filePosition);
                }
            }
        return cl;
    }


    /**
     * Notifica di scrittura della versione di un file.
     *
     * @param filename Nome del file di cui si deve aggiornare la versione.
     * @param newAddress nuova Posizione del file.
     * @param version versione del file aggiornato
     * @param oldAddress vecchia posizione del file (in caso di spostamento)
     */
    @Override
    public void writeAck(String filename, String newAddress, int version, String oldAddress){

        boolean find;
        boolean delete_oldAddress = false;

        // Verifica che l'indirizzo compare nell'elenco di DataNode gestito da questo Master:
        synchronized (dataNodeAddressesLock){
            find = dataNodeAddresses.contains(newAddress);
            if(oldAddress != null) {
                // Contiene la 'old' ma non la 'new':
                if(dataNodeAddresses.contains(oldAddress) && !find) {
                    delete_oldAddress = true;
                }
            }
        }
        if(find) {
            try {
                if(oldAddress != null){
                    masterDAO.insertOrUpdateSingleFilePositionAndVersion(filename, oldAddress, newAddress, version);
                }
                masterDAO.insertOrUpdateSingleFilePositionAndVersion(filename, newAddress, newAddress, version);
            }
            catch (MasterException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE,e.getMessage());
            }
        }
        if(delete_oldAddress) {
            try {
                masterDAO.deleteFilePosition(filename, oldAddress);
            }
            catch (MasterException e) {
                e.printStackTrace();
            }
        }
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
     * Algoritmo Round - Robin per la distribuzione delle "prime richieste" di scrittura
     *
     * @return un array di indirizzi estratte tramite round robin di dimensione pari al coefficente di replicazione(interno)
     */
    private ArrayList<String> roundRobinDistribution(){
        int startPosition = -1;
        ArrayList<String> extractedAddress = new ArrayList<>();
        synchronized (dataNodeAddressesLock) {
            if (lastChosenServer != null) {
                startPosition = dataNodeAddresses.indexOf(lastChosenServer);
            }
            for (int i = 1; i <= Config.REPLICATION_FACTORY; i++) {
                int index = (startPosition + i) % (dataNodeAddresses.size());
                extractedAddress.add(dataNodeAddresses.get(index));
                lastChosenServer = dataNodeAddresses.get(index);
            }
        }
        return extractedAddress;
    }

    /**
     * Avvia l'operazione di bilanciamento di file , verifica l'elegibilità del server destinatario ,
     * invia il file al server destinatario e aggiorna le tabelle del master.
     *
     * @param filename FIle spostare e reletive informazioni
     * @param oldAddress indirizzo di provenienza
     * @param newServerAddress Destinazione del file
     */
    private static void balanceFile(String filename, String oldAddress, String newServerAddress) throws MasterException, FileNotFoundException, DataNodeException, NotBalancableFile, AlreadyMovedFileException {

        writeOutput("Tento di mandare " + filename + " in " + newServerAddress + " da " + oldAddress);
        System.out.println("Tento di mandare " + filename + " in " + newServerAddress + " da " + oldAddress);

        if (masterDAO.serverContainsFile(filename, newServerAddress)) {
            throw new NotBalancableFile("A replica of this file " +filename+" is already in the server " + newServerAddress);
        }
        if(!masterDAO.serverContainsFile(filename, oldAddress)){
            throw new AlreadyMovedFileException("File " +filename+" is not in the server " + oldAddress +" anymore.");
        }

        sendFileToDataNode(filename, newServerAddress, oldAddress);
        System.out.println("Spostato " + filename + " da " + oldAddress + " in " + newServerAddress);
        writeOutput("Spostato " + filename + " da " + oldAddress + " in " + newServerAddress);
    }

    /**
     * "Ordina" ad un DataNode di spostare un file ad un altro DataNode.
     *
     * @param fileName nome del file da spostare
     * @param newsServerAddress indirizzo di destinazione
     * @param oldServerAddress indirizzo mittente
     */
    private static void sendFileToDataNode(String fileName, String newsServerAddress, String oldServerAddress) throws MasterException, FileNotFoundException, DataNodeException {

        try {
            StorageInterface dataNode = (StorageInterface) registryLookup(oldServerAddress, Config.dataNodeServiceName);
            dataNode.moveFile(fileName,newsServerAddress,masterDAO.getFileVersion(fileName, oldServerAddress), oldServerAddress);
        }
        catch (NotBoundException | IOException e) {
            e.printStackTrace();
            throw new MasterException("Error in bind to DataNode on Address " + oldServerAddress);
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

    private static void handleDataNodeCrash(String address){

        String replacedDataNode = createDataNodeInstance();
        if(getLifeSignal(address))
            return;
        synchronized (dataNodeAddressesLock){
            dataNodeAddresses.remove(address);
        }
        synchronized (statisticLock){
            dataNodesStatisticMap.remove(address);
        }
        synchronized (lifeSignalMapLock) {
            lifeSignalMap.remove(address);
        }
        try {
            ArrayList<String> movedFile ;
            try {
                movedFile = masterDAO.getServerFiles(address);
            } catch (FileNotFoundException e) {
                LOGGER.log(Level.WARNING,"Failed server has none file");
                return;
            }
            System.out.println("MovedFie "+movedFile+" Address "+address);
            for(String file : movedFile){
                masterDAO.deleteFilePosition(file,address);
                ArrayList<String> filePositions = masterDAO.getFilePosition(file);
                filePositions.remove(address);
                for(String position : filePositions){
                    try {
                        Date date = new Date();
                        long timeInMillis = date.getTime();
                        while(!getLifeSignal(replacedDataNode)){
                            Date now = new Date();
                            if(now.getTime()-timeInMillis>1000) {
                                LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT SERVER "+replacedDataNode);
                                break;
                            }
                        }
                        balanceFile(file,position,replacedDataNode);
                        break;
                    } catch (FileNotFoundException | DataNodeException | NotBalancableFile | AlreadyMovedFileException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (MasterException e) {
            e.printStackTrace();
        }

    }

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

        long maxRequest = Config.dataNodeMaxRequest - Config.requestThreshold;
        long maxSize = Config.dataNodeMemory - Config.loadThreshold;

        @Override
        public void run() {

            while (!exit) {
                writeOutput("Inizio il Bilanciamento!");

                // Uccido i DataNode che sono vuoti da un lungo periodo di tempo:
                try {
                    deleteEmptyDataNode();
                }
                catch (ImpossibleToTerminateDatanodeException e) {
                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO TERMINATE DataNode " + e.getMessage());
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

                //Finchè il server non sotto la soglia di richieste e size metto file nel buffer di spostamento
                while(stats.getServerSize() > maxSize || stats.getServerRequests() > maxRequest){
                    FileInfo fileToMove ;
                    //Il file da spostare viene preso tra quelli piu pesanti o con piu richieste a seconda di quale soglia
                    //viene superata
                    if(stats.getServerSize()>maxSize){
                        fileToMove = stats.getFilePerSize().get(0);
                    }
                    else {
                        fileToMove = stats.getFilePerRequest().get(0);

                    }
                    //Aggiungo il file al buffer
                    buffer.add(fileToMove);
                    //Aggiorno le statistiche del server
                    stats.remove(fileToMove.getFileName());
                }
                //Aggiorno la lista delle statistiche globale
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
            for(DataNodeStatistic serverStat : statisticAfterBalancing){
                //Per ogni file nel buffer
                for(FileInfo file : buffer){
                    //Se l'aggiunto del file a un server non comporta il superamento delle soglie effettuo lo spostamento
                    if(serverStat.getServerSize() + file.getFileSize() < maxSize &&
                            serverStat.getServerRequests() + file.getFileRequests() <maxRequest){
                        try {
                            if (masterDAO.serverContainsFile(file.getFileName(), serverStat.getDataNodeAddress())) {
                                System.out.println(serverStat.getDataNodeAddress() + " contiene già " + file.getFileName());
                            }
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

                        }catch (MasterException | FileNotFoundException | DataNodeException e){
                            LOGGER.log(Level.SEVERE,e.getMessage());
                        } catch (NotBalancableFile notBalancableFile) {
                            LOGGER.log(Level.WARNING,notBalancableFile.getMessage());
                        } catch (AlreadyMovedFileException e) {
                            LOGGER.log(Level.WARNING,e.getMessage());
                            bufferRebalanced.remove(file);
                        }
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

            int count = 0;
            ArrayList<FileInfo> temp= new ArrayList<>(buffer);
            temp.sort(FileInfo.getCompByRequests());
            if(buffer.get(0).getFileSize()>maxSize || temp.get(0).getFileRequests()>maxRequest)
                return new ArrayList<>();
            while (!buffer.isEmpty()) {
                if (count > 2) //Per evitare ciclo infinito
                    break;
                //Creo un nuovo dataNode.
                String newServerAddress = createDataNodeInstance();
                long serverSize = 0;
                long serverRequest = 0;
                //Finchè il nuovo server non è saturo e il buffer è non vuoto
                ArrayList<FileInfo> tmpBuffer = new ArrayList<>(buffer);
                System.out.println("!! " + buffer);
                for (FileInfo file : buffer) {
                        //Se il server può accettare un file lo invio
                    try{
                        if (serverSize + file.getFileSize() < maxSize && serverRequest + file.getFileRequests() < maxRequest) {
                            if (masterDAO.serverContainsFile(file.getFileName(), newServerAddress)) {
                                System.out.println(newServerAddress+" contiene già " + file.getFileName());
                            }
                            Date time = new Date();
                            long timeInMillis = time.getTime();
                            while(!getLifeSignal(newServerAddress)){
                                Date now = new Date();
                                if(now.getTime()-timeInMillis>10000) {
                                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT SERVER "+newServerAddress);
                                    break;
                                }
                            }
                            balanceFile(file.getFileName(), file.getDataNodeOwner(), newServerAddress);
                                //Aggiorno carico del nuovo server
                            System.out.println("Bilanciato " + file);
                            serverSize = serverSize + file.getFileSize();
                            serverRequest = serverRequest + file.getFileRequests();
                            tmpBuffer.remove(file);

                            //Altrimenti il file è inserito tra quelli non bilanciati.
                        }
                    }catch (MasterException masterException){
                        LOGGER.log(Level.SEVERE,masterException.getMessage()+ " ME");
                    }catch (NotBalancableFile notBalancableFile) {
                        LOGGER.log(Level.WARNING,notBalancableFile.getMessage() + " NBFE");
                    } catch (FileNotFoundException e) {
                        LOGGER.log(Level.SEVERE,e.getMessage() + " FNTE");
                    } catch (DataNodeException e) {
                        LOGGER.log(Level.SEVERE,e.getMessage() + " DNE");
                    } catch (AlreadyMovedFileException e) {
                        LOGGER.log(Level.WARNING,e.getMessage());
                        tmpBuffer.remove(file);
                    }
                }

                buffer.clear();
                buffer.addAll(tmpBuffer);
                //Ripopolo il buffer con i file che non sono riuscito a ricollocare
                count++;
            }

            //Ritorna i file non ricollocabili
            return buffer;
        }

        /**
         *  Uccide i DataNode che sono vuoti da un lungo periodo di tempo.
         *
         */
        private void deleteEmptyDataNode() throws ImpossibleToTerminateDatanodeException {

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
                        try {
                            StorageInterface dataNode = (StorageInterface) registryLookup(stats.getDataNodeAddress(), Config.dataNodeServiceName);
                            dataNode.terminate(); // Il DataNode viene terminato.
                        }
                        catch (NotBoundException | RemoteException e){

                            // DataNode terminato correttamente:
                            if(e.getCause().toString().equals("java.io.EOFException")){
                                writeOutput("INFO: EMPTY DATANODE " + stats.getDataNodeAddress() + " TERMINATED!");
                                LOGGER.log(Level.INFO,"EMPTY DATANODE " + stats.getDataNodeAddress() + " TERMINATED!");
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

            if(dataNodes_number > Config.MAX_DATANODE_PER_MASTER) {

                // Crea il nuovo Master:
                String newMasterAddress = "";
                try {
                    newMasterAddress= createMasterInstance("Splitting");
                } catch (MasterException e) {
                    e.printStackTrace();
                }
                masterAddresses.add(newMasterAddress); //TODO Comunicazione tra master
                writeOutput("Creato nuovo Master all'indirizzo: " + newMasterAddress);
                System.out.println("Creato nuovo Master all'indirizzo: " + newMasterAddress);

                // Prende metà degli indirizzi dei DataNode:
                int dataNode_to_move = dataNodes_number / 2;
                ArrayList<String> addresses = new ArrayList<>();
                int index;

                synchronized (dataNodeAddressesLock) {
                    while(!dataNodeAddresses.isEmpty() && dataNode_to_move > 0) {

                        // Numero random tra [ 0 , dataNodeAddresses.size() - 1 ] :
                        index = (int) (Math.random() * (dataNodeAddresses.size()));

                        addresses.add(dataNodeAddresses.get(index));
                        dataNodeAddresses.remove(index);

                        dataNode_to_move--;
                    }
                }
                // Rimuove gli indirizzi dalla StatisticMap e dalla LifeMap:
                synchronized (statisticLock){
                    for (String addr : addresses) {
                        dataNodesStatisticMap.remove(addr);
                    }
                }
                synchronized (lifeSignalMapLock) {
                    for (String addr : addresses) {
                        lifeSignalMap.remove(addr);
                    }
                }

                try {
                    Date date = new Date();
                    long timeInMillis = date.getTime();
                    // Invia al nuovo Master gli indirizzi dei DataNode che deve gestire:
                    while (!contactNewMaster(newMasterAddress, addresses)) {
                        Date now = new Date();
                        if (now.getTime() - timeInMillis > 10000) {
                            writeOutput("SEVERE: IMPOSSIBLE TO CONTACT New Master " + newMasterAddress);
                            LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT New Master " + newMasterAddress);
                            return;
                        }
                    }

                    // Rimuove dal DB tutte le informazioni relative ai DataNode traferito al nuovo Master:
                    for(String address : addresses){
                        masterDAO.deleteAllAddress(address);
                    }

                    writeOutput("DataNode Spostati: " + addresses);
                    System.out.println("DataNode Spostati: " + addresses);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        private boolean contactNewMaster(String newMasterAddress, ArrayList<String> addresses) {

            try {
                MasterInterface master = (MasterInterface) registryLookup(newMasterAddress, Config.masterServiceName);

                master.dataNodeToManage(addresses);
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }
    };

    /**
     * Metodo che riceve una lista di indirizzi di DataNode ed esegue tutte le operazini per prendere in gestione quei DataNode.
     *
     * @param addresses Indirizzi dei DataNode che il nuovo Master deve gestire.
     */
    @Override
    public void dataNodeToManage(ArrayList<String> addresses) {

        try {
            for (String dataNodeAddress : addresses) {

                StorageInterface dataNode = (StorageInterface) registryLookup(dataNodeAddress, Config.dataNodeServiceName);

                ArrayList<ArrayList<String>> db_data;
                // Richiede al DataNode la lista di file che possiede:
                db_data = dataNode.getDatabaseData();
                for(ArrayList<String> file_info : db_data){
                    // Inserisce le informazioni nel suo DB:
                    masterDAO.insertOrUpdateSingleFilePositionAndVersion(file_info.get(0), file_info.get(1), file_info.get(1), Integer.parseInt(file_info.get(2)));
                }

                // Iserisce l'indirizzo del DataNode in quelli globali:
                synchronized (dataNodeAddressesLock) {
                    dataNodeAddresses.add(dataNodeAddress);
                }

                // Informa il DataNode del cambio di indirizzo del Master:
                dataNode.changeMasterAddress(address);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  Thread che contatta il Master Principale per verificare che sia attivo.
     *
     */
    private static Thread shadowThread =  new Thread("shadowThread"){

        @Override
        public void run() {

            while (!exit) {
                System.out.println("shadowThread");

                try {
                    Date date = new Date();
                    long timeInMillis = date.getTime();
                    while (!contactMainMaster()) {
                        Date now = new Date();
                        if (now.getTime() - timeInMillis > 1000) {
                            writeOutput("IMPOSSIBLE TO CONTACT Main Master " + mainMasterAddress);
                            LOGGER.log(Level.WARNING, "IMPOSSIBLE TO CONTACT Main Master " + mainMasterAddress);
                            return;
                        }
                    }

                    sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private boolean contactMainMaster() {

            ArrayList<String> temp;

            try {
                MasterInterface master = (MasterInterface) registryLookup(mainMasterAddress, Config.masterServiceName);

                temp = master.getDataNodeAddresses();
            }
            catch (RemoteException | NotBoundException e) {
                return false;
            }

            dataNodeAddresses = temp;
            return true;
        }
    };

}
