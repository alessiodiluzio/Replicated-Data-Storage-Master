package com.sdcc_project.master;

import com.sdcc_project.config.Config;
import com.sdcc_project.dao.MasterDAO;
import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.entity.FileInfo;
import com.sdcc_project.entity.FileLocation;
import com.sdcc_project.exception.*;
import com.sdcc_project.service_interface.MasterInterface;
import com.sdcc_project.service_interface.StorageInterface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.rmi.registry.LocateRegistry.createRegistry;

public class Master extends UnicastRemoteObject implements MasterInterface {

    private static final Logger LOGGER = Logger.getLogger(Master.class.getName());
    // Shadow Master:
    private static ArrayList<ArrayList<String>> dbData = new ArrayList<>();
    private static int mainMasterPort;
    private static int shadowMasterRegistryPort;

    private static MasterDAO masterDAO;
    private static Registry registry;
    private static HashMap<String, DataNodeStatistic> dataNodesStatisticMap = new HashMap<>();
    private static Integer lastChosenServer = 0;
    private static boolean exit = false;
    private static HashMap<String,Long> lifeSignalMap = new HashMap<>();
    // Lock:
    private static final Object statisticLock = new Object();
    private static final Object dataNodePortsLock = new Object();
    private static final Object lifeSignalMapLock = new Object();

    /**
     * Elenco delle porte dei server DataNode attivi
     */
    private static ArrayList<String> dataNodePorts = new ArrayList<>();

    /**
     * Elenco dei processi su cui sono lanciati i DataNode attivi
     */
    private static HashMap<String,Process> processes = new HashMap<>();

    /**
     * Ultima porta usata per lanciare un DataNode.
     *
     */
    private static int lastPort = Config.dataNodeStartPort;

    private Master() throws RemoteException {

        super();
    }

    public static void main(String args[]){

        if(args.length < 1){
            System.out.println("Usage: Master Main || Master Shadow <main_master_port>");
            System.exit(1);
        }
        else if (args[0].equals("Shadow")) {

            if(args.length != 2) {
                System.out.println("Usage: Master Shadow <main_master_port>");
                System.exit(1);
            }

            try {
                mainMasterPort = Integer.parseInt(args[1]);
                shadowMasterRegistryPort = mainMasterPort + 1;
                masterDAO = MasterDAO.getInstance(shadowMasterRegistryPort);

                final int REGISTRY_PORT = shadowMasterRegistryPort;
                String registryHost = Config.registryHost;
                String serviceName = Config.masterServiceName;

                String completeName = "//" + registryHost + ":" + REGISTRY_PORT + "/" + serviceName;
                Master master = new Master();
                registry = createRegistry(REGISTRY_PORT);
                registry.rebind(completeName, master);
                System.out.println("Shadow Master Bound ");

                shadowThread.start();

                // Quando lo Shadow Thread termina vuol dire che il Master principale è caduto.
                // Lo Shadow Master deve quindi prendere il suo posto.
                shadowThread.join();
            }
            catch (MasterException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, e.getMessage() + "SHADOW MASTER SHUTDOWN");
                System.exit(1);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("DataNode Ports:\n" + dataNodePorts);

            try {
                for (String dataNodePort : dataNodePorts) {

                    String completeName = "//" + Config.registryHost + ":" + dataNodePort + "/" + Config.dataNodeServiceName;
                    registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(dataNodePort));
                    StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);

                    // Informa il DataNode del cambio di indirizzo del Master:
                    dataNode.changeMasterAddress(shadowMasterRegistryPort);

                    ArrayList<ArrayList<String>> db_data;
                    // Prende le informazioni dal DataNode:
                    db_data = dataNode.getDatabaseData();
                    for(ArrayList<String> file_info : db_data){
                        // Inserisce le informazioni nel suo DB:
                        masterDAO.insertFilePosition(file_info.get(0), file_info.get(1), Integer.parseInt(file_info.get(2)));
                    }
                }
                //System.out.println("DB Data:\n" + masterDAO.getAllData());
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            //createShadowMaster(shadowMasterRegistryPort);

            balancingThread.start();
            lifeThread.start();

        }
        else if (args[0].equals("Main")) {

            try {
                masterDAO = MasterDAO.getInstance(Config.masterRegistryPort);

                final int REGISTRY_PORT = Config.masterRegistryPort;
                String registryHost = Config.registryHost;
                String serviceName = Config.masterServiceName;

                String completeName = "//" + registryHost + ":" + REGISTRY_PORT + "/" + serviceName;
                Master master = new Master();
                registry = createRegistry(REGISTRY_PORT);
                registry.rebind(completeName, master);
                System.out.println("Master Bound ");
            }
            catch (MasterException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, e.getMessage() + "MASTER SHUTDOWN");
                System.exit(1);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // Creazione dei DataNode:
            for (int i = 0; i < Config.DATANODE_NUMBER; i++) {
                createDataNode();
            }

            //createShadowMaster(Config.masterRegistryPort);

            balancingThread.start();
            lifeThread.start();

            //Quando il processo Master termina "uccide" anche i processi DataNode creati nel main.
            Runtime.getRuntime().addShutdownHook(
                    new Thread("app-shutdown-hook") {
                        @Override
                        public void run() {
                            exit = true;
                            System.out.println("Salvo DB.");
                            masterDAO.saveDB();
                            masterDAO.closeDBConnection();
                            // Verifica con il metodo hasNext() che nella hashmap
                            // ci siano altri elementi su cui ciclare
                            for (String port : dataNodePorts) {
                                sendKillSignal(port);
                            }
                        }
                    });
        }
    }

    @Override
    public ArrayList<String> getDataNodePorts() {

        synchronized (dataNodePortsLock) {

            return dataNodePorts;
        }
    }

    /**
     * Funzione che invia il segnale di attesa terminazione del Thread.
     *
     * @param port Porta da contattare.
     */
    private static void sendKillSignal(String port){

        String serviceName = Config.dataNodeServiceName;
        String completeName = "//" + Config.registryHost + ":" +port + "/" + serviceName;

        // Cerca l'oggetto remoto per nome nel registro dell'host del server
        Registry registry;
        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(port));
            StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
            dataNode.killSignal();
        }
        catch (NotBoundException | IOException e) {
            LOGGER.log(Level.WARNING,port + " shutdowned");
        }
    }

    /**
     * Funzione per creare un nuovo DataNode.
     *
     */
    private synchronized static int createDataNode(){

        String cmd;
        lastPort++;
        String arguments = Integer.toString(lastPort);

        try {
            String os_name = System.getProperty("os.name");
            if(os_name.startsWith("Mac OS")){
                cmd = Config.MAC_CREATE_DATANODE + arguments;
            }
            else if(os_name.startsWith("Windows")){
                cmd = Config.WINDOWS_CREATE_DATANODE + arguments;
            }
            else {
                cmd = Config.OTHERS_CREATE_DATANODE + arguments;
                System.out.println("Sistema Operativo Terzo. Se il programma non si avvia correttamente, provare a cambiare la seguente stringa: " + Config.OTHERS_CREATE_DATANODE);
            }

            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            System.out.println(in.readLine());
            //Aggiungo il processo e la porta del DataNode creato nei rispettivi Array.
            synchronized (dataNodePortsLock) {
                dataNodePorts.add(Integer.toString(lastPort));
            }
            processes.put(Integer.toString(lastPort),process);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        Date now = new Date();
        lifeSignalMap.put(Integer.toString(lastPort), now.getTime());


        return lastPort;
    }

    /**
     * Funzione per creare un nuovo Shadow Master.
     *
     */
    private static void createShadowMaster(int mainMasterPort) {

        String cmd;
        String arguments = "Shadow " + Integer.toString(mainMasterPort);

        try {
            String os_name = System.getProperty("os.name");
            if(os_name.startsWith("Mac OS")){
                cmd = Config.MAC_CREATE_SHADOW_MASTER + arguments;
            }
            else if(os_name.startsWith("Windows")){
                cmd = Config.WINDOWS_CREATE_SHADOW_MASTER + arguments;
            }
            else {
                cmd = Config.OTHERS_CREATE_SHADOW_MASTER + arguments;
                System.out.println("Sistema Operativo Terzo. Se il programma non si avvia correttamente, provare a cambiare la seguente stringa: " + Config.OTHERS_CREATE_SHADOW_MASTER);
            }

            Runtime.getRuntime().exec(cmd);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
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
     * @param newPort nuova Posizione del file.
     * @param version versione del file aggiornato
     * @param oldPort vecchia posizione del file (in caso di spostamento)
     */
    @Override
    public void writeAck(String filename, String newPort,int version,String oldPort){

        try {
            if(oldPort!=null)
                masterDAO.insertOrUpdateSingleFilePositionAndVersion(filename,oldPort,newPort,version);
            masterDAO.insertOrUpdateSingleFilePositionAndVersion(filename,newPort,newPort,version);
        }
        catch (MasterException e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE,e.getMessage());
        }
    }

    /**
     *  Inserisce in un oggetto 'dataNodesStatistic' le informazioni sulle statistiche del DataNode.
     *
     */
    @Override
    public void setStatistic(DataNodeStatistic dataNodeStatistic) {

        synchronized (statisticLock) {
            dataNodesStatisticMap.put(Integer.toString(dataNodeStatistic.getDataNodePort()), dataNodeStatistic);
        }
    }

    @Override
    public void lifeSignal(String port)  {
        Date now = new Date();
        long timeInMillis = now.getTime();

        lifeSignalMap.remove(port);
        lifeSignalMap.put(port, timeInMillis);
    }

    /**
     * Algoritmo Round - Robin per la distribuzione delle "prime richieste" di scrittura
     *
     * @return un array di porte estratte tramite round robin di dimensione pari al coefficente di replicazione(interno)
     */
    private ArrayList<String> roundRobinDistribution(){

        int startPosition = -1;
        ArrayList<String> extractedPort = new ArrayList<>();
        synchronized (dataNodePortsLock) {
            if (lastChosenServer != 0) {

                startPosition = dataNodePorts.indexOf(Integer.toString(lastChosenServer));

            }
            for (int i = 1; i <= Config.REPLICATION_FACTORY; i++) {
                int index = (startPosition + i) % (dataNodePorts.size());
                if (i == 1)
                    lastChosenServer = Integer.parseInt(dataNodePorts.get(index));
                extractedPort.add(dataNodePorts.get(index));
            }
        }

        return extractedPort;
    }

    /**
     * Avvia l'operazione di bilanciamento di file , verifica l'elegibilità del server destinatario ,
     * invia il file al server destinatario e aggiorna le tabelle del master.
     *
     * @param filename FIle spostare e reletive informazioni
     * @param oldPort porta di provenienza
     * @param newServerPort Destinazione del file
     */
    private static void balanceFile(String filename,String oldPort, String newServerPort) throws MasterException, FileNotFoundException, DataNodeException, NotBalancableFile, AlreadyMovedFileException {
        System.out.println("Tento di mandare " + filename + " in " + newServerPort + " da "+oldPort);
        if (masterDAO.serverContainsFile(filename, newServerPort)) {
            throw new NotBalancableFile("A replica of this file " +filename+" is already in the server " +newServerPort);
        }
        if(!masterDAO.serverContainsFile(filename,oldPort)){
            throw new AlreadyMovedFileException("File " +filename+" is not in the server "+oldPort +" anymore.");
        }
        sendFileToDataNode(filename, newServerPort, oldPort);
        System.out.println("Spostato " + filename + " da " + oldPort + " in " + newServerPort);

    }

    /**
     * "Ordina" ad un DataNode di spostare un file ad un altro DataNode.
     *
     * @param fileName nome del file da spostare
     * @param newsServerPort indirizzo di destinazione
     * @param oldServerPort indirizzo mittente
     */
    private static void sendFileToDataNode(String fileName,String newsServerPort,String oldServerPort) throws MasterException, FileNotFoundException, DataNodeException {

        String serviceName = Config.dataNodeServiceName;
        String completeName = "//" + Config.registryHost + ":" +oldServerPort + "/" + serviceName;
        // Cerca l'oggetto remoto per nome nel registro dell'host del server
        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(oldServerPort));
            StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
            dataNode.moveFile(fileName,newsServerPort,masterDAO.getFileVersion(fileName,oldServerPort),oldServerPort);

        }
        catch (NotBoundException | IOException e) {
            e.printStackTrace();
            throw new MasterException("Error in bind to DataNode Port "+oldServerPort+" "+completeName);
        }
    }

    private static boolean getLifeSignal(String port){
        String serviceName = Config.dataNodeServiceName;
        String completeName = "//" + Config.registryHost + ":" +port + "/" + serviceName;
        // Cerca l'oggetto remoto per nome nel registro dell'host del server
        try {
            registry = LocateRegistry.getRegistry(Config.registryHost, Integer.parseInt(port));
            StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
            return dataNode.lifeSignal();
        }
        catch (NotBoundException | IOException e) {
            //LOGGER.log(Level.WARNING,"Impossible to bind to "+port + " "+completeName);
            return false;
        }
    }

    private static void handleDataNodeCrash(String address){

        String replacedDataNode = Integer.toString(createDataNode());
        if(getLifeSignal(address))
            return;
        synchronized (dataNodePortsLock){
            dataNodePorts.remove(address);
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
            System.out.println("MovedFie "+movedFile+" port "+address);
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

                            LOGGER.log(Level.INFO, "DATANODE " + entry.getKey() + " NOT RESPONDING SINCE " + entry.getValue());
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
                /*
                // Uccido i DataNode che sono vuoti da un lungo periodo di tempo:
                try {
                    deleteEmptyDataNode();
                }
                catch (ImpossibleToTerminateDatanodeException e) {
                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO TERMINATE DataNode " + e.getMessage());
                }*/

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
                            if (masterDAO.serverContainsFile(file.getFileName(), Integer.toString(serverStat.getDataNodePort()))) {
                                System.out.println(Integer.toString(serverStat.getDataNodePort()) + " contiene già " + file.getFileName());
                            }
                            Date time = new Date();
                            long timeInMillis = time.getTime();
                            while(!getLifeSignal(Integer.toString(serverStat.getDataNodePort()))){
                                Date now = new Date();
                                if(now.getTime()-timeInMillis>1000) {
                                    LOGGER.log(Level.SEVERE, "IMPOSSIBLE TO CONTACT SERVER " + serverStat.getDataNodePort());
                                    break;
                                }
                            }
                            balanceFile(file.getFileName(),Integer.toString(file.getDataNodeOwner()), Integer.toString(serverStat.getDataNodePort()));
                            //Il file viene rimosso dal buffer e aggiunto alle statistiche del server a cui è stato inviato
                            bufferRebalanced.remove(file);
                            file.setFileRequests(Integer.toUnsignedLong(0));
                            file.setDataNodeOwner(serverStat.getDataNodePort());
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
                int newServerPort = createDataNode();
                long serverSize = 0;
                long serverRequest = 0;
                //Finchè il nuovo server non è saturo e il buffer è non vuoto
                ArrayList<FileInfo> tmpBuffer = new ArrayList<>(buffer);
                System.out.println("!! " + buffer);
                for (FileInfo file : buffer) {
                        //Se il server può accettare un file lo invio
                    try{
                        if (serverSize + file.getFileSize() < maxSize && serverRequest + file.getFileRequests() < maxRequest) {
                            if (masterDAO.serverContainsFile(file.getFileName(), Integer.toString(newServerPort))) {
                                System.out.println(Integer.toString(newServerPort)+" contiene già " + file.getFileName());
                            }
                            Date time = new Date();
                            long timeInMillis = time.getTime();
                            while(!getLifeSignal(Integer.toString(newServerPort))){
                                Date now = new Date();
                                if(now.getTime()-timeInMillis>1000) {
                                    LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT SERVER "+newServerPort);
                                    break;
                                }
                            }
                            balanceFile(file.getFileName(),Integer.toString(file.getDataNodeOwner()), Integer.toString(newServerPort));
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

                    System.out.println("Empty DataNode: " + stats.getDataNodePort() + " - Timer: " + stats.getMilliseconds_timer());
                    // Rimuove dalla lista di porte globale la porta del DataNode:
                    synchronized (dataNodePortsLock) {
                        dataNodePorts.remove(Integer.toString(stats.getDataNodePort()));
                    }
                    // Contatta il DataNode per verificare che le statistiche sia ancora vuote:
                    String serviceName = Config.dataNodeServiceName;
                    String completeName = "//" + Config.registryHost + ":" + stats.getDataNodePort() + "/" + serviceName;
                    boolean isEmpty = false;
                    try {
                        registry = LocateRegistry.getRegistry(Config.registryHost, stats.getDataNodePort());
                        StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
                        isEmpty = dataNode.isEmpty();
                    }
                    catch (NotBoundException | IOException e) {

                        LOGGER.log(Level.SEVERE,"IMPOSSIBLE TO CONTACT DataNode "+ stats.getDataNodePort());
                    }
                    if(isEmpty){
                        try {
                            registry = LocateRegistry.getRegistry(Config.registryHost, stats.getDataNodePort());
                            StorageInterface dataNode = (StorageInterface) registry.lookup(completeName);
                            dataNode.terminate(); // Il DataNode viene terminato.
                        }
                        catch (NotBoundException | RemoteException e){

                            // DataNode terminato correttamente:
                            if(e.getCause().toString().equals("java.io.EOFException")){
                                LOGGER.log(Level.INFO,"EMPTY DATANODE " + stats.getDataNodePort() + " TERMINATED!");
                            }
                            // DataNode NON terminato correttamente:
                            else {
                                // Rinserisce la porta del DataNode in quelle globali:
                                synchronized (dataNodePortsLock) {
                                    dataNodePorts.add(Integer.toString(stats.getDataNodePort()));
                                }
                                throw new ImpossibleToTerminateDatanodeException(Integer.toString(stats.getDataNodePort()));
                            }
                        }
                        // Rimuove le statistiche di quel DataNode:
                        synchronized (statisticLock) {
                            dataNodesStatisticMap.remove(Integer.toString(stats.getDataNodePort()));
                        }
                        synchronized (lifeSignalMapLock) {
                            lifeSignalMap.remove(Integer.toString(stats.getDataNodePort()));
                        }
                    }
                    // DataNode non vuoto:
                    else {
                        // Rinserisce la porta del DataNode in quelle globali:
                        synchronized (dataNodePortsLock) {
                            dataNodePorts.add(Integer.toString(stats.getDataNodePort()));
                        }
                        // Il DataNode non viene terminato.
                    }
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
                System.out.println("shadowThread");

                try {
                    Date date = new Date();
                    long timeInMillis = date.getTime();
                    while (!contactMainMaster()) {
                        Date now = new Date();
                        if (now.getTime() - timeInMillis > 1000) {
                            LOGGER.log(Level.WARNING, "IMPOSSIBLE TO CONTACT Main Master " + mainMasterPort);
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
                String completeName = "//" + Config.registryHost + ":" + mainMasterPort + "/" + Config.masterServiceName;
                registry = LocateRegistry.getRegistry(Config.registryHost, mainMasterPort);
                MasterInterface master = (MasterInterface) registry.lookup(completeName);

                temp = master.getDataNodePorts();
            }
            catch (RemoteException | NotBoundException e) {
                return false;
            }

            dataNodePorts = temp;
            return true;
        }
    };

}
