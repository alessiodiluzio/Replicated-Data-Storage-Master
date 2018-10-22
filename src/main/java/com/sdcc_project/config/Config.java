package com.sdcc_project.config;

/**
 * Classe di configurazione con informazioni "statiche" per il sistema.
 */
public class Config {

    /**
     * Porta RMI:
     */
    public static final int port = 1099;

    /**
     * Servizi RMI offerti dai nodi del sistema
     */
    public static final String masterServiceName = "MasterService";
    public static final String cloudLetServiceName = "CloudLetService";
    public static final String dataNodeServiceName = "StorageService";

    /**
     * Nome dei DB locali ai Master e DataNode
     */
    public static final String MASTER_DATABASE_NAME = "DB_Master";
    public static final String DATANODE_DATABASE_NAME = "DB_DataNode";

    /**
     * File di log per master e DataNode
     */
    public static final String MASTER_FILE_LOGGING_NAME = "File_Logging_Master";
    public static final String DATANODE_FILE_LOGGING_NAME = "File_Logging_DataNode";

    /**
     * Tempo massimo di avvio di un nodo, scaduto il quale si ritiene il nodo perduto.
     */
    public static final long SYSTEM_STARTUP_TYME = 500000;

    /**
     * Intervallo massimo tra lifeSignal da parte di un nodo del sistema avviato,scaduto il quale
     * si ritiene il nodo perduto
     */
    public static final int MAX_TIME_NOT_RESPONDING_DATANODE = 30000;
    public static final long MAX_TIME_NOT_RESPONDING_CLOUDLET = 30000;

    /**
     * Tempi di sleep per i thread che implementano funzionalità del sistema.
     */
    public static final int BALANCING_THREAD_SLEEP_TIME = 20000;
    public static final int LIFE_THREAD_SLEEP_TIME = 10000;
    public static final int SHADOW_THREAD_SLEEP_TIME = 20000;
    public static final int STATISTIC_THREAD_SLEEP_TIME = 20000;
    /**
     * Intervallo di tempo massimo in cui un DataNode può essere vuoto ,scaduto il quale
     * si procede alla cancellazione del nodo.
     */
    public static final int MAX_TIME_EMPTY_DATANODE = 6000000;


    /**
     * Comandi per l'avvio dei dnodi del sistema
     */
    public static final String launchDataNode = "mvn exec:java@DataNode -Dexec.args=";
    public static final String launchMaster ="mvn exec:java@Master -Dexec.args=";
    public static final String launchCloudlet = "mvn spring-boot:run -Dspring-boot.run.arguments=";



}
