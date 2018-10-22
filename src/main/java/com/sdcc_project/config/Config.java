package com.sdcc_project.config;

public class Config {

    // Porta RMI:
    public static final int port = 1099;

    // Servizi
    public static final String masterServiceName = "MasterService";
    public static final String cloudLetServiceName = "CloudLetService";
    public static final String dataNodeServiceName = "StorageService";

    public static final String MASTER_DATABASE_NAME = "DB_Master";
    public static final String MASTER_FILE_LOGGING_NAME = "File_Logging_Master";

    public static final String DATANODE_DATABASE_NAME = "DB_DataNode";
    public static final String DATANODE_FILE_LOGGING_NAME = "File_Logging_DataNode";

    //Tempo di avvio del sistema
    public static final long SYSTEM_STARTUP_TYME = 500000;

    //Tempi di sleep per i thread del sistema.
    public static final int BALANCING_THREAD_SLEEP_TIME = 20000;
    public static final int LIFE_THREAD_SLEEP_TIME = 20000;
    public static final int SHADOW_THREAD_SLEEP_TIME = 20000;
    public static final int MAX_TIME_EMPTY_DATANODE = 6000000; // 600 secondi
    public static final int MAX_TIME_NOT_RESPONDING_DATANODE = 30000; // 3 minuti
    public static final int MAX_TIME_WAITING_FOR_INSTANCE_RUNNING = 300000;
    public static final int STATISTIC_THREAD_SLEEP_TIME = 20000;
    public static final int SAVE_DB_THREAD_SLEEP_TIME = 10000;



    // Comandi per lanciare DataNode e Master:
    public static final String launchDataNode = "mvn exec:java@DataNode -Dexec.args=";
    public static final String launchMaster ="mvn exec:java@Master -Dexec.args=";
    public static final String launchCloudlet = "mvn spring-boot:run -Dspring-boot.run.arguments=";
    public static final long MAX_TIME_NOT_RESPONDING_CLOUDLET = 30000;


}
