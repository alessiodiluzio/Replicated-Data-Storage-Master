package com.sdcc_project.config;

public class Config {

    // Porta RMI:
    public static final int port = 1099;

    // Master:
    public static final String masterServiceName = "MasterService";
    public static final String cloudLetServiceName = "CloudLetService";
    public static final String MASTER_DATABASE_NAME = "DB_Master";
    public static final String MASTER_FILE_LOGGING_NAME = "File_Logging_Master";

    public static final int REPLICATION_FACTORY = 3;

    public static final int NUMBER_OF_DATANODES = 1;
    public static final int CLOUDLET_NUMBER = 1;
    public static final int BALANCING_THREAD_SLEEP_TIME = 10000;
    public static final int LIFE_THREAD_SLEEP_TIME = 20000;
    public static final int SHADOW_THREAD_SLEEP_TIME = 2000;
    public static final int MAX_TIME_EMPTY_DATANODE = 6000000; // 600 secondi
    public static final int MAX_TIME_NOT_RESPONDING_DATANODE = 180000; // 3 minuti
    public static final int MAX_TIME_WAITING_FOR_INSTANCE_RUNNING = 300000; // (2 minuti) Attenzione può volerci molto tempo.

    // DataNode:
    public static final int STATISTIC_THREAD_SLEEP_TIME = 20000;
    public static final int SAVE_DB_THREAD_SLEEP_TIME = 10000;
    public static final String dataNodeServiceName = "StorageService";
    public static final String DATANODE_DATABASE_NAME = "DB_DataNode";
    public static final String DATANODE_FILE_LOGGING_NAME = "File_Logging_DataNode";

    // Soglie:
    public static final Long dataNodeMemory = Integer.toUnsignedLong(1000);
    public static final Long loadThreshold = Integer.toUnsignedLong(20);
    public static final Long dataNodeMaxRequest = Integer.toUnsignedLong(1000);
    public static final Long requestThreshold = Integer.toUnsignedLong(20);

    // Comandi per lanciare DataNode e Master:
    public static final String launchDataNode = "mvn exec:java@DataNode -Dexec.args=";
    public static final String launchMaster ="mvn exec:java@Master -Dexec.args=";
    public static final String launchCloudlet = "mvn spring-boot:run -Dspring-boot.run.arguments=";
    public static final long MAX_TIME_NOT_RESPONDING_CLOUDLET = 600000;

    public static final double cpuMaxUsage = 70.0;
    public static final double ramMaxUsage = 70.0;
}
