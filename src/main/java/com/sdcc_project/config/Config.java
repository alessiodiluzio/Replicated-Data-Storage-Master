package com.sdcc_project.config;

public class Config {

    //
    public static final int port = 1099;

    // Master:
    public static final String masterServiceName = "MasterService";
    public static final String registryHost = "localhost";
    public static final String MASTER_DATABASE_NAME = "DB_Master";
    public static final String MASTER_FILE_LOGGING_NAME = "File_Logging_Master";
    // Per creare un DataNode:
    public static final String MAC_CREATE_DATANODE = "mvn exec:java@DataNode -Dexec.args=";
    public static final String WINDOWS_CREATE_DATANODE = "cmd.exe /c mvn exec:java@DataNode -Dexec.args=" ;
    public static final String OTHERS_CREATE_DATANODE = "mvn exec:java@DataNode -Dexec.args=" ;
    // Per creare uno Shadow Master:
    public static final String MAC_CREATE_MASTER = "mvn exec:java@Master -Dexec.args=";
    public static final String WINDOWS_CREATE_MASTER = "cmd.exe /c mvn exec:java@Master -Dexec.args=" ;
    public static final String OTHERS_CREATE_MASTER = "mvn exec:java@Master -Dexec.args=";
    public static final int REPLICATION_FACTORY = 1;
    public static final int DATANODE_NUMBER = 3;
    public static final int BALANCING_THREAD_SLEEP_TIME = 10000;
    public static final int LIFE_THREAD_SLEEP_TIME = 2000;
    public static final int MAX_TIME_EMPTY_DATANODE = 6000000; // 120 secondi
    public static final int MAX_TIME_NOT_RESPONDING_DATANODE = 180000; // 30 secondi
    public static final int MAX_DATANODE_PER_MASTER = 3;

    // DataNode:
    public static final int STATISTIC_THREAD_SLEEP_TIME = 2000;
    public static final int SAVE_DB_THREAD_SLEEP_TIME = 10000;
    public static final String dataNodeServiceName = "StorageService";
    public static final String DATANODE_DATABASE_NAME = "DB_DataNode";
    public static final String DATANODE_FILE_LOGGING_NAME = "File_Logging_DataNode";

    // Soglie:
    public static final Long dataNodeMemory = Integer.toUnsignedLong(1000);
    public static final Long loadThreshold = Integer.toUnsignedLong(20);
    public static final Long dataNodeMaxRequest = Integer.toUnsignedLong(1000);
    public static final Long requestThreshold = Integer.toUnsignedLong(20);
    public static String launchDataNode = "mvn exec:java@DataNode -Dexec.args=";
    public static String launchMaster ="mvn exec:java@Master -Dexec.args=";
}
