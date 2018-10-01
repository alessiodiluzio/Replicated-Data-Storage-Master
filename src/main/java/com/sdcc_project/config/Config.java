package com.sdcc_project.config;

public class Config {

    // Master:
    public static final String masterServiceName = "MasterService";
    public static final int masterRegistryPort = 1099;
    public static final String registryHost = "localhost";
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
    public static final int dataNodeStartPort = 1400;
    public static final int BALANCING_THREAD_SLEEP_TIME = 10000;
    public static final int LIFE_THREAD_SLEEP_TIME = 2000;
    public static final int MAX_TIME_EMPTY_DATANODE = 120000; // 120 secondi
    public static final int MAX_TIME_NOT_RESPONDING_DATANODE = 180000; // 30 secondi
    public static final int MAX_DATANODE_PER_MASTER = 3;

    // DataNode:
    public static final int STATISTIC_THREAD_SLEEP_TIME = 2000;
    public static final int SAVE_DB_THREAD_SLEEP_TIME = 10000;
    public static final String dataNodeServiceName = "StorageService";

    // Soglie:
    public static final Long dataNodeMemory = Integer.toUnsignedLong(300);
    public static final Long loadThreshold = Integer.toUnsignedLong(20);
    public static final Long dataNodeMaxRequest = Integer.toUnsignedLong(200);
    public static final Long requestThreshold = Integer.toUnsignedLong(20);
}
