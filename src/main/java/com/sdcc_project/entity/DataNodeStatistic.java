package com.sdcc_project.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Contiene le statistiche relative a un DataNode
 * - File Contenuti con rispettive richieste e peso
 * - Statisiche sull'uso delle risorse di CPU e RAM
 */
public class DataNodeStatistic implements Serializable {

    private Long serverRequests = 0L;
    private Long serverSize = 0L;
    private boolean overCpuUsage = false;
    private boolean ramUsage = false;
    private boolean underUsage = false;

    private ArrayList<FileInfo> fileInfos = new ArrayList<>();
    private ArrayList<FileInfo> filePerSize = new ArrayList<>();
    private ArrayList<FileInfo> filePerRequest = new ArrayList<>();
    private String dataNodeAddress;
    private long milliseconds_timer;



    public DataNodeStatistic(String dataNodeAddress) {

        this.dataNodeAddress = dataNodeAddress;
    }

    public void incrementSingleFileRequest(String fileName){

        FileInfo fileInfo = arrayContainsFileName(fileName,fileInfos);
        if(fileInfo == null){
            fileInfo = new FileInfo(fileName,Integer.toUnsignedLong(0), Integer.toUnsignedLong(0), dataNodeAddress);
            fileInfos.add(fileInfo);
        }
        else{
            fileInfos.remove(fileInfo);
            long request = fileInfo.getFileRequests() +1;
            fileInfo.setFileRequests(request);
            fileInfos.add(fileInfo);
        }
        calculate();
    }

    public void incrementSingleFileSize(String fileName,Long fileSize){


        FileInfo fileInfo = arrayContainsFileName(fileName,fileInfos);
        if(fileInfo == null){
            fileInfo = new FileInfo(fileName,Integer.toUnsignedLong(0),fileSize, dataNodeAddress);
            fileInfos.add(fileInfo);
        }
        else{
            fileInfos.remove(fileInfo);
            fileInfo.setFileSize(fileSize);
            fileInfos.add(fileInfo);
        }
        calculate();

    }

    public boolean isUnderUsage() {
        return underUsage;
    }

    public void setUnderUsage(boolean underUsage) {
        this.underUsage = underUsage;
    }

    public boolean isOverCpuUsage() {
        return overCpuUsage;
    }

    public void setOverCpuUsage(boolean overCpuUsage) {
        this.overCpuUsage = overCpuUsage;
    }

    public boolean isRamUsage() {
        return ramUsage;
    }

    public void setRamUsage(boolean ramUsage) {
        this.ramUsage = ramUsage;
    }

    public ArrayList<FileInfo> getFileInfos() {
        return fileInfos;
    }



    private Long getServerSize() {
        return serverSize;
    }


    public ArrayList<FileInfo> getFilePerRequest() {
        return filePerRequest;
    }


    public ArrayList<FileInfo> getFilePerSize() {
        return filePerSize;
    }

    public String getDataNodeAddress() {
        return dataNodeAddress;
    }

    public long getMilliseconds_timer() {
        return milliseconds_timer;
    }

    public void setMilliseconds_timer(long milliseconds_timer) {
        this.milliseconds_timer = milliseconds_timer;
    }

    public void orderStatistics(){
        fileInfos.sort(FileInfo.getCompByRequests());
        filePerRequest.clear();
        filePerRequest.addAll(fileInfos);
        fileInfos.sort(FileInfo.getCompBySize());
        filePerSize.clear();
        filePerSize.addAll(fileInfos);
        calculate();
    }


    private FileInfo arrayContainsFileName(String fileName, ArrayList<FileInfo> fileInfo){
        for(FileInfo info : fileInfo){
            if(info.getFileName().equals(fileName))
                return info;
        }
        return null;

    }

    public static Comparator<DataNodeStatistic> getCrescentCompBySize()
    {
        return Comparator.comparing(DataNodeStatistic::getServerSize);
    }

    public void insert(FileInfo fileInfo) {
        fileInfos.add(fileInfo);
        serverSize = serverSize + fileInfo.getFileSize();
        serverRequests = serverRequests + fileInfo.getFileRequests();
        orderStatistics();
    }

    public void remove(String fileName) {
        FileInfo fileInfo = null;
        for(FileInfo info : fileInfos){
            if(info.getFileName().equals(fileName)){
                fileInfo = info;
            }
        }
        if(fileInfo!=null){
            fileInfos.remove(fileInfo);
            filePerSize.remove(fileInfo);
            filePerRequest.remove(fileInfo);
        }
        calculate();


    }

    private void calculate(){
        long size = 0;
        long request = 0;
        for(FileInfo info : fileInfos){
            size = size + info.getFileSize();
            request = request + info.getFileRequests();

        }
        serverRequests = request;
        serverSize = size;
    }

    @Override
    public String toString() {
        return "Server Address: " + dataNodeAddress + " - Server Size: "+serverSize +" - Server Requests: "+serverRequests
                +" - Files: " +fileInfos;
    }

    /*

    public void resetRequest() {
        ArrayList<FileInfo> tmp = new ArrayList<>(fileInfos);
        for(FileInfo fileInfo : tmp){
            fileInfos.remove(fileInfo);
            filePerRequest.remove(fileInfo);
            filePerRequest.remove(fileInfo);
            fileInfo.setFileRequests(0L);
            fileInfos.add(fileInfo);
            filePerRequest.add(fileInfo);
            filePerRequest.add(fileInfo);
        }
        serverRequests = 0L;
    }*/
}
