package com.sdcc_project.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

public class DataNodeStatistic implements Serializable {

    private Long serverRequests = 0L;
    private Long serverSize = 0L;

    private ArrayList<FileInfo> fileInfos = new ArrayList<>();
    private ArrayList<FileInfo> filePerSize = new ArrayList<>();
    private ArrayList<FileInfo> filePerRequest = new ArrayList<>();
    private Integer dataNodePort;
    private long milliseconds_timer;

    public DataNodeStatistic(Integer dataNodePort) {
        this.dataNodePort = dataNodePort;
    }

    public void incrementSingleFileRequest(String fileName){

        FileInfo fileInfo = arrayContainsFileName(fileName,fileInfos);
        if(fileInfo == null){
            fileInfo = new FileInfo(fileName,Integer.toUnsignedLong(0),Integer.toUnsignedLong(0),dataNodePort);
            fileInfos.add(fileInfo);
        }
        else{
            fileInfos.remove(fileInfo);
            fileInfo.setFileRequests(fileInfo.getFileRequests()+1);
            fileInfos.add(fileInfo);
        }
        calculate();
    }

    public void incrementSingleFileSize(String fileName,Long fileSize){


        FileInfo fileInfo = arrayContainsFileName(fileName,fileInfos);
        if(fileInfo == null){
            fileInfo = new FileInfo(fileName,Integer.toUnsignedLong(0),fileSize,dataNodePort);
            fileInfos.add(fileInfo);
        }
        else{
            fileInfos.remove(fileInfo);
            fileInfo.setFileSize(fileSize);
            fileInfos.add(fileInfo);
        }
        calculate();

    }

    public ArrayList<FileInfo> getFileInfos() {
        return fileInfos;
    }

    public Long getServerRequests() {
        return serverRequests;
    }


    public Long getServerSize() {
        return serverSize;
    }


    public ArrayList<FileInfo> getFilePerRequest() {
        return filePerRequest;
    }


    public ArrayList<FileInfo> getFilePerSize() {
        return filePerSize;
    }

    public Integer getDataNodePort() {
        return dataNodePort;
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


    private FileInfo arrayContainsFileName(String fileName,ArrayList<FileInfo> fileInfo){
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

    public void calculate(){
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
        return "Server Port: " + dataNodePort + " - Server Size: "+serverSize +" - Server Requests: "+serverRequests
                +" - Files: " +fileInfos;
    }

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
    }
}
