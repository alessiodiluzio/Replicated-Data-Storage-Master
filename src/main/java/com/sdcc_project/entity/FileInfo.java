package com.sdcc_project.entity;

import java.io.Serializable;
import java.util.Comparator;

public class FileInfo implements Serializable {
    private String fileName;
    private Long fileRequests;
    private Long fileSize;
    private Integer dataNodeOwner;

    public FileInfo(String fileName, Long fileRequests, Long fileSize, Integer dataNodeOwner) {
        this.fileName = fileName;
        this.fileRequests = fileRequests;
        this.fileSize = fileSize;
        this.dataNodeOwner = dataNodeOwner;
    }

    public void setDataNodeOwner(Integer dataNodeOwner) {
        this.dataNodeOwner = dataNodeOwner;
    }

    public Integer getDataNodeOwner() {
        return dataNodeOwner;
    }

    public String getFileName() {
        return fileName;
    }

    public Long getFileRequests() {
        return fileRequests;
    }

    public void setFileRequests(Long fileRequests) {
        this.fileRequests = fileRequests;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public static Comparator<FileInfo> getCompByRequests()
    {
        return (f1, f2) -> f2.getFileRequests().compareTo(f1.getFileRequests());

    }

    public static Comparator<FileInfo> getCompBySize()
    {
        return (f1, f2) -> f2.getFileSize().compareTo(f1.getFileSize());
    }

    @Override
    public String toString() {
        return fileName + " " +dataNodeOwner+" " + Long.toString(fileSize) + " " + Long.toString(fileRequests);
    }
}
