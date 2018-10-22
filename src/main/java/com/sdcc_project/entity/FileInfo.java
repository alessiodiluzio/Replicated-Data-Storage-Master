package com.sdcc_project.entity;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Statistiche e informazione di un dato file nel sistema
 */
public class FileInfo implements Serializable {
    private String fileName;
    private Long fileRequests;
    private Long fileSize;
    private String dataNodeOwner;

    FileInfo(String fileName, Long fileRequests, Long fileSize, String dataNodeOwner) {

        this.fileName = fileName;
        this.fileRequests = fileRequests;
        this.fileSize = fileSize;
        this.dataNodeOwner = dataNodeOwner;
    }

    public void setDataNodeOwner(String dataNodeOwner) {
        this.dataNodeOwner = dataNodeOwner;
    }

    public String getDataNodeOwner() {
        return dataNodeOwner;
    }

    public String getFileName() {
        return fileName;
    }

    Long getFileRequests() {
        return fileRequests;
    }

    public void setFileRequests(Long fileRequests) {
        this.fileRequests = fileRequests;
    }

    Long getFileSize() {
        return fileSize;
    }

    void setFileSize(Long fileSize) {
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
