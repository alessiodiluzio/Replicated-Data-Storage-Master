package com.sdcc_project.service_interface;

import com.sdcc_project.exception.DataNodeException;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.exception.ImpossibleToCopyFileOnDataNode;
import com.sdcc_project.exception.ImpossibleToMoveFileException;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

@SuppressWarnings("unused")
public interface StorageInterface extends Remote {

    byte[] read(String file) throws RemoteException, FileNotFoundException, DataNodeException;
    boolean write(String filename, String data, int version, int replication_factory) throws RemoteException, DataNodeException;
    boolean writeMovedFile(String base64, String filename, int version) throws RemoteException;
    void copyFileOnAnotherDataNode(String filename, String replaced_dataNode) throws RemoteException, ImpossibleToCopyFileOnDataNode;
    void moveFile(String fileName, String newServerAddress, int version) throws RemoteException, FileNotFoundException, DataNodeException, ImpossibleToMoveFileException;
    boolean lifeSignal() throws RemoteException;
    void changeMasterAddress(String newMasterAddress) throws RemoteException;
    ArrayList<ArrayList<String>> getDatabaseData() throws RemoteException;
    boolean isEmpty() throws RemoteException;
    void terminate() throws RemoteException;
    String getInstanceID() throws RemoteException;
    boolean delete(String filename) throws RemoteException;
    void shutDown(ArrayList<String> aliveDataNode) throws RemoteException;
}
