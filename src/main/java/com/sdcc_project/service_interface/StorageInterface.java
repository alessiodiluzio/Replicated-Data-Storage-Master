package com.sdcc_project.service_interface;

import com.sdcc_project.exception.DataNodeException;
import com.sdcc_project.exception.FileNotFoundException;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface StorageInterface extends Remote {

    String write(String dato, String file, ArrayList<String> dataNodePorts, int version, String oldPort) throws RemoteException, FileNotFoundException, DataNodeException;
    byte[] read(String file) throws RemoteException, FileNotFoundException, DataNodeException;
    String forwardWrite(String data, String filename, ArrayList<String> dataNodePorts, int version, long fileSize, String oldPort) throws RemoteException, FileNotFoundException, DataNodeException;
    void killSignal() throws RemoteException;
    void moveFile(String fileName, String newServerPort, int version, String oldPort) throws RemoteException, FileNotFoundException, DataNodeException;
    boolean lifeSignal() throws RemoteException;
    void changeMasterAddress(int newMasterAddress) throws RemoteException;
    ArrayList<ArrayList<String>> getDatabaseData() throws RemoteException;
    boolean isEmpty() throws RemoteException;
    void terminate() throws RemoteException;
}
