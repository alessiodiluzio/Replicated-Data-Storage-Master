package com.sdcc_project.service_interface;

import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.entity.FileLocation;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.exception.ImpossibleToFindDataNodeForReplication;
import com.sdcc_project.exception.MasterException;
import com.sdcc_project.monitor.State;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface MasterInterface extends Remote {

    FileLocation checkFile(String fileName, String operation) throws RemoteException, FileNotFoundException, MasterException;
    void writeAck(String filename, String dataNode_address, int version) throws RemoteException;
    String getDataNodeAddressForReplication(String filename, int version) throws RemoteException, ImpossibleToFindDataNodeForReplication;
    String findReplicaPosition(String filename, int version) throws RemoteException;
    String getDataNodeWithFile(String filename) throws RemoteException;
    void setStatistic(DataNodeStatistic dataNodeStatistic) throws RemoteException;
    void lifeSignal(String port) throws RemoteException;
    ArrayList<String> getDataNodeAddresses() throws RemoteException;
    ArrayList<String> getMasterAddresses() throws RemoteException;
    ArrayList<String> getCloudletAddresses() throws RemoteException;
    void dataNodesToManage_AND_listOfMasters(ArrayList<String> dataNode_addresses, ArrayList<String> master_addresses, ArrayList<String> cloudlet_address) throws RemoteException;
    void updateMasterAddresses(String newMasterAddress, String oldMasterAddress) throws RemoteException;
    String getMinorLatencyCloudlet(String sourceIP) throws RemoteException;
    ArrayList<String> getMinorLatencyLocalCloudlet(String sourceIP) throws RemoteException;
    boolean addCloudlet(String ipAddress) throws RemoteException;
    void cloudletLifeSignal(String address, State state) throws RemoteException;
    FileLocation getMostUpdatedFileLocation(String filename, String operation) throws FileNotFoundException,RemoteException;
    boolean ping() throws RemoteException;
    boolean delete(String filename);
    boolean deleteFromMaster(String filename);
}
