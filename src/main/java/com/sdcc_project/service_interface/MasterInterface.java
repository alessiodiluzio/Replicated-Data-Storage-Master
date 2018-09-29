package com.sdcc_project.service_interface;




import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.entity.FileLocation;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.exception.MasterException;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface MasterInterface extends Remote {

    FileLocation checkFile(String fileName, String operation) throws RemoteException, FileNotFoundException, MasterException;
    void writeAck(String filename, String port, int version, String oldPort) throws RemoteException;
    void setStatistic(DataNodeStatistic dataNodeStatistic) throws RemoteException;
    void lifeSignal(String port) throws RemoteException;
    ArrayList<String> getDataNodePorts() throws  RemoteException;
}
