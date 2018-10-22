package com.sdcc_project.service_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CloudletInterface extends Remote {

    Double getLatency(String ipAddress) throws RemoteException;
    void newMasterAddress(String newMasterAddress) throws RemoteException;

    void shutdownSignal() throws RemoteException;
}
