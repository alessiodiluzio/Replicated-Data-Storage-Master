package com.sdcc_project.service_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CloudLetService extends Remote {

    void publishUpdate(String filename, String data, int version) throws RemoteException;
}
