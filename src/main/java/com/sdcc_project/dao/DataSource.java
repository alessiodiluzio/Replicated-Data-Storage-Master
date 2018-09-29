package com.sdcc_project.dao;

import org.apache.derby.drda.NetworkServerControl;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;

public class DataSource {

    private static DataSource instance;


    private DataSource(){

    }

    public static DataSource getInstance(){
        if(instance==null)
            instance = new DataSource();
        return instance;
    }

    /**
     * Crea una connessione a un DataBase
     *
     * @param dbURI URI del db a cui connettersi
     * @return la connessione al db.
     * @throws Exception eccezione in caso di problemi alla connessione al database
     */
    public Connection getConnection(String dbURI, int port) throws Exception {

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection connection = DriverManager.getConnection(dbURI);
        NetworkServerControl nsc = new NetworkServerControl(InetAddress.getByName("localhost"), port + 50);
        nsc.start(new PrintWriter(System.out, true));
        return connection;
    }
}
