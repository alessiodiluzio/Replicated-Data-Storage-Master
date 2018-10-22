package com.sdcc_project.dao;

import java.sql.Connection;
import java.sql.DriverManager;

class DataSource {

    private static DataSource instance;


    private DataSource(){

    }

    static DataSource getInstance(){
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
    Connection getConnection(String dbURI) throws Exception {

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        return DriverManager.getConnection(dbURI);
    }
}
