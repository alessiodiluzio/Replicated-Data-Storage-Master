package com.sdcc_project.dao;

import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.exception.MasterException;

import java.sql.*;
import java.util.ArrayList;

public class MasterDAO {

    private static MasterDAO instance;
    private Connection connection = null;

    private MasterDAO(String dbName) throws MasterException {

        loadDB(dbName);
    }

    public static MasterDAO getInstance(String dbName) throws MasterException {

        if (instance == null) {
            instance = new MasterDAO(dbName);
        }
        return instance;
    }

    public ArrayList<ArrayList<String>> getAllData()  throws MasterException {

        String query = "SELECT * FROM MasterTable";
        ArrayList<ArrayList<String>> result = new ArrayList<>();

        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            ResultSet set = preparedStatement.executeQuery();
            while (set.next()){
                ArrayList<String> row = new ArrayList<>();
                row.add(set.getString(1)); // Filename
                row.add(set.getString(2)); // Porta
                row.add(set.getString(3)); // Versione

                result.add(row);
            }
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("SQL ERROR");
        }

        return result;
    }

    public void deleteFilePosition(String filename, String address) throws MasterException {

        String deleteQuery = "DELETE FROM MasterTable WHERE filename=? AND dataNodeAddress=?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)){
            preparedStatement.setString(1, filename);
            preparedStatement.setString(2, address);
            preparedStatement.execute();
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to delete file "+filename);
        }
    }

    public void deleteFilePosition(String filename) throws MasterException {

        String deleteQuery = "DELETE FROM MasterTable WHERE filename=?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)){
            preparedStatement.setString(1, filename);
            preparedStatement.execute();
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to delete file " + filename);
        }
    }

    /**
     * Cancella tutte le informazioni relative all'indirizzo del DataNode passato.
     *
     * @param address Indirizzo del DataNode di cui si devono cancellare le informazioni.
     * @throws MasterException
     */
    public void deleteAllAddress(String address) throws MasterException {

        String deleteQuery = "DELETE FROM MasterTable WHERE dataNodeAddress = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)){
            preparedStatement.setString(1, address);
            preparedStatement.execute();
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to delete " + address);
        }
    }

    public ArrayList<String> getServerFiles(String address) throws FileNotFoundException, MasterException {

        String query = "SELECT filename FROM MasterTable WHERE dataNodeAddress = ?";
        ArrayList<String> result = new ArrayList<>();

        try(PreparedStatement preparedStatement=connection.prepareStatement(query)){
            preparedStatement.setString(1, address);
            ResultSet set = preparedStatement.executeQuery();
            while (set.next()){
                result.add(set.getString(1));
            }
            if(result.isEmpty())
                throw new FileNotFoundException("No file in server " + address);
            return result;
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("SQL ERROR " + address);
        }

    }

    public int getFileVersion(String filename, String address) throws MasterException {

        String query = "SELECT version FROM MasterTable WHERE filename=? AND dataNodeAddress=?";
        try(PreparedStatement preparedStatement=connection.prepareStatement(query)){
            preparedStatement.setString(1, filename);
            preparedStatement.setString(2, address);
            ResultSet resultSet = preparedStatement.executeQuery();
            int result = -1;
            if(resultSet.next()) result = resultSet.getInt(1);
            resultSet.close();
            return result;
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to retrieve file "+filename);
        }


    }

    /**
     * Funzione che restituisce una posizione del file, memorizzata nel DB.
     *
     * @param filename Nome del file di cui recuperare la posizione.
     * @return Ritorna una posizione del file.
     */
    public String getFilePosition(String filename) throws MasterException {

        String query = "SELECT dataNodeAddress FROM MasterTable WHERE filename=?";
        String result;

        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1,filename);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(!resultSet.next())
                return null;
            result = resultSet.getString(1);
            resultSet.close();
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to Find Position of File: " + filename);
        }

        return result;
    }

    public boolean serverContainsFile(String filename, String port) throws MasterException {

        String query = "SELECT * FROM MasterTable WHERE filename=? AND dataNodeAddress=?";
        boolean result = false;
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1,filename);
            preparedStatement.setString(2,port);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next())
                result = true;
            resultSet.close();
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to retrieve file "+filename);
        }
        return result;
    }

    /**
     * Funzione che restituisce la posizione della replica primaria di un file.
     *
     * @param filename Nome del file di cui si vuole ottenere la posizione della replica primaria.
     * @return Posizione della replica primaria del file.
     */
    public String getPrimaryReplicaFilePosition(String filename) throws FileNotFoundException, MasterException {

        String query ="SELECT dataNodeAddress FROM MasterTable WHERE version = (SELECT  MAX(version) FROM MasterTable WHERE filename=? ) " +
                "AND filename=?" ;
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1,filename);
            preparedStatement.setString(2,filename);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()) {
                String result = resultSet.getString(1);
                resultSet.close();
                return result;
            }
            throw new FileNotFoundException("Impossible to find primary replica -->File not here "+filename);
        }catch (SQLException e){
            e.printStackTrace();
            throw  new MasterException("Internal server Error");
        }

    }

    /**
     * Funzione per salvare le informazioni di posizione dei file nel DB.
     *
     * @param filename Nome del file di cui si deve salvare le informazioni di posizione.
     * @param dataNodeAdress indirizzo del datanode in cui è salvato il file
     * @param version versione del file
     */
    public void insertFilePosition(String filename, String dataNodeAdress, int version) throws MasterException {

        String updateQuery = "INSERT into MasterTable values (?,?,?)";
        try(PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
            preparedStatement.setString(1,filename);
            preparedStatement.setString(2,dataNodeAdress);
            preparedStatement.setInt(3,version);
            if(preparedStatement.executeUpdate()!=1)
                throw new MasterException("Impossible to store file "+filename);
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to store file "+filename);
        }
    }

    /**
     * Funzione per aggiornare una SINGOLA posizione del file nel Database.
     *
     * @param filename          Nome del file di cui si deve aggiornare una posizione.
     * @param new_address           Nuova porta che deve essere inserita.
     * @param newVersion        Nuova versione da inserire.
     */
    public void insertOrUpdateSingleFilePositionAndVersion(String filename, String new_address, int newVersion) throws MasterException {

        deleteFilePosition(filename);
        insertFilePosition(filename, new_address, newVersion);
    }

    private  void createTable() throws MasterException {
        try (Statement createStatement = connection.createStatement()) {
            String createIDL = "create TABLE MasterTable(filename varchar(50),dataNodeAddress varchar(100),version int)";
            createStatement.execute(createIDL);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new MasterException("Impossible to create Table");
        }

    }

    /**
     * Funzione per chudere la connessione con MapDB.
     */
    public void closeDBConnection() {

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Ricarica dal disco il Database con le informazioni di posizione dei file.
     *
     */
    private void loadDB(String dbName) throws MasterException {
        try{
            createDB(true, dbName);
        }
        catch (Exception e) {
            try {
                createDB(false, dbName);
            }
            catch (Exception e1) {
                e1.printStackTrace();
                throw new MasterException("Impossible to load/create DB");
            }
        }
    }

    private void createDB(boolean restore, String dbName) throws Exception {

        String dbUri = "jdbc:derby:memory:" + dbName + ";create=true;user=" + "master" + ";password=" + "master";

        if(restore) {
            dbUri = "jdbc:derby:memory:" + dbName + ";restoreFrom=db/" + dbName + ";user="
                    + "master" + ";password=" + "master";
        }
        DataSource dataSource = DataSource.getInstance();
        this.connection = dataSource.getConnection(dbUri);
        if(!restore){
            createTable();
        }
    }

    /**
     * Funzione per il salvataggio delle informazioni del DB in-memory del Master nel DB persistente su disco.
     */
    @SuppressWarnings("all")
    public void saveDB()  {

        String backupdirectory = "db/" ;

        try(CallableStatement cs = connection.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)")) {
            cs.setString(1, backupdirectory);
            cs.execute();
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

}
