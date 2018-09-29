package com.sdcc_project.dao;

import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.exception.MasterException;

import java.sql.*;
import java.util.ArrayList;

public class MasterDAO {

    private static MasterDAO instance;
    private Connection connection = null;

    private MasterDAO(int port) throws MasterException {

        loadDB(port);
    }

    public static MasterDAO getInstance(int port) throws MasterException {

        if (instance == null) {
            instance = new MasterDAO(port);
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

    public void deleteFilePosition(String filename,String address) throws MasterException {

        String deleteQuery = "DELETE FROM MasterTable WHERE filename=? AND dataNodeAddress=?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)){
            preparedStatement.setString(1,filename);
            preparedStatement.setString(2,address);
            preparedStatement.execute();
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to delete "+filename);
        }
    }

    public ArrayList<String> getServerFiles(String port) throws FileNotFoundException, MasterException {

        String query = "SELECT filename FROM MasterTable WHERE dataNodeAddress = ?";
        ArrayList<String> result = new ArrayList<>();
        try(PreparedStatement preparedStatement=connection.prepareStatement(query)){
            preparedStatement.setString(1,port);
            ResultSet set = preparedStatement.executeQuery();
            while (set.next()){
                result.add(set.getString(1));
            }
            if(result.isEmpty())
                throw new FileNotFoundException("No file in server "+port);
            return result;
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("SQL ERROR "+port);
        }

    }

    public int getFileVersion(String filename,String port) throws MasterException {

        String query = "SELECT version FROM MasterTable WHERE filename=? AND dataNodeAddress=?";
        try(PreparedStatement preparedStatement=connection.prepareStatement(query)){
            preparedStatement.setString(1,filename);
            preparedStatement.setString(2,port);
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
     * Restituisce un'array con gli indirizzi delle repliche e le relative versioni :
     *      [Address1][VersionOfAddress1][Address2][VersionOfAddress2].....
     *
     * @param filename Nome del file di cui recuperare le informazioni di posizione.
     * @return Ritorna una posizione del file.
     */
    public ArrayList<String> getFilePosition(String filename) throws MasterException {

        String query = "SELECT * FROM MasterTable WHERE filename=? ORDER BY version DESC";
        ArrayList<String> resultArray = new ArrayList<>();
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1,filename);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                resultArray.add(resultSet.getString(2));
            }
            resultSet.close();
        }catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to retrieve file positions "+filename);
        }
        return resultArray;
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
     * @param oldPort           Vecchia porta che deve essere cambiata.
     * @param newPort           Nuova porta che deve essere inserita.
     * @param newVersion        Nuova versione da inserire.
     */
    public void insertOrUpdateSingleFilePositionAndVersion(String filename, String oldPort, String newPort, int newVersion) throws MasterException {
        deleteFilePosition(filename,oldPort);
        insertFilePosition(filename, newPort, newVersion);

    }

    public void updateSingleFilePosition(String filename, String oldPort, String newPort) throws MasterException {
        String updateQuery = "UPDATE MasterTable SET dataNodeAddress = ? WHERE filename=? AND dataNodeAddress=?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
            preparedStatement.setString(1,newPort);
            preparedStatement.setString(2,filename);
            preparedStatement.setString(3,oldPort);
            if(preparedStatement.executeUpdate()!=1)
                throw new MasterException("Impossible to update file position "+filename);
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new MasterException("Impossible to update file position SQL Error"+filename);
        }
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
    private void loadDB(int port) throws MasterException {
        try{
            createDB(true, port);
        }
        catch (Exception e) {
            try {
                createDB(false, port);
            }
            catch (Exception e1) {
                e1.printStackTrace();
                throw new MasterException("Impossible to load/create DB");
            }
        }
    }

    private void createDB(boolean restore, int port) throws Exception {

        String dbUri = "jdbc:derby:memory:"+ port +"DB"+";create=true;user=" + "master"+";password="+"master";

        if(restore) {
            dbUri = "jdbc:derby:memory:" + port + "DB" + ";restoreFrom=db/" + port + "DB" + ";user="
                    + "master" + ";password=" + "master";
        }
        DataSource dataSource = DataSource.getInstance();
        this.connection = dataSource.getConnection(dbUri, port);
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
