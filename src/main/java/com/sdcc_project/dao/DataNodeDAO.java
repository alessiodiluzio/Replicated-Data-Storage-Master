package com.sdcc_project.dao;

import com.sdcc_project.entity.DataNodeStatistic;
import com.sdcc_project.exception.DataNodeException;
import com.sdcc_project.exception.FileNotFoundException;
import com.sdcc_project.util.FileManager;
import com.sdcc_project.util.Util;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;


public class DataNodeDAO {

    private static DataNodeDAO instance;
    private static DataNodeStatistic dataNodeStatistic;
    private String dbName;
    private Connection connection = null;
    private File file;


    public DataNodeStatistic getDataNodeStatistic() {
         return dataNodeStatistic;
    }

    private DataNodeDAO(Integer registryPort) throws DataNodeException {
        this.dbName = Integer.toString(registryPort);
        file = new File(dbName+"dao.txt");
        dataNodeStatistic = new DataNodeStatistic(registryPort);
        loadDB();
    }

    public static DataNodeDAO getInstance(Integer registryPort) throws DataNodeException {

        if(instance == null){
            instance = new DataNodeDAO(registryPort);
        }
        return instance;
    }

    public ArrayList<ArrayList<String>> getAllFilesInformation(String dataNodeAddress)  throws DataNodeException {

        String query = "SELECT filename, version FROM DataNodeTable";
        ArrayList<ArrayList<String>> result = new ArrayList<>();

        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            ResultSet set = preparedStatement.executeQuery();
            while (set.next()){
                ArrayList<String> row = new ArrayList<>();
                row.add(set.getString(1)); // Filename
                row.add(dataNodeAddress); // Porta
                row.add(set.getString(2)); // Versione

                result.add(row);
            }
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new DataNodeException("SQL ERROR");
        }

        return result;
    }


    public void deleteFile(String fileName) throws DataNodeException{

        String deleteQuery = "DELETE FROM DataNodeTable WHERE filename = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)){
            preparedStatement.setString(1,fileName);
            preparedStatement.execute();
            dataNodeStatistic.remove(fileName);
        }catch (SQLException e){
            Util.writeOutput(e.getMessage(),file);
            e.printStackTrace();
            throw new DataNodeException("Impossible to delete "+fileName);
        }
    }

    /**
     * Estrae il testo relativo di un file
     *
     * @param fileName nome del file da leggere
     * @param increment true se occorre incrementare le statistiche per le richieste al file(Richieste esterne) o meno(
     *                  richieste interne)
     * @return il testo del file
     */
    public String getFileString(String fileName,boolean increment) throws FileNotFoundException, DataNodeException {

        String query = "SELECT data FROM DataNodeTable WHERE filename = ?";
        if(increment) {
            dataNodeStatistic.incrementSingleFileRequest(fileName);
        }
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1,fileName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                Blob blobData = resultSet.getBlob(1);
                String result = new String(blobData.getBytes(1L, (int) blobData.length()));
                resultSet.close();
                return result;
            }
            throw new FileNotFoundException("Impossible to find "+fileName);
        }
        catch (SQLException e){
            e.printStackTrace();
            Util.writeOutput(e.getMessage(),file);
            throw new DataNodeException("Internal to find "+fileName);
        }
    }

    /**
     * Aggiorna il valore del testo di un file.
     *
     * @param fileName nome del file da aggiornare
     * @param updatedBase64 testo aggiornato
     */
    public synchronized void setFileString(String fileName, String updatedBase64, int version) throws DataNodeException {

        dataNodeStatistic.incrementSingleFileSize(fileName, FileManager.getStringMemorySize(updatedBase64));
        dataNodeStatistic.incrementSingleFileRequest(fileName);
        try {
            getFileString(fileName, false);
            String updateQuery = "UPDATE DataNodeTable SET data = ?, version = ? WHERE filename = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
                byte[] byteData = updatedBase64.getBytes();//Better to specify encoding
                Blob blobData = connection.createBlob();
                blobData.setBytes(1, byteData);
                preparedStatement.setBlob(1, blobData);
                preparedStatement.setInt(2, version);
                preparedStatement.setString(3, fileName);
                preparedStatement.execute();

            }
            catch (SQLException e) {
                Util.writeOutput(e.getMessage(), file);
                e.printStackTrace();
                throw new DataNodeException("Impossible to Update " + fileName);
            }
        }
        catch (FileNotFoundException e){
            String insertQuery = "INSERT into DataNodeTable values (?,?,?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                byte[] byteData = updatedBase64.getBytes();//Better to specify encoding
                Blob blobData = connection.createBlob();
                blobData.setBytes(1, byteData);
                preparedStatement.setString(1, fileName);
                preparedStatement.setBlob(2, blobData);
                preparedStatement.setInt(3, 1);
                preparedStatement.execute();
            }
            catch (SQLException esql) {
                Util.writeOutput(esql.getMessage(),file);
                esql.printStackTrace();
                throw new DataNodeException("Impossible to Insert " + fileName);
            }
        }
    }

    /**
     * Crea la tabella usata dal DB in memory locale al DataNode
     * La tabella ha due colonne [Filename][Data][Version]
     *
     */
    private  void createTable(){

        try (Statement createStatement = connection.createStatement()) {
            String createIDL = "create TABLE DataNodeTable(filename varchar(50), data blob, version int)";
            createStatement.execute(createIDL);
        } catch (SQLException e) {
            e.printStackTrace();
            Util.writeOutput(e.getMessage(),file);
        }

    }

    /**
     * Carica il DB all'avvio , se è presente un backup allora avviene il restore altrimenti viene creato ,
     *
     */
    private void loadDB() throws DataNodeException {

        try {
            createDB(true);
        } catch (Exception e) {
            try {
                createDB(false);
            } catch (Exception e1) {
                e1.printStackTrace();
                throw new DataNodeException("Impossible to create/load DB");
            }
        }

    }

    private void createDB(boolean restore) throws Exception {
        String dbUri = "jdbc:derby:memory:" + dbName + "DB" + ";create=true;user=" + "dataNode" + ";password=" + "dataNode";
        if (restore)
            dbUri = "jdbc:derby:memory:" + dbName + "DB" + ";restoreFrom=db/" + dbName + "DB" + " ;user=" + "dataNode" + ";password=" + "dataNode";
        DataSource dataSource = DataSource.getInstance();
        this.connection = dataSource.getConnection(dbUri, Integer.parseInt(dbName));
        if (!restore){
            createTable();
        }else loadStatistic();
    }

    /**
     * All'avvio del data node vengono calcolate le statistiche sul peso dei file già presenti in memoria
     *
     */
    private void loadStatistic(){

        String query = "SELECT * FROM dataNodeTable";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String filename = resultSet.getString(1);
                Blob blobData = resultSet.getBlob(2);
                String fileData = new String(blobData.getBytes(1L, (int) blobData.length()));
                dataNodeStatistic.incrementSingleFileSize(filename,FileManager.getStringMemorySize(fileData));
            }
            resultSet.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @SuppressWarnings("all")
    /**
     * Salva un backup del DB in memory
     *
     */
    public synchronized void saveDB(){
        // Get today's date as a string:
        String backupdirectory = "db/" ;

        try(CallableStatement cs = connection.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)")) {
            cs.setString(1, backupdirectory);
            cs.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Util.writeOutput(e.getMessage(),file);
        }
    }

    public void resetStatistic() {
        dataNodeStatistic.resetRequest();
    }
}
