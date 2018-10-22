package com.sdcc_project.util;

import java.io.FileOutputStream;
import java.util.Base64;

public class FileManager {

    /**
     * Converte una stringa in base64 nel relativo file rappresentato.
     *
     * @param base64 stringa in base 64 da decodificare
     * @param fileName nome del file decofidicato
     */
    public static void convertStringToFile(String base64,String fileName) {
        try {

            byte[] imageByteArray = decodeFile(base64);
            FileOutputStream fileOutFile = new FileOutputStream(fileName);
            fileOutFile.write(imageByteArray);
            fileOutFile.flush();
            fileOutFile.close();
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    /**
     * Codifica una stringa in base 64
     * @param toBeEncodedString stringa da codificare
     * @return stringa codificata
     */
    public static String encodeString(String toBeEncodedString){

        byte[] data = toBeEncodedString.getBytes();
        return Base64.getEncoder().encodeToString(data);
    }

    /**
     * Decodifica una stringa in base 64
     *
     * @param base64String stringa codificata
     * @return stringa decodificata
     */
    public static String decodeString(String base64String){
        byte[] decocedByte = Base64.getDecoder().decode(base64String);
        return new String(decocedByte);

    }


    /**
     * Decodifica una stringa in base64 in un array di byte
     *
     * @param fileDataString stringa da decodificare
     * @return array di byte della stringa decodificata.
     */
    private static byte[] decodeFile(String fileDataString) {

        return Base64.getDecoder().decode(fileDataString.getBytes());
    }

    /**
     * Calcola il peso in byte di una stringa
     * @param string stringa di cui calcolare il peso
     * @return peso calcolato
     */
    public static long getStringMemorySize(String string){
        return Integer.toUnsignedLong(8 * ((((string.length()) * 2) + 45) / 8)); // (1024 * 1024);

    }


}