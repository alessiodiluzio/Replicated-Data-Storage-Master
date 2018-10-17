package com.sdcc_project.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Util {

    public static void writeOutput(String message, File file){

        try(BufferedWriter bw = new BufferedWriter(new FileWriter(file,true))) {
            message+="\n";
            bw.append(message);
            bw.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getLocalIPAddress(){
        InetAddress ip;
        try {

            ip = InetAddress.getLocalHost();
            return ip.getHostAddress();

        } catch (UnknownHostException e) {

            e.printStackTrace();

        }
        return null;
    }

    public static ArrayList<String> extractMinFromMap(HashMap<String,Double> map){
        double min = 100000000;
        String minString ="";
        for(Map.Entry<String,Double> entry : map.entrySet()){
            if(entry.getValue()>0 && entry.getValue()<min){
                min = entry.getValue();
                minString = entry.getKey();
            }
        }
        ArrayList<String> result = new ArrayList<>();
        result.add(minString);
        result.add(Double.toString(min));
        return result;

    }
}
