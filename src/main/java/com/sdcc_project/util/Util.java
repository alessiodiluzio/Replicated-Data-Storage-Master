package com.sdcc_project.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
}
