package com.sdcc_project.monitor;

import com.sdcc_project.config.Config;
import com.sdcc_project.util.SystemProperties;
import com.sdcc_project.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Monitor {

    private static Monitor instance;
    private boolean overCpuUsage = false;
    private boolean overRamUsage = false;
    private boolean underUsage = false;
    private SystemProperties systemProperties;
    private File file = new File("Monitor.txt");

    public static Monitor getInstance(){
        if(instance==null)
            instance = new Monitor();
        return instance;
    }

    public void startThread(){
        monitorThread.start();
    }

    private Monitor(){
        systemProperties = SystemProperties.getInstance();
    }

    private double getUsage(Components component){
        String command ;
        if(component.equals(Components.CPU))
            command = "bash /home/ubuntu/get_cpu_usage.sh 5";
        else command = "bash /home/ubuntu/get_memory_usage.sh";
        try {
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader preader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String cpuUsageStr = preader.readLine();
            return Double.parseDouble(cpuUsageStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private Thread monitorThread = new Thread("MonitorThread"){
        @Override
        public void run() {
            int cpuOverUsageTime = 0;
            int ramOverUsageTime = 0;
            int cpuUnderUsageTime = 0;
            int ramUnderUsageTime = 0;
            while (true){
                double cpuUsage = getUsage(Components.CPU);
                double memoryUsage = getUsage(Components.RAM);
                System.out.println("Uso Locale : CPU "+ cpuUsage + " RAM " + memoryUsage);
                Util.writeOutput("Uso Locale : CPU "+ cpuUsage + " RAM " + memoryUsage,file);
                if(cpuUsage >= systemProperties.getCpuMaxUsage())
                    cpuOverUsageTime++;
                else {
                    cpuOverUsageTime = 0;
                    overCpuUsage = false;
                }
                if(memoryUsage >= systemProperties.getRamMaxUsage())
                    ramOverUsageTime++;
                else {
                    ramOverUsageTime = 0;
                    overRamUsage = false;
                }
                if(cpuUsage <=systemProperties.getCpuMinUsage()){
                    cpuUnderUsageTime++;
                }else cpuUnderUsageTime =0;
                if(memoryUsage <=systemProperties.getRamMinUsage()){
                    ramUnderUsageTime++;
                }else ramUnderUsageTime = 0;
                if(cpuOverUsageTime>=8)
                    overCpuUsage = true;
                if(ramOverUsageTime>=8)
                    overRamUsage = true;
                if(cpuUnderUsageTime>=20 && ramUnderUsageTime >=20)
                    underUsage = true;
                try {
                    sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };


    public boolean isOverCpuUsage() {
        return overCpuUsage;
    }

    public boolean isOverRamUsage() {
        return overRamUsage;
    }



    public boolean isUnderUsage() {
        return underUsage;
    }
}
