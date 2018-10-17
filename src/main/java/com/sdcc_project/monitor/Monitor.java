package com.sdcc_project.monitor;

import com.sdcc_project.config.Config;
import com.sdcc_project.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Monitor {

    private static Monitor instance;
    private double cpuUsage;
    private double memoryUsage;
    private boolean running = true;
    private boolean overCpuUsage = false;
    private boolean overRamUsage = false;
    private File file = new File("Monitor.txt");

    public static Monitor getInstance(){
        if(instance==null)
            instance = new Monitor();
        return instance;
    }

    private Monitor(){
        monitorThread.start();
    };

    private double getUsage(Components component){
        String command = "";
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
            while (running){
                cpuUsage = getUsage(Components.CPU);
                memoryUsage = getUsage(Components.RAM);
                System.out.println("Uso Locale : CPU "+cpuUsage + " RAM " + memoryUsage);
                Util.writeOutput("Uso Locale : CPU "+cpuUsage + " RAM " + memoryUsage,file);
                if(cpuUsage>= Config.cpuMaxUsage)
                    cpuOverUsageTime++;
                else {
                    cpuOverUsageTime = 0;
                    overCpuUsage = false;
                }
                if(cpuUsage>= Config.ramMaxUsage)
                    ramOverUsageTime++;
                else {
                    ramOverUsageTime = 0;
                    overRamUsage = false;
                }
                if(cpuOverUsageTime>=5)
                    overCpuUsage = true;
                if(ramOverUsageTime>=5)
                    overRamUsage = true;
                try {
                    sleep(15000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isOverCpuUsage() {
        return overCpuUsage;
    }

    public boolean isOverRamUsage() {
        return overRamUsage;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }
}
