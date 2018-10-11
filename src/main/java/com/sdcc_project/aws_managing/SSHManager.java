package com.sdcc_project.aws_managing;

import com.jcraft.jsch.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

public class SSHManager {

    /*private static SSHManager istanza ;

    private SSHManager(){};

    public static SSHManager getInstance(){
        if(istanza==null)
            istanza=new SSHManager();
        return istanza;
    }


    public void launchService(String user,String host,String keyPath,String command){
        JSch jSch = new JSch();
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("user : "+user+" | hostname : "+host+ " | key path : "+keyPath+" | command :"+command);
            jSch.addIdentity(keyPath);
            jSch.setConfig("StrictHostKeyChecking", "no");
            Session session = jSch.getSession(user,host,22);
            session.connect();
            Channel channel = session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);
            ((ChannelExec) channel).setErrStream(System.err);
            channel.setInputStream(null);
            channel.connect();

            InputStream input = channel.getInputStream();
//start reading the input from the executed commands on the shell
            byte[] tmp = new byte[1024];
            Date ora = new Date();
            long timeInMillisStart = ora.getTime();
            long timeInMillisLoop = ora.getTime();
            while (timeInMillisLoop-timeInMillisStart<30000) {
                Date loopDate = new Date();
                timeInMillisLoop = loopDate.getTime();
                while (input.available()>0) {
                    int i = input.read(tmp, 0, 1024);
                    if (i < 0) break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (channel.isClosed()){
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
            }

            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    };*/
}
