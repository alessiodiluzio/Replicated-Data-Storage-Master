package com.sdcc_project.util;

import com.amazonaws.regions.Regions;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SystemProperties {
    private static SystemProperties instance;

    private String aws_access_key;
    private String aws_secret_key;
    private String aws_deploy_region;
    private String aws_ec2_region_key;
    private String aws_ec2_region_security_group;
    private String aws_account_id;
    private String aws_ec2_instance_ami;
    private Regions region;

    private SystemProperties(){
        loadProperties();
    };


    public static SystemProperties getInstance(){
        if(instance==null)
            instance=new SystemProperties();
        return instance;
    }

    private void loadProperties(){

        Properties prop = new Properties();
        try(InputStream input = new FileInputStream("config.properties")) {

            // load a properties file
            prop.load(input);
            this.aws_access_key=prop.getProperty("aws_access_key");
            this.aws_secret_key=prop.getProperty("aws_secret_key");
            this.aws_deploy_region=prop.getProperty("aws_deploy_region");
            this.aws_ec2_region_key=prop.getProperty("aws_ec2_region_key");
            this.aws_ec2_region_security_group=prop.getProperty("aws_ec2_region_security_group");
            this.aws_account_id = prop.getProperty("aws_account_id");
            this.aws_ec2_instance_ami = prop.getProperty("aws_ec2_instance_ami");
            this.region = Regions.valueOf(this.aws_deploy_region);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getAws_ec2_instance_ami() {
        return aws_ec2_instance_ami;
    }

    public String getAws_access_key() {
        return aws_access_key;
    }

    public String getAws_secret_key() {
        return aws_secret_key;
    }

    public String getAws_deploy_region() {
        return aws_deploy_region;
    }

    public String getAws_ec2_region_key() {
        return aws_ec2_region_key;
    }

    public String getAws_ec2_region_security_group() {
        return aws_ec2_region_security_group;
    }

    public Regions getRegion() {
        return region;
    }

    public String getAws_account_id() {
        return aws_account_id;
    }
}
