package com.sdcc_project.util;

import com.amazonaws.regions.Regions;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Classe che offre la funzionalità di parsing del file di config del sistema
 */
public class SystemProperties {
    private static SystemProperties instance;

    private String aws_access_key;
    private String aws_secret_key;
    private String aws_ec2_region_key;
    private String aws_ec2_region_security_group;
    private String aws_account_id;
    private String aws_ec2_instance_ami_name;
    private String aws_s3_bucket_name;
    private String aws_s3_cloudlet_address_folder_name;
    private Regions region;

    private double cpuMaxUsage ;
    private double ramMaxUsage ;
    private double cpuMinUsage ;
    private double ramMinUsage ;


    private int replication_factory ;
    private int start_number_of_master ;
    private int start_number_of_data_node_for_master ;
    private int start_number_of_cloudlet_for_master ;

    private SystemProperties(){
        loadProperties();
    }


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
            this.aws_ec2_region_key=prop.getProperty("aws_ec2_region_key");
            this.aws_ec2_region_security_group=prop.getProperty("aws_ec2_region_security_group");
            this.aws_account_id = prop.getProperty("aws_account_id");
            this.aws_ec2_instance_ami_name = prop.getProperty("aws_ec2_instance_ami_name");
            this.aws_s3_bucket_name=prop.getProperty("aws_s3_bucket_name");
            this.aws_s3_cloudlet_address_folder_name=prop.getProperty("aws_s3_cloudlet_address_folder_name");
            this.cpuMaxUsage = Double.parseDouble(prop.getProperty("cpuMaxUsage"));
            this.ramMaxUsage = Double.parseDouble(prop.getProperty("ramMaxUsage"));
            this.cpuMinUsage = Double.parseDouble(prop.getProperty("cpuMinUsage"));
            this.ramMinUsage = Double.parseDouble(prop.getProperty("ramMinUsage"));
            this.region = Regions.valueOf(prop.getProperty("aws_deploy_region"));
            this.replication_factory = Integer.parseInt(prop.getProperty("replication_factory"));
            this.start_number_of_master = Integer.parseInt(prop.getProperty("start_number_of_master"));
            this.start_number_of_cloudlet_for_master = Integer.parseInt(prop.getProperty("start_number_of_cloudlet_for_master"));
            this.start_number_of_data_node_for_master = Integer.parseInt(prop.getProperty("start_number_of_data_node_for_master"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getAws_s3_bucket_name() {
        return aws_s3_bucket_name;
    }

    public String getAws_s3_cloudlet_address_folder_name() {
        return aws_s3_cloudlet_address_folder_name;
    }

    public String getAws_ec2_instance_ami_name() {
        return aws_ec2_instance_ami_name;
    }

    public String getAws_access_key() {
        return aws_access_key;
    }

    public String getAws_secret_key() {
        return aws_secret_key;
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

    public double getCpuMaxUsage() {
        return cpuMaxUsage;
    }

    public double getRamMaxUsage() {
        return ramMaxUsage;
    }

    public double getCpuMinUsage() {
        return cpuMinUsage;
    }

    public double getRamMinUsage() {
        return ramMinUsage;
    }

    public int getReplication_factory() {
        return replication_factory;
    }

    public int getStart_number_of_master() {
        return start_number_of_master;
    }

    public int getStart_number_of_data_node_for_master() {
        return start_number_of_data_node_for_master;
    }

    public int getStart_number_of_cloudlet_for_master() {
        return start_number_of_cloudlet_for_master;
    }
}
