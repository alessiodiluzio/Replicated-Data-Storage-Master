package com.sdcc_project.aws_managing;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.sdcc_project.config.Config;
import com.sdcc_project.util.NodeType;
import com.sdcc_project.util.SystemProperties;
import com.sdcc_project.util.Util;
import org.apache.commons.codec.binary.Base64;
import java.util.*;

/**
 * Classe che gestisce l'interazione con il servizio di Amazon EC2
 */
public class EC2InstanceFactory {

    private static AWSCredentials AWS_CREDENTIALS;
    private static EC2InstanceFactory istanza;
    private SystemProperties systemProperties;
    private AmazonEC2 amazonEC2Client;


    private EC2InstanceFactory(){

        systemProperties = SystemProperties.getInstance();
        AWS_CREDENTIALS = new BasicAWSCredentials(
                systemProperties.getAws_access_key(),
                systemProperties.getAws_secret_key()
        );
        amazonEC2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .withRegion(systemProperties.getRegion())
                .build();
    }

    public static EC2InstanceFactory getInstance(){

        if(istanza==null)
            istanza= new EC2InstanceFactory();
        return istanza;
    }

    /**
     * Richiede l'id dell'ami usata per il deploy del progetto.
     *
     * @param region regione di deploy del progetto
     * @return l'id dell ami ottenuta
     */
    private String getAmiID(Regions region){
        AmazonEC2 amazonEC2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .withRegion(region)
                .build();
        DescribeImagesRequest request = new DescribeImagesRequest().withFilters(new LinkedList<>());
        request.getFilters().add(new Filter().withName("owner-id").withValues(systemProperties.getAws_account_id()));
        DescribeImagesResult describeImagesResult= amazonEC2Client.describeImages(request);
        List<Image> images = describeImagesResult.getImages();
        String ami = "";
        String amiName= systemProperties.getAws_ec2_instance_ami_name();
        for(Image img : images){
            if(img.getName().equals(amiName)){
                ami = img.getImageId();
            }
        }
        return ami;
    }

    /**
     * Crea un istanza EC2 avviandoci un nodo del sistema.
     *
     * @param nodeType tipo di nodo da avviare (Master,DataNode,Cloudlet)
     * @param arguments argomenti per il running del sistema
     * @return un array contenente l'id dell'istanza creata e il suo indirizzo IP pubblico
     */
    public ArrayList<String> createEC2Instance(NodeType nodeType, String arguments){
        ArrayList<String> result = new ArrayList<>();
        String userData = getUserDataScript(nodeType,arguments);

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest();
        runInstancesRequest.withImageId(getAmiID(systemProperties.getRegion()))
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(systemProperties.getAws_ec2_region_key())
                .withSecurityGroups(systemProperties.getAws_ec2_region_security_group())
                .withUserData(userData);
        RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);

        Instance instance = runInstancesResult.getReservation().getInstances().get(0);

        String instanceId = instance.getInstanceId();
        result.add(instanceId);
        while(getInstanceStatus(instanceId)!=16){
            waitFor();
        }
        String address  = amazonEC2Client.describeInstances(new DescribeInstancesRequest()
                    .withInstanceIds(instanceId))
                    .getReservations()
                    .stream()
                    .map(Reservation::getInstances)
                    .flatMap(List::stream)
                    .findFirst()
                    .map(Instance::getPublicIpAddress)
                    .orElse(null);

        result.add(address);
        return result;
    }

    /**
     *  funzione che blocca il flusso di esecuzione per 1 secondo
     */
    private  void waitFor(){


        long startTime = Util.getTimeInMillies();
        long comparisonTime = Util.getTimeInMillies();

        while(comparisonTime-startTime< (long) 1000){
            comparisonTime = Util.getTimeInMillies();
        }
    }

    /**
     * Richiede ad amazon lo stato di esecuzione di un istanza di ec2
     *
     * @param instanceId id dell'istanza da monitorare
     * @return il codice che indica lo stato di esecuzione dell'istanza
     */
    private  Integer getInstanceStatus(String instanceId) {
        try {
            DescribeInstancesRequest describeInstanceRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult describeInstanceResult = amazonEC2Client.describeInstances(describeInstanceRequest);
            InstanceState state = describeInstanceResult.getReservations().get(0).getInstances().get(0).getState();
            return state.getCode();
        }catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }


    /**
     * Crea uno script bash per l'esecuzione di istruzioni iniziali all'avvio di un istanza di ec2 (questo script
     * è passato all'istanza come user data)
     *
     * @param nodeType nodo da avviare (Master,DataNode,Cloudlet)
     * @param arguments parametri di avvio del sistema
     * @return lo script bash creato
     */
    private  String getUserDataScript(NodeType nodeType,String arguments){
        String command;
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        if(nodeType.equals(NodeType.DataNode)){
            command = Config.launchDataNode;
            lines.add("java -jar /home/ubuntu/DownloadMasterFromS3-1.0-SNAPSHOT-jar-with-dependencies.jar");
            lines.add("cd /home/ubuntu/ && unzip master.zip && cd Master && mvn compile && "+command+arguments);

        }
        else if(nodeType.equals(NodeType.Master)){
            command = Config.launchMaster;
            lines.add("java -jar /home/ubuntu/DownloadMasterFromS3-1.0-SNAPSHOT-jar-with-dependencies.jar");
            lines.add("cd /home/ubuntu/ && unzip master.zip && cd Master && mvn compile && "+command+arguments);

        }
        else if(nodeType.equals(NodeType.CloudLet)){
            command = Config.launchCloudlet;
            lines.add("java -jar /home/ubuntu/DownloadCloudletFromS3-1.0-SNAPSHOT-jar-with-dependencies.jar");
            lines.add("cd /home/ubuntu/ && unzip cloudlet.zip && cd cloudlet && mvn compile && "+command+arguments);
        }
        return new String(Base64.encodeBase64(join(lines).getBytes()));
    }

    private String join(Collection<String> s) {

        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();

        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append("\n");
        }

        return builder.toString();
    }

    /**
     * Termina un'istanza di EC2
     *
     * @param instanceID ID dell'instanza da terminare.
     * @return Successo/Fallimento
     */
    public boolean terminateEC2Instance(String instanceID) {

        ArrayList<String> instanceIds = new ArrayList<>();

        instanceIds.add(instanceID);
        try {
            TerminateInstancesRequest deleteRequest = new TerminateInstancesRequest(instanceIds);

            TerminateInstancesResult deleteResponse = amazonEC2Client.terminateInstances(deleteRequest);

            for (InstanceStateChange item : deleteResponse.getTerminatingInstances()) {

                if (item.getInstanceId().equals(instanceID)) {
                    return true;
                }
            }
        }catch (Exception e){
            return false;
        }

        return false;
    }
}
