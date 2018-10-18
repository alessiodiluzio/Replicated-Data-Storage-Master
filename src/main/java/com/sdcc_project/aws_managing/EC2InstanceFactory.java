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
import org.apache.commons.codec.binary.Base64;
import java.util.*;

public class EC2InstanceFactory {

    private static AWSCredentials AWS_CREDENTIALS;
    private static EC2InstanceFactory istanza;
    private SystemProperties systemProperties;
    private AmazonEC2 amazonEC2Client;

    private EC2InstanceFactory(){

        systemProperties = SystemProperties.getInstance();
        AWS_CREDENTIALS = new BasicAWSCredentials(
                systemProperties.getAws_access_key(),
                systemProperties.getInstance().getAws_secret_key()
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

    private String getAmiID(Regions region,NodeType nodeType){
        AmazonEC2 amazonEC2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .withRegion(region)
                .build();
        DescribeImagesRequest request = new DescribeImagesRequest().withFilters(new LinkedList<Filter>());
        request.getFilters().add(new Filter().withName("owner-id").withValues(systemProperties.getAws_account_id()));
        DescribeImagesResult describeImagesResult= amazonEC2Client.describeImages(request);
        List<Image> images = describeImagesResult.getImages();
        String ami = "";
        String amiName="";
        if(nodeType.equals(NodeType.Master) || nodeType.equals(NodeType.DataNode))
            amiName = systemProperties.getAws_ec2_master_instance_ami_name();
        else if(nodeType.equals(NodeType.CloudLet))
            amiName = systemProperties.getAws_ec2_cloudlet_instance_ami_name();
        for(Image img : images){
            if(img.getName().equals(amiName)){
                ami = img.getImageId();
            }
        }
        return ami;
    }

    public String createEC2Instance(NodeType nodeType, String arguments){

        String userData = getUserDataScript(nodeType,arguments);

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest();
        runInstancesRequest.withImageId(getAmiID(systemProperties.getRegion(),nodeType))
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(systemProperties.getAws_ec2_region_key())
                .withSecurityGroups(systemProperties.getAws_ec2_region_security_group())
                .withUserData(userData);
        RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);

        Instance instance = runInstancesResult.getReservation().getInstances().get(0);

        String instanceId = instance.getInstanceId();
        long waitedTime = 0;
        while(getInstanceStatus(instanceId)!=16){
            waitFor(1000);
            waitedTime += 1000;
        }
        String address = "";
        if(nodeType.equals(NodeType.CloudLet)){
            address = amazonEC2Client.describeInstances(new DescribeInstancesRequest()
                    .withInstanceIds(instanceId))
                    .getReservations()
                    .stream()
                    .map(Reservation::getInstances)
                    .flatMap(List::stream)
                    .findFirst()
                    .map(Instance::getPublicIpAddress)
                    .orElse(null);
        }
        else {
            address = amazonEC2Client.describeInstances(new DescribeInstancesRequest()
                    .withInstanceIds(instanceId))
                    .getReservations()
                    .stream()
                    .map(Reservation::getInstances)
                    .flatMap(List::stream)
                    .findFirst()
                    .map(Instance::getPrivateIpAddress)
                    .orElse(null);
        }
        return address;




    }

    private  void waitFor(long milliseconds){

        Date startDate = new Date();
        long startTime = startDate.getTime();
        long comparisonTime = startDate.getTime();

        while(comparisonTime-startTime<milliseconds){
            Date comparisonDate = new Date();
            comparisonTime = comparisonDate.getTime();
        }
    }

    private  Integer getInstanceStatus(String instanceId) {

        DescribeInstancesRequest describeInstanceRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
        DescribeInstancesResult describeInstanceResult = amazonEC2Client.describeInstances(describeInstanceRequest);
        InstanceState state = describeInstanceResult.getReservations().get(0).getInstances().get(0).getState();

        return state.getCode();
    }

    private  String getUserDataScript(NodeType nodeType,String arguments){
        String command=null;
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
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    private String join(Collection<String> s, String delimiter) {

        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();

        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }

        return builder.toString();
    }

    /**
     * Termina l'instanza di un DataNode.
     *
     * @param dataNode_instanceID ID dell'instanza da terminare.
     * @return Successo/Fallimento
     */
    public boolean terminateDataNodeEC2Instance(String dataNode_instanceID) {

        ArrayList<String> instanceIds = new ArrayList<>();

        instanceIds.add(dataNode_instanceID);
        TerminateInstancesRequest deleteRequest = new TerminateInstancesRequest(instanceIds);

        TerminateInstancesResult deleteResponse = amazonEC2Client.terminateInstances(deleteRequest);

        for(InstanceStateChange item : deleteResponse.getTerminatingInstances()) {

            if(item.getInstanceId().equals(dataNode_instanceID)){
                return true;
            }
        }

        return false;
    }
}
