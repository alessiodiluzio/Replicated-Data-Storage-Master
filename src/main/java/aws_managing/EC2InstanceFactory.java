package aws_managing;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EC2InstanceFactory {

    private static final AWSCredentials AWS_CREDENTIALS;
    private static EC2InstanceFactory istanza;

    private EC2InstanceFactory(){};

    public static EC2InstanceFactory getInstance(){
        if(istanza==null)
            istanza= new EC2InstanceFactory();
        return istanza;
    }



    static {
        // Your accesskey and secretkey
        AWS_CREDENTIALS = new BasicAWSCredentials(
                "AKIAIPIOOHYRCAVS3DQQ",
                "NuySvCg3mRxtvU9srn90Q7c/anaaldqEhzG4lwhk"
        );
    }

    private String getAmiID(Regions region){
        AmazonEC2 amazonEC2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .withRegion(region)
                .build();
        DescribeImagesRequest request = new DescribeImagesRequest().withFilters(new LinkedList<Filter>());
        request.getFilters().add(new Filter().withName("owner-id").withValues("554279596710"));
        DescribeImagesResult describeImagesResult= amazonEC2Client.describeImages(request);
        List<Image> images = describeImagesResult.getImages();
        for(Image img : images){
            System.out.println("Image ID: " +img.getImageId());
        }
        return images.get(0).getImageId();
    }

    public String createEC2Instance(Regions region,String keyName,String securityGroupName){       AmazonEC2 amazonEC2Client = AmazonEC2ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
            .withRegion(region)
            .build();

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest();
        runInstancesRequest.withImageId(getAmiID(region))
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(keyName)
                .withSecurityGroups(securityGroupName);
        RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);

        Instance instance = runInstancesResult.getReservation().getInstances().get(0);

        String instanceId = instance.getInstanceId();
        System.out.println("EC2 Instance Id: " +instanceId);
        String publicAddress = null;
        while(publicAddress==null) {
            publicAddress = amazonEC2Client.describeInstances(new DescribeInstancesRequest()
                    .withInstanceIds(instanceId))
                    .getReservations()
                    .stream()
                    .map(Reservation::getInstances)
                    .flatMap(List::stream)
                    .findFirst()
                    .map(Instance::getPublicIpAddress)
                    .orElse(null);
        }
        System.out.println("ip: " + publicAddress);
        return publicAddress;



    }
}
