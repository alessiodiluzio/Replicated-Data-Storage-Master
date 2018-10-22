package com.sdcc_project.aws_managing;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.sdcc_project.util.SystemProperties;


import java.io.File;


public class S3Upload {

    private SystemProperties systemProperties;
    private AmazonS3 amazonS3Client;
    private static S3Upload instance;

    private String bucketName;

    private S3Upload(){
        systemProperties = SystemProperties.getInstance();
        AWSCredentials AWS_CREDENTIALS = new BasicAWSCredentials(
                systemProperties.getAws_access_key(),
                systemProperties.getAws_secret_key()
        );
        amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .build();
        this.bucketName = systemProperties.getAws_s3_bucket_name();
    }

    public static S3Upload getInstance(){
        if(instance==null)
            instance= new S3Upload();
        return instance;
    }

    public  void uploadFile(String fileName) {
        String fileObjKeyName =systemProperties.getAws_s3_cloudlet_address_folder_name()+"/"+fileName ;
        try {

            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("x-amz-meta-title", "Master and DataNode code");
            request.setMetadata(metadata);
            amazonS3Client.putObject(request);
        } catch(SdkClientException e) {
            e.printStackTrace();
        }
    }
}
