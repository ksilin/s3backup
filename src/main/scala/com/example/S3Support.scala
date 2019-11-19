package com.example

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{Bucket, PutObjectResult}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.scalalogging.StrictLogging

object S3Support extends StrictLogging {

  val createClient: MinioAccessConfig => AmazonS3 = (minioConfig) => {
    val credentials = new BasicAWSCredentials(minioConfig.accessKey, minioConfig.secretKey)
    val clientConfiguration = new ClientConfiguration
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")

    AmazonS3ClientBuilder.standard
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(minioConfig.url, Regions.US_EAST_1.name))
      .withPathStyleAccessEnabled(true)
      .withClientConfiguration(clientConfiguration)
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .build
  }

  val createBucketIfNotExists: (AmazonS3, String) => Unit = (s3Client, bucket) => {
    val bucketExist = s3Client.doesBucketExist(bucket)
    if (!bucketExist) {
      logger.debug("bucket does not exist")
      val created: Bucket = s3Client.createBucket(bucket)
      logger.info("bucket created: ${created.toString}")
    } else {
      logger.info(s"bucket $bucket already exists")
    }
  }

  val deleteObjectIfExists: (AmazonS3, String, String) => Unit = (s3Client, bucket, fileName) => {
    val objectExists = s3Client.doesObjectExist(bucket, fileName)
    if (objectExists) {
      val deleted = s3Client.deleteObject(bucket, fileName)
      logger.info(s"deleted $deleted")
    } else logger.info(s"object $fileName does not exist")
  }

  val printPutObjectResult: PutObjectResult => Unit = res => {
    logger.info("object created: ")
    logger.info(res.getMetadata.toString)
    logger.info(res.getContentMd5)
    logger.info(res.getETag)
    logger.info(res.getExpirationTime.toString)
    logger.info(res.getVersionId)
  }

}
