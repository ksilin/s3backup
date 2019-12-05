package com.example

import com.typesafe.scalalogging.StrictLogging
import io.minio.MinioClient

object MinioSupport extends StrictLogging {

  val createBucketIfNotExists: (MinioClient, String) => Unit = (client, bucket) => {
    val bucketExist = client.bucketExists(bucket)
    if (!bucketExist) {
      logger.info(s"bucket $bucket does not exist")
      client.makeBucket(bucket)
      println(s"bucket $bucket created")
    }
  }

  val deleteObjectIfExists: (MinioClient, String, String) => Unit = (client, bucket, fileName) => {
    client.removeObject(bucket, fileName)
      }

}
