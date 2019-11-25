package com.example

import sttp.model.Uri

case class S3SourceConnector(name: String, configMap: Map[String, String], connectUri: Uri, storeUrl: String, maxTasks: Int) extends S3Connector(name, configMap, connectUri, storeUrl, maxTasks) {

}

case object S3SourceConnector {
  def apply(name: String, topic: String, bucket: String, connectUri: Uri, storeUrl: String = "http://minio1:9000", maxTasks: Int = 2): S3SourceConnector = {

    val connectorConfigMap: Map[String, String] = Map(
      "name" -> name,
      "connector.class" -> "io.confluent.connect.s3.source.S3SourceConnector",
      "tasks.max" -> maxTasks.toString,

      "store.url" -> storeUrl,
      "s3.bucket.name" -> bucket,
      "s3.region" -> "us-east-1",

      "key.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "value.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "value.converter.schemas.enable" -> "false",
      "key.converter.schemas.enable" -> "false",
      //"format.class" -> "io.confluent.connect.s3.format.json.JsonFormat",
      "format.class" -> "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
      "schemas.enable" -> "false",

      "partitioner.class" -> "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "path.format" -> "'date'=YYYY-MM-dd/'hour'=HH",

      "confluent.topic.bootstrap.servers" -> "kafka1:19091",
      "confluent.topic.replication.factor" -> "1",
    )

    S3SourceConnector(name, connectorConfigMap, connectUri, storeUrl, maxTasks)
  }
}


