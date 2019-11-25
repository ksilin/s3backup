package com.example

import com.typesafe.scalalogging.StrictLogging
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri
import io.circe._
import io.circe.generic.auto._

case class S3SinkConnector(name: String, configMap: Map[String, String], connectUri: Uri, storeUrl: String, maxTasks: Int) extends S3Connector(name, configMap, connectUri, storeUrl, maxTasks) {

}

case object S3SinkConnector {
  def apply(name: String, topic: String, bucket: String, connectUri: Uri, storeUrl: String = "http://minio1:9000", maxTasks: Int = 2): S3SinkConnector = {

    val connectorConfigMap: Map[String, String] = Map(
      "name" -> name,
      "connector.class" -> "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max" -> maxTasks.toString,

      "topics" -> topic,

      //"key.converter" -> "org.apache.kafka.connect.storage.StringConverter",
      //"value.converter" -> "org.apache.kafka.connect.storage.StringConverter",
      "key.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "value.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "format.class" -> "io.confluent.connect.s3.format.json.JsonFormat",
      // JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields
      "value.converter.schemas.enable" -> "false",
      "schemas.enable" -> "false", // isnt it the default already?
      // s3.compression.type -> gzip

      "flush.size" -> "3", // 1 bin file per 3 records

      "schema.generator.class" -> "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
      "schema.compatibility" -> "NONE",

      "s3.bucket.name" -> bucket,
      "s3.region" -> "us-east-1",
      "s3.part.size" -> "5242880",
      "storage.class" -> "io.confluent.connect.s3.storage.S3Storage",
      "store.url" -> storeUrl,

      "partitioner.class" -> "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "partition.duration.ms" -> "3600000",
      "path.format" -> "'date'=YYYY-MM-dd/'hour'=HH",
      "locale" -> "en",
      "timezone" -> "UTC",
      "timestamp.extractor" -> "Record")

    S3SinkConnector(name, connectorConfigMap, connectUri, storeUrl, maxTasks)
  }
}
