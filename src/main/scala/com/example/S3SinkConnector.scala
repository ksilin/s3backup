package com.example

import com.typesafe.scalalogging.StrictLogging
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri
import io.circe._
import io.circe.generic.auto._

case class S3SinkConnector(name: String, configMap: Map[String, String], connectUri: Uri, storeUrl: String, maxTasks: Int) extends StrictLogging {

  import sttp.client._
  import sttp.client.circe._

  import monix.execution.Scheduler.Implicits.global
  import monix.eval._
  import monix.reactive._
  implicit val sttpBackend = AsyncHttpClientMonixBackend().runSyncUnsafe()

  val connectorDef = ConnectorConfig(name, configMap)

  val connectorsUri = connectUri.path("connectors")
  val connectorUri: Uri = connectUri.path("connectors", name)
  val tasksUri = connectUri.path("connectors", name, "tasks")

  def createConnector = {
    basicRequest.body(connectorDef).post(connectorsUri).send()
  }

  def deleteConnector = {
    basicRequest.body(connectorDef).delete(connectorUri).send()
  }

  def getConnectorInfo = {
    basicRequest.get(connectorUri).send()
  }

  def getTaskInfo = {
    basicRequest.get(tasksUri).send()
  }

  def getTaskState = {

  }

}

case object S3SinkConnector {
  def apply(name: String, topic: String, bucket: String, connectUri: Uri, storeUrl: String = "http://minio1:9000", maxTasks: Int = 2): S3SinkConnector = {

    val connectorConfigMap: Map[String, String] = Map(
      "schema.generator.class" -> "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
      "name" -> name,
      "connector.class" -> "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max" -> maxTasks.toString,
      "key.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "value.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "topics" -> topic,
      "format.class" -> "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
      "flush.size" -> "3", // 1 bin file per 3 records
      "rotate.interval.ms" -> "600", // TODO - file/object rotation in bucket? is there a rotate size?
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
