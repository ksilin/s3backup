package com.example

import java.nio.ByteBuffer
import java.util
import java.util.Properties

import com.example.S3Support.{createBucketIfNotExists, createClient, deleteAllObjectsInBucket}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.KafkaFuture
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import monix.eval.Task
import monix.reactive.Observable
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.avro.generic.GenericRecord
import sttp.client.asynchttpclient.WebSocketHandler

import scala.collection.mutable
import scala.jdk.javaapi.CollectionConverters._
import scala.concurrent.Future
import concurrent.Await
import concurrent.duration._

class S3SinkConnectorTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed with ImplicitCirceSerde with GenericRecordAvro {

  import sttp.client._
  import monix.execution.Scheduler.Implicits.global

  implicit val sttpBackend: SttpBackend[Task, Observable[ByteBuffer], WebSocketHandler] = AsyncHttpClientMonixBackend().runSyncUnsafe()

  val connectUri: Uri = uri"http://localhost:8083"
  val bootstrapServers = "localhost:9091"
  private val schemaRegistryUri = "http://localhost:8081"

  //val testTopicName = "s3TestTopicBytes"
  val testTopicName = "s3TestTopicAvroExt"

  val bucketName = "connectortestbucket"
  val bucketNameAuxTopics = "auxbucket"

  val adminProps = new Properties()
  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminProps)

  val minioConfig = MinioAccessConfig( url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  val s3Client = createClient(minioConfig)

  val testRecords = TestRecords(testTopicName, bootstrapServers, schemaRegistryUri)

  override def beforeAll() {

    //truncate offsets
    AdminHelper.truncateTopic(adminClient, "_connect-offsets", 25)
    AdminHelper.truncateTopic(adminClient, "_connect-configs", 25)
    AdminHelper.truncateTopic(adminClient, "_connect-status", 25)
    AdminHelper.truncateTopic(adminClient, testTopicName, 1)
    // AdminHelper.truncateTopic(adminClient, "__consumer_offsets", 50) <- TODO - endless loop

    val createdRecords: List[(ProducerRecord[String, GenericRecord], RecordMetadata)] = testRecords.produceAvroRecords(100)

    createBucketIfNotExists(s3Client, bucketName)
    deleteAllObjectsInBucket(s3Client, bucketName)
    createBucketIfNotExists(s3Client, bucketNameAuxTopics)
    deleteAllObjectsInBucket(s3Client, bucketNameAuxTopics)
  }


  "get connectors" in {
    val req = basicRequest.get(connectUri.path("connectors"))
    val res: Response[Either[String, String]] = req.send().runSyncUnsafe()
    res.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
  }

  "get connector plugins info" in {
    val req = basicRequest.get(connectUri.path("connector-plugins"))
    val res: Response[Either[String, String]] = req.send().runSyncUnsafe()
    res.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
  }

  "data topic connector" - {

    val connectorName = "s3SinkConnector"

    val smtConfigMap = Map(
      "transforms" -> "addOffset,addPartition,addTimestamp",

      "transforms.addOffset.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addOffset.offset.field"-> "offset",

      "transforms.addPartition.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addPartition.partition.field"-> "partition",

      "transforms.addTimestamp.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addTimestamp.timestamp.field"-> "ts",
    )

    val connector = S3SinkConnector(name = connectorName, topics = testTopicName, bucket = bucketName, connectUri, configOverride = smtConfigMap)

    "get connector info" in {
      val info = connector.getConnectorInfo.runSyncUnsafe()
      info.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
    }

    "get connector task info" in {
      val info = connector.getTaskInfo.runSyncUnsafe()
      info.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
    }

    "create sink connector" in {
      val create = connector.createConnector
      val res: Response[Either[String, String]] = create.runSyncUnsafe()
      println(res.body fold(e => s"failed: $e", r => s"success: $r"))
    }

    "change connector config" in {

    }

    "delete sink connector" in {
      val delete = connector.deleteConnector.runSyncUnsafe()
      delete.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
    }
  }

  "schemas and offsets" - {

    val consumerOffsetsTopic = "__consumer_offsets"
    val schemaTopic = "_schemas"
    val topics = List(schemaTopic,consumerOffsetsTopic)

    val tombstoneHandlerConfig = Map(
      "transforms" -> "tombstoneHandler",
      // ignores the tombstone silently and writes a WARN message to log.
      "transforms.tombstoneHandler.type" -> "io.confluent.connect.transforms.TombstoneHandler",
    )

    val formatOverride = Map(
      "key.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "value.converter" -> "org.apache.kafka.connect.converters.ByteArrayConverter",
      "format.class" -> "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
    )

    val failureHandlingOverride = Map(
      "errors.tolerance"  -> "all",
      "errors.log.enable" -> "true",
      "errors.log.include.messages" -> "true",
    )

    val connectorName = "s3SinkConnectorAux"
    val connector = S3SinkConnector(name = connectorName, topics = topics.mkString(","), bucket = bucketNameAuxTopics, connectUri, configOverride = formatOverride ++ failureHandlingOverride ++ tombstoneHandlerConfig)

    "create sink connector for schemas and consumer offsets" in {
      val create = connector.createConnector
      val res: Response[Either[String, String]] = create.runSyncUnsafe()
      println(res.body fold(e => s"failed: $e", r => s"success: $r"))
    }
  }

}
