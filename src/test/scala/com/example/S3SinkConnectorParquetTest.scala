package com.example

import java.nio.ByteBuffer
import java.util.{Collections, Properties}

import com.example.S3Support.{createBucketIfNotExists, createClient, deleteAllObjectsInBucket}
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DeleteConsumerGroupsResult}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import sttp.client.asynchttpclient.WebSocketHandler
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri

import scala.jdk.javaapi.CollectionConverters.asScala
import scala.concurrent.Await
import scala.concurrent.duration._

class S3SinkConnectorParquetTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed with ImplicitCirceSerde with GenericRecordAvro {

  import monix.execution.Scheduler.Implicits.global
  import sttp.client._

  implicit val sttpBackend: SttpBackend[Task, Observable[ByteBuffer], WebSocketHandler] = AsyncHttpClientMonixBackend().runSyncUnsafe()

  private val connectUri: Uri = uri"http://localhost:8083"
  private val bootstrapServers = "localhost:9091"
  private val schemaRegistryUri = "http://localhost:8081"

  private val testTopicName = "s3AvroTopic"

  private val bucketName = "parquetbucket"

  val connectorName = "parquetSink"
  val consumerGroupName = s"connect-${connectorName}"

  private val adminProps = new Properties()
  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  private val adminClient: AdminClient = AdminClient.create(adminProps)

  private val minioConfig = MinioAccessConfig( url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  private val s3Client = createClient(minioConfig)

  private val testRecords = TestRecords( bootstrapServers, schemaRegistryUri)

  override def beforeAll() {

    AdminHelper.truncateTopic(adminClient, testTopicName, 1)

    val listCGroupsResult = toScalaFuture(adminClient.listConsumerGroups().all())
    val cGroups = Await.result(listCGroupsResult, 10.seconds)
     asScala(cGroups).find(_.groupId() == consumerGroupName).foreach { _ =>
       // reset offsets of the connector consumer group. Normally called `connect-<connectorName>`
       val deleteConsumerGroupsResult: DeleteConsumerGroupsResult = adminClient.deleteConsumerGroups(Collections.singletonList(consumerGroupName))
       try {
         Await.result(toScalaFuture(deleteConsumerGroupsResult.all), 10.seconds)
       } catch  {
         case e: GroupIdNotFoundException => println(s" consumer group $consumerGroupName not found: ${e.getMessage}")
         case e: Throwable => println(s" caught $e")
       }
     }

    // registering an Avro schema beforehand is required - otherwise
    // org.apache.kafka.common.errors.SerializationException: Error retrieving Avro schema: {"type":"record","name":"UserMessage","namespace":"com.example","fields":[{"name":"userId","type":"int"},{"name":"username","type":"string"},{"name":"data","type":"string"},{"name":"createdAt","type":{"type":"long","logicalType":"timestamp-nanos"}}]}
    // Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject 's3AvroTopic-value' not found.; error code: 40401
    // at io.confluent.kafka.schemaregistry.client.rest.RestService.sendHttpRequest(RestService.java:230)
    testRecords.srClient.register(testTopicName + "-value", testRecords.userMessageSchema)
    val records = testRecords.makeUserMessageAvroRecords(testTopicName, 100)
    val createdRecords: List[(ProducerRecord[String, GenericRecord], RecordMetadata)] = testRecords.produceAvroRecords(records)

    createBucketIfNotExists(s3Client, bucketName)
    deleteAllObjectsInBucket(s3Client, bucketName)
  }

  "parquet generation" - {

    val defaultParquetConfig = Map(
      "flush.size" -> "100",
      "rotate.schedule.interval.ms" -> "20000",
      "auto.register.schemas" -> "false", // TODO - try this as well
      "tasks.max" -> "1",
      "s3.part.size" -> "5242880",
      "parquet.codec" -> "snappy",
      "format.class" -> "io.confluent.connect.s3.format.parquet.ParquetFormat",
      "value.converter" -> "io.confluent.connect.avro.AvroConverter",
      "key.converter" -> "org.apache.kafka.connect.storage.StringConverter",
    )

    val nameBasedPartitioning = Map(
      "partitioner.class" -> "io.confluent.connect.storage.partitioner.FieldPartitioner",
      "partition.field.name" -> "userName"
    )

    val connector = S3SinkConnector(name = connectorName, topics = testTopicName, bucket = bucketName, connectUri, configOverride = defaultParquetConfig ++ nameBasedPartitioning)

    "create sink connector for writing parquet files" in {
      val deleted = connector.deleteConnector.runSyncUnsafe()
      val create = connector.createConnector
      val res: Response[Either[String, String]] = create.runSyncUnsafe()
      println("connector creation:")
      println(res.body fold(e => s"failed: $e", r => s"success: $r"))
      println(res)
      val getConnectorInfo = connector.getConnectorInfo
      val connectorInfo = getConnectorInfo.runSyncUnsafe()
      println("connector info:")
      println(connectorInfo.body fold(e => s"failed: $e", r => s"success: $r")) // often fails fith a 409
      // read connector offsets and status

    }

  }

}
