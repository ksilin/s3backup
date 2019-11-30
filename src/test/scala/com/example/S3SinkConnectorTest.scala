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
  val testTopicName = "s3TestTopicAvro"
  val connectorName = "s3SinkConnector"
  val bucketName = "connectortestbucket"

  val connector = S3SinkConnector(name = connectorName, topic = testTopicName, bucket = bucketName, connectUri)

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
    // AdminHelper.truncateTopic(adminClient, "__consumer_offsets", 50) <- TODO - hangs

    // create topic if not exists
    val getTopicNames: Future[util.Set[String]] = toScalaFuture(adminClient.listTopics().names())
    val createTopicIfNotExists = getTopicNames flatMap { topicNames: util.Set[String] =>
      val newTopics: Set[NewTopic] = (Set(testTopicName) -- asScala(topicNames)) map {
        new NewTopic(_, 1, 1)
      }
      val createTopicResults: mutable.Map[String, KafkaFuture[Void]] = asScala(adminClient.createTopics(asJava(newTopics)).values())
      Future.sequence(createTopicResults.values.map(f => toScalaFuture(f)))
    }
    Await.result(createTopicIfNotExists, 10.seconds)

    val createdRecords: List[(ProducerRecord[String, GenericRecord], RecordMetadata)] = testRecords.produceRecords(100)

    createBucketIfNotExists(s3Client, bucketName)
    deleteAllObjectsInBucket(s3Client, bucketName)
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
