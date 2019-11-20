package com.example

import java.util
import java.util.Properties

import com.example.S3Support.{createBucketIfNotExists, createClient}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.javaapi.CollectionConverters._
import scala.util.Random

class S3SourceConnectorTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed {

  import monix.execution.Scheduler.Implicits.global
  import sttp.client._


  implicit val sttpBackend = AsyncHttpClientMonixBackend().runSyncUnsafe()

  val connectUri: Uri = uri"http://localhost:8083"
  val bootstrapServers = "localhost:9091"
  val testTopicName = "s3TestTopic"
  val connectorName = "s3SourceTestConnector"
  val bucketName = "connectortestbucket"

  val connector = S3SourceConnector(name = connectorName, topic = testTopicName, bucket = bucketName, connectUri)

  val minioConfig = MinioAccessConfig( url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  val s3Client = createClient(minioConfig)

  override def beforeAll() {
    createBucketIfNotExists(s3Client, bucketName)
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

  "create source connector" in {
    val create = connector.createConnector
    val res: Response[Either[String, String]] = create.runSyncUnsafe()
    println(res.body fold(e => s"failed: $e", r => s"success: $r"))
  }

  "change connector config" in {

  }

  "delete connector" in {
    val delete = connector.deleteConnector.runSyncUnsafe()
    delete.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
  }

}
