package com.example

import java.util.Properties

import com.example.S3Support.{createBucketIfNotExists, createClient}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri

class S3SourceConnectorTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed {

  import monix.execution.Scheduler.Implicits.global
  import sttp.client._


  implicit val sttpBackend = AsyncHttpClientMonixBackend().runSyncUnsafe()

  private val connectUri: Uri = uri"http://localhost:8083"
  private val bootstrapServers = "localhost:9091"
  private val testTopicName = "s3TestTopicAvroExt-restore" // adding test suffix from SMT config
  private val connectorName = "s3SourceConnector5"
  private val bucketName = "connectortestbucket"

  private val connector: S3SourceConnector = S3SourceConnector(name = connectorName, bucket = bucketName, connectUri)

  private val minioConfig = MinioAccessConfig( url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  private val s3Client = createClient(minioConfig)

  private val adminProps = new Properties()
  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  private val adminClient: AdminClient = AdminClient.create(adminProps)

  override def beforeAll() {
    AdminHelper.truncateTopic(adminClient, "_connect-offsets", 25)
    AdminHelper.truncateTopic(adminClient, "_connect-status", 25)
    AdminHelper.truncateTopic(adminClient, "_connect-configs", 1)
    AdminHelper.truncateTopic(adminClient, testTopicName, 1)
    //AdminHelper.truncateTopic(adminClient, "__consumer_offsets", 50) // TODO - consumer offsets cannot be recreated?

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

  "delete source connector" in {
    val delete = connector.deleteConnector.runSyncUnsafe()
    delete.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
  }

}
