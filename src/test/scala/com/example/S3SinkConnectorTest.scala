package com.example

import java.util
import java.util.Properties

import com.example.S3Support.{createBucketIfNotExists, createClient}
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.KafkaFuture
import io.circe._
import io.circe.generic.auto._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serdes

import scala.collection.mutable
import scala.jdk.javaapi.CollectionConverters._
import scala.concurrent.Future
import concurrent.Await
import concurrent.duration._
import scala.util.Random

class S3SinkConnectorTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed {

  import sttp.client._
  import sttp.client.circe._
  import monix.execution.Scheduler.Implicits.global
  import monix.eval._
  import monix.reactive._


  implicit val sttpBackend = AsyncHttpClientMonixBackend().runSyncUnsafe()

  val connectUri: Uri = uri"http://localhost:8083"
  val bootstrapServers = "localhost:9091"
  val testTopicName = "s3TestTopic"
  val connectorName = "s3SinkTestConnector"
  val bucketName = "connectortestbucket"

  val connector = S3SinkConnector(name = connectorName, topic = testTopicName, bucket = bucketName, connectUri)

  val adminProps = new Properties()

  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminProps)

  val minioConfig = MinioAccessConfig( url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  val s3Client = createClient(minioConfig)

  val records: List[ProducerRecord[String, String]] = (1 to 100).toList map { i =>
    new ProducerRecord[String, String](testTopicName, 0, i.toString, s"$i + ${Random.alphanumeric.take(10).mkString}")
  }

  val stringSerdes = Serdes.String()
  val producer = makeProducer

  override def beforeAll() {

    // create topic if not exists
    val getTopicNames: Future[util.Set[String]] = toScalaFuture(adminClient.listTopics().names())
    val createTopicOfNotExists = getTopicNames flatMap { topicNames: util.Set[String] =>
      val newTopics: Set[NewTopic] = (Set(testTopicName) -- asScala(topicNames)) map {
        new NewTopic(_, 1, 1)
      }
      val createTopicResults: mutable.Map[String, KafkaFuture[Void]] = asScala(adminClient.createTopics(asJava(newTopics)).values())
      Future.sequence(createTopicResults.values.map(f => toScalaFuture(f)))
    }
    Await.result(createTopicOfNotExists, 10.seconds)

    // produce records
    val x: Future[List[RecordMetadata]] = Future.traverse(records.toList) {
      r: ProducerRecord[String, String] =>
        toScalaFuture(producer.send(r, loggingProducerCallback))
    }
    val metadata: List[RecordMetadata] = Await.result(x, 10.seconds)
    createBucketIfNotExists(s3Client, bucketName)
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

  "delete connector" in {
    val delete = connector.deleteConnector.runSyncUnsafe()
    delete.body fold(e => logger.error(s"failed: $e"), r => logger.info(s"success: $r"))
  }



  private def makeProducer = {
    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    new KafkaProducer[String, String](producerJavaProps,
      stringSerdes.serializer(),
      stringSerdes.serializer())
  }

  val loggingProducerCallback = new Callback {
    override def onCompletion(meta: RecordMetadata, e: Exception): Unit =
      if (e == null)
        logger.info(
          s"published to kafka: ${meta.topic()} : ${meta.partition()} : ${meta.offset()} : ${meta.timestamp()} "
        )
      else logger.error(s"failed to publish to kafka: $e")
  }



}
