package com.example

import com.amazonaws.services.s3.model.{ DeleteObjectsRequest, ListObjectsV2Result, ObjectListing, S3ObjectSummary}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, MustMatchers}
import java.util
import java.util.Properties

import com.amazonaws.services.s3.AmazonS3
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DeleteRecordsResult, NewTopic, RecordsToDelete}
import sttp.model.Uri

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.CollectionConverters.{asJava, asScala}

class BackupRestoreTest extends FreeSpec
  with MustMatchers
  with LazyLogging
  with FutureConverter
  with ScalaFutures with BeforeAndAfterAll with Timed{

  import S3Support._
  import sttp.client._
  import sttp.client.circe._
  import monix.execution.Scheduler.Implicits.global
  import monix.eval._
  import monix.reactive._

  val minioConfig = MinioAccessConfig(url = "http://localhost:9001", accessKey = "AKIAIOSFODNN7EXAMPLE", secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  val bucketName = "connectortestbucket"

  val s3Client = createClient(minioConfig)
  createBucketIfNotExists(s3Client, bucketName)

  val bootstrapServers: String = "localhost:9091"
  val topicName = "s3TestTopic"

  val stringSerdes = Serdes.String()
  val producer = makeProducer
  val consumer1 = makeConsumer
  val consumer2 = makeConsumer
  val pollDuration = java.time.Duration.ofMillis(100)

  val records: List[ProducerRecord[String, String]] = (1 to 100).toList map { i =>
    val r = new ProducerRecord[String, String](topicName, 0, i.toString, s"$i + ${Random.alphanumeric.take(10).mkString}")
    // r.si
    r
  }

  val adminProps = new Properties()
  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminProps)

  val connectUri: Uri = uri"http://localhost:8083"
  val sinkConnectorName = "s3SinkConnector"
  val sinkConnector = S3SinkConnector(sinkConnectorName, topicName, bucketName, connectUri: Uri, storeUrl = "http://minio1:9000", 2)

  // TODO: does it make sense for the source connector have two workers?
  // TODO - using random suffix since IDK yet, where the offsets are stored
  val sourceConnectorName = s"s3SourceConnector-${Random.alphanumeric.take(10).mkString}"
  val sourceConnector = S3SourceConnector(sourceConnectorName, bucketName, connectUri: Uri, storeUrl = "http://minio1:9000", 1)

  override def beforeAll() {

    // create topic if not exists
    val getTopicNames: Future[util.Set[String]] = toScalaFuture(adminClient.listTopics().names())
    val createTopicIfNotExists = getTopicNames flatMap { topicNames: util.Set[String] =>
      val newTopics: Set[NewTopic] = (Set(topicName) -- asScala(topicNames)) map {
        new NewTopic(_, 1, 1)
      }
      val createTopicResults: mutable.Map[String, KafkaFuture[Void]] = asScala(adminClient.createTopics(asJava(newTopics)).values())
      Future.sequence(createTopicResults.values.map(f => toScalaFuture(f)))
    }
    Await.result(createTopicIfNotExists, 10.seconds)
    AdminHelper.waitForTopicToExist(adminClient, topicName)
    Thread.sleep(1000)
    // clear topic
    AdminHelper.truncateTopic(adminClient, topicName, 1)
    // clear connect offsets
    // TODO - does not seem to influence the last file the Source connector read
    AdminHelper.truncateTopic(adminClient, "_connect-offsets", 25)
    AdminHelper.truncateTopic(adminClient, "_connect-status", 25)
    // AdminHelper.truncateTopic(adminClient, "__consumer_offsets", 50) <- TODO - endless loop

    // delete connectors
    sinkConnector.deleteConnector.runSyncUnsafe()
    sourceConnector.deleteConnector.runSyncUnsafe()

    createBucketIfNotExists(s3Client, bucketName)
    deleteAllObjectsInBucket(s3Client, bucketName)
  }

  override def afterAll() {
    sinkConnector.deleteConnector.runSyncUnsafe()
    sourceConnector.deleteConnector.runSyncUnsafe()
  }

  "backup and restore test" in {

    // verify bucket empty
    val listObjects: ListObjectsV2Result = s3Client.listObjectsV2(bucketName)
    val objectSummaries: List[S3ObjectSummary] = asScala(listObjects.getObjectSummaries()).toList
    objectSummaries mustBe Symbol("empty")

    // produce messages to topic
    val x: Future[List[RecordMetadata]] = Future.traverse(records.toList) {
      r: ProducerRecord[String, String] =>
        toScalaFuture(producer.send(r, loggingProducerCallback))
    }
    val metadata: List[RecordMetadata] = Await.result(x, 10.seconds)

    // verify messages are in topic
    println("consuming original messages")
    consumer1.assign(List(new TopicPartition(topicName, 0)).asJava)

    var consumed1: ConsumerRecords[String, String] = null
    var initialPollAttempts1 = 0
    val pollDuration = java.time.Duration.ofMillis(100)
    // subscription is not immediate
    while (consumed1 == null || consumed1.isEmpty) {
      consumed1 = consumer1.poll(pollDuration)
      initialPollAttempts1 = initialPollAttempts1 + 1
    }
    println(s"required ${initialPollAttempts1} polls to get first data")

    val consumedRecords1: List[ConsumerRecord[String, String]] = consumed1.records(topicName).asScala.toList
    println("records original: ")
    consumedRecords1 foreach println

    println("creating sink connector")
    val connectorCreated: Response[Either[String, String]] = sinkConnector.createConnector.runSyncUnsafe()
    connectorCreated mustBe Symbol("success")

    // wait for connector to write messages
    Thread.sleep(5000)

    // verify files are in sink
    val objects: ObjectListing = s3Client.listObjects(bucketName)
    // println("found objects")
    val keysAfter = asScala(objects.getObjectSummaries).map(_.getKey)
    keysAfter.isEmpty mustBe false
    // keysAfter foreach println

    println("deleting sink connector")
    sinkConnector.deleteConnector.runSyncUnsafe() mustBe Symbol("success")
    // wait for connector to be deleted
    Thread.sleep(5000)
    println("recreating topic")
    AdminHelper.truncateTopic(adminClient, topicName, 1)

    // create source
    println("creating source connector")
    sourceConnector.createConnector.runSyncUnsafe() mustBe Symbol("success")
    Thread.sleep(5000)

    // verify messages are in topic
    println("consuming restored messages")
    consumer2.assign(List(new TopicPartition(topicName, 0)).asJava)

    var consumed: ConsumerRecords[String, String] = null
    var initialPollAttempts = 0
    var maxPollAttempts = 100

    // subscription is not immediate
    while (consumed == null || consumed.isEmpty || initialPollAttempts < maxPollAttempts) {
      consumed = consumer2.poll(pollDuration)
      initialPollAttempts = initialPollAttempts + 1
    }
    println(s"required ${initialPollAttempts} polls to get first data")

    val consumedRecords: List[ConsumerRecord[String, String]] = consumed.records(topicName).asScala.toList
    println("records recreated: ")
    consumedRecords foreach println

    // maintenance: delete source
    sourceConnector.deleteConnector.runSyncUnsafe() mustBe Symbol("success")
  }

  private def makeProducer = {
    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    new KafkaProducer[String, String](producerJavaProps,
      stringSerdes.serializer(),
      stringSerdes.serializer())
  }

  private def makeConsumer = {
    val consumerGroup = s"claimCheckGroup-${Random.alphanumeric.take(10).mkString}"
    val consumerJavaProps = new java.util.Properties
    consumerJavaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerJavaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerJavaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    //consumerJavaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")       // record-by-record consuming
    consumerJavaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    consumerJavaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
    new KafkaConsumer[String, String](consumerJavaProps, stringSerdes.deserializer(),
      stringSerdes.deserializer())
  }

  val loggingProducerCallback = new Callback {
    override def onCompletion(meta: RecordMetadata, e: Exception): Unit =
      if (e != null)
//        logger.info(
//          s"published to kafka: ${meta.topic()} : ${meta.partition()} : ${meta.offset()} : ${meta.timestamp()} "
//        )
      logger.error(s"failed to publish to kafka: $e")
  }

}
