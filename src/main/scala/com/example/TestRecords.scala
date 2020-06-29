package com.example

import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.typesafe.scalalogging.StrictLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serdes

import scala.concurrent.{Await, Future}
import concurrent.duration._
import scala.util.Random

case class TestRecords( bootstrapServers: String = "localhost:9091", schemaRegistryUri: String = "http://localhost:8081") extends GenericRecordAvro with StrictLogging with FutureConverter {

  import monix.execution.Scheduler.Implicits.global

  val simpleMessageSchema: Schema = AvroSchema[SimpleMessage]
  implicit val simpleMessageFormat: RecordFormat[SimpleMessage] = RecordFormat(simpleMessageSchema)
  val userMessageSchema: Schema = AvroSchema[UserMessage]
  implicit val userMessageFormat: RecordFormat[UserMessage] = RecordFormat(userMessageSchema)
  //val fmt: RecordFormat[SimpleMessage] = implicitly[RecordFormat[SimpleMessage]]

  val srClient         = new CachedSchemaRegistryClient(schemaRegistryUri, 50)
  val avroSerde: GenericAvroSerde = new GenericAvroSerde(srClient)


  val makeSimpleMessageAvroRecords: (String, Int) =>  List[ProducerRecord[String, GenericRecord]] = (topicName, count) => {
    (1 to count).toList map { i =>
      val msg = SimpleMessage(i.toString, s"$i + ${Random.alphanumeric.take(10).mkString}".getBytes(StandardCharsets.UTF_8))
      // val msg = SimpleMessage(i.toString, s"$i + ${Random.alphanumeric.take(10).mkString}".getBytes(StandardCharsets.UTF_8))//.asJson.noSpaces
      new ProducerRecord[String, GenericRecord](topicName, 0, i.toString, toAvro(msg))//s"$i + ${Random.alphanumeric.take(10).mkString}")
    }
  }

  val makeUserMessageAvroRecords: (String, Int) =>  List[ProducerRecord[String, GenericRecord]] = (topicName, count) => {
    (1 to count).toList map { i =>
      // userId: Int, username: String, data: String, createdAt: LocalDateTime
      val msg = UserMessage(userId = i, username = s"$i + ${Random.alphanumeric.take(5).mkString}",  data =  s"$i + ${Random.alphanumeric.take(10).mkString}", createdAt = LocalDateTime.now)
      new ProducerRecord[String, GenericRecord](topicName, 0, i.toString, toAvro(msg))
    }
  }

  val produceSimpleMessageAvroRecords: (String, Int) => List[(ProducerRecord[String, GenericRecord], RecordMetadata)] = (topicName, count) => {
    // is this required? Is the schema used?
    srClient.register(s"$topicName-value", simpleMessageSchema)
    val records = makeSimpleMessageAvroRecords(topicName, count)
    val x: Future[List[(ProducerRecord[String, GenericRecord], RecordMetadata)]] = Future.traverse(records) {
      r =>
        toScalaFuture(producer.send(r, loggingProducerCallback)).map((r -> _))
    }
    Await.result(x, 10.seconds)
  }

  val produceAvroRecords: List[ProducerRecord[String, GenericRecord]] => List[(ProducerRecord[String, GenericRecord], RecordMetadata)] = (records) => {
    // is this required? Is the schema used?
    // srClient.register(s"$topicName-value", simpleMessageSchema)
    val x: Future[List[(ProducerRecord[String, GenericRecord], RecordMetadata)]] = Future.traverse(records) {
      r =>
        toScalaFuture(producer.send(r, loggingProducerCallback)).map((r -> _))
    }
    Await.result(x, 10.seconds)
  }

  val makeStringProducer: String => KafkaProducer[String, String] = (bootstrapServers: String) => {
    val stringSerdes = Serdes.String()
    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    new KafkaProducer[String, String](producerJavaProps,
      stringSerdes.serializer(),
      stringSerdes.serializer())
  }

  val makeAvroProducer: String => KafkaProducer[String, GenericRecord] = (bootstrapServers: String) => {
    val stringSerdes = Serdes.String()

    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    new KafkaProducer[String, GenericRecord](producerJavaProps,
      stringSerdes.serializer(),
      avroSerde.serializer())
  }

  val producer: KafkaProducer[String, GenericRecord] = makeAvroProducer(bootstrapServers)

  val loggingProducerCallback = new Callback {
    override def onCompletion(meta: RecordMetadata, e: Exception): Unit =
      if (e == null)
        logger.info(
          s"published to kafka: ${meta.topic()} : ${meta.partition()} : ${meta.offset()} : ${meta.timestamp()} "
        )
      else logger.error(s"failed to publish to kafka: $e")
  }


}
