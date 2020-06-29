package com.example

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.javaapi.CollectionConverters.asJava
import scala.util.Try


case object AdminHelper extends FutureConverter with StrictLogging {

  // TODO - eliminate waiting by catching the exceptions on retry
  val truncateTopic = (adminClient: AdminClient, topic: String, partitions: Int) => {

    val javaTopicSet = asJava(Set(topic))
    logger.info(s"deleting topic $topic")
    val deleted: Try[Void] = Try { Await.result(toScalaFuture(adminClient.deleteTopics(javaTopicSet).all()), 10.seconds) }
    waitForTopicToBeDeleted(adminClient, topic)
    Thread.sleep(2000)
    logger.info(s"creating topic $topic")
    val created: Try[Void] = Try {
      val newTopic = new NewTopic(topic, partitions, Short.box(1)) // need to box the short here to prevent ctor ambiguity
      val createTopicsResult: CreateTopicsResult = adminClient.createTopics(asJava(Set(newTopic)))
      Await.result(toScalaFuture(createTopicsResult.all()), 10.seconds) }
    waitForTopicToExist(adminClient, topic)
    Thread.sleep(2000)
  }

  val waitForTopicToExist = (adminClient: AdminClient, topic: String) => {
    var topicExists = false
    while (!topicExists) {
      Thread.sleep(100)
      topicExists = doesTopicExist(adminClient, topic)
      if(!topicExists) logger.info(s"topic $topic still does not exist")
    }
  }

  val waitForTopicToBeDeleted = (adminClient: AdminClient, topic: String) => {
    var topicExists = true
    while (topicExists) {
      Thread.sleep(100)
      topicExists = doesTopicExist(adminClient, topic)
      if(topicExists) logger.info(s"topic $topic still exists")
    }
  }

  val doesTopicExist = (adminClient: AdminClient, topic: String) => {
    val names = Await.result(toScalaFuture(adminClient.listTopics().names()), 10.seconds)
    names.contains(topic)
  }
}
