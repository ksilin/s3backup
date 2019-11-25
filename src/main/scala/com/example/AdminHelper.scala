package com.example

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.javaapi.CollectionConverters.asJava


case object AdminHelper extends FutureConverter {

  // TODO - eliminate waiting by catching the exceptions on retry
  val truncateTopic = (adminClient: AdminClient, topic: String, partitions: Int) => {

    val javaTopicSet = asJava(Set(topic))

    val deleted = Await.result(toScalaFuture(adminClient.deleteTopics(javaTopicSet).all()), 10.seconds)
    waitForTopicToBeDeleted(adminClient, topic)
    Thread.sleep(2000)
    val created = Await.result(toScalaFuture(adminClient.createTopics(asJava(Set(new NewTopic(topic, partitions, 1)))).all()), 10.seconds)
    waitForTopicToExist(adminClient, topic)
    Thread.sleep(2000)
  }

  val waitForTopicToExist = (adminClient: AdminClient, topic: String) => {
    var topicExists = false
    while (!topicExists) {
      Thread.sleep(100)
      topicExists = doesTopicExist(adminClient, topic)
      println(s"topic $topic still does not exist")
    }
  }

  val waitForTopicToBeDeleted = (adminClient: AdminClient, topic: String) => {
    var topicExists = true
    while (topicExists) {
      Thread.sleep(100)
      topicExists = doesTopicExist(adminClient, topic)
      println(s"topic $topic still exists")
    }
  }

  val doesTopicExist = (adminClient: AdminClient, topic: String) => {
    val names = Await.result(toScalaFuture(adminClient.listTopics().names()), 10.seconds)
    names.contains(topic)
  }
}
