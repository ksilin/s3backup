package com.example

import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import sttp.client.asynchttpclient.monix.AsyncHttpClientMonixBackend
import sttp.model.Uri

class S3Connector(name: String, configMap: Map[String, String], connectUri: Uri, storeUrl: String, maxTasks: Int) extends StrictLogging {

  import monix.execution.Scheduler.Implicits.global
  import sttp.client._
  import sttp.client.circe._
  implicit val sttpBackend = AsyncHttpClientMonixBackend().runSyncUnsafe()

  val connectorDef = ConnectorConfig(name, configMap)

  val connectorsUri = connectUri.path("connectors")
  val connectorUri: Uri = connectUri.path("connectors", name)
  val tasksUri = connectUri.path("connectors", name, "tasks")

  def createConnector = {
    basicRequest.body(connectorDef).post(connectorsUri).send()
  }

  def deleteConnector = {
    basicRequest.body(connectorDef).delete(connectorUri).send()
  }

  def getConnectorInfo = {
    basicRequest.get(connectorUri).send()
  }

  def getTaskInfo = {
    basicRequest.get(tasksUri).send()
  }

  def getTaskState = {

  }

}


