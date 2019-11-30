package com

import java.io.InputStream

package object example {

  case class SimpleMessage(key: String, payload: Array[Byte])
  case class StringMessage(payload: String)
  case class EnrichedStringMessage(payload: String, ts: String, partition: Long, offset: Long)

  case class MinioAccessConfig(url: String, accessKey: String, secretKey: String)

  case class ConnectorConfig(name: String, config: Map[String, String])

}
