package com

import java.io.InputStream

package object example {

  case class SimpleMessage(key: String, payload: Array[Byte])

  case class MinioAccessConfig(url: String, accessKey: String, secretKey: String)

  case class ConnectorConfig(name: String, config: Map[String, String])

}
