package com

import java.time.LocalDateTime

package object example {

  case class SimpleMessage(key: String, payload: Array[Byte])
  case class StringMessage(payload: String)

  case class UserMessage(userId: Int, userName: String, age: Int, data: String, createdAt: LocalDateTime)
  
  case class PurchaseMessage(userId: Int, product: String, price: Int, purchasedAt: LocalDateTime)

  case class EnrichedStringMessage(payload: String, ts: String, partition: Long, offset: Long)

  case class MinioAccessConfig(url: String, accessKey: String, secretKey: String)

  case class ConnectorConfig(name: String, config: Map[String, String])

}
