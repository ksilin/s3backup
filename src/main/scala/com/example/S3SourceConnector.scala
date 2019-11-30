package com.example

import sttp.model.Uri

case class S3SourceConnector(name: String, configMap: Map[String, String], connectUri: Uri, storeUrl: String, maxTasks: Int) extends S3Connector(name, configMap, connectUri, storeUrl, maxTasks) {

}

case object S3SourceConnector {
  def apply(name: String, bucket: String, connectUri: Uri, storeUrl: String = "http://minio1:9000", maxTasks: Int = 2, configOverride: Map[String, String] = Map.empty): S3SourceConnector = {

    val connectorConfigMap: Map[String, String] = Map(
      "name" -> name,
       "topics.dir" -> "topics",
      "connector.class" -> "io.confluent.connect.s3.source.S3SourceConnector",
      "tasks.max" -> maxTasks.toString,
      "record.batch.max.size" -> "1",

      "store.url" -> storeUrl,
      "s3.bucket.name" -> bucket,
      "s3.region" -> "us-east-1",

      // "value.converter" -> "org.apache.kafka.connect.storage.StringConverter",
      "key.converter" -> "org.apache.kafka.connect.storage.StringConverter",
      //"value.converter" -> "org.apache.kafka.connect.json.JsonConverter",
      "value.converter" -> "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url" -> "http://schema-registry:8081",
      // "value.converter.schemas.enable" -> "false",
      // "key.converter.schemas.enable" -> "false",
      // "format.class" -> "io.confluent.connect.s3.format.json.JsonFormat",
      // "schemas.enable" -> "false",
      "format.class" -> "io.confluent.connect.s3.format.avro.AvroFormat",
      "schema.registry.url" -> "http://schema-registry:8081",

      "partitioner.class" -> "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "path.format" -> "'date'=YYYY-MM-dd/'hour'=HH",

      "confluent.topic.bootstrap.servers" -> "kafka1:19091",
      "confluent.topic.replication.factor" -> "1",

      // reroute records
      // "transforms"  -> "routeRecords",
      // "transforms.routeRecords.type" ->  "org.apache.kafka.connect.transforms.RegexRouter",
      // "transforms.routeRecords.regex" -> "(.*)",
      // "transforms.routeRecords.replacement" -> "$1-test"

      "transforms" -> "createKey",
      "transforms.createKey.type" -> "org.apache.kafka.connect.transforms.ValueToKey",
      "transforms.createKey.fields"-> "key",

    ) ++ configOverride

    S3SourceConnector(name, connectorConfigMap, connectUri, storeUrl, maxTasks)
  }
}


