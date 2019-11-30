# Kafka S3 backup and restore example

Storing kafka topic data in a cold storage and restoring from this storage.

This example contains a simple implementation of the concept using [Min.io](https://min.io/), an S3-compatible object store.

## S3 sink connector

storing data to S3

https://www.confluent.io/hub/confluentinc/kafka-connect-s3

## S3 source connector

reading stored data from S3

https://www.confluent.io/hub/confluentinc/kafka-connect-s3-source

## tips

HTTPie is a great tool: 

* list connectors : `http :8083/connectors`

* delete connector: `http DELETE :8083/connectors/s3SinkConnector`

## run tests

`> sbt dockerComposeTest`

## details

### inserting fields with sink

https://docs.confluent.io/current/connect/transforms/insertfield.html#insertfield

### record key

The key is not stored along with the record. If your record does not include the key, youmight want to include id with an SMT in the sink connector and extract it with the source connector.

```
      "transforms" -> "createKey",
      "transforms.createKey.type" -> "org.apache.kafka.connect.transforms.ValueToKey",
      "transforms.createKey.fields"-> "key",
```


### offset

`offset.field`

### timestamp

`timestamp.field`

### removing fields with source

while writing the data back to the brokers, you might want to drop the extra fields we added in the sink. 

## utils

`_connect-offsets`

`kafkacat -b localhost:9091 -t _connect-offsets -K'='   `

`["s3SourceConnector",{"folder":"topics/s3TestTopicAvro/partition=0/"}]={"lastFileRead":"topics/s3TestTopicAvro/partition=0/s3TestTopicAvro+0+0000000096.avro","fileOffset":"3","eof":"true"}
`

You will need to recreate the connect machine to flush the offsets

`docker-compose up -d --build --no-deps --force connect1`

## caveat

The implementation demonstrates a naive approach. In order to be used in production, more aspects need to be covered, e.g.:

* error handling
* authentication
* retention
* metadata

Sometimes, if the test fails, the docker containers will not be stopped. You will need to stop them manually. 

`> docker ps -q | xargs docker stop ` 

## current

## have fun and help improve the project with your pull requests
