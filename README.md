# Kafka S3 backup and restore example

Storing kafka topic data in a cold storage and restoring from this storage.

This example contains a simple implementation of the concept using [Min.io](https://min.io/), an S3-compatible object store.

## S3 sink connector

storing data to S3

https://www.confluent.io/hub/confluentinc/kafka-connect-s3

## S3 source connector

reading stored data from S3

https://www.confluent.io/hub/confluentinc/kafka-connect-s3-source

## common configuration 

see https://github.com/confluentinc/kafka-connect-object-store-source/blob/master/cloud-storage-source-common/src/main/java/io/confluent/connect/cloud/storage/source/CloudStorageSourceConnectorCommonConfig.java

## tips

HTTPie is a great tool: 

* list connectors : `http :8083/connectors`

* delete connector: `http DELETE :8083/connectors/s3SinkConnector`

## run tests

`> sbt dockerComposeTest`


## topics

What topics do we need to resume processing with minimal data loss except the data topic itself?

consumer offsets -> 
schemas -> 

You do not need to recover the schema topic for restoring the records as the schema is stored along with the records on S3. However you want to restore the schemas for your applications to work. 

## SMTs

### inserting fields with sink

https://docs.confluent.io/current/connect/transforms/insertfield.html#insertfield

`"transforms" -> "addOffset,addPartition,addTimestamp",`

### record key

The key is not stored along with the record. If your record does not include the key, you might want to include id with an SMT in the sink connector and extract it with the source connector.


### offset - `offset.field`

```
      "transforms.addOffset.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addOffset.offset.field"-> "offset",
```

### partition - `partition.field`

```
      "transforms.addPartition.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addPartition.partition.field"-> "partition",
```

### timestamp - `timestamp.field`

```
      "transforms.addTimestamp.type" -> "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addTimestamp.timestamp.field"-> "ts",
```

### restoring and removing fields with source

while writing the data back to the brokers, you might want to drop the extra fields we added in the sink.

###  record key - ValueToKey

```
      "transforms" -> "createKey",
      "transforms.createKey.type" -> "org.apache.kafka.connect.transforms.ValueToKey",
      "transforms.createKey.fields"-> "key",
```

### remove partition field

### remove offset field

### remove timestamp fieild

## utils

`_connect-offsets`

`kafkacat -b localhost:9091 -t _connect-offsets -K'='   `

`["s3SourceConnector",{"folder":"topics/s3TestTopicAvro/partition=0/"}]={"lastFileRead":"topics/s3TestTopicAvro/partition=0/s3TestTopicAvro+0+0000000096.avro","fileOffset":"3","eof":"true"}
`

You will need to recreate the connect machine to flush the offsets

`docker-compose up -d --build --no-deps --force connect1`

## notes

### how big do I want the individual files to be?

you can rotate

Why not a single file per record?

Because each file stores the schema. 

Why would you want to rotate the files at all?




## TODO

* offset translation for consumer failover
* exactly once 
* parallelization & sequentiality
* repartitioning 
* retention & timestamps - timestamp.extractor is currently set to Record
* compacted topics

## caveat

* no way to restrict restored topics with S3 source connector


The implementation demonstrates a naive approach. In order to be used in production, more aspects need to be covered, e.g.:

* error handling
* authentication
* retention
* metadata

Sometimes, if the test fails, the docker containers will not be stopped. You will need to stop them manually. 

`> docker ps -q | xargs docker stop ` 

## current

## have fun and help improve the project with your pull requests
