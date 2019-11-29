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
