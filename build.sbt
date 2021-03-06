lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.13.0"
    )),
    name := "s3backup"
  )
enablePlugins(DockerComposePlugin)
enablePlugins(JavaAppPackaging)

scalacOptions += "-target:jvm-1.8"
javacOptions ++= Seq("-source", "1.8")

resolvers += Resolver.bintrayRepo("ovotech", "maven")
resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
libraryDependencies += "org.apache.kafka" % "connect-api" % "2.5.0"
libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "5.5.1"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "5.5.1"
libraryDependencies += "io.confluent" % "kafka-connect-storage-partitioner" % "5.5.1"

libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.9"


libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "io.minio" % "minio" % "6.0.13"
// libraryDependencies += "software.amazon.awssdk" % "s3" % "2.8.7" <- minio does not support sdk 2.x yet
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.812"
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.1"
libraryDependencies += "commons-io" % "commons-io" % "2.6"

// filler libs, required by com.amazonaws.util.Md5Utils
libraryDependencies += "javax.xml.bind" % "jaxb-api" % "2.2.11"
libraryDependencies += "com.sun.xml.bind" % "jaxb-core" % "2.2.11"
libraryDependencies += "com.sun.xml.bind" % "jaxb-impl" % "2.2.11"
libraryDependencies += "javax.activation" % "activation" % "1.1.1"

libraryDependencies += "com.softwaremill.sttp.client" %% "core" % "2.0.9"
libraryDependencies += "com.softwaremill.sttp.client" %% "async-http-client-backend-monix" % "2.0.9"
libraryDependencies += "com.softwaremill.sttp.client" %% "circe" % "2.0.9"
libraryDependencies += "io.circe" %% "circe-generic" % "0.13.0"

// libraryDependencies += "com.ovoenergy" %% "kafka-serialization-core" % "0.5.17"
// libraryDependencies += "com.ovoenergy" %% "kafka-serialization-circe" % "0.5.17"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test


