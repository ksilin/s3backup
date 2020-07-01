package com.example

import java.util

import io.confluent.connect.storage.errors.PartitionException
import io.confluent.connect.storage.partitioner.FieldPartitioner
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.data.{Schema, Struct}
import org.slf4j.{Logger, LoggerFactory}

class CustomPartitioner[T] extends FieldPartitioner[T] {

  private val log: Logger = LoggerFactory.getLogger(classOf[CustomPartitioner[_]])
  private var fieldNames: util.List[String] = null

  override def configure(config: util.Map[String, AnyRef]): Unit = {
    this.fieldNames = config.get("partition.field.name").asInstanceOf[util.List[String]]
    this.delim = config.get("directory.delim").asInstanceOf[String]
  }

  override def encodePartition(sinkRecord: SinkRecord): String = {
    val value = sinkRecord.value()

    value match {
      case struct: Struct =>
        val valueSchema: Schema = sinkRecord.valueSchema()
        val builder: StringBuilder = new StringBuilder()
        val var6 = this.fieldNames.iterator();

        while (var6.hasNext) {
          val fieldName: String = var6.next()
          if (builder.nonEmpty) {
            builder.append(this.delim);
          }

          val partitionKey: Object = struct.get(fieldName);
          val tpe: Schema.Type = valueSchema.field(fieldName).schema().`type`()
          tpe match {
            case Schema.Type.INT8 => if(partitionKey.asInstanceOf[Int] % 2 == 0) "even" else "odd"
            case Schema.Type.INT16 => if(partitionKey.asInstanceOf[Int] % 2 == 0) "even" else "odd"
            case Schema.Type.INT32 => if(partitionKey.asInstanceOf[Int] % 2 == 0) "even" else "odd"
            case _ =>  log.error("Type {} is not supported as a partition key.", tpe.getName);
          throw new PartitionException("Error encoding partition.");
          }
        }

        builder.toString();
      case _ =>
        log.error("Value is not Struct type.");
        throw new PartitionException("Error encoding partition.");
    }
  }

}
