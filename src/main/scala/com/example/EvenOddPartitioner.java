package com.example;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class EvenOddPartitioner<T> extends FieldPartitioner<T> {

    private static final Logger log = LoggerFactory.getLogger(EvenOddPartitioner.class);
    private List<String> fieldNames;

    public EvenOddPartitioner() {
    }

    public void configure(Map<String, Object> config) {
        this.fieldNames = (List)config.get("partition.field.name");
        this.delim = (String)config.get("directory.delim");
    }

    public String encodePartition(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            Schema valueSchema = sinkRecord.valueSchema();
            Struct struct = (Struct)value;
            StringBuilder builder = new StringBuilder();

            for (String fieldName : this.fieldNames) {
                if (builder.length() > 0) {
                    builder.append(this.delim);
                }

                Object partitionKeyField = struct.get(fieldName);
                Schema.Type type = valueSchema.field(fieldName).schema().type();
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        boolean isEven = ((Number) partitionKeyField).longValue() % 2 == 0;
                        builder.append(fieldName).append("=").append(isEven ? "even" : "odd");
                        break;
                    default:
                        log.error("Type {} is not supported as a partition key.", type.getName());
                        throw new PartitionException("Error encoding partition.");
                }
            }

            return builder.toString();
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }
    }

    public List<T> partitionFields() {
        if (this.partitionFields == null) {
            this.partitionFields = this.newSchemaGenerator(this.config).newPartitionFields(Utils.join(this.fieldNames, ","));
        }

        return this.partitionFields;
    }



}
