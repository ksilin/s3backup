package com.example

import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord

trait GenericRecordAvro {

  def fromAvro[A](a: Any)(implicit recordFormat: RecordFormat[A]): A = a match {
    case v: GenericRecord => recordFormat.from(v)
    case x => throw new IllegalArgumentException(s"element is not of type GenericRecord: $x")
  }

  def toAvro[A](a: A)(implicit recordFormat: RecordFormat[A]): GenericRecord =
    recordFormat.to(a)
}