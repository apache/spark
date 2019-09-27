/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.avro

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.Try

import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.util.Utf8

import org.apache.spark.sql.avro.SchemaConverters.SchemaType
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.{AbstractDataType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

object ISODatetimeLogicalType extends LogicalType("datetime") {
  def register(): Unit = LogicalTypes.register(getName, new LogicalTypes.LogicalTypeFactory() {
    override def fromSchema(schema: Schema): LogicalType = ISODatetimeLogicalType.this
  })
  override def validate(schema: Schema): Unit = {
    if (schema.getType ne Schema.Type.STRING) {
      throw new IllegalArgumentException(
        "Datetime (iso8601) can only be used with an underlying string type")
    }
  }
}

class TestSuitAvroLogicalCatalystMapper extends AvroLogicalTypeCatalystMapper {
  override def deserialize
  : PartialFunction[LogicalType, (CatalystDataUpdater, Int, Any) => Unit] = {
    case ISODatetimeLogicalType =>
      (updater, ordinal, value) =>
        val datetime = value match {
          case s: String => UTF8String.fromString(s)
          case s: Utf8 => val bytes = new Array[Byte](s.getByteLength)
            System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
            UTF8String.fromBytes(bytes)
        }
        val timestamp = Timestamp.from(
          OffsetDateTime.parse(datetime.toString)
            .atZoneSameInstant(ZoneOffset.UTC)
            .toInstant
        )
        updater.setLong(ordinal, timestamp.toInstant.toEpochMilli * 1000L)
  }

  override def toSqlType: PartialFunction[LogicalType, SchemaConverters.SchemaType] = {
    case ISODatetimeLogicalType => SchemaType(TimestampType, nullable = false)
  }

  override def toAvroSchema: PartialFunction[(AbstractDataType, String, String), Schema] = {
    case (TimestampType, _, _) =>
      ISODatetimeLogicalType.addToSchema(SchemaBuilder.builder().stringType())
  }

  override def serialize: PartialFunction[LogicalType, (SpecializedGetters, Int) => Any] = {
    case ISODatetimeLogicalType =>
      (getter, ordinal) =>
        val datetime = OffsetDateTime.ofInstant(
          Instant.ofEpochMilli(getter.getLong(ordinal) / 1000), ZoneOffset.UTC
        ).toString

        Try(DateTimeFormatter.ISO_DATE_TIME.parse(datetime))
          .map(_ => datetime)
          .getOrElse(throw new IncompatibleSchemaException(
            s"Cannot Serialize to Avro logical type ISO8601Datetime: " +
              s"'$datetime' is not a valid datetime."))

  }

}
