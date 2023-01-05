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

import java.io.File
import java.sql.Timestamp

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StructField, StructType, TimestampNTZType, TimestampType}

abstract class AvroLogicalTypeSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val dateSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "date", "type": {"type": "int", "logicalType": "date"}}
        ]
      }
    """

  val dateInputData = Seq(7, 365, 0)

  def dateFile(path: String): String = {
    val schema = new Schema.Parser().parse(dateSchema)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    val result = s"$path/test.avro"
    dataFileWriter.create(schema, new File(result))

    dateInputData.foreach { x =>
      val record = new GenericData.Record(schema)
      record.put("date", x)
      dataFileWriter.append(record)
    }
    dataFileWriter.flush()
    dataFileWriter.close()
    result
  }

  test("Logical type: date") {
    withTempDir { dir =>
      val expected = dateInputData.map(t => Row(DateTimeUtils.toJavaDate(t)))
      val dateAvro = dateFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(dateAvro)

      checkAnswer(df, expected)

      checkAnswer(spark.read.format("avro").option("avroSchema", dateSchema).load(dateAvro),
        expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  // scalastyle:off line.size.limit
  val timestampSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "timestamp_millis", "type": {"type": "long","logicalType": "timestamp-millis"}},
          {"name": "timestamp_micros", "type": {"type": "long","logicalType": "timestamp-micros"}},
          {"name": "local_timestamp_millis", "type": {"type": "long","logicalType": "local-timestamp-millis"}},
          {"name": "local_timestamp_micros", "type": {"type": "long","logicalType": "local-timestamp-micros"}},
          {"name": "long", "type": "long"}
        ]
      }
    """
  // scalastyle:on line.size.limit

  val timestampInputData =
    Seq((1000L, 2000L, 1000L, 2000L, 3000L), (666000L, 999000L, 666000L, 999000L, 777000L))

  def timestampFile(path: String): String = {
    val schema = new Schema.Parser().parse(timestampSchema)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    val result = s"$path/test.avro"
    dataFileWriter.create(schema, new File(result))

    timestampInputData.foreach { t =>
      val record = new GenericData.Record(schema)
      record.put("timestamp_millis", t._1)
      // For microsecond precision, we multiple the value by 1000 to match the expected answer as
      // timestamp with millisecond precision.
      record.put("timestamp_micros", t._2 * 1000)
      record.put("local_timestamp_millis", t._3)
      record.put("local_timestamp_micros", t._4 * 1000)
      record.put("long", t._5)
      dataFileWriter.append(record)
    }
    dataFileWriter.flush()
    dataFileWriter.close()
    result
  }

  test("Logical type: timestamp_millis") {
    withTempDir { dir =>
      val expected = timestampInputData.map(t => Row(new Timestamp(t._1)))
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(timestampAvro).select($"timestamp_millis")

      checkAnswer(df, expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: timestamp_micros") {
    withTempDir { dir =>
      val expected = timestampInputData.map(t => Row(new Timestamp(t._2)))
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(timestampAvro).select($"timestamp_micros")

      checkAnswer(df, expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: local_timestamp_millis") {
    withTempDir { dir =>
      val expected = timestampInputData.map(t =>
        Row(DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._3))))
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(timestampAvro).select($"local_timestamp_millis")

      checkAnswer(df, expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: local_timestamp_micros") {
    withTempDir { dir =>
      val expected = timestampInputData.map(t =>
        Row(DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._4))))
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(timestampAvro).select($"local_timestamp_micros")

      checkAnswer(df, expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        val df2 = spark.read.format("avro").load(path.toString)
        assert(df2.schema ==
          StructType(StructField("local_timestamp_micros", TimestampNTZType, true) :: Nil))
        checkAnswer(df2, expected)
      }
    }
  }

  test("Logical type: user specified output schema with different timestamp types") {
    withTempDir { dir =>
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df =
        spark.read.format("avro").load(timestampAvro)
          .select($"timestamp_millis", $"timestamp_micros")

      val expected = timestampInputData.map(t => Row(new Timestamp(t._1), new Timestamp(t._2)))

      val userSpecifiedTimestampSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "timestamp_millis",
            "type": [{"type": "long","logicalType": "timestamp-micros"}, "null"]},
          {"name": "timestamp_micros",
            "type": [{"type": "long","logicalType": "timestamp-millis"}, "null"]}
        ]
      }
    """

      withTempPath { path =>
        df.write
          .format("avro")
          .option("avroSchema", userSpecifiedTimestampSchema)
          .save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: user specified output schema with different timestamp ntz types") {
    withTempDir { dir =>
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val df = spark.read.format("avro").load(timestampAvro).select(
        $"local_timestamp_millis", $"local_timestamp_micros")

      val expected = timestampInputData.map(t =>
        Row(DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._3)),
          DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._4))))

      val userSpecifiedTimestampSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "local_timestamp_millis",
            "type": [{"type": "long","logicalType": "local-timestamp-millis"}, "null"]},
          {"name": "local_timestamp_micros",
            "type": [{"type": "long","logicalType": "local-timestamp-micros"}, "null"]}
        ]
      }
    """

      withTempPath { path =>
        df.write
          .format("avro")
          .option("avroSchema", userSpecifiedTimestampSchema)
          .save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Read Long type as Timestamp") {
    withTempDir { dir =>
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val schema = StructType(StructField("long", TimestampType, true) :: Nil)
      val df = spark.read.format("avro").schema(schema).load(timestampAvro).select($"long")

      val expected = timestampInputData.map(t => Row(new Timestamp(t._5)))

      checkAnswer(df, expected)
    }
  }

  test("Read Long type as Timestamp without time zone") {
    withTempDir { dir =>
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val schema = StructType(StructField("long", TimestampNTZType, true) :: Nil)
      val df = spark.read.format("avro").schema(schema).load(timestampAvro).select($"long")

      val expected = timestampInputData.map(t =>
        Row(DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._5))))

      checkAnswer(df, expected)
    }
  }

  test("Logical type: user specified read schema") {
    withTempDir { dir =>
      val timestampAvro = timestampFile(dir.getAbsolutePath)
      val expected = timestampInputData.map(t => Row(new Timestamp(t._1), new Timestamp(t._2),
        DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._3)),
        DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(t._4)), t._5))

      val df = spark.read.format("avro").option("avroSchema", timestampSchema).load(timestampAvro)
      checkAnswer(df, expected)
    }
  }

  val decimalInputData = Seq("1.23", "4.56", "78.90", "-1", "-2.31")

  def decimalSchemaAndFile(path: String): (String, String) = {
    val precision = 4
    val scale = 2
    val bytesFieldName = "bytes"
    val bytesSchema = s"""{
         "type":"bytes",
         "logicalType":"decimal",
         "precision":$precision,
         "scale":$scale
      }
    """

    val fixedFieldName = "fixed"
    val fixedSchema = s"""{
         "type":"fixed",
         "size":5,
         "logicalType":"decimal",
         "precision":$precision,
         "scale":$scale,
         "name":"foo"
      }
    """
    val avroSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "$bytesFieldName", "type": $bytesSchema},
          {"name": "$fixedFieldName", "type": $fixedSchema}
        ]
      }
    """
    val schema = new Schema.Parser().parse(avroSchema)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    val decimalConversion = new DecimalConversion
    val avroFile = s"$path/test.avro"
    dataFileWriter.create(schema, new File(avroFile))
    val logicalType = LogicalTypes.decimal(precision, scale)

    decimalInputData.foreach { x =>
      val avroRec = new GenericData.Record(schema)
      val decimal = new java.math.BigDecimal(x).setScale(scale)
      val bytes =
        decimalConversion.toBytes(decimal, schema.getField(bytesFieldName).schema, logicalType)
      avroRec.put(bytesFieldName, bytes)
      val fixed =
        decimalConversion.toFixed(decimal, schema.getField(fixedFieldName).schema, logicalType)
      avroRec.put(fixedFieldName, fixed)
      dataFileWriter.append(avroRec)
    }
    dataFileWriter.flush()
    dataFileWriter.close()

    (avroSchema, avroFile)
  }

  test("Logical type: Decimal") {
    withTempDir { dir =>
      val (avroSchema, avroFile) = decimalSchemaAndFile(dir.getAbsolutePath)
      val expected =
        decimalInputData.map { x => Row(new java.math.BigDecimal(x), new java.math.BigDecimal(x)) }
      val df = spark.read.format("avro").load(avroFile)
      checkAnswer(df, expected)
      checkAnswer(spark.read.format("avro").option("avroSchema", avroSchema).load(avroFile),
        expected)

      withTempPath { path =>
        df.write.format("avro").save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: write Decimal with BYTES type") {
    val specifiedSchema = """
      {
        "type" : "record",
        "name" : "topLevelRecord",
        "namespace" : "topLevelRecord",
        "fields" : [ {
          "name" : "bytes",
          "type" : [ {
            "type" : "bytes",
            "namespace" : "topLevelRecord.bytes",
            "logicalType" : "decimal",
            "precision" : 4,
            "scale" : 2
          }, "null" ]
        }, {
          "name" : "fixed",
          "type" : [ {
            "type" : "bytes",
            "logicalType" : "decimal",
            "precision" : 4,
            "scale" : 2
          }, "null" ]
        } ]
      }
    """
    withTempDir { dir =>
      val (avroSchema, avroFile) = decimalSchemaAndFile(dir.getAbsolutePath)
      assert(specifiedSchema != avroSchema)
      val expected =
        decimalInputData.map { x => Row(new java.math.BigDecimal(x), new java.math.BigDecimal(x)) }
      val df = spark.read.format("avro").load(avroFile)

      withTempPath { path =>
        df.write.format("avro").option("avroSchema", specifiedSchema).save(path.toString)
        checkAnswer(spark.read.format("avro").load(path.toString), expected)
      }
    }
  }

  test("Logical type: Decimal with too large precision") {
    withTempDir { dir =>
      val schema = new Schema.Parser().parse("""{
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [{
          "name": "decimal",
          "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}
        }]
      }""")
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)
      val decimal = new java.math.BigDecimal("0.12345678901234567890123456789012345678")
      val bytes = (new DecimalConversion).toBytes(decimal, schema, LogicalTypes.decimal(39, 38))
      avroRec.put("decimal", bytes)
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      checkError(
        exception = intercept[SparkException] {
          spark.read.format("avro").load(s"$dir.avro").collect()
        }.getCause.getCause.asInstanceOf[SparkArithmeticException],
        errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
        parameters = Map(
          "value" -> "0",
          "precision" -> "4",
          "scale" -> "2",
          "config" -> "\"spark.sql.ansi.enabled\"")
      )
    }
  }
}

class AvroV1LogicalTypeSuite extends AvroLogicalTypeSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "avro")
}

class AvroV2LogicalTypeSuite extends AvroLogicalTypeSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
