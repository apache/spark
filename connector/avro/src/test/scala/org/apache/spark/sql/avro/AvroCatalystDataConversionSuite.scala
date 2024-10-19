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

import java.util
import java.util.Collections

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.avro.message.{BinaryMessageDecoder, BinaryMessageEncoder}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, NoopFilters, OrderedFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.{EqualTo, Not}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

class AvroCatalystDataConversionSuite extends SparkFunSuite
  with SharedSparkSession
  with ExpressionEvalHelper {

  private def roundTripTest(data: Literal): Unit = {
    val avroType = SchemaConverters.toAvroType(data.dataType, data.nullable)
    checkResult(data, avroType.toString, data.eval())
  }

  private def checkResult(data: Literal, schema: String, expected: Any): Unit = {
    checkEvaluation(
      AvroDataToCatalyst(CatalystDataToAvro(data, None), schema, Map.empty),
      prepareExpectedResult(expected))
  }

  protected def checkUnsupportedRead(data: Literal, schema: String): Unit = {
    val binary = CatalystDataToAvro(data, None)
    intercept[Exception] {
      AvroDataToCatalyst(binary, schema, Map("mode" -> "FAILFAST")).eval()
    }

    val expected = {
      val avroSchema = new Schema.Parser().parse(schema)
      SchemaConverters.toSqlType(avroSchema, false, "").dataType match {
        case st: StructType => Row.fromSeq((0 until st.length).map(_ => null))
        case _ => null
      }
    }

    checkEvaluation(AvroDataToCatalyst(binary, schema, Map("mode" -> "PERMISSIVE")),
      expected)
  }

  private val testingTypes = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType(8, 0),   // 32 bits decimal without fraction
    DecimalType(8, 4),   // 32 bits decimal
    DecimalType(16, 0),  // 64 bits decimal without fraction
    DecimalType(16, 11), // 64 bits decimal
    DecimalType(38, 0),
    DecimalType(38, 38),
    StringType,
    BinaryType)

  protected def prepareExpectedResult(expected: Any): Any = expected match {
    // Spark byte and short both map to avro int
    case b: Byte => b.toInt
    case s: Short => s.toInt
    case row: GenericInternalRow =>
      InternalRow.fromSeq(row.values.map(prepareExpectedResult).toImmutableArraySeq)
    case array: GenericArrayData => new GenericArrayData(array.array.map(prepareExpectedResult))
    case map: MapData =>
      val keys = new GenericArrayData(
        map.keyArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      val values = new GenericArrayData(
        map.valueArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      new ArrayBasedMapData(keys, values)
    case other => other
  }

  testingTypes.foreach { dt =>
    val seed = scala.util.Random.nextLong()
    test(s"single $dt with seed $seed") {
      val rand = new scala.util.Random(seed)
      val data = RandomDataGenerator.forType(dt, rand = rand).get.apply()
      val converter = CatalystTypeConverters.createToCatalystConverter(dt)
      val input = Literal.create(converter(data), dt)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomSchema(rand, 5, testingTypes)
    test(s"flat schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomNestedSchema(rand, 10, testingTypes)
    test(s"nested schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  test("array of nested schema with seed") {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = StructType(
      StructField("a",
        ArrayType(
          RandomDataGenerator.randomNestedSchema(rand, 10, testingTypes),
          containsNull = false),
        nullable = false
      ) :: Nil
    )

    withClue(s"Schema: $schema\nseed: $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  test("read int as string") {
    val data = Literal(1)
    val avroTypeJson =
      s"""
         |{
         |  "type": "string",
         |  "name": "my_string"
         |}
       """.stripMargin

    // When read int as string, avro reader is not able to parse the binary and fail.
    checkUnsupportedRead(data, avroTypeJson)
  }

  test("read string as int") {
    val data = Literal("abc")
    val avroTypeJson =
      s"""
         |{
         |  "type": "int",
         |  "name": "my_int"
         |}
       """.stripMargin

    // When read string data as int, avro reader is not able to find the type mismatch and read
    // the string length as int value.
    checkResult(data, avroTypeJson, 3)
  }

  test("read float as double") {
    val data = Literal(1.23f)
    val avroTypeJson =
      s"""
         |{
         |  "type": "double",
         |  "name": "my_double"
         |}
       """.stripMargin

    // When read float data as double, avro reader fails(trying to read 8 bytes while the data have
    // only 4 bytes).
    checkUnsupportedRead(data, avroTypeJson)
  }

  test("read double as float") {
    val data = Literal(1.23)
    val avroTypeJson =
      s"""
         |{
         |  "type": "float",
         |  "name": "my_float"
         |}
       """.stripMargin

    // avro reader reads the first 4 bytes of a double as a float, the result is totally undefined.
    checkResult(data, avroTypeJson, 5.848603E35f)
  }

  test("Handle unsupported input of record type") {
    val actualSchema = StructType(Seq(
      StructField("col_0", StringType, false),
      StructField("col_1", ShortType, false),
      StructField("col_2", DecimalType(8, 4), false),
      StructField("col_3", BooleanType, true),
      StructField("col_4", DecimalType(38, 38), false)))

    val expectedSchema = StructType(Seq(
      StructField("col_0", BinaryType, false),
      StructField("col_1", DoubleType, false),
      StructField("col_2", DecimalType(18, 4), false),
      StructField("col_3", StringType, true),
      StructField("col_4", DecimalType(38, 38), false)))

    val seed = scala.util.Random.nextLong()
    withClue(s"create random record with seed $seed") {
      val data = RandomDataGenerator.randomRow(new scala.util.Random(seed), actualSchema)
      val converter = CatalystTypeConverters.createToCatalystConverter(actualSchema)
      val input = Literal.create(converter(data), actualSchema)
      val avroSchema = SchemaConverters.toAvroType(expectedSchema).toString
      checkUnsupportedRead(input, avroSchema)
    }
  }

  test("user-specified output schema") {
    val data = Literal("SPADES")
    val jsonFormatSchema =
      """
        |{ "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}
      """.stripMargin

    val message = intercept[SparkException] {
      AvroDataToCatalyst(
        CatalystDataToAvro(
          data,
          None),
        jsonFormatSchema,
        options = Map.empty).eval()
    }.getMessage
    assert(message.contains("Malformed records are detected in record parsing."))

    checkEvaluation(
      AvroDataToCatalyst(
        CatalystDataToAvro(
          data,
          Some(jsonFormatSchema)),
        jsonFormatSchema,
        options = Map.empty),
      data.eval())
  }

  test("invalid user-specified output schema") {
    val message = intercept[IncompatibleSchemaException] {
      CatalystDataToAvro(Literal("SPADES"), Some("\"long\"")).eval()
    }.getMessage
    assert(message === "Cannot convert SQL type STRING to Avro type \"long\".")
  }

  private def checkDeserialization(
      schema: Schema,
      data: GenericData.Record,
      expected: Option[Any],
      filters: StructFilters = new NoopFilters): Unit = {
    val dataType = SchemaConverters.toSqlType(schema, false, "").dataType
    val deserializer = new AvroDeserializer(
      schema,
      dataType,
      false,
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      filters,
      false,
      "",
      -1)
    val deserialized = deserializer.deserialize(data)
    expected match {
      case None => assert(deserialized == None)
      case Some(d) =>
        assert(checkResult(d, deserialized.get, dataType, exprNullable = false))
    }
  }

  test("avro array can be generic java collection") {
    val jsonFormatSchema =
      """
        |{ "type": "record",
        |  "name": "record",
        |  "fields" : [{
        |    "name": "array",
        |    "type": {
        |      "type": "array",
        |      "items": ["null", "int"]
        |    }
        |  }]
        |}
      """.stripMargin
    val avroSchema = new Schema.Parser().parse(jsonFormatSchema)

    def validateDeserialization(array: java.util.Collection[Integer]): Unit = {
      val data = new GenericRecordBuilder(avroSchema)
        .set("array", array)
        .build()
      val expected = InternalRow(new GenericArrayData(new util.ArrayList[Any](array)))
      checkDeserialization(avroSchema, data, Some(expected))

      val reEncoded = new BinaryMessageDecoder[GenericData.Record](new GenericData(), avroSchema)
        .decode(new BinaryMessageEncoder(new GenericData(), avroSchema).encode(data))
      checkDeserialization(avroSchema, reEncoded, Some(expected))
    }

    validateDeserialization(Collections.emptySet())
    validateDeserialization(util.Arrays.asList(1, null, 3))
  }

  test("SPARK-32346: filter pushdown to Avro deserializer") {
    val schema =
      """
        |{
        |  "type" : "record",
        |  "name" : "test_schema",
        |  "fields" : [
        |    {"name": "Age", "type": "int"},
        |    {"name": "Name", "type": "string"}
        |  ]
        |}
        """.stripMargin
    val avroSchema = new Schema.Parser().parse(schema)
    val sqlSchema = new StructType().add("Age", "int").add("Name", "string")
    val data = new GenericRecordBuilder(avroSchema)
      .set("Age", 39)
      .set("Name", "Maxim")
      .build()
    val expectedRow = Some(InternalRow(39, UTF8String.fromString("Maxim")))

    checkDeserialization(avroSchema, data, expectedRow)
    checkDeserialization(
      avroSchema,
      data,
      expectedRow,
      new OrderedFilters(Seq(EqualTo("Age", 39)), sqlSchema))
    checkDeserialization(
      avroSchema,
      data,
      None,
      new OrderedFilters(Seq(Not(EqualTo("Age", 39))), sqlSchema))
  }

  test("AvroDeserializer with binary type") {
    val jsonFormatSchema =
      """
        |{
        |  "type": "record",
        |  "name": "record",
        |  "fields" : [
        |    {"name": "a", "type": "bytes"}
        |  ]
        |}
      """.stripMargin
    val avroSchema = new Schema.Parser().parse(jsonFormatSchema)
    val avroRecord = new GenericData.Record(avroSchema)
    val bb = java.nio.ByteBuffer.wrap(Array[Byte](97, 48, 53))
    avroRecord.put("a", bb)

    val expected = InternalRow(Array[Byte](97, 48, 53))
    checkDeserialization(avroSchema, avroRecord, Some(expected))
    checkDeserialization(avroSchema, avroRecord, Some(expected))
  }
}
