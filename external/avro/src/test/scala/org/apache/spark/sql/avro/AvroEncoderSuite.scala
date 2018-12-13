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

import java.nio.ByteBuffer

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}

class AvroEncoderSuite extends SharedSQLContext {
  import testImplicits._

  test("encoder from json schema") {
    val jsonFormatSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "simple_record",
        |  "fields" :
        |   [
        |    { "name" : "myUInt", "type" : [ "int", "null" ], "default" : 1 },
        |    { "name" : "myULong", "type" : [ "long", "null" ], "default" : 2 },
        |    { "name" : "myUBool", "type" : [ "boolean", "null" ], "default" : true },
        |    { "name" : "myUDouble", "type" : [ "double", "null" ], "default" : 3 },
        |    { "name" : "myUFloat", "type" : [ "float", "null" ], "default" : 4.5 },
        |    { "name" : "myUString", "type" : [ "string", "null" ], "default" : "foo" },
        |
        |    { "name" : "myInt", "type" : "int", "default" : 10 },
        |    { "name" : "myLong", "type" : "long", "default" : 11 },
        |    { "name" : "myBool", "type" : "boolean", "default" : false },
        |    { "name" : "myDouble", "type" : "double", "default" : 12 },
        |    { "name" : "myFloat", "type" : "float", "default" : 13.14 },
        |    { "name" : "myString", "type" : "string", "default" : "bar" },
        |
        |    { "name" : "myArray", "type" :
        |     { "type" : "array", "items" : "bytes" }, "default" : [ "a12b", "cc50" ] },
        |    { "name" : "myMap", "type" : { "type" : "map", "values" : "string" },
        |     "default" : {"a":"A", "b":"B"} }
        |   ]
        |}
      """.stripMargin
    val encoder = AvroEncoder.of(jsonFormatSchema)
    val expressionEncoder = encoder.asInstanceOf[ExpressionEncoder[GenericData.Record]]
    val schema = new Schema.Parser().parse(jsonFormatSchema)
    val record = new GenericRecordBuilder(schema).build
    val row = expressionEncoder.toRow(record)
    val recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)
    assert(record.toString == recordFromRow.toString)
  }

  test("generic record converts to row and back") {
    // complex schema including type of basic type, array with int/string/record/enum,
    // nested record and map.
    val jsonFormatSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "record",
        |  "fields" : [ {
        |    "name" : "boolean",
        |    "type" : "boolean",
        |    "default" : false
        |  }, {
        |    "name" : "int",
        |    "type" : "int",
        |    "default" : 0
        |  }, {
        |    "name" : "long",
        |    "type" : "long",
        |    "default" : 0
        |  }, {
        |    "name" : "float",
        |    "type" : "float",
        |    "default" : 0.0
        |  }, {
        |    "name" : "double",
        |    "type" : "double",
        |    "default" : 0.0
        |  }, {
        |    "name" : "string",
        |    "type" : "string",
        |    "default" : "string"
        |  }, {
        |    "name" : "bytes",
        |    "type" : "bytes",
        |    "default" : "bytes"
        |  }, {
        |    "name" : "nested",
        |    "type" : {
        |      "type" : "record",
        |      "name" : "simple_record",
        |      "fields" : [ {
        |        "name" : "nested1",
        |        "type" : "int",
        |        "default" : 0
        |      }, {
        |        "name" : "nested2",
        |        "type" : "string",
        |        "default" : "string"
        |      } ]
        |    },
        |    "default" : {
        |      "nested1" : 0,
        |      "nested2" : "string"
        |    }
        |  }, {
        |    "name" : "enum",
        |    "type" : {
        |      "type" : "enum",
        |      "name" : "simple_enums",
        |      "symbols" : [ "SPADES", "HEARTS", "CLUBS", "DIAMONDS" ]
        |    },
        |    "default" : "SPADES"
        |  }, {
        |    "name" : "int_array",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "int"
        |    },
        |    "default" : [ 1, 2, 3 ]
        |  }, {
        |    "name" : "string_array",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "string"
        |    },
        |    "default" : [ "a", "b", "c" ]
        |  }, {
        |    "name" : "record_array",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "simple_record"
        |    },
        |    "default" : [ {
        |      "nested1" : 0,
        |      "nested2" : "string"
        |    }, {
        |      "nested1" : 0,
        |      "nested2" : "string"
        |    } ]
        |  }, {
        |    "name" : "enum_array",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "simple_enums"
        |    },
        |    "default" : [ "SPADES", "HEARTS", "SPADES" ]
        |  }, {
        |    "name" : "fixed_array",
        |    "type" : {
        |      "type" : "array",
        |      "items" : {
        |        "type" : "fixed",
        |        "name" : "simple_fixed",
        |        "size" : 3
        |      }
        |    },
        |    "default" : [ "foo", "bar", "baz" ]
        |  }, {
        |    "name" : "fixed",
        |    "type" : {
        |      "type" : "fixed",
        |      "name" : "simple_fixed_item",
        |      "size" : 16
        |    },
        |    "default" : "string_length_16"
        |  }, {
        |    "name" : "map",
        |    "type" : {
        |      "type" : "map",
        |      "values" : "string"
        |    },
        |    "default" : {
        |      "a" : "A"
        |    }
        |  } ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(jsonFormatSchema)
    val encoder = AvroEncoder.of[GenericData.Record](schema)
    val expressionEncoder = encoder.asInstanceOf[ExpressionEncoder[GenericData.Record]]
    val record = new GenericRecordBuilder(schema).build
    val row = expressionEncoder.toRow(record)
    val recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)
    assert(record.toString == recordFromRow.toString)
  }

  test("encoder resolves union types to rows") {
    val jsonFormatSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "record",
        |  "fields" : [ {
        |    "name" : "int_null_union",
        |    "type" : [ "null", "int" ],
        |    "default" : null
        |  }, {
        |    "name" : "string_null_union",
        |    "type" : [ "null", "string" ],
        |    "default" : null
        |  }, {
        |    "name" : "int_long_union",
        |    "type" : [ "int", "long" ],
        |    "default" : 0
        |  }, {
        |    "name" : "float_double_union",
        |    "type" : [ "float", "double" ],
        |    "default" : 0.0
        |  } ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(jsonFormatSchema)
    val encoder = AvroEncoder.of[GenericData.Record](schema)
    val expressionEncoder = encoder.asInstanceOf[ExpressionEncoder[GenericData.Record]]
    val record = new GenericRecordBuilder(schema).build
    val row = expressionEncoder.toRow(record)
    val recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)
    assert(record.get(0) == recordFromRow.get(0))
    assert(record.get(1) == recordFromRow.get(1))
    assert(record.get(2) == recordFromRow.get(2))
    assert(record.get(3) == recordFromRow.get(3))
    record.put(0, 0)
    record.put(1, "value")
    val updatedRow = expressionEncoder.toRow(record)
    val updatedRecordFromRow = expressionEncoder.resolveAndBind().fromRow(updatedRow)
    assert(record.get(0) == updatedRecordFromRow.get(0))
    assert(record.get(1) == updatedRecordFromRow.get(1))
  }

  test("encoder resolves complex unions to rows") {
    val nested =
      SchemaBuilder.record("simple_record").fields()
        .name("nested1").`type`("int").withDefault(0)
        .name("nested2").`type`("string").withDefault("foo").endRecord()
    val schema = SchemaBuilder.record("record").fields()
      .name("int_float_string_record").`type`(
      SchemaBuilder.unionOf()
        .`type`("null").and()
        .`type`("int").and()
        .`type`("float").and()
        .`type`("string").and()
        .`type`(nested).endUnion()
    ).withDefault(null).endRecord()

    val encoder = AvroEncoder.of[GenericData.Record](schema)
    val expressionEncoder = encoder.asInstanceOf[ExpressionEncoder[GenericData.Record]]
    val record = new GenericRecordBuilder(schema).build
    var row = expressionEncoder.toRow(record)
    var recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)

    assert(row.getStruct(0, 4).get(0, IntegerType) == null)
    assert(row.getStruct(0, 4).get(1, FloatType) == null)
    assert(row.getStruct(0, 4).get(2, StringType) == null)
    assert(row.getStruct(0, 4).getStruct(3, 2) == null)
    assert(record == recordFromRow)

    record.put(0, 1)
    row = expressionEncoder.toRow(record)
    recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)

    assert(row.getStruct(0, 4).get(1, FloatType) == null)
    assert(row.getStruct(0, 4).get(2, StringType) == null)
    assert(row.getStruct(0, 4).getStruct(3, 2) == null)
    assert(record == recordFromRow)

    record.put(0, 1F)
    row = expressionEncoder.toRow(record)
    recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)

    assert(row.getStruct(0, 4).get(0, IntegerType) == null)
    assert(row.getStruct(0, 4).get(2, StringType) == null)
    assert(row.getStruct(0, 4).getStruct(3, 2) == null)
    assert(record == recordFromRow)

    record.put(0, "bar")
    row = expressionEncoder.toRow(record)
    recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)

    assert(row.getStruct(0, 4).get(0, IntegerType) == null)
    assert(row.getStruct(0, 4).get(1, FloatType) == null)
    assert(row.getStruct(0, 4).getStruct(3, 2) == null)
    assert(record == recordFromRow)

    record.put(0, new GenericRecordBuilder(nested).build())
    row = expressionEncoder.toRow(record)
    recordFromRow = expressionEncoder.resolveAndBind().fromRow(row)

    assert(row.getStruct(0, 4).get(0, IntegerType) == null)
    assert(row.getStruct(0, 4).get(1, FloatType) == null)
    assert(row.getStruct(0, 4).get(2, StringType) == null)
    assert(record == recordFromRow)
  }

  test("create Dataset from GenericRecord") {
    // need a spark context with kryo as serializer
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.master", "local[2]")
      .set("spark.app.name", "AvroSuite")
    val context = new SparkContext(conf)

    val schema: Schema =
      SchemaBuilder
        .record("GenericRecordTest")
        .fields()
        .requiredString("field1")
        .name("enumVal").`type`().enumeration("letters").symbols("a", "b", "c").enumDefault("a")
        .name("fixedVal").`type`().fixed("MD5").size(16).fixedDefault(ByteBuffer.allocate(16))
        .endRecord()

    implicit val enc = AvroEncoder.of[GenericData.Record](schema)

    val genericRecords = (1 to 10) map { i =>
      new GenericRecordBuilder(schema)
        .set("field1", "field-" + i)
        .build()
    }

    val rdd: RDD[GenericData.Record] = context.parallelize(genericRecords)
    val ds = rdd.toDS()
    assert(ds.count() == genericRecords.size)
    context.stop()
  }
}
