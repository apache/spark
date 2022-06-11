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

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

@deprecated("This test suite will be removed.", "3.0.0")
class DeprecatedAvroFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("roundtrip in to_avro and from_avro - int and string") {
    val df = spark.range(10).select($"id", $"id".cast("string").as("str"))

    val avroDF = df.select(to_avro($"id").as("a"), to_avro($"str").as("b"))
    val avroTypeLong = s"""
      |{
      |  "type": "int",
      |  "name": "id"
      |}
    """.stripMargin
    val avroTypeStr = s"""
      |{
      |  "type": "string",
      |  "name": "str"
      |}
    """.stripMargin
    checkAnswer(avroDF.select(from_avro($"a", avroTypeLong), from_avro($"b", avroTypeStr)), df)
  }

  test("roundtrip in to_avro and from_avro - struct") {
    val df = spark.range(10).select(struct($"id", $"id".cast("string").as("str")).as("struct"))
    val avroStructDF = df.select(to_avro($"struct").as("avro"))
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "col1", "type": "long"},
      |    {"name": "col2", "type": "string"}
      |  ]
      |}
    """.stripMargin
    checkAnswer(avroStructDF.select(from_avro($"avro", avroTypeStruct)), df)
  }

  test("roundtrip in to_avro and from_avro - array with null") {
    val dfOne = Seq(Tuple1(Tuple1(1) :: Nil), Tuple1(null :: Nil)).toDF("array")
    val avroTypeArrStruct = s"""
      |[ {
      |  "type" : "array",
      |  "items" : [ {
      |    "type" : "record",
      |    "name" : "x",
      |    "fields" : [ {
      |      "name" : "y",
      |      "type" : "int"
      |    } ]
      |  }, "null" ]
      |}, "null" ]
    """.stripMargin
    val readBackOne = dfOne.select(to_avro($"array").as("avro"))
      .select(from_avro($"avro", avroTypeArrStruct).as("array"))
    checkAnswer(dfOne, readBackOne)
  }

  test("SPARK-27798: from_avro produces same value when converted to local relation") {
    val simpleSchema =
      """
        |{
        |  "type": "record",
        |  "name" : "Payload",
        |  "fields" : [ {"name" : "message", "type" : "string" } ]
        |}
      """.stripMargin

    def generateBinary(message: String, avroSchema: String): Array[Byte] = {
      val schema = new Schema.Parser().parse(avroSchema)
      val out = new ByteArrayOutputStream()
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val rootRecord = new GenericRecordBuilder(schema).set("message", message).build()
      writer.write(rootRecord, encoder)
      encoder.flush()
      out.toByteArray
    }

    // This bug is hit when the rule `ConvertToLocalRelation` is run. But the rule was excluded
    // in `SharedSparkSession`.
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val df = Seq("one", "two", "three", "four").map(generateBinary(_, simpleSchema))
        .toDF()
        .withColumn("value", from_avro(col("value"), simpleSchema))

      assert(df.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
      assert(df.collect().map(_.get(0)) === Seq(Row("one"), Row("two"), Row("three"), Row("four")))
    }
  }
}
