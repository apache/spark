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

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class AvroFunctionsSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  import testImplicits._

  test("roundtrip in to_avro and from_avro - int and string") {
    val df = spark.range(10).select('id, 'id.cast("string").as("str"))

    val avroDF = df.select(to_avro('id).as("a"), to_avro('str).as("b"))
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
    checkAnswer(avroDF.select(from_avro('a, avroTypeLong), from_avro('b, avroTypeStr)), df)
  }

  test("roundtrip in to_avro and from_avro - struct") {
    val df = spark.range(10).select(struct('id, 'id.cast("string").as("str")).as("struct"))
    val avroStructDF = df.select(to_avro('struct).as("avro"))
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
    checkAnswer(avroStructDF.select(from_avro('avro, avroTypeStruct)), df)
  }

  test("handle invalid input in from_avro") {
    val count = 10
    val df = spark.range(count).select(struct('id, 'id.as("id2")).as("struct"))
    val avroStructDF = df.select(to_avro('struct).as("avro"))
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "col1", "type": "long"},
      |    {"name": "col2", "type": "double"}
      |  ]
      |}
    """.stripMargin

    intercept[SparkException] {
      avroStructDF.select(
        org.apache.spark.sql.avro.functions.from_avro(
          'avro, avroTypeStruct, Map("mode" -> "FAILFAST").asJava)).collect()
    }

    // For PERMISSIVE mode, the result should be row of null columns.
    val expected = (0 until count).map(_ => Row(Row(null, null)))
    checkAnswer(
      avroStructDF.select(
        org.apache.spark.sql.avro.functions.from_avro(
          'avro, avroTypeStruct, Map("mode" -> "PERMISSIVE").asJava)),
      expected)
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
}
