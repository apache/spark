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

package org.apache.spark.sql

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DataFrameAsSchemaSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("reorder columns by name") {
    val schema = new StructType().add("j", StringType).add("i", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b", "a"))
  }

  test("case insensitive: reorder columns by name") {
    val schema = new StructType().add("J", StringType).add("I", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b", "a"))
  }

  test("select part of the columns") {
    val schema = new StructType().add("j", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b"))
  }

  test("negative: column not found") {
    val schema = new StructType().add("non_exist", StringType)
    val e = intercept[SparkThrowable](Seq("a" -> "b").toDF("i", "j").as(schema))
    checkError(
      exception = e,
      errorClass = "UNRESOLVED_COLUMN",
      parameters = Map(
        "objectName" -> "`non_exist`",
        "objectList" -> "`i`, `j`"))
  }

  test("negative: ambiguous column") {
    val schema = new StructType().add("i", StringType)
    val e = intercept[SparkThrowable](Seq("a" -> "b").toDF("i", "I").as(schema))
    checkError(
      exception = e,
      errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
      parameters = Map(
        "name" -> "`i`",
        "n" -> "2"))
  }

  test("keep the nullability of the original column") {
    val schema = new StructType().add("j", IntegerType)
    val data = Seq("a" -> 1).toDF("i", "j")
    assert(!data.schema.fields(1).nullable)
    val df = data.as(schema)
    val finalSchema = new StructType().add("j", IntegerType, nullable = false)
    assert(df.schema == finalSchema)
    checkAnswer(df, Row(1))
  }

  test("negative: incompatible column nullability") {
    val schema = new StructType().add("i", IntegerType, nullable = false)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)")
    assert(data.schema.fields(0).nullable)
    val e = intercept[SparkThrowable](data.as(schema))
    checkError(
      exception = e,
      errorClass = "NULLABLE_COLUMN_OR_FIELD",
      parameters = Map("name" -> "`i`"))
  }

  test("upcast the original column") {
    val schema = new StructType().add("j", LongType, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j").as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(1L))
  }

  test("negative: column cannot upcast") {
    val schema = new StructType().add("i", IntegerType)
    val e = intercept[SparkThrowable](Seq("a" -> 1).toDF("i", "j").as(schema))
    checkError(
      exception = e,
      errorClass = "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
      parameters = Map(
        "name" -> "`i`",
        "type" -> "\"STRING\"",
        "expectedType" -> "\"INT\"")
    )
  }

  test("column carries the metadata") {
    val metadata1 = new MetadataBuilder().putString("a", "1").putString("b", "2").build()
    val metadata2 = new MetadataBuilder().putString("b", "3").putString("c", "4").build()
    val schema = new StructType().add("i", IntegerType, nullable = true, metadata = metadata2)
    val df = Seq((1)).toDF("i").select($"i".as("i", metadata1)).as(schema)
    // Metadata "a" remains, "b" gets overwritten by the specified schema, "c" is newly added.
    val resultMetadata = new MetadataBuilder()
      .putString("a", "1").putString("b", "3").putString("c", "4").build()
    assert(df.schema(0).metadata == resultMetadata)
  }


  test("reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row("b", "a")))
  }

  test("case insensitive: reorder inner fields by name") {
    val innerFields = new StructType().add("J", StringType).add("I", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row("b", "a")))
  }

  test("negative: field not found") {
    val innerFields = new StructType().add("non_exist", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val e = intercept[SparkThrowable] {
      Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).as(schema)
    }
    checkError(
      exception = e,
      errorClass = "UNRESOLVED_FIELD",
      parameters = Map(
        "fieldName" -> "`non_exist`",
        "columnPath" -> "`struct`",
        "proposal" -> "`i`, `j`"))
  }

  test("keep the nullability of the original field") {
    val innerFields = new StructType().add("j", IntegerType)
    val schema = new StructType().add("struct", innerFields)
    val data = Seq("a" -> 1).toDF("i", "j").select(struct($"i", $"j").as("struct"))
    assert(!data.schema.fields(0).nullable)
    assert(!data.schema.fields(0).dataType.asInstanceOf[StructType].fields(1).nullable)
    val df = data.as(schema)
    val finalFields = new StructType().add("j", IntegerType, nullable = false)
    val finalSchema = new StructType().add("struct", finalFields, nullable = false)
    assert(df.schema == finalSchema)
    checkAnswer(df, Row(Row(1)))
  }

  test("negative: incompatible field nullability") {
    val innerFields = new StructType().add("i", IntegerType, nullable = false)
    val schema = new StructType().add("struct", innerFields)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)").select(struct($"i").as("struct"))
    assert(!data.schema.fields(0).nullable)
    assert(data.schema.fields(0).dataType.asInstanceOf[StructType].fields(0).nullable)
    val e = intercept[SparkThrowable](data.as(schema))
    checkError(
      exception = e,
      errorClass = "NULLABLE_COLUMN_OR_FIELD",
      parameters = Map("name" -> "`struct`.`i`"))
  }

  test("upcast the original field") {
    val innerFields = new StructType().add("j", LongType, nullable = false)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j").select(struct($"i", $"j").as("struct")).as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row(1L)))
  }

  test("negative: field cannot upcast") {
    val innerFields = new StructType().add("i", IntegerType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val e = intercept[SparkThrowable] {
      Seq("a" -> 1).toDF("i", "j").select(struct($"i", $"j").as("struct")).as(schema)
    }
    checkError(
      exception = e,
      errorClass = "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
      parameters = Map(
        "name" -> "`struct`.`i`",
        "type" -> "\"STRING\"",
        "expectedType" -> "\"INT\"")
    )
  }

  test("inner field carries the metadata") {
    val metadata1 = new MetadataBuilder().putString("a", "1").putString("b", "2").build()
    val metadata2 = new MetadataBuilder().putString("b", "3").putString("c", "4").build()
    val innerFields = new StructType().add("i", LongType, nullable = true, metadata = metadata2)
    val schema = new StructType().add("struct", innerFields)
    val df = Seq((1)).toDF("i")
      .select($"i".as("i", metadata1))
      .select(struct($"i").as("struct"))
      .as(schema)
    // Metadata "a" remains, "b" gets overwritten by the specified schema, "c" is newly added.
    val resultMetadata = new MetadataBuilder()
      .putString("a", "1").putString("b", "3").putString("c", "4").build()
    assert(df.schema(0).dataType.asInstanceOf[StructType].fields(0).metadata == resultMetadata)
  }

  test("array element: reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val arr = ArrayType(innerFields, containsNull = false)
    val schema = new StructType().add("arr", arr, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j")
      .select(array(struct($"i", $"j")).as("arr"))
      .as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Seq(Row("b", "a"))))
  }

  test("array element: upcast the original field") {
    val innerFields = new StructType().add("j", LongType, nullable = false)
    val arr = ArrayType(innerFields, containsNull = false)
    val schema = new StructType().add("arr", arr, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j")
      .select(array(struct($"i", $"j")).as("arr"))
      .as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Seq(Row(1L))))
  }

  test("array element: incompatible array nullability") {
    val arr = ArrayType(IntegerType, containsNull = false)
    val schema = new StructType().add("arr", arr)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)").select(array($"i").as("arr"))
    assert(data.schema.fields(0).dataType.asInstanceOf[ArrayType].containsNull)
    val e = intercept[SparkThrowable](data.as(schema))
    checkError(
      exception = e,
      errorClass = "NULLABLE_ARRAY_OR_MAP_ELEMENT",
      parameters = Map("columnPath" -> "`arr`"))
  }

  test("array element: array itself carries the metadata") {
    val metadata1 = new MetadataBuilder().putString("a", "1").putString("b", "2").build()
    val metadata2 = new MetadataBuilder().putString("b", "3").putString("c", "4").build()
    val innerFields = new StructType().add("i", LongType)
    val arr = ArrayType(innerFields, containsNull = true)
    val schema = new StructType().add("arr", arr, nullable = false, metadata = metadata2)
    val df = Seq((1)).toDF("i")
      .select($"i")
      .select(array(struct($"i")).as("arr", metadata1))
      .as(schema)
    // Metadata "a" remains, "b" gets overwritten by the specified schema, "c" is newly added.
    val resultMetadata = new MetadataBuilder()
      .putString("a", "1").putString("b", "3").putString("c", "4").build()
    assert(df.schema(0).metadata == resultMetadata)
  }

  test("array element: inner field inside array carries the metadata") {
    val metadata1 = new MetadataBuilder().putString("a", "1").putString("b", "2").build()
    val metadata2 = new MetadataBuilder().putString("b", "3").putString("c", "4").build()
    val innerFields = new StructType().add("i", LongType, nullable = true, metadata = metadata2)
    val arr = ArrayType(innerFields, containsNull = true)
    val schema = new StructType().add("arr", arr, nullable = false)
    val df = Seq((1)).toDF("i")
      .select($"i".as("i", metadata1))
      .select(array(struct($"i")).as("arr"))
      .as(schema)
    // Metadata "a" remains, "b" gets overwritten by the specified schema, "c" is newly added.
    val resultMetadata = new MetadataBuilder()
      .putString("a", "1").putString("b", "3").putString("c", "4").build()
    assert(df.schema(0).dataType.asInstanceOf[ArrayType].elementType
      .asInstanceOf[StructType].fields(0).metadata == resultMetadata)
  }

  test("map key: reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val m = MapType(innerFields, StringType)
    val schema = new StructType().add("map", m, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j")
      .select(map(struct($"i", $"j"), $"i").as("map"))
      .as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Map(Row("b", "a") -> "a")))
  }

  test("map value: reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val m = MapType(StringType, innerFields, valueContainsNull = false)
    val schema = new StructType().add("map", m, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j")
      .select(map($"i", struct($"i", $"j")).as("map"))
      .as(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Map("a" -> Row("b", "a"))))
  }
}
