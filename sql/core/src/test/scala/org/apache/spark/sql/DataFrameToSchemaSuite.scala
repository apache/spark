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

class DataFrameToSchemaSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("reorder columns by name") {
    val schema = new StructType().add("j", StringType).add("i", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b", "a"))
  }

  test("case insensitive: reorder columns by name") {
    val schema = new StructType().add("J", StringType).add("I", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b", "a"))
  }

  test("select part of the columns") {
    val schema = new StructType().add("j", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row("b"))
  }

  test("nullable column with default null value") {
    val schema = new StructType().add("non_exist", StringType).add("j", StringType)
    val df = Seq("a" -> "b").toDF("i", "j").to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(null, "b"))
  }

  test("negative: non-nullable column not found") {
    val schema = new StructType().add("non_exist", StringType, nullable = false)
    val e = intercept[SparkThrowable](Seq("a" -> "b").toDF("i", "j").to(schema))
    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`non_exist`",
        "proposal" -> "`i`, `j`"))
  }

  test("negative: ambiguous column") {
    val schema = new StructType().add("i", StringType)
    val e = intercept[SparkThrowable](Seq("a" -> "b").toDF("i", "I").to(schema))
    checkError(
      exception = e,
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      parameters = Map(
        "name" -> "`i`",
        "n" -> "2"))
  }

  test("keep the nullability of the original column") {
    val schema = new StructType().add("j", IntegerType)
    val data = Seq("a" -> 1).toDF("i", "j")
    assert(!data.schema.fields(1).nullable)
    val df = data.to(schema)
    val finalSchema = new StructType().add("j", IntegerType, nullable = false)
    assert(df.schema == finalSchema)
    checkAnswer(df, Row(1))
  }

  test("negative: incompatible column nullability") {
    val schema = new StructType().add("i", IntegerType, nullable = false)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)")
    assert(data.schema.fields(0).nullable)
    val e = intercept[SparkThrowable](data.to(schema))
    checkError(
      exception = e,
      condition = "NULLABLE_COLUMN_OR_FIELD",
      parameters = Map("name" -> "`i`"))
  }

  test("upcast the original column") {
    val schema = new StructType().add("j", LongType, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j").to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(1L))
  }

  test("negative: column cannot upcast") {
    val schema = new StructType().add("i", IntegerType)
    val e = intercept[SparkThrowable](Seq("a" -> 1).toDF("i", "j").to(schema))
    checkError(
      exception = e,
      condition = "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
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
    val df = Seq((1)).toDF("i").select($"i".as("i", metadata1)).to(schema)
    // Metadata "a" remains, "b" gets overwritten by the specified schema, "c" is newly added.
    val resultMetadata = new MetadataBuilder()
      .putString("a", "1").putString("b", "3").putString("c", "4").build()
    assert(df.schema(0).metadata == resultMetadata)
  }


  test("reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row("b", "a")))
  }

  test("case insensitive: reorder inner fields by name") {
    val innerFields = new StructType().add("J", StringType).add("I", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row("b", "a")))
  }

  test("nullable field with default null value") {
    val innerFields = new StructType().add("J", StringType).add("non_exist", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row("b", null)))
  }

  test("negative: non-nullable field not found") {
    val innerFields = new StructType().add("non_exist", StringType, nullable = false)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val e = intercept[SparkThrowable] {
      Seq("a" -> "b").toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    }
    checkError(
      exception = e,
      condition = "UNRESOLVED_FIELD.WITH_SUGGESTION",
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
    val df = data.to(schema)
    val finalFields = new StructType().add("j", IntegerType, nullable = false)
    val finalSchema = new StructType().add("struct", finalFields, nullable = false)
    assert(df.schema == finalSchema)
    checkAnswer(df, Row(Row(1)))
  }

  test("struct value: compatible field nullability") {
    val innerFields = new StructType().add("i", LongType, nullable = false)
    val schema = new StructType().add("a", LongType).add("b", innerFields)
    val data = sql("VALUES (1, STRUCT(1 as i)), (NULL, NULL) as t(a, b)")
    assert(data.schema.fields(1).nullable)
    assert(!data.schema.fields(1).dataType.asInstanceOf[StructType].fields(0).nullable)
    val df = data.to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Seq(Row(1, Row(1)), Row(null, null)))
  }

  test("negative: incompatible field nullability") {
    val innerFields = new StructType().add("i", IntegerType, nullable = false)
    val schema = new StructType().add("struct", innerFields)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)").select(struct($"i").as("struct"))
    assert(!data.schema.fields(0).nullable)
    assert(data.schema.fields(0).dataType.asInstanceOf[StructType].fields(0).nullable)
    val e = intercept[SparkThrowable](data.to(schema))
    checkError(
      exception = e,
      condition = "NULLABLE_COLUMN_OR_FIELD",
      parameters = Map("name" -> "`struct`.`i`"))
  }

  test("upcast the original field") {
    val innerFields = new StructType().add("j", LongType, nullable = false)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Row(1L)))
  }

  test("negative: field cannot upcast") {
    val innerFields = new StructType().add("i", IntegerType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val e = intercept[SparkThrowable] {
      Seq("a" -> 1).toDF("i", "j").select(struct($"i", $"j").as("struct")).to(schema)
    }
    checkError(
      exception = e,
      condition = "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
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
      .to(schema)
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
      .to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Seq(Row("b", "a"))))
  }

  test("array element: upcast the original field") {
    val innerFields = new StructType().add("j", LongType, nullable = false)
    val arr = ArrayType(innerFields, containsNull = false)
    val schema = new StructType().add("arr", arr, nullable = false)
    val df = Seq("a" -> 1).toDF("i", "j")
      .select(array(struct($"i", $"j")).as("arr"))
      .to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Seq(Row(1L))))
  }

  test("array element: compatible field nullability") {
    val innerFields = ArrayType(LongType, containsNull = false)
    val schema = new StructType().add("a", LongType).add("b", innerFields)
    val data = sql("VALUES (1, ARRAY(1, 2)), (NULL, NULL) as t(a, b)")
    assert(data.schema.fields(1).nullable)
    assert(!data.schema.fields(1).dataType.asInstanceOf[ArrayType].containsNull)
    val df = data.to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Seq(Row(1, Seq(1, 2)), Row(null, null)))
  }

  test("array element: incompatible array nullability") {
    val arr = ArrayType(IntegerType, containsNull = false)
    val schema = new StructType().add("arr", arr)
    val data = sql("SELECT i FROM VALUES 1, NULL as t(i)").select(array($"i").as("arr"))
    assert(data.schema.fields(0).dataType.asInstanceOf[ArrayType].containsNull)
    val e = intercept[SparkThrowable](data.to(schema))
    checkError(
      exception = e,
      condition = "NOT_NULL_CONSTRAINT_VIOLATION.ARRAY_ELEMENT",
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
      .to(schema)
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
      .to(schema)
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
      .to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Map(Row("b", "a") -> "a")))
  }

  test("map value: reorder inner fields by name") {
    val innerFields = new StructType().add("j", StringType).add("i", StringType)
    val m = MapType(StringType, innerFields, valueContainsNull = false)
    val schema = new StructType().add("map", m, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j")
      .select(map($"i", struct($"i", $"j")).as("map"))
      .to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Row(Map("a" -> Row("b", "a"))))
  }

  test("map value: compatible field nullability") {
    val innerFields = MapType(StringType, LongType, valueContainsNull = false)
    val schema = new StructType().add("a", LongType).add("b", innerFields)
    val data = sql("VALUES (1, MAP('a', 1, 'b', 2)), (NULL, NULL) as t(a, b)")
    assert(data.schema.fields(1).nullable)
    assert(!data.schema.fields(1).dataType.asInstanceOf[MapType].valueContainsNull)
    val df = data.to(schema)
    assert(df.schema == schema)
    checkAnswer(df, Seq(Row(1, Map("a" -> 1, "b" -> 2)), Row(null, null)))
  }

  test("map value: incompatible map nullability") {
    val m = MapType(StringType, StringType, valueContainsNull = false)
    val schema = new StructType().add("map", m, nullable = false)
    val data = Seq("a" -> null).toDF("i", "j").select(map($"i", $"j").as("map"))
    assert(data.schema.fields(0).dataType.asInstanceOf[MapType].valueContainsNull)
    val e = intercept[SparkThrowable](data.to(schema))
    checkError(
      exception = e,
      condition = "NOT_NULL_CONSTRAINT_VIOLATION.MAP_VALUE",
      parameters = Map("columnPath" -> "`map`"))
  }
}
