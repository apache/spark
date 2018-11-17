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

import org.apache.spark.SparkException

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

trait FunctionsTests extends QueryTest with SharedSQLContext {
  import testImplicits._

  def testEmptyOptions(
      from: (Column, Column, java.util.Map[String, String]) => Column,
      input: String): Unit = {
    val df = Seq(input).toDS()
    val schema = "a int"

    checkAnswer(
      df.select(from($"value", lit(schema), Map[String, String]().asJava)),
      Row(Row(1)) :: Nil)
  }

  def testOptions(
      from: (Column, StructType, Map[String, String]) => Column,
      input: String): Unit = {
    val df = Seq(input).toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(from($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }

  def testSchemaInferring(
      schema_of_col: Column => Column,
      col: Column,
      schema_of_str: String => Column,
      str: String): Unit = {
    checkAnswer(
      spark.range(1).select(schema_of_col(col)),
      Seq(Row("struct<_c0:double,_c1:bigint>")))
    checkAnswer(
      spark.range(1).select(schema_of_str(str)),
      Seq(Row("struct<_c0:double,_c1:bigint>")))
  }

  def testSchemaInferringOpts(
      schema_of: (Column, java.util.Map[String, String]) => Column,
      options: Map[String, String],
      input: String): Unit = {
    val df = spark
      .range(1)
      .select(schema_of(lit(input), options.asJava))

    checkAnswer(df, Seq(Row("struct<_c0:double,_c1:bigint>")))
  }

  def testCorruptColumn(
      from: (Column, StructType, Map[String, String]) => Column,
      corrupted: String,
      corruptColumn: String): Unit = {
    val columnNameOfCorruptRecord = "_unparsed"
    val df = Seq(corrupted).toDS()
    val schema = new StructType().add("b", TimestampType)
    val schemaWithCorrField1 = schema.add(columnNameOfCorruptRecord, StringType)
    val df2 = df
      .select(from($"value", schemaWithCorrField1, Map(
        "mode" -> "Permissive", "columnNameOfCorruptRecord" -> columnNameOfCorruptRecord)))

    checkAnswer(df2, Seq(Row(Row(null, corruptColumn))))
  }

  def testToStruct(to: Column => Column, expected: String): Unit = {
    val df = Seq(Tuple1(Tuple1(1))).toDF("a")

    checkAnswer(df.select(to($"a")), Row(expected) :: Nil)
  }

  def testToStructOpts(
      to: (Column, java.util.Map[String, String]) => Column,
      input: String,
      expected: String): Unit = {
    val df = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf(input)))).toDF("a")
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm").asJava

    checkAnswer(df.select(to($"a", options)), Row(expected) :: Nil)
  }

  def testModesInFrom(
      from: (Column, StructType, Map[String, String]) => Column,
      input: String,
      badRec: String): Unit = {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val df = Seq(badRec, input).toDS()

      checkAnswer(
        df.select(from($"value", schema, Map("mode" -> "PERMISSIVE"))),
        Row(Row(null, null, badRec)) :: Row(Row(2, 12, null)) :: Nil)

      val exception1 = intercept[SparkException] {
        df.select(from($"value", schema, Map("mode" -> "FAILFAST"))).collect()
      }.getMessage
      assert(exception1.contains(
        "Malformed records are detected in record parsing. Parse Mode: FAILFAST."))

      val exception2 = intercept[SparkException] {
        df.select(from($"value", schema, Map("mode" -> "DROPMALFORMED")))
          .collect()
      }.getMessage
      assert(exception2.contains(
        "doesn't support the DROPMALFORMED mode. Acceptable modes are PERMISSIVE and FAILFAST."))
    }
  }
}
