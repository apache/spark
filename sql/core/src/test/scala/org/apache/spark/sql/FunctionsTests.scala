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

import scala.collection.JavaConverters._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StructType, TimestampType}

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
}
