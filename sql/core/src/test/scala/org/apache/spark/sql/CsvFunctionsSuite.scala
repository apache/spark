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
import org.apache.spark.sql.types._

class CsvFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("from_csv with empty options") {
    val df = Seq("1").toDS()
    val schema = "a int"

    checkAnswer(
      df.select(from_csv($"value", lit(schema), Map[String, String]().asJava)),
      Row(Row(1)) :: Nil)
  }

  test("from_csv with option") {
    val df = Seq("26/08/2015 18:00").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(from_csv($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }


  test("checking the columnNameOfCorruptRecord option") {
    val columnNameOfCorruptRecord = "_unparsed"
    val df = Seq("0,2013-111-11 12:13:14", "1,1983-08-04").toDS()
    val schema = new StructType().add("a", IntegerType).add("b", TimestampType)
    val schemaWithCorrField1 = schema.add(columnNameOfCorruptRecord, StringType)
    val df2 = df
      .select(from_csv($"value", schemaWithCorrField1, Map(
        "mode" -> "Permissive", "columnNameOfCorruptRecord" -> columnNameOfCorruptRecord)))

    checkAnswer(df2, Seq(
      Row(Row(null, null, "0,2013-111-11 12:13:14")),
      Row(Row(1, java.sql.Date.valueOf("1983-08-04"), null))))
  }

  test("schema_of_csv - infers schemas") {
    checkAnswer(
      spark.range(1).select(schema_of_csv(lit("0.1,1"))),
      Seq(Row("struct<_c0:double,_c1:int>")))
    checkAnswer(
      spark.range(1).select(schema_of_csv("0.1,1")),
      Seq(Row("struct<_c0:double,_c1:int>")))
  }

  test("schema_of_csv - infers schemas using options") {
    val df = spark.range(1)
      .select(schema_of_csv(lit("0.1 1"), Map("sep" -> " ").asJava))
    checkAnswer(df, Seq(Row("struct<_c0:double,_c1:int>")))
  }
}
