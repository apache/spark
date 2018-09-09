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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class CsvFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  val noOptions = Map[String, String]()

  test("from_csv") {
    val df = Seq("1").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_csv($"value", schema, noOptions)),
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

  test("from_csv missing columns") {
    val df = Seq("1").toDS()
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)

    checkAnswer(
      df.select(from_csv($"value", schema, noOptions)),
      Row(Row(1, null)) :: Nil)
  }

  test("from_csv invalid CSV") {
    val df = Seq("???").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_csv($"value", schema, noOptions)),
      Row(Row(null)) :: Nil)
  }

  test("Support from_csv in SQL") {
    val df1 = Seq("1").toDS()
    checkAnswer(
      df1.selectExpr("from_csv(value, 'a INT')"),
      Row(Row(1)) :: Nil)
  }
}
