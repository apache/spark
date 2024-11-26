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

import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for xpath expressions.
 */
class XPathFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("xpath_boolean") {
    val df = Seq("<a><b>b</b></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_boolean(xml, 'a/b')"), Row(true))
    checkAnswer(df.select(xpath_boolean(col("xml"), lit("a/b"))), Row(true))
  }

  test("xpath_short, xpath_int, xpath_long") {
    val df = Seq("<a><b>1</b><b>2</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_short(xml, 'sum(a/b)')",
        "xpath_int(xml, 'sum(a/b)')",
        "xpath_long(xml, 'sum(a/b)')"),
      Row(3.toShort, 3, 3L))
    checkAnswer(
      df.select(
        xpath_short(col("xml"), lit("sum(a/b)")),
        xpath_int(col("xml"), lit("sum(a/b)")),
        xpath_long(col("xml"), lit("sum(a/b)"))),
      Row(3.toShort, 3, 3L))
  }

  test("xpath_float, xpath_double, xpath_number") {
    val df = Seq("<a><b>1.0</b><b>2.1</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_float(xml, 'sum(a/b)')",
        "xpath_double(xml, 'sum(a/b)')",
        "xpath_number(xml, 'sum(a/b)')"),
      Row(3.1.toFloat, 3.1, 3.1))
    checkAnswer(
      df.select(
        xpath_float(col("xml"), lit("sum(a/b)")),
        xpath_double(col("xml"), lit("sum(a/b)")),
        xpath_number(col("xml"), lit("sum(a/b)"))),
      Row(3.1.toFloat, 3.1, 3.1))
  }

  test("xpath_string") {
    val df = Seq("<a><b>b</b><c>cc</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_string(xml, 'a/c')"), Row("cc"))
    checkAnswer(df.select(xpath_string(col("xml"), lit("a/c"))), Row("cc"))
  }

  test("xpath") {
    val df = Seq("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath(xml, 'a/*/text()')"), Row(Seq("b1", "b2", "b3", "c1", "c2")))
    checkAnswer(df.select(xpath(col("xml"), lit("a/*/text()"))),
      Row(Seq("b1", "b2", "b3", "c1", "c2")))
  }

  test("The replacement of `xpath*` functions should be NullIntolerant") {
    def check(df: DataFrame, expected: Seq[Row]): Unit = {
      val filter = df.queryExecution
        .sparkPlan
        .find(_.isInstanceOf[FilterExec])
        .get.asInstanceOf[FilterExec]
      assert(filter.condition.find(_.isInstanceOf[IsNotNull]).nonEmpty)
      checkAnswer(df, expected)
    }
    withTable("t") {
      sql("CREATE TABLE t AS SELECT * FROM VALUES ('<a><b>1</b></a>'), (NULL) T(xml)")
      check(sql("SELECT * FROM t WHERE xpath_boolean(xml, 'a/b') = true"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_short(xml, 'a/b') = 1"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_int(xml, 'a/b') = 1"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_long(xml, 'a/b') = 1"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_float(xml, 'a/b') = 1"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_double(xml, 'a/b') = 1"),
        Seq(Row("<a><b>1</b></a>")))
      check(sql("SELECT * FROM t WHERE xpath_string(xml, 'a/b') = '1'"),
        Seq(Row("<a><b>1</b></a>")))
    }
    withTable("t") {
      sql("CREATE TABLE t AS SELECT * FROM VALUES " +
        "('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>'), (NULL) T(xml)")
      check(sql("SELECT * FROM t WHERE xpath(xml, 'a/b/text()') = array('b1', 'b2', 'b3')"),
        Seq(Row("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>")))
    }
  }
}
