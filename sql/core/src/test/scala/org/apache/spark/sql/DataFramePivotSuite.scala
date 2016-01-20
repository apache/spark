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

class DataFramePivotSuite extends QueryTest with SharedSQLContext{
  import testImplicits._

  test("pivot courses with literals") {
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings")),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }

  test("pivot year with literals") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).agg(sum($"earnings")),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot courses with literals and multiple aggregations") {
    checkAnswer(
      courseSales.groupBy($"year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings"), avg($"earnings")),
      Row(2012, 15000.0, 7500.0, 20000.0, 20000.0) ::
        Row(2013, 48000.0, 48000.0, 30000.0, 30000.0) :: Nil
    )
  }

  test("pivot year with string values (cast)") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq("2012", "2013")).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot year with int values") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot courses with no values") {
    // Note Java comes before dotNet in sorted order
    checkAnswer(
      courseSales.groupBy("year").pivot("course").agg(sum($"earnings")),
      Row(2012, 20000.0, 15000.0) :: Row(2013, 30000.0, 48000.0) :: Nil
    )
  }

  test("pivot year with no values") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year").agg(sum($"earnings")),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot max values enforced") {
    sqlContext.conf.setConf(SQLConf.DATAFRAME_PIVOT_MAX_VALUES, 1)
    intercept[AnalysisException](
      courseSales.groupBy("year").pivot("course")
    )
    sqlContext.conf.setConf(SQLConf.DATAFRAME_PIVOT_MAX_VALUES,
      SQLConf.DATAFRAME_PIVOT_MAX_VALUES.defaultValue.get)
  }

  test("pivot with UnresolvedFunction") {
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg("earnings" -> "sum"),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }
}
