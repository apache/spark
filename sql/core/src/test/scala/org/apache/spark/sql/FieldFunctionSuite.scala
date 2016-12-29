package org.apache.spark.sql

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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.functions._

class FieldFunctionSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("field") {
    val df = Seq(("花花世界", "x", "b", "花花世界")).toDF("a", "b", "c", "d")
    checkAnswer(df.select(field($"a", $"b", $"c", $"d")), Row(3))
    checkAnswer(df.selectExpr("field('花花世界', 's', 'b', '花花世界')"), Row(3))

    val df2 = Seq(("aaa", "aaa", "aaa", "花花世界")).toDF("a", "b", "c", "d")
    checkAnswer(df2.select(field($"a", $"b", $"c", $"d")), Row(1))
    checkAnswer(df2.selectExpr("field('aaa', 'aaa', 'aaa', '花花世界')"), Row(1))

    val df3 = Seq(("", "", "", "花花世界")).toDF("a", "b", "c", "d")
    checkAnswer(df3.select(field($"a", $"b", $"c", $"d")), Row(1))
    checkAnswer(df3.selectExpr("field('', '', '', '花花世界')"), Row(1))

    val df4 = Seq((true, false, true, true)).toDF("a", "b", "c", "d")
    checkAnswer(df4.select(field($"a", $"b", $"c", $"d")), Row(2))
    checkAnswer(df4.selectExpr("field(true, false, true, true)"), Row(2))

    val df5 = Seq((1, 2, 3, 1)).toDF("a", "b", "c", "d")
    checkAnswer(df5.select(field($"a", $"b", $"c", $"d")), Row(3))
    checkAnswer(df5.selectExpr("field(1, 2, 3, 1)"), Row(3))

    val df6 = Seq((1.222, 1.224, 1.221, 1.222)).toDF("a", "b", "c", "d")
    checkAnswer(df6.select(field($"a", $"b", $"c", $"d")), Row(3))
    checkAnswer(df6.selectExpr("field(1.222, 1.224, 1.221, 1.222)"), Row(3))

    val df7 = Seq((new Timestamp(2016, 12, 27, 14, 22, 1, 1), new Timestamp(1988, 6, 3, 1, 1, 1, 1), new Timestamp(1990, 6, 5, 1, 1, 1, 1), new Timestamp(2016, 12, 27, 14, 22, 1, 1))).toDF("a", "b", "c", "d")
    checkAnswer(df7.select(field($"a", $"b", $"c", $"d")), Row(3))

    val df8 = Seq((new Date(1949, 1, 1), new Date(1949, 1, 1), new Date(1979, 1, 1), new Date(2016, 1, 1))).toDF("a", "b", "c", "d")
    checkAnswer(df8.select(field($"a", $"b", $"c", $"d")), Row(1))

    val df9 = Seq((999, 1.224, "999", 999)).toDF("a", "b", "c", "d")
    checkAnswer(df9.select(field($"a", $"b", $"c", $"d")), Row(3))
    checkAnswer(df9.selectExpr("field(999, 1.224, '999', 999)"), Row(3))

    val df10 = Seq(("Cannot find me", "abc", "bcd", "花花世界")).toDF("a", "b", "c", "d")
    checkAnswer(df10.select(field($"a", $"b", $"c", $"d")), Row(0))
    checkAnswer(df10.selectExpr("field('Cannot find me', 'abc', 'bcd', '花花世界')"), Row(0))
  }
}
