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

/**
 * A test suite to test DataFrame/SQL functionalities with complex types (i.e. array, struct, map).
 */
class DataFrameComplexTypeSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("primitive type on array") {
    val rows = sparkContext.parallelize(Seq(1, 2), 1).toDF("v").
      selectExpr("Array(v + 2, v + 3)").collect
    QueryTest.sameRows(Seq(Row(Array(3, 4)), Row(Array(4, 5))), rows.toSeq)
  }

  test("primitive type and null on array") {
    val rows = sparkContext.parallelize(Seq(1, 2), 1).toDF("v").
      selectExpr("Array(v + 2, null, v + 3)").collect
    QueryTest.sameRows(Seq(Row(Array(3, null, 4)), Row(Array(4, null, 5))), rows.toSeq)
  }

  test("array with null on array") {
    val rows = sparkContext.parallelize(Seq(1, 2), 1).toDF("v").
      selectExpr("Array(Array(v, v + 1)," +
                        "null," +
                        "Array(v, v - 1))").collect
    QueryTest.sameRows(Seq(
      Row(Array(Array(1, 2), null, Array(3, 4))),
      Row(Array(Array(2, 3), null, Array(4, 5)))), rows.toSeq)
  }

  test("primitive type on map") {
    val rows = sparkContext.parallelize(Seq(1, 2), 1).toDF("v").
      selectExpr("map(v + 3, v + 4)").collect
    QueryTest.sameRows(Seq(Row(Map(4 -> 5)), Row(Map(5 -> 6))), rows.toSeq)
  }

  test("map with null value on map") {
    val rows = sparkContext.parallelize(Seq(1, 2), 1).toDF("v").
      selectExpr("map(v, null)").collect
    QueryTest.sameRows(Seq(Row(Map(1 -> null)), Row(Map(2 -> null))), rows.toSeq)
  }

  test("UDF on struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(struct($"a").as("s")).select(f($"s.a")).collect()
  }

  test("UDF on named_struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.selectExpr("named_struct('a', a) s").select(f($"s.a")).collect()
  }

  test("UDF on array") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(array($"a").as("s")).select(f($"s".getItem(0))).collect()
  }

  test("UDF on map") {
    val f = udf((a: String) => a)
    val df = Seq("a" -> 1).toDF("a", "b")
    df.select(map($"a", $"b").as("s")).select(f($"s".getItem("a"))).collect()
  }

  test("SPARK-12477 accessing null element in array field") {
    val df = sparkContext.parallelize(Seq((Seq("val1", null, "val2"),
      Seq(Some(1), None, Some(2))))).toDF("s", "i")
    val nullStringRow = df.selectExpr("s[1]").collect()(0)
    assert(nullStringRow == org.apache.spark.sql.Row(null))
    val nullIntRow = df.selectExpr("i[1]").collect()(0)
    assert(nullIntRow == org.apache.spark.sql.Row(null))
  }
}
