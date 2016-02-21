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

class DataFrameAggregatorSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  def sum[I, N : Numeric : Encoder](f: I => N): TypedColumn[I, N] =
    new SumOf(f).toColumn

  test("typed aggregation: TypedAggregator") {
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("k", "v")
    val sumFunc = sum[(String, Int), Int] _

    checkAnswer(
      df.groupBy($"k").agg(sumFunc(_._2)),
      Seq(Row("a", 30), Row("b", 3), Row("c", 1)))
  }

  test("typed aggregation: TypedAggregator, expr, expr") {
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("k", "v")
    val sumFunc = sum[(String, Int), Int] _

    checkAnswer(
      df.groupBy($"k").agg(
        sumFunc(_._2),
        expr("sum(v)").as[Long],
        count("*")),
      Seq(Row("a", 30, 30L, 2L), Row("b", 3, 3L, 2L), Row("c", 1, 1L, 1L)))
  }

  test("typed aggregation: complex case") {
    val df = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDF("k", "v")

    checkAnswer(
      df.groupBy($"k").agg(
        expr("avg(v)").as[Double],
        TypedAverage.toColumn),
      Seq(Row("a", 2.0, 2.0), Row("b", 3.0, 3.0)))
  }

  test("typed aggregation: complex result type") {
    val df = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDF("k", "v")

    checkAnswer(
      df.groupBy($"k").agg(
        expr("avg(v)").as[Double],
        ComplexResultAgg.toColumn),
      Seq(Row("a", 2.0, Row(2L, 4L)), Row("b", 3.0, Row(1L, 3L))))
  }

  test("typed aggregation: class input with reordering") {
    val df = Seq(AggData(1, "one")).toDF

    checkAnswer(
      df.groupBy($"b").agg(ClassInputAgg.toColumn),
      Seq(Row("one", 1)))
  }

  test("typed aggregation: complex input") {
    val df = Seq(AggData(1, "one"), AggData(2, "two")).toDF

    checkAnswer(
      df.groupBy($"b").agg(ComplexBufferAgg.toColumn),
      Seq(Row("one", 1), Row("two", 1)))
  }
}
