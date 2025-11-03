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

import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

@deprecated("This test suite will be removed.", "3.0.0")
class DeprecatedDatasetAggregatorSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("typed aggregation: TypedAggregator") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(typed.sum(_._2)),
      ("a", 30.0), ("b", 3.0), ("c", 1.0))
  }

  test("typed aggregation: TypedAggregator, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(
        typed.sum(_._2),
        expr("sum(_2)").as[Long],
        count("*")),
      ("a", 30.0, 30L, 2L), ("b", 3.0, 3L, 2L), ("c", 1.0, 1L, 1L))
  }

  test("typed aggregation: in project list") {
    val ds = Seq(1, 3, 2, 5).toDS()

    checkDataset(
      ds.select(typed.sum((i: Int) => i)),
      11.0)
    checkDataset(
      ds.select(typed.sum((i: Int) => i), typed.sum((i: Int) => i * 2)),
      11.0 -> 22.0)
  }

  test("typed aggregate: avg, count, sum") {
    val ds = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDS()
    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(
        typed.avg(_._2), typed.count(_._2), typed.sum(_._2), typed.sumLong(_._2)),
      ("a", 2.0, 2L, 4.0, 4L), ("b", 3.0, 1L, 3.0, 3L))
  }

  test("spark-15114 shorter system generated alias names") {
    val ds = Seq(1, 3, 2, 5).toDS()
    assert(ds.select(typed.sum((i: Int) => i)).columns.head === "TypedSumDouble(int)")
    val ds2 = ds.select(typed.sum((i: Int) => i), typed.avg((i: Int) => i))
    assert(ds2.columns.head === "TypedSumDouble(int)")
    assert(ds2.columns.last === "TypedAverage(int)")
    val df = Seq(1 -> "a", 2 -> "b", 3 -> "b").toDF("i", "j")
    assert(df.groupBy($"j").agg(RowAgg.toColumn).columns.last ==
      "RowAgg(org.apache.spark.sql.Row)")
    assert(df.groupBy($"j").agg(RowAgg.toColumn as "agg1").columns.last == "agg1")
  }
}
