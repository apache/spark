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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class OptimizeSampleForRangePartitioningSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    val data = Seq(
      Seq(9, null, "x"),
      Seq(null, 3, "y"),
      Seq(1, 7, null),
      Seq(null, 0, null),
      Seq(1, null, null),
      Seq(null, null, "b"),
      Seq(5, 3, "z"),
      Seq(7, 1, "a"),
      Seq(5, 1, "b"),
      Seq(8, 2, "a"))
    val schema = new StructType().add("c1", IntegerType, nullable = true)
      .add("c2", IntegerType, nullable = true)
      .add("c3", StringType, nullable = true)
    val rdd = spark.sparkContext.parallelize(data)
    spark.createDataFrame(rdd.map(s => Row.fromSeq(s)), schema)
      .write.format("parquet").saveAsTable("t")
  }

  protected override def afterAll(): Unit = {
    spark.sql("DROP TABLE IF EXISTS t")
    super.afterAll()
  }

  private def checkQuery(query: => DataFrame, optimized: Boolean): Unit = {
    withSQLConf(SQLConf.OPTIMIZE_SAMPLE_FOR_RANGE_PARTITION_ENABLED.key -> "true") {
      val df = query
      assert(collect(df.queryExecution.executedPlan) {
        case shuffle @ ShuffleExchangeExec(r: RangePartitioning, _, _)
            if r.planForSample.isDefined => shuffle
      }.nonEmpty == optimized)

      var expected: Array[Row] = null
      withSQLConf(SQLConf.OPTIMIZE_SAMPLE_FOR_RANGE_PARTITION_ENABLED.key -> "false") {
        expected = query.collect()
      }
      checkAnswer(df, expected)
    }
  }

  test("Optimize range partitioning") {
    Seq(
      ("", "ORDER BY c1"),
      ("", " ORDER BY c1, c2"),
      ("/*+ repartition_by_range(c1) */", ""),
      ("/*+ repartition_by_range(c1, c2) */", "")).foreach { case (head, tail) =>
      checkQuery(
        sql(s"SELECT $head * FROM t $tail"),
        true)

      checkQuery(
        sql(s"SELECT $head * FROM t WHERE c1 > 4 $tail"),
        true)

      checkQuery(
        sql(s"SELECT $head * FROM t WHERE c2 > 1 $tail"),
        true)

      checkQuery(
        sql(s"SELECT $head * FROM (SELECT * FROM t WHERE c2 > 1 $tail) WHERE c1 > rand()"),
        true)
    }
  }

  test("Do not optimize range partitioning") {
    Seq(
      ("", "ORDER BY c1"),
      ("/*+ repartition_by_range(c1) */", "")).foreach { case (head, tail) =>
      // more than one shuffle
      checkQuery(
        sql(s"SELECT $head c1 FROM t GROUP BY c1 $tail"),
        false)
    }

    Seq(
      ("", "ORDER BY c1, c2"),
      ("/*+ repartition_by_range(c1, c2) */", "")).foreach { case (head, tail) =>
      // references of sort order is same with query output
      checkQuery(
        sql(s"SELECT $head c1, c2 FROM t $tail"),
        false)
    }
  }
}
