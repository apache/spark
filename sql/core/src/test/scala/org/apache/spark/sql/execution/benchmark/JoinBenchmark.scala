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

package org.apache.spark.sql.execution.benchmark

import org.scalatest.Assertions._

import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Benchmark to measure performance for joins.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/JoinBenchmark-results.txt".
 * }}}
 */
object JoinBenchmark extends SqlBasedBenchmark {

  def broadcastHashJoinLongKey(): Unit = {
    val N = 20 << 20
    val M = 1 << 16

    val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v"))
    codegenBenchmark("Join w long", N) {
      val df = spark.range(N).join(dim, (col("id") % M) === col("k"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinLongKeyWithDuplicates(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(spark.range(M).selectExpr("cast(id/10 as long) as k"))
    codegenBenchmark("Join w long duplicated", N) {
      val df = spark.range(N).join(dim, (col("id") % M) === col("k"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinTwoIntKey(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim2 = broadcast(spark.range(M)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    codegenBenchmark("Join w 2 ints", N) {
      val df = spark.range(N).join(dim2,
        (col("id") % M).cast(IntegerType) === col("k1")
          && (col("id") % M).cast(IntegerType) === col("k2"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinTwoLongKey(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim3 = broadcast(spark.range(M)
      .selectExpr("id as k1", "id as k2", "cast(id as string) as v"))

    codegenBenchmark("Join w 2 longs", N) {
      val df = spark.range(N).join(dim3,
        (col("id") % M) === col("k1") && (col("id") % M) === col("k2"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinTwoLongKeyWithDuplicates(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim4 = broadcast(spark.range(M)
      .selectExpr("cast(id/10 as long) as k1", "cast(id/10 as long) as k2"))

    codegenBenchmark("Join w 2 longs duplicated", N) {
      val df = spark.range(N).join(dim4,
        (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinOuterJoinLongKey(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v"))
    codegenBenchmark("outer join w long", N) {
      val df = spark.range(N).join(dim, (col("id") % M) === col("k"), "left")
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def broadcastHashJoinSemiJoinLongKey(): Unit = {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v"))
    codegenBenchmark("semi join w long", N) {
      val df = spark.range(N).join(dim, (col("id") % M) === col("k"), "leftsemi")
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
      df.noop()
    }
  }

  def sortMergeJoin(): Unit = {
    val N = 2 << 20
    codegenBenchmark("sort merge join", N) {
      val df1 = spark.range(N).selectExpr(s"id * 2 as k1")
      val df2 = spark.range(N).selectExpr(s"id * 3 as k2")
      val df = df1.join(df2, col("k1") === col("k2"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined)
      df.noop()
    }
  }

  def sortMergeJoinWithDuplicates(): Unit = {
    val N = 2 << 20
    codegenBenchmark("sort merge join with duplicates", N) {
      val df1 = spark.range(N)
        .selectExpr(s"(id * 15485863) % ${N*10} as k1")
      val df2 = spark.range(N)
        .selectExpr(s"(id * 15485867) % ${N*10} as k2")
      val df = df1.join(df2, col("k1") === col("k2"))
      assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined)
      df.noop()
    }
  }

  def shuffleHashJoin(): Unit = {
    val N: Long = 4 << 20
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000000",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false") {
      codegenBenchmark("shuffle hash join", N) {
        val df1 = spark.range(N).selectExpr(s"id as k1")
        val df2 = spark.range(N / 3).selectExpr(s"id * 3 as k2")
        val df = df1.join(df2, col("k1") === col("k2"))
        assert(df.queryExecution.sparkPlan.find(_.isInstanceOf[ShuffledHashJoinExec]).isDefined)
        df.noop()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Join Benchmark") {
      broadcastHashJoinLongKey()
      broadcastHashJoinLongKeyWithDuplicates()
      broadcastHashJoinTwoIntKey()
      broadcastHashJoinTwoLongKey()
      broadcastHashJoinTwoLongKeyWithDuplicates()
      broadcastHashJoinOuterJoinLongKey()
      broadcastHashJoinSemiJoinLongKey()
      sortMergeJoin()
      sortMergeJoinWithDuplicates()
      shuffleHashJoin()
    }
  }
}
