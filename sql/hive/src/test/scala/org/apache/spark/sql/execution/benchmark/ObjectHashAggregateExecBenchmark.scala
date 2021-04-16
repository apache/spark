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

import scala.concurrent.duration._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.hive.execution.TestingTypedCount
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

/**
 * Benchmark to measure hash based aggregation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark catalyst test jar>,<spark core test jar>,<spark sql test jar>
 *        <spark hive test jar>
 *   2. build/sbt "hive/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "hive/test:runMain <this class>"
 *      Results will be written to "benchmarks/ObjectHashAggregateExecBenchmark-results.txt".
 * }}}
 */
object ObjectHashAggregateExecBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = TestHive.sparkSession

  private val sql = spark.sql _
  import spark.implicits._

  private def hiveUDAFvsSparkAF(N: Int): Unit = {
    val benchmark = new Benchmark(
      name = "hive udaf vs spark af",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true,
      output = output
    )

    sql(
      s"CREATE TEMPORARY FUNCTION hive_percentile_approx AS '" +
        s"${classOf[GenericUDAFPercentileApprox].getName}'"
    )

    spark.range(N).createOrReplaceTempView("t")

    benchmark.addCase("hive udaf w/o group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        sql("SELECT hive_percentile_approx(id, 0.5) FROM t").noop()
      }
    }

    benchmark.addCase("spark af w/o group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        sql("SELECT percentile_approx(id, 0.5) FROM t").noop()
      }
    }

    benchmark.addCase("hive udaf w/ group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        sql(
          s"SELECT hive_percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)"
        ).noop()
      }
    }

    benchmark.addCase("spark af w/ group by w/o fallback") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        sql(s"SELECT percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)")
          .noop()
      }
    }

    benchmark.addCase("spark af w/ group by w/ fallback") { _ =>
      withSQLConf(
        SQLConf.USE_OBJECT_HASH_AGG.key -> "true",
        SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "2") {
        sql(s"SELECT percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)")
          .noop()
      }
    }

    benchmark.run()
  }

  private def objectHashAggregateExecVsSortAggregateExecUsingTypedCount(N: Int): Unit = {
    val benchmark = new Benchmark(
      name = "object agg v.s. sort agg",
      valuesPerIteration = N,
      minNumIters = 1,
      warmupTime = 10.seconds,
      minTime = 45.seconds,
      outputPerIteration = true,
      output = output
    )

    def typed_count(column: Column): Column =
      Column(TestingTypedCount(column.expr).toAggregateExpression())

    val df = spark.range(N)

    benchmark.addCase("sort agg w/ group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).noop()
      }
    }

    benchmark.addCase("object agg w/ group by w/o fallback") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).noop()
      }
    }

    benchmark.addCase("object agg w/ group by w/ fallback") { _ =>
      withSQLConf(
        SQLConf.USE_OBJECT_HASH_AGG.key -> "true",
        SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "2") {
        df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).noop()
      }
    }

    benchmark.addCase("sort agg w/o group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        df.select(typed_count($"id")).noop()
      }
    }

    benchmark.addCase("object agg w/o group by w/o fallback") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        df.select(typed_count($"id")).noop()
      }
    }

    benchmark.run()
  }

  private def objectHashAggregateExecVsSortAggregateExecUsingPercentileApprox(N: Int): Unit = {
    val benchmark = new Benchmark(
      name = "object agg v.s. sort agg",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 15.seconds,
      minTime = 45.seconds,
      outputPerIteration = true,
      output = output
    )

    val df = spark.range(N).coalesce(1)

    benchmark.addCase("sort agg w/ group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).noop()
      }
    }

    benchmark.addCase("object agg w/ group by w/o fallback") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).noop()
      }
    }

    benchmark.addCase("object agg w/ group by w/ fallback") { _ =>
      withSQLConf(
        SQLConf.USE_OBJECT_HASH_AGG.key -> "true",
        SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "2") {
        df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).noop()
      }
    }

    benchmark.addCase("sort agg w/o group by") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        df.select(percentile_approx($"id", 0.5)).noop()
      }
    }

    benchmark.addCase("object agg w/o group by w/o fallback") { _ =>
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
        df.select(percentile_approx($"id", 0.5)).noop()
      }
    }

    benchmark.run()
  }

  private def percentile_approx(
      column: Column, percentage: Double, isDistinct: Boolean = false): Column = {
    val approxPercentile = new ApproximatePercentile(column.expr, Literal(percentage))
    Column(approxPercentile.toAggregateExpression(isDistinct))
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Hive UDAF vs Spark AF") {
      hiveUDAFvsSparkAF(2 << 15)
    }

    runBenchmark("ObjectHashAggregateExec vs SortAggregateExec - typed_count") {
      objectHashAggregateExecVsSortAggregateExecUsingTypedCount(1024 * 1024 * 100)
    }

    runBenchmark("ObjectHashAggregateExec vs SortAggregateExec - percentile_approx") {
      objectHashAggregateExecVsSortAggregateExecUsingPercentileApprox(2 << 20)
    }
  }
}
