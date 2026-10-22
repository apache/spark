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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * The base class for synthetic benchmark for nested schema pruning performance.
 */
abstract class NestedSchemaPruningBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  val dataSourceName: String
  val benchmarkName: String

  protected val N = 1000000
  protected val numIters = 10

  // We use `col1 BIGINT, col2 STRUCT<_1: BIGINT, _2: STRING>,
  // col3 ARRAY<STRUCT<_1: BIGINT, _2: STRING>>` as a test schema.
  // col1, col2._1 and col3._1 are used for comparison. col2._2 and col3._2 mimics the burden
  // for the other columns
  private val df = spark
    .range(N * 10)
    .sample(false, 0.1)
    .map { x =>
      val col3 = (0 until 5).map(i => (x + i, s"$x".repeat(5)))
      (x, (x, s"$x".repeat(100)), col3)
    }.toDF("col1", "col2", "col3")

  private def addCase(benchmark: Benchmark, name: String, sql: String): Unit = {
    benchmark.addCase(name) { _ =>
      spark.sql(sql).noop()
    }
  }

  protected def selectBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Selection", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column", "SELECT col1 FROM (SELECT col1 FROM t1)")
      addCase(benchmark, "Nested column", "SELECT col2._1 FROM (SELECT col2 FROM t2)")
      addCase(benchmark, "Nested column in array", "SELECT col3._1 FROM (SELECT col3 FROM t3)")

      benchmark.run()
    }
  }

  protected def limitBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Limiting", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 LIMIT ${Int.MaxValue})")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 LIMIT ${Int.MaxValue})")
      addCase(benchmark, "Nested column in array",
        s"SELECT col3._1 FROM (SELECT col3 FROM t3 LIMIT ${Int.MaxValue})")

      benchmark.run()
    }
  }

  protected def repartitionBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Repartitioning", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT /*+ REPARTITION(1) */ col1 FROM t1)")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT /*+ REPARTITION(1) */ col2 FROM t2)")
      addCase(benchmark, "Nested column in array",
        s"SELECT col3._1 FROM (SELECT /*+ REPARTITION(1) */ col3 FROM t3)")

      benchmark.run()
    }
  }

  protected def repartitionByExprBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Repartitioning by exprs", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 DISTRIBUTE BY col1)")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 DISTRIBUTE BY col2._1)")
      addCase(benchmark, "Nested column in array",
        s"SELECT col3._1 FROM (SELECT col3 FROM t3 DISTRIBUTE BY col3._1)")

      benchmark.run()
    }
  }

  protected def sampleBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Sample", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 TABLESAMPLE(100 percent))")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 TABLESAMPLE(100 percent))")
      addCase(benchmark, "Nested column in array",
        s"SELECT col3._1 FROM (SELECT col3 FROM t3 TABLESAMPLE(100 percent))")

      benchmark.run()
    }
  }

  protected def sortBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Sorting", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column", "SELECT col1 FROM t1 ORDER BY col1")
      addCase(benchmark, "Nested column", "SELECT col2._1 FROM t2 ORDER BY col2._1")
      addCase(benchmark, "Nested column in array", "SELECT col3._1 FROM t3 ORDER BY col3._1")

      benchmark.run()
    }
  }

  protected def explodeBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // col4 ARRAY<STRUCT<_1: BIGINT, _2: BIGINT, _3: STRING>>: _1 and _2 are read through an
      // explode, _3 mimics the burden for the other fields
      spark.range(numRows)
        .map(x => (0 until 5).map(i => (x + i, x * i, s"$x".repeat(100))))
        .toDF("col4")
        .write.format(dataSourceName).save(path + "/4")
      spark.read.format(dataSourceName).load(path + "/4").createOrReplaceTempView("t4")

      // col5 ARRAY<STRUCT<_1: BIGINT, _2: ARRAY<STRUCT<_1: BIGINT, _2: STRING>>>>: _2._1 is read
      // through two chained explodes, _2._2 mimics the burden for the other fields
      spark.range(numRows / 5)
        .map(x => (0 until 5).map(i => (x + i, (0 until 5).map(j => (x * j, s"$x".repeat(20))))))
        .toDF("col5")
        .write.format(dataSourceName).save(path + "/5")
      spark.read.format(dataSourceName).load(path + "/5").createOrReplaceTempView("t5")

      val benchmark = new Benchmark(s"Explode", numRows, numIters, output = output)
      val cases = Seq(
        "Two nested fields of an exploded array" ->
          "SELECT e._1, e._2 FROM t4 LATERAL VIEW explode(col4) AS e",
        "A nested field through two chained explodes" ->
          "SELECT f._1 FROM t5 LATERAL VIEW explode(col5) AS e LATERAL VIEW explode(e._2) AS f")
      for ((name, sql) <- cases; enabled <- Seq(false, true)) {
        benchmark.addCase(s"$name, through generate $enabled") { _ =>
          withSQLConf(
              SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_ENABLED.key -> enabled.toString) {
            spark.sql(sql).noop()
          }
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark(benchmarkName) {
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        selectBenchmark(N, numIters)
        limitBenchmark(N, numIters)
        repartitionBenchmark(N, numIters)
        repartitionByExprBenchmark(N, numIters)
        sampleBenchmark(N, numIters)
        sortBenchmark(N, numIters)
        explodeBenchmark(N, numIters)
      }
    }
  }
}
