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

  // We use `col1 BIGINT, col2 STRUCT<_1: BIGINT, _2: STRING>` as a test schema.
  // col1 and col2._1 is used for comparision. col2._2 mimics the burden for the other columns
  private val df = spark
    .range(N * 10)
    .sample(false, 0.1)
    .map(x => (x, (x, s"$x" * 100)))
    .toDF("col1", "col2")

  private def addCase(benchmark: Benchmark, name: String, sql: String): Unit = {
    benchmark.addCase(name) { _ =>
      spark.sql(sql).write.format("noop").save()
    }
  }

  protected def selectBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Selection", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column", "SELECT col1 FROM (SELECT col1 FROM t1)")
      addCase(benchmark, "Nested column", "SELECT col2._1 FROM (SELECT col2 FROM t2)")

      benchmark.run()
    }
  }

  protected def limitBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Limiting", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 LIMIT ${Int.MaxValue})")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 LIMIT ${Int.MaxValue})")

      benchmark.run()
    }
  }

  protected def repartitionBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Repartitioning", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT /*+ REPARTITION(1) */ col1 FROM t1)")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT /*+ REPARTITION(1) */ col2 FROM t2)")

      benchmark.run()
    }
  }

  protected def repartitionByExprBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Repartitioning by exprs", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 DISTRIBUTE BY col1)")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 DISTRIBUTE BY col2._1)")

      benchmark.run()
    }
  }

  protected def sampleBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Sample", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column",
        s"SELECT col1 FROM (SELECT col1 FROM t1 TABLESAMPLE(100 percent))")
      addCase(benchmark, "Nested column",
        s"SELECT col2._1 FROM (SELECT col2 FROM t2 TABLESAMPLE(100 percent))")

      benchmark.run()
    }
  }

  protected def sortBenchmark(numRows: Int, numIters: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2).foreach { i =>
        df.write.format(dataSourceName).save(path + s"/$i")
        spark.read.format(dataSourceName).load(path + s"/$i").createOrReplaceTempView(s"t$i")
      }

      val benchmark = new Benchmark(s"Sorting", numRows, numIters, output = output)

      addCase(benchmark, "Top-level column", "SELECT col1 FROM t1 ORDER BY col1")
      addCase(benchmark, "Nested column", "SELECT col2._1 FROM t2 ORDER BY col2._1")

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
      }
    }
  }
}
