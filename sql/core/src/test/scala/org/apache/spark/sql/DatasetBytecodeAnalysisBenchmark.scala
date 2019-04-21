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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf

object DatasetBytecodeAnalysisBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  case class Data(l: Long, s: String)
  case class NestedData(i: Int, data: Data)
  case class Event(userId: Long, deviceId: String, counts: Long) {
    def doSmth(): Boolean = true
  }

  private def runBackToBackMapsBenchmark(numRows: Long, minNumIters: Int): Unit = {
    val benchmarkName = "back-to-back maps (longs)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    benchmark.addCase("DataFrame") { _ =>
      val res = spark.range(1, numRows).toDF("l")
        .select($"l" + 1 as "l")
        .select($"l" + 2 as "l")
        .select($"l" + 3 as "l")
        .select($"l" + 4 as "l")
        .select($"l" + 5 as "l")
        .select($"l" + 6 as "l")
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .map(l => l + 1)
        .map(l => l + 2)
        .map(l => l + 3)
        .map(l => l + 4)
        .map(l => l + 5)
        .map(l => l + 6)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset (w/o bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetQuery()
      }
    }

    benchmark.addCase("Dataset (with bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetQuery()
      }
    }

    benchmark.run()
  }

  private def runFilterMapFilterBenchmark(numRows: Long, minNumIters: Int): Unit = {
    val benchmarkName = "filter -> map -> filter (longs)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    benchmark.addCase("DataFrame") { _ =>
      val res = spark.range(1, numRows).toDF("l")
        .filter($"l" >= 5000000)
        .select($"l" + 1 as "l")
        .filter($"l" <= 5000001)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .filter(l => l >= 5000000)
        .map(l => l + 1)
        .filter(l => l <= 5000001)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset (w/o bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetQuery()
      }
    }

    benchmark.addCase("Dataset (with bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetQuery()
      }
    }

    benchmark.run()
  }

  private def runTrivialMapCaseClassesBenchmark(numRows: Long, minNumIters: Int): Unit = {
    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .map(l => Data(l, l.toString))
        .map(_.l)
        .map(l => l + 13)
        .map(l => l + 22)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }
    runDatasetBenchmark(datasetQuery, "trivial map transforms (case classes)", numRows, minNumIters)
  }

  private def runMapCaseClassesBenchmark(numRows: Long, minNumIters: Int): Unit = {
    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .map(n => Data(n, n.toString))
        .map(_.l)
        .map(l => l + 13)
        .map(l => Data(l, l.toString))
        .map(data => Event(data.l, data.s, data.l))
      res.queryExecution.toRdd.foreach(_ => Unit)
    }
    runDatasetBenchmark(datasetQuery, "map transforms (case classes)", numRows, minNumIters)
  }

  private def runMapFilterCaseClassesBenchmark(numRows: Long, minNumIters: Int): Unit = {
    val func = (l: Long) => Event(l, l.toString, 0)
    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .map(func)
        .map(event => Event(event.userId, event.deviceId, event.counts + 1))
        .filter(_.userId < 10)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }
    runDatasetBenchmark(datasetQuery, "map -> filter (case classes)", numRows, minNumIters)
  }

  private def runNestedDataBenchmark(numRows: Long, minNumIters: Int): Unit = {
    def datasetQuery(): Unit = {
      val res = spark.range(1, numRows).as[Long]
        .map(l => Data(l, l.toString))
        .map(data => NestedData(data.l.toInt + 10, data))
        .filter(nestedData => nestedData.data.l < 5)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }
    runDatasetBenchmark(datasetQuery, "nested data", numRows, minNumIters)
  }

  private def runDatasetBenchmark(
      closure: () => Unit,
      name: String,
      numRows: Long,
      minNumIters: Int): Unit = {

    val benchmark = new Benchmark(name, numRows, minNumIters)

    benchmark.addCase("Dataset (w/o bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        closure()
      }
    }

    benchmark.addCase("Dataset (with bytecode analysis)") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        closure()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows = 100000000
    val minNumIters = 5

    runBackToBackMapsBenchmark(numRows, minNumIters)
    runFilterMapFilterBenchmark(numRows, minNumIters)
    runTrivialMapCaseClassesBenchmark(numRows, minNumIters)
    runMapCaseClassesBenchmark(numRows, minNumIters)
    runMapFilterCaseClassesBenchmark(numRows, minNumIters)
    runNestedDataBenchmark(numRows, minNumIters)
  }
}
