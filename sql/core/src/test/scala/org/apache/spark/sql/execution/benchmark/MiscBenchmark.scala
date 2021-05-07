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
 * Benchmark to measure whole stage codegen performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/MiscBenchmark-results.txt".
 * }}}
 */
object MiscBenchmark extends SqlBasedBenchmark {

  def filterAndAggregateWithoutGroup(numRows: Long): Unit = {
    runBenchmark("filter & aggregate without group") {
      codegenBenchmark("range/filter/sum", numRows) {
        spark.range(numRows).filter("(id & 1) = 1").groupBy().sum().noop()
      }
    }
  }

  def limitAndAggregateWithoutGroup(numRows: Long): Unit = {
    runBenchmark("range/limit/sum") {
      codegenBenchmark("range/limit/sum", numRows) {
        spark.range(numRows).limit(1000000).groupBy().sum().noop()
      }
    }
  }

  def sample(numRows: Int): Unit = {
    runBenchmark("sample") {
      codegenBenchmark("sample with replacement", numRows) {
        spark.range(numRows).sample(withReplacement = true, 0.01).groupBy().sum().noop()
      }

      codegenBenchmark("sample without replacement", numRows) {
        spark.range(numRows).sample(withReplacement = false, 0.01).groupBy().sum().noop()
      }
    }
  }

  def collect(numRows: Int): Unit = {
    runBenchmark("collect") {
      val benchmark = new Benchmark("collect", numRows, output = output)
      benchmark.addCase("collect 1 million") { iter =>
        spark.range(numRows).collect()
      }
      benchmark.addCase("collect 2 millions") { iter =>
        spark.range(numRows * 2).collect()
      }
      benchmark.addCase("collect 4 millions") { iter =>
        spark.range(numRows * 4).collect()
      }
      benchmark.run()
    }
  }

  def collectLimit(numRows: Int): Unit = {
    runBenchmark("collect limit") {
      val benchmark = new Benchmark("collect limit", numRows, output = output)
      benchmark.addCase("collect limit 1 million") { iter =>
        spark.range(numRows * 4).limit(numRows).collect()
      }
      benchmark.addCase("collect limit 2 millions") { iter =>
        spark.range(numRows * 4).limit(numRows * 2).collect()
      }
      benchmark.run()
    }
  }

  def explode(numRows: Int): Unit = {
    runBenchmark("generate explode") {
      codegenBenchmark("generate explode array", numRows) {
        val df = spark.range(numRows).selectExpr(
          "id as key",
          "array(rand(), rand(), rand(), rand(), rand()) as values")
        df.selectExpr("key", "explode(values) value").noop()
      }

      codegenBenchmark("generate explode map", numRows) {
        val df = spark.range(numRows).selectExpr(
          "id as key",
          "map('a', rand(), 'b', rand(), 'c', rand(), 'd', rand(), 'e', rand()) pairs")
        df.selectExpr("key", "explode(pairs) as (k, v)").noop()
      }

      codegenBenchmark("generate posexplode array", numRows) {
        val df = spark.range(numRows).selectExpr(
          "id as key",
          "array(rand(), rand(), rand(), rand(), rand()) as values")
        df.selectExpr("key", "posexplode(values) as (idx, value)").noop()
      }

      codegenBenchmark("generate inline array", numRows) {
        val df = spark.range(numRows).selectExpr(
          "id as key",
          "array((rand(), rand()), (rand(), rand()), (rand(), 0.0d)) as values")
        df.selectExpr("key", "inline(values) as (r1, r2)").noop()
      }

      val M = 60000
      codegenBenchmark("generate big struct array", M) {
        import spark.implicits._
        val df = spark.sparkContext.parallelize(Seq(("1",
          Array.fill(M)({
            val i = math.random
            (i.toString, (i + 1).toString, (i + 2).toString, (i + 3).toString)
          })))).toDF("col", "arr")

        df.selectExpr("*", "explode(arr) as arr_col")
          .select("col", "arr_col.*").noop()
      }

      withSQLConf(SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key -> "true") {
        codegenBenchmark("generate big nested struct array", M) {
          import spark.implicits._
          val df = spark.sparkContext.parallelize(Seq(("1",
            Array.fill(M)({
              val i = math.random
              (i.toString, (i + 1).toString, (i + 2).toString, (i + 3).toString)
            })))).toDF("col", "arr")
            .selectExpr("col", "struct(col, arr) as st")
            .selectExpr("col", "st.col as col1", "explode(st.arr) as arr_col")
          df.noop()
        }
      }
    }
  }

  def stack(numRows: Int): Unit = {
    runBenchmark("generate regular generator") {
      codegenBenchmark("generate stack", numRows) {
        val df = spark.range(numRows).selectExpr(
          "id as key",
          "id % 2 as t1",
          "id % 3 as t2",
          "id % 5 as t3",
          "id % 7 as t4",
          "id % 13 as t5")
        df.selectExpr("key", "stack(4, t1, t2, t3, t4, t5)").noop()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    filterAndAggregateWithoutGroup(500L << 22)
    limitAndAggregateWithoutGroup(500L << 20)
    sample(500 << 18)
    collect(1 << 20)
    collectLimit(1 << 20)
    explode(1 << 24)
    stack(1 << 24)
  }
}
