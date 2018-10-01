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

/**
 * Benchmark to measure whole stage codegen performance.
 * To run this benchmark:
 * 1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 * 2. build/sbt "sql/test:runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *    Results will be written to "benchmarks/MiscBenchmark-results.txt".
 */
class MiscBenchmark extends SqlBasedBenchmark {

  /** Runs function `f` with whole stage codegen on and off. */
  def runMiscBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase(s"$name wholestage off", numIters = 2) { iter =>
      spark.conf.set("spark.sql.codegen.wholeStage", value = false)
      f
    }

    benchmark.addCase(s"$name wholestage on", numIters = 5) { iter =>
      spark.conf.set("spark.sql.codegen.wholeStage", value = true)
      f
    }

    benchmark.run()
  }

  override def benchmark(): Unit = {
    runBenchmark("filter & aggregate without group") {
      val N = 500L << 22
      runMiscBenchmark("range/filter/sum", N) {
        spark.range(N).filter("(id & 1) = 1").groupBy().sum().collect()
      }
    }

    runBenchmark("range/limit/sum") {
      val N = 500L << 20
      runMiscBenchmark("range/limit/sum", N) {
        spark.range(N).limit(1000000).groupBy().sum().collect()
      }
    }

    runBenchmark("sample") {
      val N = 500 << 18
      runMiscBenchmark("sample with replacement", N) {
        spark.range(N).sample(withReplacement = true, 0.01).groupBy().sum().collect()
      }

      runMiscBenchmark("sample without replacement", N) {
        spark.range(N).sample(withReplacement = false, 0.01).groupBy().sum().collect()
      }
    }

    runBenchmark("collect") {
      val N = 1 << 20

      val benchmark = new Benchmark("collect", N, output = output)
      benchmark.addCase("collect 1 million") { iter =>
        spark.range(N).collect()
      }
      benchmark.addCase("collect 2 millions") { iter =>
        spark.range(N * 2).collect()
      }
      benchmark.addCase("collect 4 millions") { iter =>
        spark.range(N * 4).collect()
      }
      benchmark.run()
    }

    runBenchmark("collect limit") {
      val N = 1 << 20

      val benchmark = new Benchmark("collect limit", N, output = output)
      benchmark.addCase("collect limit 1 million") { iter =>
        spark.range(N * 4).limit(N).collect()
      }
      benchmark.addCase("collect limit 2 millions") { iter =>
        spark.range(N * 4).limit(N * 2).collect()
      }
      benchmark.run()
    }

    runBenchmark("generate explode") {
      val N = 1 << 24
      runMiscBenchmark("generate explode array", N) {
        val df = spark.range(N).selectExpr(
          "id as key",
          "array(rand(), rand(), rand(), rand(), rand()) as values")
        df.selectExpr("key", "explode(values) value").count()
      }

      runMiscBenchmark("generate explode map", N) {
        val df = spark.range(N).selectExpr(
          "id as key",
          "map('a', rand(), 'b', rand(), 'c', rand(), 'd', rand(), 'e', rand()) pairs")
        df.selectExpr("key", "explode(pairs) as (k, v)").count()
      }

      runMiscBenchmark("generate posexplode array", N) {
        val df = spark.range(N).selectExpr(
          "id as key",
          "array(rand(), rand(), rand(), rand(), rand()) as values")
        df.selectExpr("key", "posexplode(values) as (idx, value)").count()
      }

      runMiscBenchmark("generate inline array", N) {
        val df = spark.range(N).selectExpr(
          "id as key",
          "array((rand(), rand()), (rand(), rand()), (rand(), 0.0d)) as values")
        df.selectExpr("key", "inline(values) as (r1, r2)").count()
      }

      val M = 60000
      runMiscBenchmark("generate big struct array", M) {
        import spark.implicits._
        val df = spark.sparkContext.parallelize(Seq(("1",
          Array.fill(M)({
            val i = math.random
            (i.toString, (i + 1).toString, (i + 2).toString, (i + 3).toString)
          })))).toDF("col", "arr")

        df.selectExpr("*", "explode(arr) as arr_col")
          .select("col", "arr_col.*").count
      }
    }

    runBenchmark("generate regular generator") {
      val N = 1 << 24
      runMiscBenchmark("generate stack", N) {
        val df = spark.range(N).selectExpr(
          "id as key",
          "id % 2 as t1",
          "id % 3 as t2",
          "id % 5 as t3",
          "id % 7 as t4",
          "id % 13 as t5")
        df.selectExpr("key", "stack(4, t1, t2, t3, t4, t5)").count()
      }
    }
  }
}
