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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.util.Benchmark


class CacheBenchmark extends BenchmarkBase {

  /**
   * Call collect on a [[DataFrame]] after deleting all existing temporary files.
   * This also checks whether the collected result matches the expected answer.
   */
  private def collect(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    df.sparkSession.sparkContext.parallelize(1 to 10, 10).foreach { _ =>
      SparkEnv.get.blockManager.diskBlockManager.getAllFiles().foreach { dir =>
        dir.delete()
      }
    }
    QueryTest.checkAnswer(df, expectedAnswer) match {
      case Some(errMessage) => throw new RuntimeException(errMessage)
      case None => // all good
    }
  }

  /*
   * NOTE: When running this benchmark, you will get a lot of WARN logs complaining that the
   * shuffle files do not exist. This is intentional; we delete the shuffle files manually
   * after every call to `collect` to avoid the next run to reuse shuffle files written by
   * the previous run.
   */
  test("cache with randomized keys") {
    val N = 20 << 21
    val numIters = 10
    val benchmark = new Benchmark("Cache random keys", N)
    sparkSession.range(N)
      .selectExpr("id", "floor(rand() * 10000) as k")
      .createOrReplaceTempView("test")
    val query = "select count(k), count(id) from test"
    val expectedAnswer = sparkSession.sql(query).collect().toSeq

    /**
     * Add a benchmark case, optionally specifying whether to cache the dataset.
     */
    def addBenchmark(name: String, cache: Boolean, params: Map[String, String] = Map()): Unit = {
      val ds = sparkSession.sql(query)
      val defaults = params.keys.flatMap { k => sparkSession.conf.getOption(k).map((k, _)) }
      val prepare = () => {
        params.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        if (cache) { sparkSession.catalog.cacheTable("test") }
        collect(ds, expectedAnswer)
      }
      val cleanup = () => {
        defaults.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        sparkSession.catalog.clearCache()
      }
      benchmark.addCase(name, numIters, prepare, cleanup) { _ => collect(ds, expectedAnswer) }
    }

    // All of these are codegen = T hashmap = T
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "1024")

    // Benchmark cases:
    //   (1) No caching
    //   (2) Caching without compression
    //   (3) Caching with compression
    //   (4) Caching with column batches (without compression)
    addBenchmark("cache = F", cache = false)
    addBenchmark("cache = T columnar_batches = F compress = F", cache = true, Map(
      "spark.sql.inMemoryColumnarScan" -> "false",
      "spark.sql.inMemoryColumnarStorage.compressed" -> "false"
    ))
    addBenchmark("cache = T columnar_batches = F compress = T", cache = true, Map(
      "spark.sql.inMemoryColumnarScan" -> "false",
      "spark.sql.inMemoryColumnarStorage.compressed" -> "true"
    ))
    addBenchmark("cache = T columnar_batches = T", cache = true, Map(
      "spark.sql.inMemoryColumnarScan" -> "true"
    ))
    benchmark.run()

    /*
     Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.9.5
     Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

     Cache random keys:                       Best/Avg Time(ms)   Rate(M/s)   Per Row(ns)   Relative
     -----------------------------------------------------------------------------------------------
     cache = F                                      890 /  920        47.1          21.2       1.0X
     cache = T columnar_batches = F compress = F   1950 / 1978        21.5          46.5       0.5X
     cache = T columnar_batches = F compress = T   1893 / 1927        22.2          45.1       0.5X
     cache = T columnar_batches = T                 540 /  544        77.7          12.9       1.6X
     */
  }

}
