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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Benchmark


class CacheBenchmark extends BenchmarkBase {

  ignore("cache with randomized keys - both build and read paths") {
    benchmarkRandomizedKeys(size = 16 * 1024 * 1024, readPathOnly = false)

    /*
     OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
     Intel Xeon E3-12xx v2 (Ivy Bridge)
     Cache random keys:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns) Relative
     ----------------------------------------------------------------------------------------------
     cache = T columnarBatch = F compress = T      7211 / 7366          2.3         429.8      1.0X
     cache = T columnarBatch = F compress = F      2381 / 2460          7.0         141.9      3.0X
     cache = F                                      137 /  140        122.7           8.1     52.7X
     cache = T columnarBatch = T compress = T      2109 / 2252          8.0         125.7      3.4X
     cache = T columnarBatch = T compress = F      1126 / 1184         14.9          67.1      6.4X
     */
  }

  ignore("cache with randomized keys - read path only") {
    benchmarkRandomizedKeys(size = 64 * 1024 * 1024, readPathOnly = true)

    /*
     OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
     Intel Xeon E3-12xx v2 (Ivy Bridge)
     Cache random keys:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns) Relative
     ----------------------------------------------------------------------------------------------
     cache = T columnarBatch = F compress = T      1615 / 1655         41.5          24.1      1.0X
     cache = T columnarBatch = F compress = F      1603 / 1690         41.9          23.9      1.0X
     cache = F                                      444 /  449        151.3           6.6      3.6X
     cache = T columnarBatch = T compress = T      1404 / 1526         47.8          20.9      1.2X
     cache = T columnarBatch = T compress = F       116 /  125        579.0           1.7     13.9X
     */
  }

  /**
   * Call clean on a [[DataFrame]] after deleting all existing temporary files.
   */
  private def clean(df: DataFrame): Unit = {
    df.sparkSession.sparkContext.parallelize(1 to 10, 10).foreach { _ =>
      SparkEnv.get.blockManager.diskBlockManager.getAllFiles().foreach { dir =>
        dir.delete()
      }
    }
  }

  /**
   * Benchmark caching randomized keys created from a range.
   *
   * NOTE: When running this benchmark, you will get a lot of WARN logs complaining that the
   * shuffle files do not exist. This is intentional; we delete the shuffle files manually
   * after every call to `collect` to avoid the next run to reuse shuffle files written by
   * the previous run.
   */
  private def benchmarkRandomizedKeys(size: Int, readPathOnly: Boolean): Unit = {
    val numIters = 10
    val benchmark = new Benchmark("Cache random keys", size)
    sparkSession.range(size)
      .selectExpr("id", "floor(rand() * 10000) as k")
      .createOrReplaceTempView("test")
    val query = "select count(k), count(id) from test"

    /**
     * Add a benchmark case, optionally specifying whether to cache the dataset.
     */
    def addBenchmark(name: String, cache: Boolean, params: Map[String, String] = Map()): Unit = {
      val ds = sparkSession.sql(query)
      var dsResult: DataFrame = null
      val defaults = params.keys.flatMap { k => sparkSession.conf.getOption(k).map((k, _)) }
      def prepare(): Unit = {
        clean(ds)
        params.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        if (cache && readPathOnly) {
          sparkSession.sql("cache table test")
        }
      }
      def cleanup(): Unit = {
        clean(dsResult)
        defaults.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        sparkSession.catalog.clearCache()
      }
      benchmark.addCase(name, numIters, prepare, cleanup) { _ =>
        if (cache && !readPathOnly) {
          sparkSession.sql("cache table test")
        }
        dsResult = sparkSession.sql(query)
        dsResult.collect
      }
    }

    // All of these are codegen = T hashmap = T
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    // Benchmark cases:
    //   (1) Caching with compression
    //   (2) Caching without compression
    //   (3) No caching
    //   (4) Caching using column batch with compression
    //   (5) Caching using column batch without compression
    addBenchmark("cache = T columnarBatch = F compress = T", cache = true, Map(
      SQLConf.CACHE_CODEGEN.key -> "false",
      SQLConf.COMPRESS_CACHED.key -> "true"
    ))
    addBenchmark("cache = T columnarBatch = F compress = F", cache = true, Map(
      SQLConf.CACHE_CODEGEN.key -> "false",
      SQLConf.COMPRESS_CACHED.key -> "false"
    ))
    addBenchmark("cache = F", cache = false)
    addBenchmark("cache = T columnarBatch = T compress = T", cache = true, Map(
      SQLConf.CACHE_CODEGEN.key -> "true",
      SQLConf.COMPRESS_CACHED.key -> "true"
    ))
    addBenchmark("cache = T columnarBatch = T compress = F", cache = true, Map(
      SQLConf.CACHE_CODEGEN.key -> "true",
      SQLConf.COMPRESS_CACHED.key -> "false"
    ))
    benchmark.run()
  }

}
