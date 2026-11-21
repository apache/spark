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
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, explode}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to compare the ordered `CollectLimitExec` gather
 * (`spark.sql.execution.collectLimit.orderedGather.enabled`) with the unordered one, when a root
 * LIMIT or OFFSET is read through `CollectLimitExec.doExecute`, as `toLocalIterator` does. It runs
 * on two executors, so that the gather reads map outputs from both, and some of them arrive before
 * their turn.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CollectLimitOrderedGatherBenchmark-results.txt".
 * }}}
 */
object CollectLimitOrderedGatherBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local-cluster[2,1,2048]")
      .appName(this.getClass.getCanonicalName)
      // Both executors run on this host. Fetch the map outputs of the other executor over the
      // network, as from another host, instead of reading them from its disk.
      .config("spark.shuffle.readHostLocalDisk", "false")
      .config(UI_ENABLED.key, false)
      .getOrCreate()
  }

  /** Reads all rows of `df` through `toLocalIterator`. */
  private def gather(df: DataFrame): Unit = df.toLocalIterator().forEachRemaining(_ => ())

  /**
   * Compares the two gathers on `query` over `numRows` rows in `numPartitions` partitions, which
   * the sort below `query` keeps.
   */
  private def compareGathers(name: String, numRows: Long, numPartitions: Int)(
      query: DataFrame => DataFrame): Unit = {
    val benchmark = new Benchmark(name, numRows, minNumIters = 3, output = output)
    Seq(false, true).foreach { enabled =>
      benchmark.addCase(s"ordered gather enabled = $enabled") { _ =>
        withSQLConf(
          SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> enabled.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString,
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
          gather(query(spark.range(0, numRows, 1, numPartitions).toDF()))
        }
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("CollectLimitExec gather") {
      val numRows = 10L * 1000 * 1000
      // An OFFSET without a LIMIT reads all map outputs.
      compareGathers("ORDER BY + OFFSET, 100 partitions", numRows, 100) {
        _.orderBy(col("id").desc).offset((numRows - 1000).toInt)
      }
      // The explode keeps the LIMIT from being planned as a top-K sort. The ordered gather can stop
      // once it has read the map outputs that hold the first rows.
      compareGathers("ORDER BY + explode + LIMIT 1000, 100 partitions", numRows, 100) {
        _.orderBy(col("id").desc).select(explode(array(col("id"))).as("id")).limit(1000)
      }
      // Many small map outputs, read in many batches.
      val numSmallRows = 2L * 1000 * 1000
      compareGathers("ORDER BY + OFFSET, 2000 partitions", numSmallRows, 2000) {
        _.orderBy(col("id").desc).offset((numSmallRows - 1000).toInt)
      }
    }
  }
}
