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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.StorageFilterMetrics
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure storage-filter pushdown in the vectorized Parquet reader. Every case runs
 * the same join, whose runtime bloom filter is the storage filter, with
 * `spark.sql.parquet.storageFilterPushdown.enabled` off and on.
 *
 * The fact table is written in its join key's order, so which keys a dimension table selects
 * decides where the surviving rows fall:
 *  - clustered keys put the survivors in a few pages, so the reader skips the other row groups
 *    whole and the other pages of the row group that holds them. These cases show what the
 *    feature saves. The bloom filter's false positives land anywhere, though, so the more keys
 *    it holds, the more pages hold a survivor;
 *  - scattered keys put survivors in every page, so the reader can skip nothing and decodes the
 *    non-key columns range by range. These cases show what the feature costs.
 * After each case it prints what the storage filter excluded, which does not depend on the
 * machine.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StorageFilterPushdownBenchmark-results.txt".
 * }}}
 */
object StorageFilterPushdownBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
      .set("spark.ui.enabled", "false")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val numRows = 4L * 1024 * 1024
  private val payloadColumns = 8

  // The clustered cases select a contiguous range of keys from the middle of the table.
  private val clustered = Seq("0.1%" -> 0.001, "1%" -> 0.01, "10%" -> 0.1)
  // The scattered cases select keys by their hash, so the survivors are spread over every page.
  private val scattered = Seq("3%" -> 3, "25%" -> 25, "50%" -> 50)

  private def prepareTables(dir: File): Unit = {
    val factPath = new File(dir, "fact").getCanonicalPath
    // The payload is hashed, so it neither compresses nor dictionary-encodes away, and the bytes a
    // skipped page saves are real.
    val payload = (0 until payloadColumns).map(i => s"xxhash64(id, $i) AS v$i")
    spark.range(0, numRows, 1, 1).selectExpr("id AS k" +: payload: _*).write.parquet(factPath)
    spark.read.parquet(factPath).createOrReplaceTempView("fact")

    def writeDimension(name: String, predicate: String): Unit = {
      val path = new File(dir, name).getCanonicalPath
      // `sel_id` exists for the query's filter on the dimension side, which is what makes the
      // optimizer build a bloom filter from it at all.
      spark.range(0, numRows, 1, 1).where(predicate).selectExpr("id AS k", "id AS sel_id")
        .write.parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(name)
    }
    clustered.foreach { case (label, fraction) =>
      val start = numRows / 2
      val end = start + (numRows * fraction).toLong
      writeDimension(dimension("clustered", label), s"id >= $start AND id < $end")
    }
    scattered.foreach { case (label, percent) =>
      writeDimension(dimension("scattered", label), s"pmod(xxhash64(id), 100) < $percent")
    }
  }

  private def dimension(layout: String, label: String): String =
    s"${layout}_${label.replace("%", "").replace(".", "_")}"

  private def query(dimension: String): String =
    s"SELECT f.* FROM fact f JOIN $dimension d ON f.k = d.k WHERE d.sel_id >= 0"

  private def factScan(plan: SparkPlan): FileSourceScanExec =
    plan.collectFirst {
      case scan: FileSourceScanExec if scan.output.exists(_.name == "v0") => scan
    }.getOrElse(throw new IllegalStateException(s"No fact table scan in $plan"))

  private def withPushdown[T](enabled: Boolean)(f: => T): T =
    withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> enabled.toString)(f)

  // Runs the query once, untimed, and returns the fact scan with its metrics filled in.
  private def executedFactScan(sql: String, enabled: Boolean): FileSourceScanExec =
    withPushdown(enabled) {
      val df = spark.sql(sql)
      val scan = factScan(df.queryExecution.executedPlan)
      val hasBloom = df.queryExecution.executedPlan.exists { node =>
        node.expressions.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
      }
      assert(hasBloom, s"The join must have a runtime bloom filter:\n${df.queryExecution}")
      assert(scan.storageFilters.nonEmpty == enabled,
        s"The fact scan must have a storage filter exactly when pushdown is on:\n$scan")
      df.queryExecution.toRdd.foreach(_ => ())
      scan
    }

  private def runCase(title: String, dimension: String): Unit = {
    val sql = query(dimension)
    val benchmark = new Benchmark(title, numRows, minNumIters = 3, output = output)
    Seq(false, true).foreach { enabled =>
      benchmark.addCase(s"Storage filter pushdown ${if (enabled) "on" else "off"}") { _ =>
        withPushdown(enabled)(spark.sql(sql).noop())
      }
    }
    benchmark.run()

    val off = executedFactScan(sql, enabled = false)
    val on = executedFactScan(sql, enabled = true)
    def metric(name: String): Long = on.metrics(name).value
    val avoidedBytes = metric(StorageFilterMetrics.BYTES_AVOIDED_BY_ROW_GROUP) +
      metric(StorageFilterMetrics.BYTES_AVOIDED_BY_PAGE_FILTERING)
    // scalastyle:off println
    benchmark.out.println(s"Rows out of the fact scan: ${off.metrics("numOutputRows").value} " +
      s"with pushdown off, ${on.metrics("numOutputRows").value} with it on")
    benchmark.out.println(
      s"Row groups skipped whole: ${metric(StorageFilterMetrics.ROW_GROUPS_SKIPPED)}, " +
        s"bytes not read: $avoidedBytes")
    benchmark.out.println()
    // scalastyle:on println
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
          SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1GB") {
        prepareTables(dir)
        runBenchmark("Storage filter pushdown, clustered keys") {
          clustered.foreach { case (label, _) =>
            runCase(s"$label of the keys, clustered", dimension("clustered", label))
          }
        }
        runBenchmark("Storage filter pushdown, scattered keys") {
          scattered.foreach { case (label, _) =>
            runCase(s"$label of the keys, scattered", dimension("scattered", label))
          }
        }
      }
    }
  }
}
