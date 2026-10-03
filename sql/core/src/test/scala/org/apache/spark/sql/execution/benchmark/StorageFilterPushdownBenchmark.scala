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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Literal, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.classic.ExpressionUtils.column
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.StorageFilterMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.util.Utils

/**
 * Benchmark to measure storage-filter pushdown in the vectorized Parquet reader. Every case runs
 * with `spark.sql.parquet.storageFilterPushdown.enabled` off and on, in two forms:
 *  - the join whose runtime bloom filter is the storage filter, which is the path the feature
 *    takes. The join's shuffle and sort are the same in both arms, and they grow with the rows
 *    that survive, so they dilute what the reader saves or costs;
 *  - a scan of the fact table alone under the same bloom filter, built once outside the timed
 *    body, which shows the reader's own share.
 *
 * Every case reads a fact table of about 100 MB in 34 row groups. The cases differ in four things:
 *  - the width of a row. One table has 50,000 rows of a long key and a 2 kB value that does not
 *    compress. The other has 1.4M rows of a long key and eight hashed long values;
 *  - the page size. Each table is written twice, in 8 kB pages and in parquet's default 1 MB
 *    ones. One surviving row makes the reader read its whole page, so this decides how much a
 *    survivor costs. A wide row fills a small page in a few values, while narrow ones still put
 *    about a thousand in it;
 *  - where the surviving keys fall. Keys from a contiguous range put the survivors in a few
 *    pages, so the reader skips the other row groups whole and the other pages of the row groups
 *    that hold them. Keys selected by their hash put survivors anywhere, so the reader skips
 *    only the pages none of them falls in, and decodes the rest range by range;
 *  - the share of the keys selected, 0.1%, 0.5%, 1% and 5%.
 * Each section's title gives the row groups and rows per value page its table was written with.
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

  // A fact table: its name and what its section title calls it, how many rows it has, how many of
  // them a row group holds, and its value columns.
  private case class Table(
      name: String,
      description: String,
      numRows: Long,
      rowsPerRowGroup: Int,
      values: Seq[String])

  // The values are hashed, so they neither compress nor dictionary-encode away, and the bytes a
  // skipped page saves are real.
  private val tables = Seq(
    // 64 hashes of 32 bytes each.
    Table("wide", "2 kB values", 50000L, 1500, Seq("unhex(concat_ws('', transform(" +
      "sequence(0, 63), i -> sha2(concat(CAST(id AS STRING), '-', CAST(i AS STRING)), 256)))) " +
      "AS v0")),
    Table("narrow", "eight long values", 1400000L, 42000,
      (0 until 8).map(i => s"xxhash64(id, $i) AS v$i")))

  // The two ways each fact table is written: its layout's name, and the writer options that set
  // its pages. Parquet checks a page's size only every 100 rows by default, so small pages need
  // it checked after every row.
  private val layouts = Seq(
    "small_pages" -> Map(
      ParquetOutputFormat.PAGE_SIZE -> "8192",
      ParquetOutputFormat.MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK -> "1"),
    "default_pages" -> Map.empty[String, String])

  // Each share of the keys selected, in thousandths.
  private val selected = Seq("0.1%" -> 1, "0.5%" -> 5, "1%" -> 10, "5%" -> 50)

  private def prepareTables(dir: File): Unit = tables.foreach { table =>
    val rows = spark.range(0, table.numRows, 1, 1).selectExpr("id AS k" +: table.values: _*)
    layouts.foreach { case (layout, options) =>
      val path = new File(dir, fact(table, layout)).getCanonicalPath
      rows.write
        .option(ParquetOutputFormat.BLOCK_ROW_COUNT_LIMIT, table.rowsPerRowGroup.toString)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .options(options)
        .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(fact(table, layout))
    }

    def writeDimension(name: String, predicate: String): Unit = {
      val path = new File(dir, name).getCanonicalPath
      // `sel_id` exists for the query's filter on the dimension side, which is what makes the
      // optimizer build a bloom filter from it at all.
      spark.range(0, table.numRows, 1, 1).where(predicate).selectExpr("id AS k", "id AS sel_id")
        .write.parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(name)
    }
    selected.foreach { case (label, perMille) =>
      val start = table.numRows / 2
      writeDimension(dimension(table, "clustered", label),
        s"id >= $start AND id < ${start + table.numRows * perMille / 1000}")
      writeDimension(dimension(table, "scattered", label),
        s"pmod(xxhash64(id), 1000) < $perMille")
    }
  }

  private def fact(table: Table, layout: String): String = s"fact_${table.name}_$layout"

  private def dimension(table: Table, placement: String, label: String): String =
    s"${table.name}_${placement}_${label.replace("%", "").replace(".", "_")}"

  // How many row groups a fact table has, and how many rows its first value column's pages hold
  // on average.
  private def layoutOf(table: Table, layout: String): (Int, Long) = {
    val file = new Path(spark.table(fact(table, layout)).inputFiles.head)
    Utils.tryWithResource(ParquetFileReader.open(
        HadoopInputFile.fromPath(file, spark.sessionState.newHadoopConf()))) { reader =>
      val rowGroups = reader.getRowGroups.asScala
      val valuePages = rowGroups.map { rowGroup =>
        val value = rowGroup.getColumns.asScala.find(_.getPath.toDotString == "v0").get
        reader.readOffsetIndex(value).getPageCount
      }.sum
      (rowGroups.size, table.numRows / valuePages)
    }
  }

  private def query(fact: String, dimension: String): String =
    s"SELECT f.* FROM $fact f JOIN $dimension d ON f.k = d.k WHERE d.sel_id >= 0"

  // The fact table under the bloom filter the join builds from `dimension`, with no join. The
  // bloom is built once, outside any timed body, the way `InjectRuntimeFilter` builds it for a
  // creation side with no row count, which these temp views are.
  private def scanOnly(fact: String, dimension: String): () => DataFrame = {
    val keyHash = new XxHash64(Seq(UnresolvedAttribute("k")))
    val bloom = spark.table(dimension)
      .select(column(new BloomFilterAggregate(keyHash).toAggregateExpression()))
      .head().getAs[Array[Byte]](0)
    val mightContain = BloomFilterMightContain(Literal(bloom, BinaryType), keyHash)
    () => spark.table(fact).where(column(mightContain))
  }

  private def factScan(plan: SparkPlan): FileSourceScanExec =
    plan.collectFirst {
      case scan: FileSourceScanExec if scan.output.exists(_.name == "v0") => scan
    }.getOrElse(throw new IllegalStateException(s"No fact table scan in $plan"))

  private def withPushdown[T](enabled: Boolean)(f: => T): T =
    withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> enabled.toString)(f)

  // Asserts that `df` plans its bloom filter, and hands it to the fact scan exactly when
  // pushdown is on, so neither arm can silently measure the other. Returns that scan.
  private def checkedFactScan(df: DataFrame, enabled: Boolean): FileSourceScanExec = {
    val plan = df.queryExecution.executedPlan
    val hasBloom = plan.exists { node =>
      node.expressions.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
    }
    assert(hasBloom, s"The plan must have a bloom filter:\n${df.queryExecution}")
    val scan = factScan(plan)
    assert(scan.storageFilters.nonEmpty == enabled,
      s"The fact scan must have a storage filter exactly when pushdown is on:\n$scan")
    scan
  }

  // Runs `df` with pushdown off and on, and returns the benchmark that timed it.
  private def runOffAndOn(title: String, numRows: Long, df: () => DataFrame): Benchmark = {
    val benchmark = new Benchmark(title, numRows, minNumIters = 3, output = output)
    Seq(false, true).foreach { enabled =>
      withPushdown(enabled)(checkedFactScan(df(), enabled))
      benchmark.addCase(s"Storage filter pushdown ${if (enabled) "on" else "off"}") { _ =>
        withPushdown(enabled)(df().noop())
      }
    }
    benchmark.run()
    benchmark
  }

  private def runCase(table: Table, fact: String, label: String, dimension: String): Unit = {
    runOffAndOn(s"$label, join", table.numRows, () => spark.sql(query(fact, dimension)))
    val scan = scanOnly(fact, dimension)
    val scanBenchmark = runOffAndOn(s"$label, scan only", table.numRows, scan)

    // Run once more, untimed, for what the storage filter did. With pushdown off the fact scan
    // returns every row, since the bloom filter stays above it.
    val on = withPushdown(enabled = true) {
      val df = scan()
      val factSide = checkedFactScan(df, enabled = true)
      df.queryExecution.toRdd.foreach(_ => ())
      factSide
    }
    def metric(name: String): Long = on.metrics(name).value
    val avoidedBytes = metric(StorageFilterMetrics.BYTES_AVOIDED_BY_ROW_GROUP) +
      metric(StorageFilterMetrics.BYTES_AVOIDED_BY_PAGE_FILTERING)
    // scalastyle:off println
    scanBenchmark.out.println(s"Rows out of the fact scan: ${table.numRows} with pushdown off, " +
      s"${on.metrics("numOutputRows").value} with it on")
    scanBenchmark.out.println(
      s"Row groups skipped whole: ${metric(StorageFilterMetrics.ROW_GROUPS_SKIPPED)}, " +
        s"bytes not read: $avoidedBytes")
    scanBenchmark.out.println()
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
        tables.foreach { table =>
          val shapes = layouts.map { case (layout, _) => layout -> layoutOf(table, layout) }.toMap
          shapes.foreach { case (layout, (rowGroups, _)) =>
            assert(rowGroups == (table.numRows + table.rowsPerRowGroup - 1) / table.rowsPerRowGroup,
              s"The ${table.name} $layout fact table must have row groups of " +
                s"${table.rowsPerRowGroup} rows")
          }
          // Far apart, so that neither layout can silently turn into the other.
          assert(shapes("small_pages")._2 * 10 < shapes("default_pages")._2,
            s"The ${table.name} fact table's small pages must hold far fewer rows: $shapes")
          // The bloom filter is sized for this table's dimension tables, which hold up to a
          // twentieth of its keys, rather than for the default million. The default one takes
          // 1 MB, which every query's plan description spells out in hex, and the driver keeps
          // those descriptions.
          val bloomItems = table.numRows / 10
          withSQLConf(
              SQLConf.RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key -> bloomItems.toString,
              SQLConf.RUNTIME_BLOOM_FILTER_NUM_BITS.key -> (bloomItems * 13).toString) {
            layouts.foreach { case (layout, _) =>
              val (rowGroups, rowsPerPage) = shapes(layout)
              runBenchmark(s"Storage filter pushdown, ${table.description}, $rowGroups row " +
                  s"groups, value pages of about $rowsPerPage rows") {
                Seq("clustered", "scattered").foreach { placement =>
                  selected.foreach { case (label, _) =>
                    runCase(table, fact(table, layout), s"$label of the keys $placement",
                      dimension(table, placement, label))
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
