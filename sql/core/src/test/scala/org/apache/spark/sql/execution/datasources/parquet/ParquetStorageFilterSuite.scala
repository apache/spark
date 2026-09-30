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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.{File, RandomAccessFile}
import java.net.URI
import java.time.LocalTime
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, FSInputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.column.{Encoding, ParquetProperties}
import org.apache.parquet.column.impl.ColumnWriteStoreV1
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.mem.MemPageStore
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetInputFormat, ParquetOutputFormat}
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, CompressionCodecName, ParquetMetadata}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.{SparkException, TaskContext, TaskKilledException}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{sources, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BloomFilterMightContain, BoundReference, Cast, EqualTo, Expression, GreaterThanOrEqual, IsNull, LessThanOrEqual, Literal, Or, Predicate, Rand, Remainder, SecondsToTimestamp, UnaryExpression, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter}
import org.apache.spark.sql.execution.{CollapseCodegenStages, ColumnarToRowExec, FileSourceScanExec, FilterExec, LocalLimitExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, FileSourceStrategy, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.sketch.BloomFilter

/**
 * Tests [[LateMaterializationParquetRecordReader]] driven by a [[ParquetStorageFilter]]. Writes
 * small multi-row-group parquet files, wires a hand-built filter into the reader, and asserts
 * correctness and the five storage-filter metrics.
 */
class ParquetStorageFilterSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  // Writes `df` as one parquet file under a fresh directory and returns its path. Every write
  // helper in this suite goes through here.
  private def writeSingleParquetFile(
      dir: File,
      df: DataFrame,
      rowGroupSize: Long,
      pageSize: Option[Long] = None,
      dictionary: Boolean = false): String = {
    val outDir = new File(dir, s"test-${System.nanoTime()}").getAbsolutePath
    val writer = df
      .repartition(1)
      .write
      .option(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize)
      // Dictionary encoding off keeps row-group sizing predictable. The column index is still
      // written either way.
      .option(ParquetOutputFormat.ENABLE_DICTIONARY, dictionary.toString)
    // A small page size gives each row group several pages per column, which is what lets
    // column-index filtering produce a row range narrower than the whole row group.
    pageSize.foreach(size => writer.option(ParquetOutputFormat.PAGE_SIZE, size))
    writer.parquet(outDir)
    val files = new File(outDir).listFiles((_, name) => name.endsWith(".parquet"))
    assert(files != null && files.length == 1, s"expected exactly one parquet file under $outDir")
    files(0).getAbsolutePath
  }

  // Writes a parquet file with the given rows and row-group size; returns the path.
  private def writeParquetFile(
      dir: File,
      rows: Seq[(Long, String)],
      rowGroupSize: Long = 1024L,
      pageSize: Option[Long] = None): String =
    writeSingleParquetFile(dir, rows.toDF("k", "v"), rowGroupSize, pageSize)

  // The scan node of a plan, which every test that inspects or rewrites `storageFilters` needs.
  private def scanOf(plan: SparkPlan): FileSourceScanExec =
    plan.collect { case s: FileSourceScanExec => s }.headOption
      .getOrElse(fail(s"No FileSourceScanExec found in plan: $plan"))

  // A hadoop conf whose reads go through `CountingLocalFileSystem`, so a test can measure them.
  private def countingHadoopConf(): Configuration = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set(s"fs.${CountingLocalFileSystem.scheme}.impl",
      classOf[CountingLocalFileSystem].getName)
    hadoopConf.setBoolean(s"fs.${CountingLocalFileSystem.scheme}.impl.disable.cache", true)
    hadoopConf
  }

  // Reads `path` whole through `readerFn`, and returns the rows it emitted with the bytes the read
  // transferred. The reader must have been built with `countingHadoopConf`.
  private def readCounting(
      path: String, readerFn: PartitionedFile => Iterator[_]): (Int, Long) = {
    val file = PartitionedFile(
      InternalRow.empty,
      SparkPath.fromUrlString(s"${CountingLocalFileSystem.scheme}://$path"),
      0,
      new File(path).length())
    CountingLocalFileSystem.reset()
    val emitted = drain(readerFn(file)) { it =>
      it.map {
        case batch: ColumnarBatch => batch.numRows()
        case _ => 1
      }.sum
    }
    (emitted, CountingLocalFileSystem.bytesRead())
  }

  // Consumes what a reader builder returned for one file through `consume`, and closes the reader
  // behind it whatever happens. Exhausting the iterator closes it too, but a failure part way
  // through, which is what these tests look for, would otherwise leave it and its stream open.
  private def drain[T](iter: Iterator[_])(consume: Iterator[Object] => T): T = {
    try {
      consume(iter.asInstanceOf[Iterator[Object]])
    } finally {
      iter match {
        case closeable: java.io.Closeable => closeable.close()
        case _ =>
      }
    }
  }

  // A batch reader for a file of `schema`, built through `ParquetFileFormat` the way a scan with
  // storage filters builds one.
  private def formatReader(
      schema: StructType,
      pushed: Seq[sources.Filter],
      storageFilters: Seq[Expression],
      hadoopConf: Configuration = spark.sessionState.newHadoopConf(),
      metrics: Map[String, SQLMetric] = metricMap()): PartitionedFile => Iterator[InternalRow] =
    new ParquetFileFormat().buildReaderWithStorageFilters(
      spark, schema, new StructType(), schema, pushed, storageFilters,
      Map(FileFormat.OPTION_RETURNING_BATCH -> "true"), hadoopConf, metrics)
      .getOrElse(fail("ParquetFileFormat must answer with a reader"))

  // Reads `path` whole through `readerFn`, and returns the long column at `ordinal` of every row.
  private def readLongs(
      readerFn: PartitionedFile => Iterator[InternalRow], path: String, ordinal: Int): Seq[Long] = {
    val file = PartitionedFile(
      InternalRow.empty, SparkPath.fromPathString(path), 0, new File(path).length())
    drain(readerFn(file)) { it =>
      it.flatMap {
        case batch: ColumnarBatch => (0 until batch.numRows()).map(batch.column(ordinal).getLong)
        case row: InternalRow => Seq(row.getLong(ordinal))
      }.toSeq
    }
  }

  // Runs `body`, and returns its result with whether the reader logged its WARN about reading the
  // rest of a file without page-level storage filtering.
  private def withPartialReadWarning[T](body: => T): (T, Boolean) = {
    val logAppender = new LogAppender("partial-read warning")
    var result: Option[T] = None
    withLogAppender(logAppender) {
      result = Some(body)
    }
    val warned = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
      .exists(_.contains("without page-level storage filtering"))
    (result.get, warned)
  }

  // The metric map a scan hands the format, for a test that builds a reader by hand.
  private def metricMap(): Map[String, SQLMetric] =
    new ParquetFileFormat().storageFilterMetrics(spark.sparkContext)

  // Every metric the reader updates, created the way the production path creates them. A test that
  // asserts one holds the whole set and reads the counter off it.
  private def allMetrics(): StorageFilterMetrics =
    StorageFilterMetrics.create(spark.sparkContext)

  // `ParquetStorageFilter.create` takes what the production path always supplies; most tests care
  // about neither the metrics nor the cap.
  private def createFilter(
      boundExpressions: Seq[Expression],
      requestedSchema: StructType,
      metrics: StorageFilterMetrics = allMetrics(),
      maxSplicedRowGroupBytes: Long = Long.MaxValue): ParquetStorageFilter =
    ParquetStorageFilter.create(
      boundExpressions, requestedSchema, metrics, maxSplicedRowGroupBytes)

  // The `(k, v)` schema most fixtures read. `create` reads only the types and names, and the reader
  // takes each column's repetition from the file, so `nullable` matters only to a format read.
  private def kvSchema(
      keyType: DataType = LongType,
      valueType: DataType = StringType,
      nullable: Boolean = true): StructType =
    StructType(Seq(StructField("k", keyType, nullable), StructField("v", valueType, nullable)))

  // Collects all `(k, v)` rows from a reader initialized with the given storage filter.
  private def readAll(
      filePath: String,
      storageFilter: ParquetStorageFilter): (Seq[(Long, String)], VectorizedParquetRecordReader) =
    readAllWith(filePath, Seq("k", "v"), storageFilter,
      (batch, i) => (batch.column(0).getLong(i), batch.column(1).getUTF8String(i).toString))

  // Builds a `k >= threshold` storage filter bound to position 0, over a long key.
  private def keyAtLeastFilter(
      threshold: Long,
      metrics: StorageFilterMetrics = allMetrics()): ParquetStorageFilter = {
    val expr = GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(threshold))
    val requested = kvSchema()
    createFilter(Seq(expr), requested, metrics)
  }

  // A key column of the caller's type plus one non-key column, written via Spark's `Encoder`. Every
  // key-type fixture carries that second column, because a projection of key columns alone is one
  // the reader declines. It would read what a plain scan reads, so there is nothing for phase 2 to
  // prune.
  private def writeKeyParquetFile[T : org.apache.spark.sql.Encoder](
      dir: File,
      keys: Seq[T],
      rowGroupSize: Long = 1024L): String =
    writeSingleParquetFile(
      dir,
      spark.createDataset(keys).toDF("k").selectExpr("k", "CAST(k AS STRING) AS v"),
      rowGroupSize)

  test("rejects entire row group: no data-column IO, row-group-skipped metric incremented") {
    withTempDir { dir =>
      // 40 rows come out as one row group. Parquet's first row-group size check is at record 100,
      // so `rowGroupSize` cannot split a file this small. The one row group is the one rejected.
      val rows = (1L to 40L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      val m = allMetrics()
      val filter = keyAtLeastFilter(1000L, m)
      val (result, reader) = readAll(path, filter)
      try {
        assert(result.isEmpty, "filter rejects all rows; no rows should be emitted")
        assert(m.rowGroupsSkipped.value > 0,
          s"expected at least one row group skipped; got ${m.rowGroupsSkipped.value}")
        assert(m.rowsExcludedByRowGroup.value > 0,
          s"expected rows excluded by whole-rowgroup skip; got ${m.rowsExcludedByRowGroup.value}")
        assert(m.rowsExcludedWithinRowGroup.value == 0,
          s"no partial-row-group filtering expected; got ${m.rowsExcludedWithinRowGroup.value}")
        // Schema is (k: Long, v: String). Skipping a row group avoids the v-column bytes the
        // no-storage-filter path would have read; phase 1 still pays for k. So avoided > 0.
        assert(m.bytesAvoidedByRowGroup.value > 0,
          s"expected non-key bytes avoided by whole row groups; got " +
            s"${m.bytesAvoidedByRowGroup.value}")
        assert(m.bytesAvoidedByPageFiltering.value == 0,
          s"no page-filtering bytes expected when all groups skipped; got " +
            s"${m.bytesAvoidedByPageFiltering.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("all rows survive: no skipping and no filtering") {
    withTempDir { dir =>
      val rows = (1L to 40L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      val m = allMetrics()
      val filter = keyAtLeastFilter(0L, m)
      val (result, reader) = readAll(path, filter)
      try {
        assert(result == rows, s"all rows should round-trip; got ${result.size} rows")
        assert(m.rowGroupsSkipped.value == 0,
          s"nothing should be skipped; got ${m.rowGroupsSkipped.value}")
        assert(m.rowsExcludedWithinRowGroup.value == 0,
          s"nothing should be filtered; got ${m.rowsExcludedWithinRowGroup.value}")
      } finally {
        reader.close()
      }
    }
  }

  Seq(false, true).foreach { useOffHeap =>
    val mode = if (useOffHeap) "off-heap" else "on-heap"
    test(s"mixed, $mode vectors: some row groups skipped, others partially kept") {
      withTempDir { dir =>
        // Many rows + small row groups => guaranteed multiple row groups.
        val rows = (1L to 200L).map(i => (i, s"v_$i"))
        val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

        val m = allMetrics()
        // k >= 195 keeps only the last 6 rows; earlier row groups should be skipped.
        val filter = keyAtLeastFilter(195L, m)
        val (result, reader) = readAllWith(path, Seq("k", "v"), filter,
          (b, i) => (b.column(0).getLong(i), b.column(1).getUTF8String(i).toString),
          useOffHeap = useOffHeap)
        try {
          // Output is exact. Phase 2's column readers are handed `finalRanges` and skip the rows
          // outside them within a partly kept page, so emitted rows == survivors.
          val expected = rows.filter(_._1 >= 195L)
          assert(result == expected,
            s"expected exact filtering; got ${result.map(_._1)}, expected ${expected.map(_._1)}")
          assert(m.rowGroupsSkipped.value >= 1,
            s"expected row groups skipped; got ${m.rowGroupsSkipped.value}")
          // "Partially kept" is the `rowsExcludedWithinRowGroup` half of the accounting, and only
          // this identity establishes it. A row group that was neither skipped whole nor emitted
          // has to have its rows counted there.
          val excludedRg = m.rowsExcludedByRowGroup.value
          val excludedPf = m.rowsExcludedWithinRowGroup.value
          assert(excludedPf > 0, s"expected rows excluded inside a kept row group; got $excludedPf")
          assert(result.size + excludedRg + excludedPf == rows.size,
            s"${result.size} emitted plus $excludedRg plus $excludedPf should account for all " +
              s"${rows.size} rows")
          // A mixed projection has non-key bytes to avoid, in both the skipped row groups and
          // the partially kept one. Asserting they are non-negative would prove nothing, since
          // `SQLMetric.add` drops a negative and the value cannot go below zero.
          assert(m.bytesAvoidedByRowGroup.value + m.bytesAvoidedByPageFiltering.value > 0,
            "a mixed projection with skipped row groups should avoid some non-key bytes")
        } finally {
          reader.close()
        }
      }
    }
  }

  test("an all-keys projection: the reader declines the filter and reads plainly") {
    // A scan whose projected columns are all key columns of the filter reads exactly what a plain
    // scan reads, because phase 1 has to read a key column to evaluate the filter on it, and
    // phase 2 is then left with nothing to prune. All the filter can add there is cost, so the
    // reader declines it rather than carrying a second emit path. The planner declines that shape
    // too, which is why this is asserted here, against the reader driven directly.
    withTempDir { dir =>
      val keys = (1L to 200L)
      val path = writeKeyParquetFile(dir, keys, rowGroupSize = 256L)
      val requested = StructType(Seq(StructField("k", LongType, nullable = false)))
      val expr = GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(195L))
      val metrics = allMetrics()
      val filter = createFilter(Seq(expr), requested, metrics)
      val (result, reader) = readAllWith(
        path, Seq("k"), filter, (batch, i) => batch.column(0).getLong(i))
      try {
        assert(result == keys.toSeq,
          s"every row of the file must come back unfiltered; got ${result.size} of ${keys.size}")
        val reported = Seq(metrics.rowGroupsSkipped, metrics.rowsExcludedByRowGroup,
          metrics.rowsExcludedWithinRowGroup, metrics.bytesAvoidedByRowGroup,
          metrics.bytesAvoidedByPageFiltering).map(_.value)
        assert(reported.forall(_ == 0L), s"and the filter must report nothing: $reported")
      } finally {
        reader.close()
      }
    }
  }

  test("Hadoop's 2-arg initialize initializes the reader once") {
    // That overload delegates to the 5-arg one, which runs `initializeInternal`, and it used to run
    // it a second time on top.
    withTempDir { dir =>
      val path = writeKeyParquetFile(dir, 1L to 20L)
      val requested = kvSchema()
      // What `ParquetFileFormat` sets up for the read, down to what the schema converter needs.
      val conf = spark.sessionState.newHadoopConf()
      conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
      conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requested.json)
      Seq(SQLConf.CASE_SENSITIVE, SQLConf.PARQUET_BINARY_AS_STRING,
          SQLConf.PARQUET_INT96_AS_TIMESTAMP, SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED,
          SQLConf.LEGACY_PARQUET_NANOS_AS_LONG).foreach { entry =>
        conf.set(entry.key, spark.sessionState.conf.getConfString(entry.key))
      }
      val split = new FileSplit(new Path(path), 0, new File(path).length(), Array.empty[String])
      val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
      var initializations = 0
      val reader = new VectorizedParquetRecordReader(false, 4096) {
        override protected def initializeInternal(): Unit = {
          initializations += 1
          super.initializeInternal()
        }
      }
      Utils.tryWithResource(reader) { reader =>
        reader.initialize(split, context)
        assert(initializations == 1, s"initialized $initializations times")
      }
    }
  }

  test("ParquetStorageFilter.create rejects a filter that violates a planner precondition") {
    // These are all planner bugs by construction, because storageFiltersFor pre-checks each one.
    // The rows are safe either way, since the conjunct stays in the post-scan Filter, but a soft
    // rejection would hide the bug behind a silently slower read. create fails instead.
    val requested = kvSchema()

    // Nothing to push. The caller is supposed to check this before calling.
    val empty = intercept[IllegalArgumentException] {
      createFilter(Seq.empty, requested)
    }
    assert(empty.getMessage.contains("must be non-empty"), empty.getMessage)

    // Ordinal 5 is out of range for a two-field requested schema.
    val outOfRange = intercept[IllegalArgumentException] {
      createFilter(
        Seq(GreaterThanOrEqual(BoundReference(5, LongType, nullable = false), Literal(0L))),
        requested)
    }
    assert(outOfRange.getMessage.contains("outside the 2 fields"), outOfRange.getMessage)

    // No bound reference at all, so there is no key column to read in phase 1.
    val noRefs = intercept[IllegalArgumentException] {
      createFilter(Seq(GreaterThanOrEqual(Literal(1L), Literal(0L))), requested)
    }
    assert(noRefs.getMessage.contains("no bound reference"), noRefs.getMessage)

    // A key type the reader has no value copier for.
    val variantSchema = StructType(Seq(StructField("k", VariantType, nullable = true)))
    val badType = intercept[IllegalArgumentException] {
      createFilter(
        Seq(IsNull(BoundReference(0, VariantType, nullable = true))), variantSchema)
    }
    assert(badType.getMessage.contains("isSupportedKeyType"), badType.getMessage)

    // A key column named like the synthetic row-index metadata column. This reader finds that
    // column by name and writes row indexes over whatever the file holds, so what a key column of
    // that name reads back would depend on how its row group was read. Both gates reject it. The
    // planner's means the filter is never offered, and this one means a caller past the planner
    // fails rather than reading a key the reader itself overwrote.
    val rowIndexName = ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    val rowIndexSchema = StructType(Seq(
      StructField(rowIndexName, LongType, nullable = false),
      StructField("v", StringType, nullable = false)))
    val rowIndexKey = intercept[IllegalArgumentException] {
      createFilter(
        Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(0L))),
        rowIndexSchema)
    }
    assert(rowIndexKey.getMessage.contains(rowIndexName), rowIndexKey.getMessage)
    val rowIndexAttr = AttributeReference(rowIndexName, LongType, nullable = false)()
    assert(!ParquetStorageFilter.isSupportedStorageFilter(
      BloomFilterMightContain(
        bloomLiteralOf(42L),
        new XxHash64(Seq(rowIndexAttr)))),
      "a bloom over a column named like the row-index column must not be offered")
  }

  // A runtime bloom's `Literal` holding one long key, built the way the planner builds one. It is
  // serialized as `BloomFilterAggregate` serializes it, and the key is hashed with the same seed.
  private def bloomLiteralOf(key: Long): Literal = {
    val bf = BloomFilter.create(10, 128)
    bf.putLong(new XxHash64(Seq(Literal(key))).eval(InternalRow.empty).asInstanceOf[Long])
    Literal(BloomFilterAggregate.serialize(bf), BinaryType)
  }

  test("a plain JDK exception from the key expression gives the row group up") {
    // The reader's fail-open is not narrowed to Spark's own errors, and this is why. A built-in
    // expression that `InjectRuntimeFilter` is happy to build a bloom over can raise a plain JDK
    // one. Rethrowing it would fail a query a plain read answers, and under `ignoreCorruptFiles`
    // an exception from a reader is read as a corrupt file, which drops the rest of it silently.
    //
    // The filter is selective, so returning every row is what proves the give-up. A filter that
    // ran to the end would have dropped the rows below 10 seconds.
    withTempDir { dir =>
      val df = spark.range(0, 20).selectExpr(
        "CAST(CASE WHEN id = 7 THEN 1.0000001 ELSE id END AS DECIMAL(20,7)) AS k",
        "CAST(id AS STRING) AS v")
      val path = writeSingleParquetFile(dir, df, rowGroupSize = 64 * 1024L)
      val decimalType = DecimalType(20, 7)
      val requested = kvSchema(decimalType)
      // Row 7 throws, since 1.0000001 seconds is not a whole number of microseconds.
      val secondsKey = GreaterThanOrEqual(
        SecondsToTimestamp(BoundReference(0, decimalType, nullable = true)),
        Literal.create(10L * 1000000L, TimestampType))
      val filter = createFilter(Seq(secondsKey), requested)
      val (result, reader) = readAllWith(
        path, Seq("k", "v"), filter, (b, i) => b.column(1).getUTF8String(i).toString)
      try {
        assert(result == (0 until 20).map(_.toString),
          s"the row group must come back whole; got ${result.size} rows: $result")
      } finally {
        reader.close()
      }
    }
  }

  test("a checked exception from the key expression gives the row group up") {
    // Nor is the fail-open narrowed to unchecked exceptions. A Hive UDF in the key wraps what it
    // throws in a checked SparkException (FAILED_EXECUTE_UDF), which no signature declares, so a
    // catch of RuntimeException alone would fail a query a plain read answers. The filter is
    // selective, so returning every row is what proves the give-up.
    withTempDir { dir =>
      val rows = (0L until 20L).map(i => (i, i.toString))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L)
      val requested = kvSchema()
      val key = ThrowsCheckedOn(BoundReference(0, LongType, nullable = false), bad = 7L)
      val filter = createFilter(Seq(GreaterThanOrEqual(key, Literal(10L))), requested)
      val (result, reader) = readAll(path, filter)
      try {
        assert(result == rows, s"the row group must come back whole; got ${result.size} rows")
      } finally {
        reader.close()
      }
    }
  }

  test("a task kill or a fatal error behind the key expression's error is not absorbed") {
    // The fail-open must not swallow what the executor has to see. A kill interrupts the task's
    // thread, and a UDF that wraps the interrupt raises its own error, so the kill is read off the
    // task context rather than off the error. A fatal error an expression wrapped is found in the
    // cause chain, which is where the executor looks for one. Either goes out wrapped in a checked
    // exception, since `FileScanRDD` reads a RuntimeException from a reader as a corrupt file under
    // `ignoreCorruptFiles` and would skip the rest of it.
    withTempDir { dir =>
      val rows = (0L until 20L).map(i => (i, i.toString))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L)
      val requested = kvSchema()
      val bound = BoundReference(0, LongType, nullable = false)
      def readWith(key: Expression): SparkException = intercept[SparkException] {
        val filter = createFilter(Seq(GreaterThanOrEqual(key, Literal(10L))), requested)
        readAll(path, filter)._2.close()
      }
      def assertPropagates(thrown: SparkException, original: Class[_]): Unit = {
        assert(!DataSourceUtils.shouldIgnoreCorruptFileException(thrown),
          s"ignoreCorruptFiles must not be able to read this as a corrupt file; got $thrown")
        assert(original.isInstance(thrown.getCause) &&
          thrown.getCause.getMessage.contains("refuses 7"),
          s"the key expression's own error must reach the task behind it; got ${thrown.getCause}")
      }

      def killedWhile(key: Expression): SparkException = {
        TaskContext.setTaskContext(TaskContext.empty())
        try readWith(key) finally TaskContext.unset()
      }
      assertPropagates(killedWhile(ThrowsCheckedOn(bound, bad = 7L, killsTask = true)),
        classOf[SparkException])
      assertPropagates(
        killedWhile(ThrowsCheckedOn(bound, bad = 7L, checked = false, killsTask = true)),
        classOf[IllegalStateException])

      // An unchecked error around an OutOfMemoryError, which is how `WritableColumnVector.reserve`
      // reports one.
      val fatal = readWith(ThrowsCheckedOn(bound, bad = 7L, fatalCause = true, checked = false))
      assertPropagates(fatal, classOf[IllegalStateException])
      assert(fatal.getCause.getCause.isInstanceOf[OutOfMemoryError],
        s"the fatal error must still be in the chain for the executor; got ${fatal.getCause}")
    }
  }

  test("a task kill is seen while the reader works through a row group") {
    // A plain read meets a kill in `FileScanRDD` after every batch. This reader decodes and
    // evaluates a whole row group before its first batch, and can pass row groups the filter
    // empties without emitting one, so it checks for a kill itself, per chunk and per row group.
    withTempDir { dir =>
      val rows = (0L until 100L).map(i => (i, i.toString))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L)
      assert(footerOf(path).getBlocks.size == 1, "the fixture must be one row group")
      val bound = BoundReference(0, LongType, nullable = false)
      // Marks the task killed at row 5, inside phase 1's first chunk of 16 rows. With one row
      // group, only the check at the next chunk can see it before the read ends.
      val filter = createFilter(
        Seq(GreaterThanOrEqual(MarksKilledOn(bound, at = 5L), Literal(50L))), kvSchema())
      TaskContext.setTaskContext(TaskContext.empty())
      val thrown = try {
        intercept[SparkException] {
          readAllWith(path, Seq("k", "v"), filter, (b, i) => b.column(0).getLong(i),
            capacity = 16)._2.close()
        }
      } finally {
        TaskContext.unset()
      }
      assert(thrown.getCause.isInstanceOf[TaskKilledException],
        s"the kill must reach the task; got $thrown")
      assert(!DataSourceUtils.shouldIgnoreCorruptFileException(thrown),
        s"and ignoreCorruptFiles must not be able to swallow it; got $thrown")
    }
  }

  // Returns the FileSourceScanExec `df` plans to, with the storage filters `build` makes attached.
  // `build` resolves a column by name against the scan's own output. Goes through Spark's normal
  // planning/execution machinery (not the test-only reader init), so it exercises
  // preparedStorageFilters' subquery materialization + bind,
  // ParquetFileFormat.buildReaderWithStorageFilters, and metric propagation.
  private def withStorageFilters(df: DataFrame)(
      build: (String => Attribute) => Seq[Expression]): FileSourceScanExec = {
    val scan = scanOf(df.queryExecution.executedPlan)
    def attr(name: String): Attribute =
      scan.output.find(_.name == name).getOrElse(fail(s"No $name in scan output"))
    scan.copy(storageFilters = build(attr))
  }

  // The scan of `df` with a `k >= threshold` storage filter attached.
  private def scanWithStorageFilter(df: DataFrame, threshold: Long): FileSourceScanExec =
    withStorageFilters(df)(attr => Seq(GreaterThanOrEqual(attr("k"), Literal(threshold))))

  // The same over `SELECT k, v` of the file at `path`.
  private def scanWithStorageFilter(path: String, threshold: Long): FileSourceScanExec =
    scanWithStorageFilter(spark.read.parquet(path).select("k", "v"), threshold)

  // Executes a SparkPlan that may produce columnar batches. When the plan supports columnar output
  // (typical for parquet scans with WSCG enabled), Spark's planner normally inserts a
  // ColumnarToRowExec; since these tests bypass the planner, we wrap manually.
  private def collectRows(plan: SparkPlan): Array[InternalRow] = {
    val rowPlan = if (plan.supportsColumnar) ColumnarToRowExec(plan) else plan
    rowPlan.executeCollect()
  }

  // `collectRows` over a `(k, v)` output.
  private def executePlanCollect(plan: SparkPlan): Array[(Long, String)] =
    collectRows(plan).map(r => (r.getLong(0), r.getString(1)))

  test("end-to-end via FileSourceScanExec: the filter is applied and the metrics populated") {
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, threshold = 195L)
        val collected = executePlanCollect(scan).toSeq.sorted
        val expected = rows.filter(_._1 >= 195L)
        assert(collected == expected, s"got $collected; expected $expected")

        val rgSkipped = scan.metrics(StorageFilterMetrics.ROW_GROUPS_SKIPPED)
        assert(rgSkipped.value >= 1,
          s"expected at least one row group skipped via storage filter; got ${rgSkipped.value}")
      }
    }
  }

  test("pushed data filter on a non-key column + storage filter on key: cross-propagation works") {
    // A pushed data filter on a non-key column and a storage filter on the key column have to
    // compose. Phase 0 derives its row ranges from the pushed filter, phase 1 narrows them with the
    // storage filter, and phase 2 reads the non-key columns under the intersection.
    //
    // Note the predicate deliberately uses `>` rather than `!=`. ColumnIndexFilter substitutes
    // `rangesForMissingColumns` for a predicate over a column outside its path set, and that is
    // EMPTY for Gt/GtEq/Lt/LtEq/Eq but allRows for NotEq, so a `!=` predicate here would be
    // satisfied by a phase 0 that saw the wrong schema, and would prove nothing.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, f"v_$i%03d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).select("k", "v").filter("v > 'v_000'")
        val pushed = scanOf(df.queryExecution.executedPlan).simpleString(200)
        assert(pushed.contains("GreaterThan(v,"),
          s"the data filter must actually be pushed for this test to mean anything: $pushed")
        // Storage filter on `k` (key column).
        val withSF = scanWithStorageFilter(df, threshold = 100L)

        val collected = executePlanCollect(withSF).toSeq.sorted
        // Every row satisfies v > 'v_000', so the storage filter alone decides the result.
        val expected = rows.filter(_._1 >= 100L)
        assert(collected == expected, s"got $collected; expected $expected")
      }
    }
  }

  // ----- FileSourceStrategy bloom-filter extraction -----

  // Counts BloomFilterMightContain expressions inside FilterExec nodes of a physical plan.
  // `collect` from AdaptiveSparkPlanHelper rather than SparkPlan's, which stops at an
  // `AdaptiveSparkPlanExec` and would report zero for every plan AQE wrapped.
  private def countBloomFiltersInPostScanFilters(plan: SparkPlan): Int = {
    collect(plan) {
      case f: FilterExec =>
        f.condition.collect { case _: BloomFilterMightContain => 1 }.sum
    }.sum
  }

  // Counts BloomFilterMightContain expressions inside FileSourceScanExec.storageFilters.
  private def countBloomFiltersInStorageFilters(plan: SparkPlan): Int = {
    collect(plan) {
      case s: FileSourceScanExec =>
        s.storageFilters.map(_.collect { case _: BloomFilterMightContain => 1 }.sum).sum
    }.sum
  }

  // Sets up two parquet tables and runs a join that triggers `InjectRuntimeFilter` for the
  // application-side scan. Returns the executed plan and the query result for inspection. Tables
  // are cleaned up automatically by the calling test (table names are passed through).
  private def runBloomFilterJoin(): (SparkPlan, Array[Row]) = {
    val query =
      """SELECT bf1.k, bf1.v
        |FROM bf1 JOIN bf2 ON bf1.k = bf2.k
        |WHERE bf2.v = 5
        |""".stripMargin
    val df = spark.sql(query)
    (df.queryExecution.executedPlan, df.collect())
  }

  // Creates two parquet tables for join tests, a large `bf1` and a small `bf2` with a selective
  // filter, and sets what it takes for the optimizer to build a bloom over them at all. The
  // application side must look big enough to be worth one, and the join must not become a
  // broadcast. Every test below needs both, so they are here rather than in each, and a test states
  // only what it varies.
  private def withBloomFilterTables(body: => Unit): Unit = {
    withTable("bf1", "bf2") {
      // bf1 is the large application side, 600 rows.
      spark.range(600).selectExpr("id AS k", "id AS v").write.format("parquet").saveAsTable("bf1")
      // bf2 is the small creation side, 30 rows, with a selective filter (v = 5 keeps 1 row).
      spark.range(30).selectExpr("id AS k", "id AS v").write.format("parquet").saveAsTable("bf2")
      withSQLConf(
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200") {
        body
      }
    }
  }

  Seq(false, true).foreach { ignoreCorruptFiles =>
    test("FileSourceStrategy offers the bloom to the scan and keeps it post-scan too " +
        s"(ignoreCorruptFiles = $ignoreCorruptFiles)") {
      // The scan gets the conjunct to prune with, and the post-scan Filter keeps it, so the answer
      // never depends on what the reader managed to do with it. ignoreCorruptFiles does not turn
      // the feature off.
      withBloomFilterTables {
        withSQLConf(
            SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
            SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorruptFiles.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val (plan, rows) = runBloomFilterJoin()
          val storageBlooms = countBloomFiltersInStorageFilters(plan)
          assert(storageBlooms == 1,
            s"expected the bloom on scan.storageFilters; got $storageBlooms.\nPlan:\n$plan")
          assert(countBloomFiltersInPostScanFilters(plan) >= 1,
            s"and the post-scan Filter keeps it.\nPlan:\n$plan")
          assert(rows.map(r => (r.getLong(0), r.getLong(1))).toSeq == Seq((5L, 5L)),
            s"and the query must still return the joined row; got ${rows.mkString(", ")}")
          // The three assertions above hold whether or not the reader honored the filter, since
          // the post-scan Filter answers the query either way. The metrics are what say it did.
          val scan = plan.collect { case s: FileSourceScanExec => s }
            .find(_.storageFilters.nonEmpty)
            .getOrElse(fail(s"no scan with storage filters:\n$plan"))
          val excluded =
            scan.metrics(StorageFilterMetrics.ROWS_EXCLUDED_BY_ROW_GROUP).value +
              scan.metrics(StorageFilterMetrics.ROWS_EXCLUDED_WITHIN_ROW_GROUP).value
          assert(excluded > 0, s"the reader must have excluded rows; the metrics say $excluded")
        }
      }
    }
  }

  test("FileSourceStrategy leaves the bloom behind when every projected column is a key column") {
    // Nothing is left for phase 2 to prune. The reader would read the same column for the same rows
    // as a plain scan, since it has to read a key column to evaluate the filter on it, and would
    // add only the cost of evaluating the predicate outside the generated code.
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = spark.sql("SELECT bf1.k FROM bf1 JOIN bf2 ON bf1.k = bf2.k WHERE bf2.v = 5")
        val plan = df.queryExecution.executedPlan
        val storageBlooms = countBloomFiltersInStorageFilters(plan)
        val postScanBlooms = countBloomFiltersInPostScanFilters(plan)
        assert(storageBlooms == 0,
          s"expected no bloom on scan.storageFilters for an all-keys projection; got " +
            s"$storageBlooms.\nPlan:\n$plan")
        assert(postScanBlooms >= 1,
          s"expected the bloom to stay in a post-scan FilterExec; got $postScanBlooms.\n" +
            s"Plan:\n$plan")
        assert(df.collect().map(_.getLong(0)).toSeq == Seq(5L),
          "and the query must still return the joined key")
      }
    }
  }

  test("a row group over the splice cap reads its key columns again, same rows, more bytes") {
    // Past the cap a row group gives splicing up. Phase 2 takes every projected column, key
    // columns included, so nothing is buffered. Two things have to hold. The rows must not
    // change, since a fallback that quietly dropped the predicate would return extra rows. And
    // the fallback must actually have happened, which is observable. It reads the key column a
    // second time, so it transfers strictly more bytes. Without that second assertion the test
    // would pass even if the cap never reached the reader.
    //
    // The batch size is not what makes the cap reachable, since the count is examined after
    // every surviving row. It is here because a row group that gave splicing up past the cap still
    // has to emit in batches, and 51 survivors over a capacity of 16 span several of them.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
      val fileSchema = kvSchema()
      val storageFilters =
        Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(350L)))

      def run(maxSplicedBytes: String): (Int, Long) = {
        withSQLConf(
            SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_MAX_SPLICED_ROW_GROUP_BYTES.key ->
              maxSplicedBytes,
            SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "16") {
          readCounting(path, formatReader(fileSchema, Nil, storageFilters, countingHadoopConf()))
        }
      }

      val (splicedRows, splicedBytes) = run("64MB")
      // 100 bytes is past what 16 survivors of a long key buffer, and still leaves room for the
      // row ranges. `k >= 350` keeps a contiguous run, so there is one range to hold.
      val (plainRows, plainBytes) = run("100b")
      assert(splicedRows == 51, s"the filter keeps keys 350..400; got $splicedRows")
      assert(plainRows == splicedRows,
        s"rows differ past the cap: plain=$plainRows spliced=$splicedRows")
      assert(plainBytes > splicedBytes,
        s"past the cap the key column is read twice, so the read must be larger; " +
          s"plain=$plainBytes spliced=$splicedBytes")
    }
  }

  test("row groups that splice and row groups over the cap, in both orders") {
    // The emitted batch is one object for the whole read, so its key slots have to follow the path
    // each row group took. This file makes all four cases occur in order, at 100 rows per row group
    // and a batch capacity of 16:
    //  - rows 1-100: 8 survivors, which buffer 72 bytes and fall in one range of 40, inside the
    //    150-byte cap, so the row group splices;
    //  - rows 101-200: 51 survivors, so the buffer passes the cap and phase 2 reads every projected
    //    column. The key slots must go back to the persistent vectors here, or the batch reads keys
    //    out of a vector the previous row group's last batch already released;
    //  - rows 201-300: 6 survivors, splicing again, which needs the accumulators that giving
    //    splicing up released to be allocated afresh;
    //  - rows 301-400: no survivor at all, so the row group is skipped whole.
    //
    // Hence the assertion on the values rather than on the row count alone, and the skipped row
    // group metric, which pins that the file really did split into several row groups.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      val m = allMetrics()
      val k = BoundReference(0, LongType, nullable = true)
      def between(lo: Long, hi: Long): Expression =
        And(GreaterThanOrEqual(k, Literal(lo)), LessThanOrEqual(k, Literal(hi)))
      val filter = createFilter(
        Seq(Or(Or(LessThanOrEqual(k, Literal(8L)), between(150L, 200L)), between(250L, 255L))),
        kvSchema(),
        m,
        maxSplicedRowGroupBytes = 150L)
      val (result, reader) = readAllWith(path, Seq("k", "v"), filter,
        (batch, i) => (batch.column(0).getLong(i), batch.column(1).getUTF8String(i).toString),
        capacity = 16)
      try {
        val expected = rows.filter { case (key, _) =>
          key <= 8L || (key >= 150L && key <= 200L) || (key >= 250L && key <= 255L)
        }
        assert(result == expected,
          s"expected each surviving key with its own value; got ${result.take(20)}")
        assert(m.rowGroupsSkipped.value >= 1,
          s"expected a row group with no survivor at all; got ${m.rowGroupsSkipped.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("a row group past the cap charges its second key read against the byte metric") {
    // Giving splicing up means phase 2 reads the key columns a second time, while the baseline
    // counts them once, in phase 1. That extra read is a cost against the saving, so the same file
    // and filter must report less avoided than they do while splicing. Without that term the metric
    // would credit the feature with bytes it did transfer.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
      val fileSchema = kvSchema()

      def avoidedBytes(cap: Long, threshold: Long): Long = {
        val m = allMetrics()
        val filter = createFilter(
          Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(threshold))),
          fileSchema, m, cap)
        val (_, reader) = readAllWith(path, Seq("k", "v"), filter,
          (b, i) => b.column(0).getLong(i), capacity = 16)
        try m.bytesAvoidedByPageFiltering.value finally reader.close()
      }

      val spliced = avoidedBytes(64L * 1024 * 1024, 350L)
      val plain = avoidedBytes(100L, 350L)
      assert(spliced > 0, s"page filtering has to avoid something here; got $spliced")
      assert(plain < spliced,
        s"the second key read must count against the saving; plain=$plain spliced=$spliced")

      // The two halves of the budget are weighed together, and this is the case that says so. Eight
      // survivors of a long key buffer 8 * (1 + 8) = 72 bytes and fall in one range of 40, so
      // neither half reaches the 100-byte cap on its own while the sum passes it at the seventh.
      // Weighing them separately would keep splicing here, and report the larger saving for it.
      val eightSpliced = avoidedBytes(64L * 1024 * 1024, 393L)
      val eightPlain = avoidedBytes(100L, 393L)
      assert(eightSpliced > 0, s"and avoid something with eight survivors; got $eightSpliced")
      assert(eightPlain < eightSpliced,
        s"the sum of the two halves must cross the cap; plain=$eightPlain spliced=$eightSpliced")
    }
  }

  test("an accumulator sized up front is charged for its byte child when it is sized") {
    // Every accumulator of a row group after the first sizes a variable-length key's byte child to
    // what the one before it held, before any value arrives. Charged only as values arrive, the
    // last accumulator of a row group would hold a whole accumulator's worth of memory for a few
    // survivors' charge. Six survivors of a 1000-byte key over a capacity of 4 are charged about
    // 24KB value by value, and about 32KB once the second accumulator's byte child is charged when
    // it is sized. A cap between the two gives splicing up only in the second case, which the
    // second read of the key column shows.
    withTempDir { dir =>
      val df = spark.range(1, 21).selectExpr(
        "CONCAT(LPAD(CAST(id AS STRING), 4, '0'), REPEAT('x', 996)) AS k",
        "CAST(id AS STRING) AS v")
      val path = writeSingleParquetFile(dir, df, rowGroupSize = 64 * 1024L)
      assert(footerOf(path).getBlocks.size == 1, "the fixture must be one row group")
      val fileSchema = kvSchema(keyType = StringType)
      val storageFilters = Seq(GreaterThanOrEqual(
        BoundReference(0, StringType, nullable = true), Literal.create("0015", StringType)))

      def run(maxSplicedBytes: String): (Int, Long) = withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_MAX_SPLICED_ROW_GROUP_BYTES.key ->
            maxSplicedBytes,
          SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "4") {
        readCounting(path, formatReader(fileSchema, Nil, storageFilters, countingHadoopConf()))
      }

      val (splicedRows, splicedBytes) = run("64MB")
      val (cappedRows, cappedBytes) = run("28000b")
      assert(splicedRows == 6 && cappedRows == 6,
        s"the filter keeps keys 15..20 either way; got $splicedRows and $cappedRows")
      assert(cappedBytes > splicedBytes,
        "the second accumulator's byte child must count against the cap, so the row group gives " +
          s"splicing up and reads the key column again; capped=$cappedBytes spliced=$splicedBytes")

      // And the keys spliced from the accumulator sized up front are the right ones.
      val (pairs, reader) = readAllWith(
        path, Seq("k", "v"), createFilter(storageFilters, fileSchema),
        (b, i) => (b.column(0).getUTF8String(i).toString, b.column(1).getUTF8String(i).toString),
        capacity = 4)
      try {
        assert(pairs == (15 to 20).map(id => (f"$id%04d" + "x" * 996, id.toString)),
          s"every survivor must come back with its own key; got ${pairs.map(_._2)}")
      } finally {
        reader.close()
      }
    }
  }

  // Writes a parquet file the low-level way, with an offset index for the chunks `hasOffsetIndex`
  // names by block and column. The `ParquetFileWriter.writeDataPage` overloads that take no row
  // count use parquet's no-op offset index builder, which is how a writer other than parquet-mr's
  // own produces a chunk that cannot be read in part. Every column is a required int64, so one page
  // writer serves all.
  private def writeParquetFileByHand(
      dir: File,
      blocks: Seq[Seq[(Long, Long)]],
      hasOffsetIndex: (Int, Int) => Boolean): String = {
    val schema = MessageTypeParser.parseMessageType(
      "message spark_schema { required int64 k; required int64 v; }")
    val file = new File(dir, s"no-offset-index-${System.nanoTime()}.parquet")
    val hadoopPath = new Path(file.getAbsolutePath)
    val writer = new ParquetFileWriter(
      HadoopOutputFile.fromPath(hadoopPath, spark.sessionState.newHadoopConf()),
      schema, ParquetFileWriter.Mode.CREATE, 128L * 1024 * 1024, 8)
    writer.start()
    blocks.zipWithIndex.foreach { case (block, blockIdx) =>
      writer.startBlock(block.size)
      schema.getColumns.asScala.zipWithIndex.foreach { case (cd, colIdx) =>
        val pageStore = new MemPageStore(block.size)
        val writeStore = new ColumnWriteStoreV1(pageStore,
          // The row-count check is what decides when a page is flushed, so without lowering it a
          // block of a few hundred rows comes out as one page and no column index can narrow it.
          ParquetProperties.builder().withPageSize(256).withMinRowCountForPageSizeCheck(1)
            .withDictionaryEncoding(false).build())
        val columnWriter = writeStore.getColumnWriter(cd)
        block.foreach { row =>
          columnWriter.write(if (colIdx == 0) row._1 else row._2, 0, 0)
          writeStore.endRecord()
        }
        writeStore.flush()
        writer.startColumn(cd, block.size, CompressionCodecName.UNCOMPRESSED)
        val pageReader = pageStore.getPageReader(cd)
        var written = 0L
        while (written < block.size) {
          val page = pageReader.readPage().asInstanceOf[DataPageV1]
          if (hasOffsetIndex(blockIdx, colIdx)) {
            writer.writeDataPage(page.getValueCount, page.getUncompressedSize, page.getBytes,
              page.getStatistics, page.getValueCount.toLong, page.getRlEncoding,
              page.getDlEncoding, page.getValueEncoding)
          } else {
            writer.writeDataPage(page.getValueCount, page.getUncompressedSize, page.getBytes,
              page.getStatistics, page.getRlEncoding, page.getDlEncoding, page.getValueEncoding)
          }
          written += page.getValueCount
        }
        writer.endColumn()
      }
      writer.endBlock()
    }
    writer.end(new java.util.HashMap[String, String]())
    file.getAbsolutePath
  }

  test("a file written with no offset index is read with the filter given up") {
    // Reading part of a row group needs an offset index, so a file without one cannot be filtered
    // at page level. What it can still do is skip a row group the filter empties, which needs no
    // index at all, and that is the split this test pins. The footer check sees the missing index
    // for every row group before anything is read, so no read fails here. The middle row group is
    // emptied by the filter and skipped whole, and the other two keep rows and are read whole.
    // Spark's own writer always writes the page index, hence the hand-built file. The retry after a
    // failing read is covered by the test on a file whose index parquet cannot parse.
    withTempDir { dir =>
      val blocks = Seq(
        (1L to 100L).map(i => (i, i * 10)),
        (101L to 200L).map(i => (i, i * 10)),
        (201L to 300L).map(i => (i, i * 10)))
      val path = writeParquetFileByHand(dir, blocks, hasOffsetIndex = (_, _) => false)
      // The fixture's whole point, asserted rather than assumed.
      assert(footerChunks(path).forall(_.getOffsetIndexReference == null),
        "no chunk of the hand-built file may have an offset index")
      val m = allMetrics()
      val requested = kvSchema(valueType = LongType)
      // Keeps part of the first row group, none of the second, part of the third.
      val k = BoundReference(0, LongType, nullable = false)
      val filter = createFilter(
        Seq(Or(
          And(GreaterThanOrEqual(k, Literal(50L)), LessThanOrEqual(k, Literal(60L))),
          And(GreaterThanOrEqual(k, Literal(250L)), LessThanOrEqual(k, Literal(260L))))),
        requested,
        m)
      val ((result, reader), warned) = withPartialReadWarning {
        readAllWith(path, Seq("k", "v"), filter,
          (b, i) => (b.column(0).getLong(i), b.column(1).getLong(i)))
      }
      try {
        assert(result == blocks(0) ++ blocks(2),
          s"the emptied row group is skipped and the other two read whole; got ${result.size} rows")
        // The footer says which columns have an offset index, so the reader knows before phase 1
        // buffers anything, and the phase-2 read never asks parquet for a narrowed one. Without
        // that, the first partially surviving row group copies its survivors and the exception path
        // throws them away, which is what this WARN reports.
        assert(!warned, "the footer check must get there before the exception path")
        assert(m.rowGroupsSkipped.value == 1 && m.rowsExcludedByRowGroup.value == 100,
          s"one row group skipped whole, with its rows counted; got " +
            s"${m.rowGroupsSkipped.value} and ${m.rowsExcludedByRowGroup.value}")
        assert(m.rowsExcludedWithinRowGroup.value == 0,
          s"and nothing excluded inside a row group, which needs the index; got " +
            s"${m.rowsExcludedWithinRowGroup.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("a key column with no offset index does not cost the file its page filtering") {
    // Reading part of a row group goes through the block's column index store, and parquet builds
    // that store over every column of the requested schema, emptying it altogether if one of them
    // has no offset index. So a row group whose key column has none cannot be narrowed either, and
    // the footer check has to ask about the whole projection. Asking only about the columns phase 2
    // reads would let such a row group buffer its survivors and then have phase 2 throw, and the
    // reader answers that by no longer narrowing a row group for the rest of the split. The pushed
    // data filter's page pruning survives that, so the rows would not show it. The WARN the
    // exception path logs is what does.
    //
    // In the file, the first row group's key column has no offset index while its value column
    // does, and the other two have one for both. The pushed data filter matches one value per row
    // group, so parquet's statistics filter keeps all three while its column index narrows the two
    // it can to the page that value is in.
    withTempDir { dir =>
      val blocks = (0 until 3).map(b => ((b * 100 + 1L) to (b * 100 + 100L)).map(i => (i, i)))
      val path = writeParquetFileByHand(dir, blocks,
        (blockIdx, colIdx) => blockIdx > 0 || colIdx == 1)
      // The fixture's whole point, asserted rather than assumed.
      val indexed = footerChunks(path)
        .map(c => (c.getPath.toDotString, c.getOffsetIndexReference != null))
      assert(indexed == Seq(("k", false), ("v", true), ("k", true), ("v", true),
          ("k", true), ("v", true)),
        s"the first row group's key column must be the only chunk without one; got $indexed")
      val schema = kvSchema(valueType = LongType, nullable = false)
      val pushed = Seq(sources.In("v", Array[Any](1L, 101L, 201L)))
      // Keeps part of each row group, so a row group that can be narrowed is narrowed.
      val storageFilters =
        Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(50L)))
      val (values, warned) = withPartialReadWarning {
        readLongs(formatReader(schema, pushed, storageFilters), path, ordinal = 1)
      }
      // The footer check gets there before the phase-2 read does, which is what keeps the rest of
      // the file's page filtering. This WARN is what the exception path would have reported.
      assert(!warned,
        "the footer check must decline the row group rather than the read discovering it")
      // The row group that cannot be narrowed comes back whole, since its filter is given up and
      // the empty store widens its ranges to the block.
      assert((1L to 100L).forall(values.contains),
        s"the first row group must come back whole; got ${values.count(_ <= 100L)} of its rows")
      // The other two keep their page pruning. Read whole they would contribute 200 rows rather
      // than one page each.
      assert(values.size < 300,
        s"the row groups that can be narrowed must still be; got all ${values.size} rows")
      // And every row the pushed filter actually matches survives the narrowing.
      assert(Seq(1L, 101L, 201L).forall(values.contains),
        s"the matching rows must be in the output; got ${values.size} rows")
    }
  }

  // Overwrites the bytes of one chunk's offset index while leaving the footer's reference to it.
  // Parquet then reads the reference, fails to parse what it points at, and reports the whole block
  // as having no column index, which is the one file shape that reaches phase 2's retry.
  private def breakOffsetIndex(filePath: String, blockIdx: Int, columnIdx: Int): Unit = {
    val chunk = footerOf(filePath).getBlocks.get(blockIdx).getColumns.get(columnIdx)
    val ref = chunk.getOffsetIndexReference
    assert(ref != null, s"block $blockIdx column $columnIdx has no offset index to damage")
    overwriteBytes(filePath, ref.getOffset, ref.getLength)
  }

  // Overwrites the back half of one data page of `column` in the first row group, which leaves the
  // page header, and so everything parquet reads eagerly, intact and the page body undecodable.
  private def corruptDataPage(filePath: String, column: String, pageIdx: Int): Unit = {
    val (offset, size) = Utils.tryWithResource(ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(filePath), spark.sessionState.newHadoopConf()))) {
      reader =>
        val chunk = reader.getRowGroups.get(0).getColumns.asScala
          .find(_.getPath.toDotString == column).getOrElse(fail(s"no $column chunk"))
        val index = reader.readOffsetIndex(chunk)
        (index.getOffset(pageIdx), index.getCompressedPageSize(pageIdx))
    }
    overwriteBytes(filePath, offset + size / 2, size - size / 2)
  }

  // Overwrites `length` bytes at `offset` with 0xFF.
  private def overwriteBytes(filePath: String, offset: Long, length: Int): Unit = {
    val file = new File(filePath)
    val raf = new RandomAccessFile(file, "rw")
    try {
      raf.seek(offset)
      raf.write(Array.fill(length)(0xFF.toByte))
    } finally {
      raf.close()
    }
    // Hadoop's local filesystem checks a side-car checksum that the write above invalidates, and a
    // mismatch there would fail the read for the wrong reason.
    val crc = new File(file.getParentFile, s".${file.getName}.crc")
    if (crc.exists()) assert(crc.delete(), s"could not remove $crc")
  }

  test("a corrupt key page gives the row group up and fails where a plain read fails") {
    // Parquet reads a chunk's bytes up front but decodes a page only when it is read, so a corrupt
    // key page shows in phase 1. When its decoding raises an exception, as it does here, giving the
    // row group up hands it to phase 2 whole, which reads the same pages in the same batches as a
    // plain read, so the page fails at the same batch, after the same rows. That is what makes
    // `ignoreCorruptFiles` keep exactly what it keeps of a plain read, and it is asserted here on
    // the readers themselves, as the rows each hands back before it throws. The filter is
    // selective, so a reader that kept applying it would return fewer. A codec that reports
    // corruption as an Error instead, as hadoop-lzo does, fails before the row group's first batch.
    withTempDir { dir =>
      val rows = (0L until 1000L).map(i => (i, s"v_$i"))
      val outDir = new File(dir, "damaged").getAbsolutePath
      // One row group of 100-row pages, so the fifth key page covers rows 400 to 499.
      rows.toDF("k", "v").repartition(1).write
        .option(ParquetOutputFormat.BLOCK_SIZE, 64L * 1024 * 1024)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .option(ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT, 100L)
        .parquet(outDir)
      val path = new File(outDir).listFiles((_, name) => name.endsWith(".parquet"))
        .head.getAbsolutePath
      corruptDataPage(path, column = "k", pageIdx = 4)

      def readUntilFailure(storageFilter: ParquetStorageFilter): (Seq[Long], Throwable) = {
        val reader = if (storageFilter == null) {
          new VectorizedParquetRecordReader(false, 16)
        } else {
          new LateMaterializationParquetRecordReader(false, 16, storageFilter)
        }
        val read = mutable.ArrayBuffer[Long]()
        try {
          reader.initialize(path, Seq("k", "v").asJava)
          reader.initBatch(new StructType(), null)
          val error = try {
            while (reader.nextBatch()) {
              val batch = reader.resultBatch()
              (0 until batch.numRows()).foreach(i => read += batch.column(0).getLong(i))
            }
            null
          } catch {
            case NonFatal(e) => e
          }
          (read.toSeq, error)
        } finally {
          reader.close()
        }
      }

      val (plainRows, plainError) = readUntilFailure(null)
      assert(plainError != null, "the damaged page must fail a plain read, or this proves nothing")
      assert(plainRows == (0L until 400L), s"a plain read returns the rows before that page's " +
        s"batch; got ${plainRows.size}")
      val (filteredRows, filteredError) = readUntilFailure(keyAtLeastFilter(500L))
      assert(filteredError != null && filteredError.getClass == plainError.getClass,
        s"the filtering read must fail the same way; got $filteredError, not $plainError")
      assert(filteredRows == plainRows,
        s"and after the same rows; got ${filteredRows.size}, not ${plainRows.size}")
    }
  }

  test("a row group whose offset index parquet cannot parse leaves the rest of the file its " +
    "page pruning") {
    // The retry leaves the reader unable to narrow the rest of this file, since the next row
    // group's store may be as unreadable as this one's. What it must not give up is the pushed data
    // filter's own page pruning, which parquet did before the reader saw the block, and which a
    // plain read of the same query keeps. Dropping that would read more than having no storage
    // filter at all.
    //
    // One file shape reaches the retry, a footer that references an offset index parquet cannot
    // then parse, so the fixture is hand-built and then damaged.
    withTempDir { dir =>
      val blocks = (0 until 3).map(b => ((b * 100 + 1L) to (b * 100 + 100L)).map(i => (i, i)))
      val path = writeParquetFileByHand(dir, blocks, (_, _) => true)
      breakOffsetIndex(path, blockIdx = 1, columnIdx = 0)
      // The fixture's whole point. The footer still offers the index the read cannot use, which is
      // what gets past the footer check and into the retry.
      assert(footerOf(path).getBlocks.get(1).getColumns.get(0).getOffsetIndexReference != null,
        "the damaged index must still be referenced, or the footer check declines the row group")
      val schema = kvSchema(valueType = LongType, nullable = false)
      // One value per row group, so parquet's statistics keep every one of them and its column
      // index narrows each to the page holding that value. All three are even, so all three also
      // survive the storage filter below.
      val pushed = Seq(sources.In("v", Array[Any](2L, 102L, 202L)))
      val evenKeys = Seq(EqualTo(
        Remainder(BoundReference(0, LongType, nullable = false), Literal(2L)), Literal(0L)))
      def read(storageFilters: Seq[Expression]): Seq[Long] =
        readLongs(formatReader(schema, pushed, storageFilters), path, ordinal = 0)
      val (filtered, warned) = withPartialReadWarning(read(evenKeys))
      assert(warned, "the read must have discovered the damaged index, or this test proves nothing")
      // A plain read of the same query is the bar. The filtering one must never hand the post-scan
      // Filter more rows than that.
      val plain = read(Nil)
      assert(filtered.size <= plain.size,
        s"the filtering read must not read more than a plain one; got ${filtered.size} rows " +
          s"against ${plain.size}")
      // The sharp form of the same thing. The last row group's index is intact, so its pages are
      // still pruned after the retry has latched.
      assert(filtered.count(_ > 200L) < 100,
        s"the last row group must keep its pruning; got ${filtered.count(_ > 200L)} of its 100")
      // The damaged row group comes back whole, which is the cost that is inherent to it.
      assert((101L to 200L).forall(filtered.contains),
        s"the damaged row group must come back whole; got " +
          s"${filtered.count(k => k > 100L && k <= 200L)} of its 100")
      // And every row the pushed filter matches is in the output, all three being even.
      assert(Seq(2L, 102L, 202L).forall(filtered.contains),
        s"the matching rows must survive; got ${filtered.size} rows")
    }
  }

  test("scattered survivors: phase 2 lines its values up with the spliced keys, or gives up") {
    // An alternating filter makes one range per surviving row, which is the shape that exercises
    // phase 2's range walk and the only one where a defect there is invisible from the row count.
    // The keys come from the survivor queue and would still be right while the values came from
    // other rows. So this asserts the pairs.
    //
    // Past the cap those ranges cost more than the budget allows, and the filter is given up for
    // that row group. Every one of its rows is emitted and the post-scan Filter narrows them.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
      val fileSchema = kvSchema()
      val everyOtherRow = EqualTo(
        Remainder(BoundReference(0, LongType, nullable = true), Literal(2L)), Literal(0L))

      def readWithCap(cap: Long, capacity: Int = 4096): Seq[(Long, String)] = {
        val filter =
          createFilter(Seq(everyOtherRow), fileSchema, allMetrics(), cap)
        val (emitted, reader) = readAllWith(path, Seq("k", "v"), filter,
          (b, i) => (b.column(0).getLong(i), b.column(1).getUTF8String(i).toString), capacity)
        try emitted finally reader.close()
      }

      assert(readWithCap(64L * 1024 * 1024) == rows.filter(_._1 % 2 == 0),
        "with room for the ranges, every surviving row comes back with its own value")
      // Each survivor costs 9 bytes of long key and 40 of its own range. The sum passes the cap of
      // 1024 at survivor 21, which releases the buffer, and the ranges alone at survivor 26, which
      // gives the filter up. The capacity decides what the buffer holds when it is released. At
      // 4096 that is one partly filled set of accumulators, and at 16 a full set in the queue as
      // well. Both have to end with the queue empty and the filter given up. Keeping one without
      // the other would leave phase 2 reading non-key columns while emit spliced keys from a queue
      // holding only the first survivors.
      assert(readWithCap(1024L) == rows,
        "past the cap the filter is given up and the row group comes back whole")
      assert(readWithCap(1024L, capacity = 16) == rows,
        "and the same when the buffer has been rolled over first")
    }
  }

  test("a row group that gives the filter up does not take the next one with it") {
    // `filterGivenUp` is per row group, reset at the top of each one, so a row group whose ranges
    // pass the budget must not disarm the filter for the rest of the file. The first row group here
    // scatters into 50 ranges and gives up, the last has its survivors in one run and keeps
    // filtering, and the two in between are emptied by the filter and skipped whole.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      val k = BoundReference(0, LongType, nullable = true)
      val scatteredInFirst = And(LessThanOrEqual(k, Literal(100L)),
        EqualTo(Remainder(k, Literal(2L)), Literal(0L)))
      val runInLast = And(GreaterThanOrEqual(k, Literal(301L)), LessThanOrEqual(k, Literal(320L)))
      val m = allMetrics()
      val filter = createFilter(
        Seq(Or(scatteredInFirst, runInLast)),
        kvSchema(),
        m,
        maxSplicedRowGroupBytes = 1024L)
      val (result, reader) = readAllWith(path, Seq("k", "v"), filter,
        (b, i) => (b.column(0).getLong(i), b.column(1).getUTF8String(i).toString))
      try {
        // The first row group comes back whole, the last only its surviving run.
        val expected = rows.filter(_._1 <= 100L) ++ rows.filter(r => r._1 >= 301L && r._1 <= 320L)
        assert(result == expected,
          s"expected the given-up row group whole and the last filtered; got ${result.size} rows")
        assert(m.rowGroupsSkipped.value == 2,
          s"and the two emptied row groups skipped; got ${m.rowGroupsSkipped.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("ANSI mode: a cast join key is pushed, and an evaluation error gives the row group up") {
    // `InjectRuntimeFilter` hashes the join key, so a string-to-long join hands the bloom a
    // `CAST(s AS BIGINT)`, which throws in ANSI mode on a row whose string is not a number. In the
    // plan the bloom runs after the conjunct that excludes such rows, while the reader evaluates it
    // on every row of the ranges the pushed filter left, so it does meet that row.
    //
    // It is pushed all the same. When the evaluation throws, the reader gives the filter up for
    // that row group, and the post-scan Filter evaluates the conjuncts in their own order, where
    // `kind = 'num'` drops the row before the cast runs. So the query returns its rows instead of
    // failing, which is what it does with the feature off.
    withTable("ansi_app", "ansi_build") {
      // One file, one row group, with the non-numeric row inside it. A separate file would be
      // pruned whole by the pushed `kind = 'num'` filter and the reader would never see the row.
      spark.range(600)
        .selectExpr(
          "CASE WHEN id = 7 THEN 'not-a-number' ELSE CAST(id AS STRING) END AS k",
          "CASE WHEN id = 7 THEN 'text' ELSE 'num' END AS kind")
        .repartition(1).write.format("parquet").saveAsTable("ansi_app")
      spark.range(30).selectExpr("id AS k", "id AS v")
        .write.format("parquet").saveAsTable("ansi_build")
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val query = "SELECT a.k FROM ansi_app a JOIN ansi_build b ON CAST(a.k AS BIGINT) = b.k " +
          "WHERE b.v = 5 AND a.kind = 'num'"
        val df = spark.sql(query)
        val plan = df.queryExecution.executedPlan
        assert(countBloomFiltersInPostScanFilters(plan) >= 1,
          s"a bloom is expected above the scan, otherwise this proves nothing.\nPlan:\n$plan")
        assert(countBloomFiltersInStorageFilters(plan) == 1,
          s"the cast key is pushed.\nPlan:\n$plan")
        assert(df.collect().map(_.getString(0)).toSeq == Seq("5"),
          "and the query must run rather than fail on the non-numeric row")
      }
    }
  }

  test("FileSourceStrategy leaves a non-deterministic bloom in the post-scan Filter") {
    // The reader drops the rows a storage filter rejects and the post-scan Filter evaluates it
    // again on the rows the reader keeps, so the two evaluations have to agree, and a
    // non-deterministic conjunct has to stay behind. No producer builds one today, hence the
    // hand-built plan. `InjectRuntimeFilter`'s blooms hash join keys, which are deterministic.
    withTempDir { dir =>
      val rows = (1L to 50L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val bloomLit = bloomLiteralOf(42L)
        val relation = spark.read.parquet(path).select("k", "v").queryExecution.optimizedPlan
        val k = relation.output.find(_.name == "k").getOrElse(fail("no k in the relation output"))

        def extractedBlooms(valueExpr: Expression): (Int, Int) = {
          val logical = LogicalFilter(BloomFilterMightContain(bloomLit, valueExpr), relation)
          val physical = FileSourceStrategy(logical).headOption
            .getOrElse(fail(s"FileSourceStrategy did not plan $logical"))
          (countBloomFiltersInStorageFilters(physical),
            countBloomFiltersInPostScanFilters(physical))
        }

        // As a control, the same bloom over the key alone is offered to the scan, so the arms
        // differ in exactly one thing. It stays in the post-scan Filter either way.
        val (deterministicInScan, deterministicPostScan) =
          extractedBlooms(new XxHash64(Seq(k)))
        assert(deterministicInScan == 1 && deterministicPostScan == 1,
          s"a deterministic bloom must reach the scan; got scan=$deterministicInScan " +
            s"postScan=$deterministicPostScan")

        // `Rand` contributes no reference, so `k` is still the only key column. The planner's
        // `deterministic` test on the conjunct is the gate that rejects this one, and the only one
        // that does, since the format's hook answers about shapes and types alone.
        val nonDeterministic = new XxHash64(Seq(k, Rand(Literal(1L))))
        assert(!nonDeterministic.deterministic, "the value expression must be non-deterministic")
        val (inScan, postScan) = extractedBlooms(nonDeterministic)
        assert(inScan == 0, s"a non-deterministic bloom must not be extracted; got $inScan")
        assert(postScan == 1, s"it must stay in the post-scan Filter; got $postScan")
      }
    }
  }

  test("FileSourceStrategy offers conjuncts under the relation's own column names") {
    // The analyzer spells a reference the way the query did, so under case-insensitive resolution
    // a bloom can name `_TMP_METADATA_ROW_INDEX` while the relation, and the schema the reader
    // requests, call the column `_tmp_metadata_row_index`. The planner normalizes what it offers to
    // the relation's names, so the format's gate sees the name the reader will see and declines.
    // Without that the gate passed it, `ParquetStorageFilter.create` rejected it on the driver, and
    // a query that runs with the conf off failed with it on.
    withTempDir { dir =>
      val rowIndexName = ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
      val path = writeSingleParquetFile(dir,
        spark.range(0, 50).selectExpr("id AS k", s"id AS $rowIndexName", "CAST(id AS STRING) AS v"),
        rowGroupSize = 64 * 1024L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val bloomLit = bloomLiteralOf(42L)
        val relation = spark.read.parquet(path).select("k", rowIndexName, "v")
          .queryExecution.optimizedPlan
        def column(name: String): Attribute =
          relation.output.find(_.name == name).getOrElse(fail(s"no $name in the relation output"))

        def offered(key: Attribute): Seq[Expression] = {
          val logical = LogicalFilter(
            BloomFilterMightContain(bloomLit, new XxHash64(Seq(key))), relation)
          val physical = FileSourceStrategy(logical).headOption
            .getOrElse(fail(s"FileSourceStrategy did not plan $logical"))
          scanOf(physical).storageFilters
        }

        // As a control, an ordinary key spelled in another case is offered, under its own name.
        val upperK = offered(column("k").withName("K"))
        assert(upperK.size == 1, s"a bloom over `K` must reach the scan; got $upperK")
        assert(upperK.head.references.map(_.name).toSet == Set("k"),
          s"and it must carry the relation's name for the column; got ${upperK.head}")

        val upperRowIndex =
          offered(column(rowIndexName).withName(rowIndexName.toUpperCase(Locale.ROOT)))
        assert(upperRowIndex.isEmpty,
          s"a bloom over the row-index column must not be offered in any case; got $upperRowIndex")
      }
    }
  }

  // ----- Generic reader plumbing for the coverage tests below -----

  // Writes a single-column (`k`) parquet file from a SQL expression over `id`, avoiding the need
  // for an Encoder per key type. `keyExpr` is evaluated over `spark.range(1, n + 1)`.
  private def writeKeyParquetFileFromSql(
      dir: File,
      keyExpr: String,
      n: Long = 100L,
      rowGroupSize: Long = 256L,
      dictionary: Boolean = false,
      valueCopies: Int = 1): String = {
    // Each of the `n` ids is written `valueCopies` times in a row, which is what makes parquet
    // actually pick dictionary encoding when it is asked for. A dictionary writer falls back to
    // PLAIN as soon as the dictionary plus the encoded ids is no smaller than the raw values, and
    // all-distinct keys guarantee exactly that. The copies have to be consecutive, since the
    // decision is per column chunk, and a row group holding one copy of each value is no better
    // off than a row group of distinct ones.
    // `div`, not `/`, since `/` is floating-point division in Spark SQL, which would hand every
    // key expression a double and quietly change the written type.
    val ids = spark.range(0, n * valueCopies).selectExpr(s"(id div $valueCopies) + 1 AS id")
    writeSingleParquetFile(dir, ids.selectExpr(s"$keyExpr AS k", "CAST(id AS STRING) AS v"),
      rowGroupSize, dictionary = dictionary)
  }

  // Every column chunk of the file, from its footer. The facts the tests below assert about a
  // fixture are read off these rather than assumed from the write options.
  private def footerChunks(filePath: String): Seq[ColumnChunkMetaData] =
    footerOf(filePath).getBlocks.asScala.flatMap(_.getColumns.asScala).toSeq

  private def footerOf(filePath: String): ParquetMetadata = {
    val conf = spark.sessionState.newHadoopConf()
    ParquetFooterReader.readFooter(
      HadoopInputFile.fromPath(new Path(filePath), conf), ParquetMetadataConverter.NO_FILTER)
  }

  // Asserted rather than assumed, because asking for dictionary encoding does not mean getting it.
  private def encodingsOf(filePath: String, column: String = "k"): Set[Encoding] =
    footerChunks(filePath).filter(_.getPath.toDotString == column)
      .flatMap(_.getEncodings.asScala).toSet

  // Reads every batch, projecting each row through `extract`. `storageFilter` may be null, which
  // builds the plain vectorized reader rather than the late-materialization one. Every read helper
  // in this suite goes through here.
  //
  // `tryInitializeResource` closes the reader if anything inside throws and leaves it open
  // otherwise, which is the contract these helpers need. The caller closes it once its assertions
  // pass. Without it a failure in the read loop, which is what these tests are looking for, leaks
  // the reader, its input stream and its off-heap vectors for the rest of the JVM, and can cascade
  // into unrelated failures in the same suite. `initialize` throws too, so the wrap starts at
  // construction.
  private def readAllWith[T](
      filePath: String,
      columns: Seq[String],
      storageFilter: ParquetStorageFilter,
      extract: (ColumnarBatch, Int) => T,
      capacity: Int = 4096,
      useOffHeap: Boolean = false,
      partitionColumns: StructType = new StructType(),
      partitionValues: InternalRow = null): (Seq[T], VectorizedParquetRecordReader) = {
    Utils.tryInitializeResource {
      if (storageFilter == null) {
        new VectorizedParquetRecordReader(useOffHeap, capacity)
      } else {
        new LateMaterializationParquetRecordReader(useOffHeap, capacity, storageFilter)
      }
    } { reader =>
      reader.initialize(filePath, columns.asJava)
      reader.initBatch(partitionColumns, partitionValues)
      val collected = mutable.ArrayBuffer[T]()
      while (reader.nextBatch()) {
        val batch = reader.resultBatch()
        var i = 0
        val n = batch.numRows()
        while (i < n) {
          collected += extract(batch, i)
          i += 1
        }
      }
      (collected.toSeq, reader)
    }
  }

  // A key value as a string in its *internal* representation, so the splicing path and the plain
  // path can be compared without knowing the type and without external type conversion.
  // `InternalRow.getAccessor` does the per-type read, which is why this takes the batch's row
  // rather than the column vector.
  private def renderValue(row: InternalRow, ordinal: Int, dt: DataType): String =
    InternalRow.getAccessor(dt)(row, ordinal) match {
      case null => "null"
      // Two equal arrays are two objects, so the default toString would compare identities.
      case bytes: Array[Byte] => bytes.mkString(",")
      case value => value.toString
    }

  // Reads a key-only file through the plain (no storage filter) path and keeps the rows the given
  // bound predicate accepts. This is the oracle for the splicing path. Whatever the plain reader
  // returns, filtered in Scala, is exactly what splicing must produce.
  private def survivorsViaPlainPath(
      filePath: String,
      dt: DataType,
      boundPredicate: org.apache.spark.sql.catalyst.expressions.Expression): Seq[String] = {
    val predicate = Predicate.create(boundPredicate)
    val (rows, reader) = readAllWith(
      filePath, Seq("k"), null,
      (b, i) => (renderValue(b.getRow(i), 0, dt), b.getRow(i).copy()))
    try {
      rows.collect { case (rendered, row) if predicate.eval(row) => rendered }
    } finally {
      reader.close()
    }
  }

  // ----- Key-type coverage, one case per ValueCopier branch -----

  // (case name, SQL expression producing `k`, Spark type, threshold as an external value)
  private val keyTypeCases: Seq[(String, String, DataType, Any)] = Seq(
    ("boolean", "id % 2 = 0", BooleanType, true),
    ("byte", "CAST(id AS BYTE)", ByteType, 90.toByte),
    ("short", "CAST(id AS SHORT)", ShortType, 90.toShort),
    ("int", "CAST(id AS INT)", IntegerType, 90),
    ("long", "id", LongType, 90L),
    ("float", "CAST(id AS FLOAT)", FloatType, 90.0f),
    ("double", "CAST(id AS DOUBLE)", DoubleType, 90.0d),
    ("string", "LPAD(CAST(id AS STRING), 5, '0')", StringType, "00090"),
    ("date", "DATE '2020-01-01' + CAST(id AS INT)", DateType, java.time.LocalDate.of(2020, 4, 1)),
    ("timestamp",
      "TIMESTAMPADD(SECOND, id, TIMESTAMP '2020-01-01 00:00:00')",
      TimestampType,
      java.time.LocalDateTime.of(2020, 1, 1, 0, 1, 30)
        .atZone(java.time.ZoneId.systemDefault()).toInstant),
    ("timestamp_ntz",
      "TIMESTAMPADD(SECOND, id, TIMESTAMP_NTZ '2020-01-01 00:00:00')",
      TimestampNTZType,
      java.time.LocalDateTime.of(2020, 1, 1, 0, 1, 30)),
    ("year_month_interval", "MAKE_YM_INTERVAL(0, CAST(id AS INT))",
      YearMonthIntervalType(), java.time.Period.ofMonths(90)),
    ("day_time_interval", "MAKE_DT_INTERVAL(0, 0, 0, CAST(id AS DOUBLE))",
      DayTimeIntervalType(), java.time.Duration.ofSeconds(90)),
    ("decimal_int", "CAST(id AS DECIMAL(9,2))", DecimalType(9, 2), BigDecimal("90.00")),
    ("decimal_long", "CAST(id AS DECIMAL(18,2))", DecimalType(18, 2), BigDecimal("90.00")),
    ("decimal_binary", "CAST(id AS DECIMAL(30,2))", DecimalType(30, 2), BigDecimal("90.00")),
    ("binary", "CAST(LPAD(CAST(id AS STRING), 5, '0') AS BINARY)", BinaryType,
      "00090".getBytes("UTF-8")))

  // Run every key type both without and WITH dictionary encoding. Dictionary encoding is
  // parquet's production default, and it is the case where the phase 1 scratch vectors carry a
  // Dictionary plus dictionaryIds, so each ValueCopier reads through WritableColumnVector's
  // decode branch rather than straight out of the value array.
  for {
    (name, keyExpr, dt, threshold) <- keyTypeCases
    // Two types have no dictionary arm to take, and parquet's own writer factory says why. There is
    // "no dictionary encoding for boolean", and for FIXED_LEN_BYTE_ARRAY, which is what a
    // byte-array DECIMAL maps to, "dictionary encoding was not enabled in PARQUET 1.0", which is
    // the writer version Spark writes by default. Asking for it yields PLAIN, so that arm would
    // be the plain one over again.
    dictionary <-
      if (dt == BooleanType || DecimalType.isByteArrayDecimalType(dt)) Seq(false)
      else Seq(false, true)
  } {
    val encoding = if (dictionary) "dictionary-encoded" else "plain-encoded"
    test(s"key type $name ($encoding): survivors round-trip through the phase 1 accumulators") {
      // TIMESTAMP_MICROS rather than Spark's default INT96, which the reader only accepts with
      // int96AsTimestamp and which is not the INT64 copier branch we want to cover here.
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
        withTempDir { dir =>
          // Three copies of every value on the dictionary arm, so the writer keeps the dictionary
          // instead of falling back to PLAIN.
          val path = writeKeyParquetFileFromSql(
            dir, keyExpr, dictionary = dictionary, valueCopies = if (dictionary) 3 else 1)
          assert(encodingsOf(path).exists(_.usesDictionary) == dictionary,
            s"$name should be $encoding but parquet used ${encodingsOf(path).mkString(", ")}")
          val bound = GreaterThanOrEqual(
            BoundReference(0, dt, nullable = true), Literal.create(threshold, dt))
          val expected = survivorsViaPlainPath(path, dt, bound)
          assert(expected.nonEmpty && expected.size < 100 * (if (dictionary) 3 else 1),
            s"the $name case should keep some but not all rows; kept ${expected.size}")

          val requested = kvSchema(dt)
          val filter = createFilter(Seq(bound), requested)
          val (result, reader) = readAllWith(
            path, Seq("k", "v"), filter, (b, i) => renderValue(b.getRow(i), 0, dt))
          try {
            assert(result == expected,
              s"splicing path disagrees with the plain path for $name ($encoding):\n" +
                s"  splicing: $result\n  plain:    $expected")
          } finally {
            reader.close()
          }
        }
      }
    }
  }

  test("TIME key column is supported: isSupportedKeyType admits it and the copier handles it") {
    // This is a regression test. TimeType is an AtomicType that passes every planning-time gate
    // (it is batch-readable and XxHash64 hashes it, so InjectRuntimeFilter will build a bloom on a
    // TIME join key), so a key-type whitelist that omitted it would fail the task at reader init.
    assert(ParquetStorageFilter.isSupportedKeyType(TimeType(6)),
      "TIME must be an eligible storage-filter key type")
    withTempDir { dir =>
      val times = (1 to 100).map(i => LocalTime.ofSecondOfDay(i.toLong))
      val path = writeKeyParquetFile(dir, times, rowGroupSize = 256L)
      val dt = TimeType(6)
      val bound = GreaterThanOrEqual(
        BoundReference(0, dt, nullable = true),
        Literal.create(LocalTime.ofSecondOfDay(90L), dt))
      val expected = survivorsViaPlainPath(path, dt, bound)
      assert(expected.size == 11, s"expected the last 11 of 100 TIME keys; got ${expected.size}")

      val requested = kvSchema(dt)
      val filter = createFilter(Seq(bound), requested)
      val (result, reader) =
        readAllWith(path, Seq("k", "v"), filter, (b, i) => renderValue(b.getRow(i), 0, dt))
      try {
        assert(result == expected, s"got $result; expected $expected")
      } finally {
        reader.close()
      }
    }
  }

  test("isSupportedKeyType covers exactly the types the reader can copy") {
    // What FileSourceStrategy offers, what ParquetStorageFilter.create admits and what the reader
    // can copy are one list, because isSupportedKeyType answers from ValueCopier. Both are asserted
    // here. A type the copier has no case for would be a task failure rather than a planning-time
    // rejection, and the delegation is what rules that out.
    val supported: Seq[DataType] = Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      DecimalType(9, 2), DecimalType(18, 2), DecimalType(30, 2), DateType, TimestampType,
      TimestampNTZType, TimeType(6), YearMonthIntervalType(), DayTimeIntervalType(),
      StringType, VarcharType(10), CharType(10), BinaryType)
    supported.foreach { dt =>
      assert(ParquetStorageFilter.isSupportedKeyType(dt), s"$dt should be a supported key type")
      assert(ValueCopier.forType(dt) != null, s"$dt should have a value copier")
    }
    // Atomic but with no primitive Parquet leaf to accumulate into, plus the non-atomic types.
    val unsupported: Seq[DataType] = Seq(
      VariantType, NullType, ArrayType(IntegerType), MapType(IntegerType, IntegerType),
      new StructType().add("a", IntegerType))
    unsupported.foreach { dt =>
      assert(!ParquetStorageFilter.isSupportedKeyType(dt),
        s"$dt should NOT be a supported key type")
      intercept[IllegalStateException](ValueCopier.forType(dt))
    }
  }

  // ----- Null keys, multiple keys, partition columns, off-heap, row-at-a-time -----

  test("nullable key column: surviving null keys are copied through as nulls") {
    // The predicate deliberately accepts nulls, so appendSurvivorRowToAccumulators must take its
    // dst.putNull branch. Every other test uses a non-nullable key, leaving that branch dead.
    withTempDir { dir =>
      val path = writeKeyParquetFileFromSql(
        dir, "CASE WHEN id % 10 = 0 THEN NULL ELSE id END")
      val ref = BoundReference(0, LongType, nullable = true)
      val bound = Or(IsNull(ref), GreaterThanOrEqual(ref, Literal(90L)))
      val expected = survivorsViaPlainPath(path, LongType, bound)
      assert(expected.count(_ == "null") == 10, s"expected 10 null keys; got $expected")

      val requested = kvSchema()
      val filter = createFilter(Seq(bound), requested)
      val (result, reader) =
        readAllWith(path, Seq("k", "v"), filter, (b, i) => renderValue(b.getRow(i), 0, LongType))
      try {
        assert(result == expected, s"got $result; expected $expected")
      } finally {
        reader.close()
      }
    }
  }

  test("key ordinals collected out of order still pair with the right batch slots") {
    // `ParquetStorageFilter.create` sorts the ordinals it collects, which fixes the key row's
    // layout. The reader pairs each key column's batch slot with its key-row position, so nothing
    // depends on that order. Every other fixture mentions its keys in column order, and production
    // does not promise that, since the conjuncts arrive in `afterScanFilters` order, which follows
    // the join. So this has two key columns mentioned the other way round, and asserts both the
    // sorted layout and that every key still comes back with its own row.
    withTempDir { dir =>
      val path = writeSingleParquetFile(dir,
        spark.range(1, 201).selectExpr("id AS a", "id * 2 AS b", "CONCAT('v_', id) AS c"), 256L)

      // a >= 100 AND b <= 300, written so that `b` (ordinal 1) is collected first.
      val bound = And(
        LessThanOrEqual(BoundReference(1, LongType, nullable = true), Literal(300L)),
        GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(100L)))
      val requested = StructType(Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true),
        StructField("c", StringType, nullable = true)))
      val filter = createFilter(Seq(bound), requested)
      assert(filter.keyColumnIndices.toSeq == Seq(0, 1),
        s"key ordinals must come out ascending; got ${filter.keyColumnIndices.toSeq}")

      val (result, reader) = readAllWith(path, Seq("a", "b", "c"), filter,
        (b, i) => (b.column(0).getLong(i), b.column(1).getLong(i),
          b.column(2).getUTF8String(i).toString))
      try {
        val expected = (100L to 150L).map(i => (i, i * 2, s"v_$i"))
        assert(result == expected,
          s"expected a in [100,150] with b and c aligned; got ${result.take(5)} (${result.size})")
      } finally {
        reader.close()
      }
    }
  }

  test("partition columns are preserved alongside spliced key columns") {
    // The emit path rewrites the key slots alone, and a partition slot is not one of them. It sits
    // past the data columns and must keep the constant vector `initBatch` populated for it.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      val filter = keyAtLeastFilter(195L)
      val partitionColumns = new StructType().add("p", IntegerType)
      val (result, reader) = readAllWith(
        path, Seq("k", "v"), filter,
        (b, i) => (b.column(0).getLong(i), b.column(2).getInt(i)),
        partitionColumns = partitionColumns,
        partitionValues = InternalRow(7))
      try {
        assert(result.map(_._1) == (195L to 200L),
          s"expected keys 195..200; got ${result.map(_._1)}")
        assert(result.forall(_._2 == 7),
          s"every row should carry partition value 7; got ${result.map(_._2).distinct}")
      } finally {
        reader.close()
      }
    }
  }

  Seq(false, true).foreach { useOffHeap =>
    val mode = if (useOffHeap) "off-heap" else "on-heap"
    test(s"$mode vectors: multi-batch emit closes and reallocates survivor vectors correctly") {
      // Off-heap is where the close/free hazards actually bite. OffHeapColumnVector.close() frees
      // the native buffer, so a read after close is a crash rather than stale data. A second close
      // is harmless, since close() zeroes the addresses it freed. capacity = 16 over 71 survivors
      // forces 5 emits, each closing the previous emit's dequeued key vectors. The other 29 rows
      // are what makes a declining reader fail this.
      withTempDir { dir =>
        val path = writeKeyParquetFileFromSql(dir, "id", n = 100L, rowGroupSize = 64 * 1024L)
        val bound = GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(30L))
        val requested = kvSchema()
        val filter = createFilter(Seq(bound), requested)
        val (result, reader) = readAllWith(
          path, Seq("k", "v"), filter, (b, i) => b.column(0).getLong(i),
          capacity = 16, useOffHeap = useOffHeap)
        try {
          assert(result == (30L to 100L),
            s"expected the 71 surviving keys in order; got ${result.size} rows")
        } finally {
          reader.close()
        }
      }
    }
  }

  test("row-at-a-time path: nextKeyValue re-fetches the spliced batch per row") {
    // One ColumnarBatch is handed out for the whole read and its key slots are rewritten per
    // batch, so a consumer holding on to an earlier getCurrentValue() would read the wrong
    // vectors. Drives the non-columnar contract with
    // a capacity small enough to span several batches.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      val filter = keyAtLeastFilter(100L)
      val reader = new LateMaterializationParquetRecordReader(false, 8, filter)
      try {
        reader.initialize(path, Seq("k", "v").asJava)
        reader.initBatch(new StructType(), null)
        val collected = mutable.ArrayBuffer[(Long, String)]()
        while (reader.nextKeyValue()) {
          val row = reader.getCurrentValue().asInstanceOf[InternalRow]
          collected += ((row.getLong(0), row.getString(1)))
        }
        assert(collected.toSeq == rows.filter(_._1 >= 100L),
          s"expected keys 100..200 row by row; got ${collected.size} rows")
      } finally {
        reader.close()
      }
    }
  }

  // ----- Schema evolution, a column missing from the physical file -----

  test("non-key column missing from a file: the evolved table reads and credits avoided bytes") {
    // Schema evolution end to end, with the non-key column rather than the key missing from the
    // older file. The clipped requested schema keeps the missing column, which the byte walks skip
    // by themselves, since they go over the block's chunks and the older file has no chunk for it.
    // What this pins is that such a read returns the right rows and still credits a saving.
    withTempDir { dir =>
      val base = new File(dir, "merged").getAbsolutePath
      // The older file has (k, v), the newer one (k, v, w).
      (1L to 200L).map(i => (i, s"v_$i")).toDF("k", "v")
        .repartition(1)
        .write
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .mode("append")
        .parquet(base)
      (201L to 400L).map(i => (i, s"v_$i", i * 2)).toDF("k", "v", "w")
        .repartition(1)
        .write
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .mode("append")
        .parquet(base)

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.option("mergeSchema", "true").parquet(base).select("k", "v", "w")
        // Keeps rows from BOTH files, so the older one (where `w` is missing) is really read, and
        // drops the leading row groups of the older file so the skip path is exercised there too.
        val withSF = scanWithStorageFilter(df, threshold = 150L)

        val collected = collectRows(withSF)
          .map(r => (r.getLong(0), r.getString(1), if (r.isNullAt(2)) None else Some(r.getLong(2))))
          .toSeq.sorted
        val expected = (150L to 200L).map(i => (i, s"v_$i", None)) ++
          (201L to 400L).map(i => (i, s"v_$i", Some(i * 2)))
        assert(collected == expected,
          s"expected ${expected.size} rows across both schemas; got ${collected.size}")

        // A positive total says the byte walks ran over the older file's row groups as well.
        // Asserting non-negativity would prove nothing. `SQLMetric.add` drops a negative and the
        // value cannot go below zero.
        val bytesRg = withSF.metrics(StorageFilterMetrics.BYTES_AVOIDED_BY_ROW_GROUP)
        val bytesPf =
          withSF.metrics(StorageFilterMetrics.BYTES_AVOIDED_BY_PAGE_FILTERING)
        assert(bytesRg.value + bytesPf.value > 0,
          s"the walk must credit some avoided bytes; got rg=${bytesRg.value} pf=${bytesPf.value}")
      }
    }
  }

  // ----- Explain output -----

  test("StorageFilters shows up in the scan description only when the scan has storage filters") {
    // `simpleString` renders every metadata entry verbatim, so an unconditional entry would append
    // `StorageFilters: []` to every file-scan explain line and churn the explain golden files.
    withTempDir { dir =>
      val rows = (1L to 20L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      val scan = scanOf(spark.read.parquet(path).select("k", "v").queryExecution.executedPlan)
      assert(!scan.simpleString(100).contains("StorageFilters"),
        s"a scan with no storage filters must not mention them: ${scan.simpleString(100)}")

      val keyAttr = scan.output.find(_.name == "k").get
      val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(5L))))
      assert(withSF.simpleString(100).contains("StorageFilters"),
        s"a scan with storage filters must mention them: ${withSF.simpleString(100)}")
    }
  }

  // ----- Missing KEY column end to end (schema evolution) -----

  // Builds a parquet table whose older file predates a later ADD COLUMN, so that column is missing
  // from that file. `addColumnClause` is spliced into the ALTER, e.g. "k BIGINT DEFAULT 7".
  private def withEvolvedKeyTable(addColumnClause: String)(body: String => Unit): Unit = {
    withTable("evolved") {
      spark.sql("CREATE TABLE evolved (id BIGINT) USING parquet")
      spark.sql("INSERT INTO evolved VALUES (1), (2), (3)")
      spark.sql(s"ALTER TABLE evolved ADD COLUMN $addColumnClause")
      spark.sql("INSERT INTO evolved VALUES (4, 40), (5, 50)")
      body("evolved")
    }
  }

  // Attaches `storageFilters` to the scan of `SELECT id, k FROM <table>` and collects the result.
  private def collectWithStorageFilterOnKey(
      table: String,
      buildFilter: Attribute => Expression): Seq[(Long, Option[Long])] =
    scanWithStorageFilterOnKey(table, buildFilter)._2

  // As above, and also returns the scan, whose metrics the caller can then read. The rows come
  // back sorted, so a row returned twice shows.
  private def scanWithStorageFilterOnKey(
      table: String,
      buildFilter: Attribute => Expression): (FileSourceScanExec, Seq[(Long, Option[Long])]) = {
    val withSF = withStorageFilters(spark.sql(s"SELECT id, k FROM $table")) { attr =>
      Seq(buildFilter(attr("k")))
    }
    // Executing the scan directly bypasses the Project that would reorder to the SELECT order, so
    // rows arrive in the scan's own order, the relation's dataSchema order and not the SELECT's.
    // Resolve positions by name rather than assuming they line up.
    val idPos = withSF.output.indexWhere(_.name == "id")
    val kPos = withSF.output.indexWhere(_.name == "k")
    val collected = collectRows(withSF)
      .map(r => (r.getLong(idPos), if (r.isNullAt(kPos)) None else Some(r.getLong(kPos))))
      .toSeq.sorted
    (withSF, collected)
  }

  test("all keys missing: an error on the constant reads the file rather than skipping it") {
    // A file with none of the filter's key columns makes the predicate constant over it, so the
    // reader decides once, from the values `initBatch` materialized for those columns. That
    // evaluation can throw exactly as a row's value can. Here the older file's `k` reads as its
    // DEFAULT, which is not a number, and the cast fails under ANSI. Failing open reads the file
    // the way a plain scan would, so its rows survive. Skipping it would lose them, and rethrowing
    // would fail a query a plain read answers.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("evolved") {
        spark.sql("CREATE TABLE evolved (id BIGINT) USING parquet")
        spark.sql("INSERT INTO evolved VALUES (1), (2), (3)")
        spark.sql("ALTER TABLE evolved ADD COLUMN k STRING DEFAULT 'not-a-number'")
        spark.sql("INSERT INTO evolved VALUES (4, '40'), (5, '50')")
        val withSF = withStorageFilters(spark.sql("SELECT id, k FROM evolved")) { attr =>
          Seq(GreaterThanOrEqual(Cast(attr("k"), LongType), Literal(1L)))
        }
        val idPos = withSF.output.indexWhere(_.name == "id")
        val ids = collectRows(withSF).map(_.getLong(idPos)).toSeq.sorted
        assert(ids == Seq(1L, 2L, 3L, 4L, 5L),
          s"the older file must fail open and the newer one pass the cast; got $ids")
      }
    }
  }

  test("all keys missing: a checked exception on the constant reads the file") {
    // The constant evaluation fails open under the same rule as a row's, a checked exception
    // included. The older file's `k` reads as its DEFAULT 7, which the key expression throws on,
    // so that file must be read the way a plain scan would. The newer file's keys evaluate
    // normally and are rejected, since no Filter runs above this scan.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withEvolvedKeyTable("k BIGINT DEFAULT 7") { table =>
        val collected = collectWithStorageFilterOnKey(
          table, k => GreaterThanOrEqual(ThrowsCheckedOn(k, bad = 7L), Literal(100L)))
        assert(collected == Seq((1L, Some(7L)), (2L, Some(7L)), (3L, Some(7L))),
          s"the older file must fail open and the newer one be filtered; got $collected")
      }
    }
  }

  test("a file with none of the projected non-key columns is read plainly") {
    // The clipped requested schema keeps a column the file does not have, so a projected non-key
    // column added after the older file was written still looks like something for phase 2 to
    // read. Phase 2 reads nothing for it, so for that file the filter could only add cost, and the
    // reader declines it there. With no Filter above this scan, the older file's rows come back
    // unfiltered while the newer file's are still filtered.
    withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTable("evolved_value") {
        spark.sql("CREATE TABLE evolved_value (k BIGINT, v STRING) USING parquet")
        spark.sql("INSERT INTO evolved_value VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        spark.sql("ALTER TABLE evolved_value ADD COLUMN w STRING")
        spark.sql("INSERT INTO evolved_value VALUES (4, 'd', 'x'), (5, 'e', 'y')")
        val withSF =
          scanWithStorageFilter(spark.sql("SELECT k, w FROM evolved_value"), threshold = 5L)
        val kPos = withSF.output.indexWhere(_.name == "k")
        val keys = collectRows(withSF).map(_.getLong(kPos)).toSeq.sorted
        assert(keys == Seq(1L, 2L, 3L, 5L),
          s"the older file must be read unfiltered and the newer one filtered; got $keys")
      }
    }
  }

  test("a file with none of a projected struct's requested fields is read plainly") {
    // Under the legacy returnNullStructIfAllFieldsMissing, the clipped schema keeps a struct none
    // of whose requested fields the file has, and adds no field it has, so phase 2 would read
    // nothing for it. Only the struct's leaves are missing columns, not the struct, so the reader
    // asks the file schema about every non-key leaf. The filter rejects every row, so a declined
    // file comes back whole and one that is not comes back empty.
    withTempDir { dir =>
      val path = writeSingleParquetFile(dir,
        spark.range(0, 20).selectExpr("id AS k", "named_struct('a', CAST(id AS INT)) AS s"),
        rowGroupSize = 64 * 1024L)
      val schema =
        new StructType().add("k", LongType).add("s", new StructType().add("b", IntegerType))
      val storageFilters =
        Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(1000L)))
      withSQLConf(SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key -> "true") {
        val keys = readLongs(formatReader(schema, Nil, storageFilters), path, ordinal = 0)
        assert(keys == (0L until 20L), s"the file must be read plainly; got ${keys.size} rows")
      }
    }
  }

  test("missing key column with an existence DEFAULT is filtered on the default, not on null") {
    // The older file has no `k`, so the reader materializes k = 7 for its rows. The predicate must
    // be evaluated against 7, which keeps the file. Evaluating it against null yields null, which
    // is not true, so the whole older file would be skipped and its rows lost.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withEvolvedKeyTable("k BIGINT DEFAULT 7") { table =>
        val collected = collectWithStorageFilterOnKey(
          table, k => GreaterThanOrEqual(k, Literal(5L)))
        // k reads as 7 for the old rows (7 >= 5, kept) and as 40/50 for the new ones.
        val expected: Seq[(Long, Option[Long])] =
          Seq((1L, Some(7L)), (2L, Some(7L)), (3L, Some(7L)), (4L, Some(40L)), (5L, Some(50L)))
        assert(collected == expected, s"got $collected; expected $expected")
      }
    }
  }

  test("missing key column with an existence DEFAULT that fails the filter skips the older file") {
    // Mirror image of the previous test. The default does NOT satisfy the predicate, so the older
    // file must be skipped in full while the newer file is still filtered normally.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withEvolvedKeyTable("k BIGINT DEFAULT 7") { table =>
        val (scan, collected) = scanWithStorageFilterOnKey(
          table, k => GreaterThanOrEqual(k, Literal(30L)))
        val expected: Seq[(Long, Option[Long])] = Seq((4L, Some(40L)), (5L, Some(50L)))
        assert(collected == expected, s"got $collected; expected $expected")
        // Rejecting a file whole is its own metric path, which walks every one of its row groups
        // rather than going through the per-row-group loop. Nothing else asserts that walk, so it
        // could report zero and only the rows above would notice.
        def metric(name: String): Long = scan.metrics(name).value
        assert(metric(StorageFilterMetrics.ROW_GROUPS_SKIPPED) >= 1,
          "the older file's row groups count as skipped")
        assert(metric(StorageFilterMetrics.ROWS_EXCLUDED_BY_ROW_GROUP) == 3,
          "and all three of its rows as excluded by a row group, got " +
            metric(StorageFilterMetrics.ROWS_EXCLUDED_BY_ROW_GROUP))
        assert(metric(StorageFilterMetrics.BYTES_AVOIDED_BY_ROW_GROUP) > 0,
          "and its projected bytes as avoided")
      }
    }
  }

  test("missing key column with no DEFAULT reads as null and the predicate decides on null") {
    // Without a DEFAULT the column really does read as null, so a null-rejecting predicate skips
    // the older file and a null-accepting one keeps it. Both directions are checked so the test
    // pins the semantics rather than just one outcome.
    withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withEvolvedKeyTable("k BIGINT") { table =>
        val rejectsNull = collectWithStorageFilterOnKey(
          table, k => GreaterThanOrEqual(k, Literal(5L)))
        assert(rejectsNull == Seq((4L, Some(40L)), (5L, Some(50L))),
          s"a null-rejecting predicate should drop the older file; got $rejectsNull")

        val acceptsNull = collectWithStorageFilterOnKey(
          table, k => Or(IsNull(k), GreaterThanOrEqual(k, Literal(45L))))
        val expected: Seq[(Long, Option[Long])] =
          Seq((1L, None), (2L, None), (3L, None), (5L, Some(50L)))
        assert(acceptsNull == expected,
          s"a null-accepting predicate should keep the older file; got $acceptsNull")
      }
    }
  }

  // ----- Metadata columns and complex non-key columns -----

  test("_metadata.row_index is correct alongside a spliced key column") {
    // The row-index slot is a synthetic non-key slot fed by ParquetRowIndexUtil from the phase-2
    // PageReadStore. It must report absolute row indexes within the file, not positions within the
    // filtered batch.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).select(
          col("k"), col("v"), col("_metadata.row_index").as("ri"))
        val withSF = scanWithStorageFilter(df, threshold = 150L)
        val collected = collectRows(withSF).map(r => (r.getLong(0), r.getLong(2))).toSeq.sorted
        // Rows were written in ascending k order in a single file, so row_index == k - 1.
        val expected = (150L to 200L).map(k => (k, k - 1))
        assert(collected == expected,
          s"row_index must be the absolute index in the file; got ${collected.take(5)}")
      }
    }
  }

  test("complex non-key column is assembled correctly under splicing") {
    // Phase 2 reads non-key columns through initColumnReader's recursion and cv.assemble(); a
    // struct column exercises both, which a flat projection never does.
    withTempDir { dir =>
      val outDir = new File(dir, "structs").getAbsolutePath
      spark.range(1, 201)
        .selectExpr("id AS k", "named_struct('a', CAST(id AS INT), 'b', CONCAT('s_', id)) AS s")
        .repartition(1)
        .write
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .parquet(outDir)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val withSF =
          scanWithStorageFilter(spark.read.parquet(outDir).select("k", "s"), threshold = 195L)
        val collected = collectRows(withSF)
          .map { r =>
            val s = r.getStruct(1, 2)
            (r.getLong(0), s.getInt(0), s.getString(1))
          }.toSeq.sorted
        val expected = (195L to 200L).map(k => (k, k.toInt, s"s_$k"))
        assert(collected == expected, s"got $collected; expected $expected")
      }
    }
  }

  test("a storage filter binds to the right column with partition and metadata columns in output") {
    // `preparedStorageFilters` binds the filter's references against `output.take(requiredSchema
    // .length)`, and `output` is the data columns, then the generated metadata columns, then the
    // partition columns, then the constant metadata ones. A projection carrying all four is where a
    // wrong prefix would bind the predicate to another column and drop rows the filter keeps, which
    // the post-scan Filter cannot put back. The file's first column `a` is not projected, so the
    // key sits at a different position in the relation's data schema than in the scan's.
    withTempDir { dir =>
      val outDir = new File(dir, "parted").getAbsolutePath
      spark.range(1, 201)
        .selectExpr("id * 7 AS a", "id AS k", "CONCAT('v_', id) AS v", "CAST(id % 3 AS INT) AS p")
        .repartition(1)
        .write
        .partitionBy("p")
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .parquet(outDir)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.parquet(outDir).select(
          col("k"), col("v"), col("p"),
          col("_metadata.row_index").as("ri"), col("_metadata.file_size").as("fs"))
        val withSF = scanWithStorageFilter(df, threshold = 150L)
        // The assumption the binding rests on, stated where a change to `output` would break it.
        assert(withSF.output.take(withSF.requiredSchema.length).map(_.name) ==
          withSF.requiredSchema.fieldNames.toSeq,
          s"the scan's leading attributes must be its data columns; got " +
            s"${withSF.output.map(_.name)}")
        val kPos = withSF.output.indexWhere(_.name == "k")
        val pPos = withSF.output.indexWhere(_.name == "p")
        def collectAll(plan: SparkPlan): Seq[Seq[Any]] =
          collectRows(plan).map(_.toSeq(plan.output.map(_.dataType))).toSeq
            .sortBy(_(kPos).asInstanceOf[Long])
        val filtered = collectAll(withSF)
        // The same scan without the filter is the oracle, so the row indexes and file sizes come
        // from the read rather than from arithmetic over how the partitions were written.
        val expected = collectAll(withSF.copy(storageFilters = Nil))
          .filter(_(kPos).asInstanceOf[Long] >= 150L)
        assert(filtered == expected,
          s"expected ${expected.size} rows with their own partition and metadata values; " +
            s"got ${filtered.size}")
        assert(filtered.map(_(pPos)).distinct.size == 3,
          s"every partition value must still be represented; got ${filtered.map(_(pPos)).distinct}")
      }
    }
  }

  // ----- Planner gates -----

  test("supportsStorageFilter is what decides, and a subclass answers false") {
    // The planner asks the format rather than testing its class, so the expression shapes and the
    // column types a reader can evaluate stay in its own package. A ParquetFileFormat subclass
    // still answers false. It may customize reading by overriding buildReaderWithPartitionValues,
    // and a scan with storage filters routes through buildReaderWithStorageFilters instead, which
    // would bypass whatever the subclass does.
    //
    // This hook answers about shapes and types alone. Whether the conjunct is deterministic and
    // whether its references are projected columns of this scan are plan-visible, so they stay with
    // the planner; `FileSourceStrategy leaves a non-deterministic bloom in the post-scan Filter`
    // covers that half.
    val format = new ParquetFileFormat()
    val subclass = new ParquetFileFormat() {}
    val bloom = BloomFilterMightContain(
      Literal.create(null, BinaryType),
      XxHash64(Seq(AttributeReference("k", LongType)()), 42L))
    val onVariant = BloomFilterMightContain(
      Literal.create(null, BinaryType),
      XxHash64(Seq(AttributeReference("v", VariantType)()), 42L))
    // A cast key is supported. In ANSI mode it can throw on a row an earlier conjunct would have
    // dropped, and the reader handles that where it arises, by giving the filter up for the row
    // group. Declining here instead would also decline every widening cast, which is what type
    // coercion inserts for a join between an int and a bigint column.
    val onCast = BloomFilterMightContain(
      Literal.create(null, BinaryType),
      XxHash64(Seq(Cast(AttributeReference("s", StringType)(), LongType)), 42L))

    // The conf is read in the format rather than in the planner, and it is the per-scan question
    // that carries it, asked once before anything per conjunct.
    assert(!format.supportsStorageFilterPushdown(spark), "the feature is off with the conf off")
    withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      assert(format.supportsStorageFilterPushdown(spark), "and on with it on")
      assert(!subclass.supportsStorageFilterPushdown(spark), "a subclass must not claim support")
      assert(format.supportsStorageFilter(bloom), "a plain bloom on a long key is supported")
      // Not a bloom at all, and a bloom on a type the value copier has no branch for.
      assert(!format.supportsStorageFilter(Literal.TrueLiteral))
      assert(!format.supportsStorageFilter(onVariant), "VariantType has no primitive Parquet leaf")
      assert(format.supportsStorageFilter(onCast),
        "a cast key is pushed, and an evaluation error gives the row group up")
      // And the default is no support at all.
      assert(!new NoStorageFilterFileFormat().supportsStorageFilter(bloom))
    }
  }


  test("bloom stays in the post-scan Filter when the vectorized reader is unavailable") {
    // The rows are right either way, since the bloom stays in the post-scan Filter. What this gate
    // saves is offering the scan a filter its reader cannot apply.
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val (plan, _) = runBloomFilterJoin()
        assert(countBloomFiltersInStorageFilters(plan) == 0,
          s"no bloom should be extracted when the vectorized reader is off.\nPlan:\n$plan")
        assert(countBloomFiltersInPostScanFilters(plan) >= 1,
          s"the bloom must remain as a post-scan FilterExec.\nPlan:\n$plan")
      }
    }
  }

  test("a scan reads plainly when the vectorized reader is disabled after planning") {
    // preparedStorageFilters deliberately does not re-check the conf. The reader then cannot honor
    // the filter, and since the post-scan Filter keeps it, not honoring it is a slower read rather
    // than a wrong one. Every row of the file comes back from the scan.
    withTempDir { dir =>
      val rows = (1L to 50L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      val withSF = withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        scanWithStorageFilter(path, threshold = 25L)
      }
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val keys = executePlanCollect(withSF).map(_._1).toSeq.sorted
        assert(keys == (1L to 50L),
          s"the filter is a hint, so an unfiltered read is expected; got ${keys.size} rows")
      }
    }
  }

  test("a file format without storage-filter support declines rather than delegating") {
    // The default `FileFormat.buildReaderWithStorageFilters` answers None, and the caller falls
    // back to the ordinary builder. That is what keeps the two builders from being able to call
    // each other, and declining is safe because the post-scan Filter still holds the conjunct.
    val storageFilters =
      Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(1L)))
    val declined = new NoStorageFilterFileFormat().buildReaderWithStorageFilters(
      spark, new StructType(), new StructType(), new StructType(), Nil, storageFilters,
      Map.empty, new Configuration(), Map.empty)
    assert(declined.isEmpty, "the default must not build a reader of its own")
    assert(!new NoStorageFilterFileFormat().supportsStorageFilterPushdown(spark),
      "and it must not claim support either")
  }

  test("a scan whose format declines falls back to the ordinary reader and returns every row") {
    // The check above is that the default answers None. This is the other half, that `inputRDD`
    // then builds the ordinary reader and the scan still produces its rows. A `ParquetFileFormat`
    // subclass is the shape that reaches it, since it inherits the override and declines by class,
    // and its ordinary builder is the one an extension would have customized.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val withSF = scanWithStorageFilter(path, threshold = 195L)
        val declining = withSF.copy(
          relation = withSF.relation.copy(fileFormat = new ParquetFileFormat() {})(spark))
        val collected = executePlanCollect(declining).toSeq.sorted
        assert(collected == rows,
          s"a declining format reads the file plainly; got ${collected.size} of ${rows.size} rows")
      }
    }
  }

  Seq(false, true).foreach { aqe =>
    test(s"FileSourceStrategy extraction preserves query results (AQE = $aqe)") {
      // AQE is on by default in production, and it is where the bloom subquery is planned by
      // PlanAdaptiveSubqueries rather than PlanSubqueries, which is the path
      // preparedStorageFilters' ScalarSubquery materialization depends on.
      withBloomFilterTables {
        val baseConf = Map(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString)
        def run(pushdown: Boolean): (Int, Seq[(Long, Long)]) = withSQLConf(
            (baseConf +
              (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> pushdown.toString)).toSeq: _*
          ) {
          val (plan, rows) = runBloomFilterJoin()
          (countBloomFiltersInStorageFilters(plan),
            rows.map(r => (r.getLong(0), r.getLong(1))).toSeq.sorted)
        }
        val (offFilters, off) = run(false)
        val (onFilters, on) = run(true)
        assert(on == off, s"results differ between conf-on and conf-off: on=$on off=$off")
        assert(on.nonEmpty, "the join should return rows, otherwise this proves nothing")
        // Equal results are guaranteed by the post-scan Filter whether or not the scan applied the
        // filter, so they alone would not notice AQE dropping it on the way to the final plan.
        assert(offFilters == 0 && onFilters == 1,
          s"the scan must carry the filter with the conf on and not with it off; " +
            s"got on=$onFilters off=$offFilters")
      }
    }
  }

  test("canonicalization keeps a storage-filter scan distinct from a plain one") {
    // storageFilters is in doCanonicalize and in the case class equality, which is what stops
    // exchange and subquery reuse from serving one scan's result to the other. Reuse compares
    // canonicalized plans, so a scan that filters must not match a plain scan of the same file, and
    // two scans carrying the same filter must still match.
    withTempDir { dir =>
      val rows = (1L to 50L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val withSF = scanWithStorageFilter(path, threshold = 25L)
        val plain = withSF.copy(storageFilters = Nil)
        assert(withSF != plain, "case class equality must take storageFilters into account")
        assert(!withSF.sameResult(plain),
          s"a filtering scan must not be reusable as a plain one:\n${withSF.canonicalized}\n" +
            s"${plain.canonicalized}")
        // A second, independently planned scan of the same file with the same filter still
        // matches, so reuse is not disabled wholesale. Its key attribute carries a different
        // exprId, which is what canonicalization normalizes away.
        val sameSF = scanWithStorageFilter(path, threshold = 25L)
        assert(withSF.sameResult(sameSF),
          s"two scans with the same storage filter must stay reusable:\n" +
            s"${withSF.canonicalized}\n${sameSF.canonicalized}")
        val otherSF = scanWithStorageFilter(path, threshold = 30L)
        assert(!withSF.sameResult(otherSF), "a different threshold is a different result")
      }
    }
  }

  test("whole-stage codegen off: the row-at-a-time path still applies the extracted bloom") {
    // The planner's extraction gate is `fileFormat.supportBatch`, which does not look at
    // whole-stage codegen, while `FileSourceScanExec.supportsColumnar` does. So with codegen off
    // the bloom is still extracted, `returningBatch` is false, and the reader serves the spliced
    // batch one row at a time. That is the only combination where the planner's gate is weaker
    // than the runtime's.
    withBloomFilterTables {
      val baseConf = Map(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
      def run(pushdown: Boolean): (Seq[(Long, Long)], Int, Boolean, Long) = withSQLConf(
          (baseConf +
            (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> pushdown.toString)).toSeq: _*
        ) {
        val (plan, result) = runBloomFilterJoin()
        val scans = plan.collect { case s: FileSourceScanExec => s }
        val excluded = scans.filter(_.storageFilters.nonEmpty).map { s =>
          s.metrics(StorageFilterMetrics.ROWS_EXCLUDED_BY_ROW_GROUP).value +
            s.metrics(StorageFilterMetrics.ROWS_EXCLUDED_WITHIN_ROW_GROUP).value
        }.sum
        (result.map(r => (r.getLong(0), r.getLong(1))).toSeq.sorted,
          countBloomFiltersInStorageFilters(plan), scans.forall(!_.supportsColumnar), excluded)
      }
      val (off, _, _, _) = run(false)
      val (on, storageBlooms, noColumnarScan, excluded) = run(true)
      assert(storageBlooms >= 1, s"the bloom must still be extracted with codegen off; " +
        s"got $storageBlooms")
      assert(noColumnarScan, "with codegen off no scan should output columnar batches")
      assert(on == off, s"results differ between conf-on and conf-off: on=$on off=$off")
      assert(on.nonEmpty, "the join should return rows, otherwise this proves nothing")
      // The assertions above hold whether or not the row path honored the filter, since the
      // post-scan Filter answers the query either way. The metrics are what say it did.
      assert(excluded > 0, s"the row path must have excluded rows; the metrics say $excluded")
    }
  }

  test("off-heap column vectors through the planner: spliced values survive the free") {
    // Off-heap is where the vector lifecycle actually bites. The previous batch's key vectors are
    // freed at the next nextBatch(), so a stale reference reads released native memory rather than
    // old bytes. This drives it through the planner, where the batch also crosses
    // ColumnarToRowExec.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, threshold = 150L)
        val collected = executePlanCollect(scan).toSeq.sorted
        val expected = rows.filter(_._1 >= 150L)
        assert(collected == expected,
          s"got ${collected.size} rows; expected ${expected.size}. first few: ${collected.take(3)}")
      }
    }
  }

  // ----- Page-level pushedFilterRanges (a strict subset of the row group) -----

  test("page-subset ranges still credit the avoided bytes") {
    // A pushed data filter that drops some of a row group's pages narrows `pushedFilterRanges`
    // below the block. That is where phase 1's alignment stops holding trivially, that the r-th row
    // readBatch delivers pairs with rowIndexIter.nextLong(), so the exact rows below check it. It
    // is also the only case where the byte walks take the offset-index branch.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
      assert(footerOf(path).getBlocks.size == 1, "the fixture must be one row group")

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        // Pages hold 100 rows, so this drops the first page and keeps the other three.
        val df = spark.read.parquet(path).select("k", "v").filter("v > 'v_0100'")
        val withSF = scanWithStorageFilter(df, threshold = 390L)
        // The fixture's whole point, asserted rather than assumed. The scan alone with no storage
        // filter returns the rows the pushed filter kept, fewer than the file only if it narrowed.
        val pushedOnly = executePlanCollect(withSF.copy(storageFilters = Nil)).length
        assert(pushedOnly < rows.size,
          s"the pushed filter must narrow the row group; it kept $pushedOnly of ${rows.size} rows")
        val collected = executePlanCollect(withSF).toSeq.sorted
        assert(collected == rows.filter(_._1 >= 390L), s"got ${collected.size} rows")

        // One row group that is kept, so every avoided byte is page filtering's. Non-negativity is
        // not assertable, since `SQLMetric.add` drops a negative, so the value cannot go below zero
        // however wrong the arithmetic is. A positive one can fail.
        val bytesPf = withSF.metrics(StorageFilterMetrics.BYTES_AVOIDED_BY_PAGE_FILTERING).value
        assert(bytesPf > 0, s"page-subset ranges must still credit avoided bytes; got $bytesPf")
      }
    }
  }

  test("column-index filtering off: the filter keeps only the row groups it empties") {
    // `parquet.filter.columnindex.enabled=false` is the escape hatch for a file whose page index is
    // wrong, and it has to cover phase 2 as well as phase 0. Phase 2 reads part of a row group
    // through the offset index, which parquet consults whatever that conf says, so a wrong index
    // there would pair a row's key with another row's values. The post-scan Filter cannot catch
    // that, since the key it sees is the right one.
    //
    // What the filter keeps in that case is the row groups it empties, which needs no index at all.
    // So the two arms differ in granularity, not in correctness. With the conf on the scan returns
    // exactly the surviving rows, with it off the row groups that hold one come back whole. This
    // test executes the scan alone, so nothing re-applies the conjunct above it, and no data filter
    // is pushed, so statistics-level row-group pruning stays out of it.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 1024L, pageSize = Some(512L))

      def run(columnIndex: Boolean): (Seq[(Long, String)], Long, Long) = withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED -> columnIndex.toString) {
        val withSF = scanWithStorageFilter(path, threshold = 350L)
        val collected = executePlanCollect(withSF).toSeq.sorted
        (collected,
          withSF.metrics(StorageFilterMetrics.ROW_GROUPS_SKIPPED).value,
          withSF.metrics(StorageFilterMetrics.ROWS_EXCLUDED_WITHIN_ROW_GROUP).value)
      }

      val expected = rows.filter(_._1 >= 350L)
      val (rowsOff, skippedOff, withinOff) = run(columnIndex = false)
      val (rowsOn, skippedOn, withinOn) = run(columnIndex = true)
      assert(rowsOn == expected,
        s"with the column index on, got ${rowsOn.size} rows; expected ${expected.size}")
      assert(withinOn > 0 && skippedOn > 0,
        s"the on arm must both skip row groups and filter inside one, or this test proves " +
          s"nothing; skipped=$skippedOn within=$withinOn")
      assert(rowsOff.distinct == rowsOff && expected.forall(rowsOff.contains) &&
          rowsOff.size > expected.size,
        s"with the column index off the surviving rows' row groups come back whole; got " +
          s"${rowsOff.size} rows for ${expected.size} survivors")
      assert(rowsOff.size < rows.size && skippedOff > 0,
        s"and the row groups the filter empties are still skipped; got ${rowsOff.size} rows of " +
          s"${rows.size}, skipped=$skippedOff")
      assert(withinOff == 0,
        s"nothing can be excluded inside a row group without the page index; got $withinOff")
    }
  }

  Seq(true, false).foreach { pushDataFilter =>
    test("the byte walks cost no IO of their own " +
      s"(pushed data filter narrowing the row group = $pushDataFilter)") {
      // The byte walks report IO that did not happen, so they must not cause any IO themselves,
      // which would make the counters change what they measure. A storage filter that keeps every
      // row reads exactly the pages a plain read of the same projection reads. Phase 1 reads the
      // key pages and phase 2 the rest. So the two arms must transfer the same bytes over a
      // filesystem that counts them, and a walk that read an offset index of its own would show up
      // as a difference.
      //
      // Both range shapes are covered, because the walk answers them from different places. With a
      // pushed data filter the column index narrows `pushedFilterRanges` to a page subset and the
      // walk reads the offset index, which is free only because column-index filtering built and
      // memoized the store first. Without one the range is the whole block and the answer comes
      // from the footer's `getTotalSize()`, which is the case where nothing else has built that
      // store.
      withTempDir { dir =>
        val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
        val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
        val schema = kvSchema()
        // Keeps every row, so the filter changes which pages are read in neither arm.
        val storageFilters =
          Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(0L)))
        val pushedFilters =
          if (pushDataFilter) Seq(sources.GreaterThan("v", "v_0100")) else Nil
        val metrics = metricMap()

        def run(withStorageFilter: Boolean): (Int, Long) = {
          val hadoopConf = countingHadoopConf()
          val readerFn = if (withStorageFilter) {
            formatReader(schema, pushedFilters, storageFilters, hadoopConf, metrics)
          } else {
            new ParquetFileFormat().buildReaderWithPartitionValues(spark, schema, new StructType(),
              schema, pushedFilters, Map(FileFormat.OPTION_RETURNING_BATCH -> "true"), hadoopConf)
          }
          readCounting(path, readerFn)
        }

        val (emittedPlain, bytesPlain) = run(withStorageFilter = false)
        val (emittedFiltered, bytesFiltered) = run(withStorageFilter = true)

        assert(emittedFiltered == emittedPlain,
          s"a filter that keeps every row must not change the rows; got " +
            s"filtered=$emittedFiltered plain=$emittedPlain")
        assert(bytesPlain > 0, "the counting filesystem must have seen the read at all")
        assert(bytesFiltered == bytesPlain,
          s"applying the filter must read the same bytes as a plain read of the same projection; " +
            s"filtered $bytesFiltered, plain $bytesPlain")

        // The walks really ran, and on the shape each arm is meant to exercise.
        val excludedWithin =
          metrics(StorageFilterMetrics.ROWS_EXCLUDED_WITHIN_ROW_GROUP).value
        assert(excludedWithin == 0,
          s"the filter keeps every row, so nothing is excluded; got $excludedWithin")
        if (pushDataFilter) {
          assert(emittedFiltered < rows.size,
            s"the pushed filter must narrow the ranges below the block, so the offset-index " +
              s"branch is the one measured; emitted $emittedFiltered of ${rows.size}")
        } else {
          assert(emittedFiltered == rows.size,
            s"with no pushed filter every row reaches phase 1, so the footer branch is the one " +
              s"measured; emitted $emittedFiltered of ${rows.size}")
        }
      }
    }
  }

  // ----- Projection order and batch boundaries -----

  test("non-key column before the key column: each key's survivors go to its batch slot") {
    // Each key column's survivors go back to that column's batch slot. Every other test puts the
    // keys in the leading slots, where an off-by-one in that pairing is invisible.
    withTempDir { dir =>
      val path = writeSingleParquetFile(
        dir,
        spark.range(1, 201).selectExpr("CONCAT('v_', id) AS v", "id AS k", "id * 10 AS w"),
        rowGroupSize = 256L)

      // Key is `k`, at slot 1 of the (v, k, w) projection.
      val bound = GreaterThanOrEqual(BoundReference(1, LongType, nullable = true), Literal(195L))
      val requested = StructType(Seq(
        StructField("v", StringType, nullable = true),
        StructField("k", LongType, nullable = true),
        StructField("w", LongType, nullable = true)))
      val filter = createFilter(Seq(bound), requested)
      assert(filter.keyColumnIndices.toSeq == Seq(1), "the key must be recognized at slot 1")

      val (result, reader) = readAllWith(path, Seq("v", "k", "w"), filter,
        (b, i) => (b.column(0).getUTF8String(i).toString, b.column(1).getLong(i),
          b.column(2).getLong(i)))
      try {
        val expected = (195L to 200L).map(k => (s"v_$k", k, k * 10))
        assert(result == expected, s"got $result; expected $expected")
      } finally {
        reader.close()
      }
    }
  }

  test("survivor count is an exact multiple of capacity: no partial trailing accumulator") {
    // Phase 1's end-of-row-group push has nothing to do only when the last accumulator came out
    // exactly full. 64 survivors at capacity 16 does that; the multi-batch tests use 71, which does
    // not. The
    // 64 are the tail of a 100-row row group, so a reader that declined the filter would return the
    // other 36 as well rather than passing.
    withTempDir { dir =>
      val path = writeKeyParquetFileFromSql(dir, "id", n = 100L, rowGroupSize = 64 * 1024L)
      val bound = GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(37L))
      val requested = kvSchema()
      val filter = createFilter(Seq(bound), requested)
      val (result, reader) = readAllWith(
        path, Seq("k", "v"), filter, (b, i) => b.column(0).getLong(i), capacity = 16)
      try {
        assert(result == (37L to 100L), s"expected the 64 surviving keys; got ${result.size}")
      } finally {
        reader.close()
      }
    }
  }

  test("early termination: the reader stops without draining the file") {
    // executeTake on a bare ColumnarToRowExec goes through ColumnarToRowEvaluatorFactory, not
    // through the generated code, so nothing closes the batch from outside here. What this covers
    // is abandoning the reader mid-file. The survivor queue still holds vectors, and
    // RecordReaderIterator closes the reader on task completion. The external close is the test
    // below.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, threshold = 50L)
        val limited = ColumnarToRowExec(scan).executeTake(5)
        assert(limited.length == 5, s"expected 5 rows from the limit; got ${limited.length}")
        assert(limited.forall(_.getLong(0) >= 50L),
          s"every row must satisfy the storage filter; got ${limited.map(_.getLong(0)).toSeq}")
      }
    }
  }

  test("a limit under whole-stage codegen closes the spliced batch from outside") {
    // `batch.close()` is emitted by ColumnarToRowExec.doProduce alone, so it only runs under
    // WholeStageCodegenExec, and only when the row loop exits with a batch still in hand. That exit
    // is the limit check, which needs a limit inside the same codegen stage, so the plan is built
    // with LocalLimitExec and handed to CollapseCodegenStages. The generated source is asserted to
    // hold that close, which says no more than that `ColumnarToRowExec` is in the stage, since it
    // emits that line unconditionally. What exercises the close is the collect below, where the
    // limit makes the row loop exit with a batch still in hand.
    //
    // What it exercises is that the spliced batch's columns are closed from outside while the
    // reader is still open, and the reader's own close() then runs over the same vectors.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, threshold = 50L)
        val planned =
          CollapseCodegenStages().apply(LocalLimitExec(5, ColumnarToRowExec(scan)))
        val stage = planned match {
          case w: WholeStageCodegenExec => w
          case other => fail(s"expected a whole-stage codegen plan, got $other")
        }
        val source = stage.doCodeGen()._2.body
        assert(source.contains(".close();"),
          s"the generated code must close the batch, which is this plan's whole point; " +
            s"source:\n$source")

        val limited = stage.executeCollect()
        assert(limited.length == 5, s"expected 5 rows from the limit; got ${limited.length}")
        assert(limited.forall(_.getLong(0) >= 50L),
          s"every row must satisfy the storage filter; got ${limited.map(_.getLong(0)).toSeq}")
      }
    }
  }

  // ----- Partially-missing key columns, end to end through the reader -----

  test("one of two key columns missing from a file: splicing runs on the values the scan returns") {
    // This is the most intricate branch of initializeLateMaterialization, where splicing engages
    // with one key column read by phase 1 and the other missing from the file. The missing key's
    // slot in the row the predicate evaluates points at the constant vector `initBatch` built for
    // it, its field lands among the non-key columns, and its output slot is filled by
    // ParquetColumnVector.
    //
    // The SELECT order (a, b, c) also differs from the table's (a, c, b), so this covers a
    // projection whose order does not match the relation's dataSchema.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withTable("partial") {
        spark.sql("CREATE TABLE partial (a BIGINT, c STRING) USING parquet")
        spark.sql("INSERT INTO partial VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        spark.sql("ALTER TABLE partial ADD COLUMN b BIGINT DEFAULT 7")
        spark.sql("INSERT INTO partial VALUES (4, 'p', 40), (5, 'q', 50)")

        val df = spark.sql("SELECT a, b, c FROM partial")
        // Two key columns. In the older file `b` is missing and reads as its default 7, so the
        // predicate must see 7 for it, and `a >= 2` still filters.
        val withSF = withStorageFilters(df) { attr =>
          Seq(GreaterThanOrEqual(attr("a"), Literal(2L)),
            GreaterThanOrEqual(attr("b"), Literal(5L)))
        }

        // The scan emits its own order (a, c, b here), not the SELECT's (a, b, c), because
        // executing it directly skips the reordering Project. Resolve positions by name.
        val aPos = withSF.output.indexWhere(_.name == "a")
        val bPos = withSF.output.indexWhere(_.name == "b")
        val cPos = withSF.output.indexWhere(_.name == "c")
        val collected = collectRows(withSF)
          .map(r => (r.getLong(aPos), r.getString(cPos), r.getLong(bPos))).toSeq.sorted
        // In the older file a in {2,3} pass a>=2, and b=7 passes b>=5. In the newer file 40 and 50
        // both pass.
        val expected = Seq((2L, "y", 7L), (3L, "z", 7L), (4L, "p", 40L), (5L, "q", 50L))
        // Compare against the plain read too, so a failure here is unambiguously the storage-filter
        // path rather than a wrong expectation. df.collect() goes through the Project, so it is in
        // the SELECT order (a, b, c).
        val baseline =
          df.collect().map(r => (r.getLong(0), r.getString(2), r.getLong(1))).toSeq.sorted
        assert(baseline == ((1L, "x", 7L) +: expected),
          s"the plain read is already wrong, so the expectation is: $baseline")
        assert(collected == expected, s"got $collected; expected $expected")
      }
    }
  }
}

/**
 * The identity on a key, except that it throws a checked [[SparkException]] on `bad`, the way a
 * Hive UDF key does through `FAILED_EXECUTE_UDF`. Scala throws it without declaring it. With
 * `fatalCause` the exception wraps an `OutOfMemoryError`, as a UDF's catch-all would.
 */
private case class ThrowsCheckedOn(
    child: Expression,
    bad: Long,
    fatalCause: Boolean = false,
    checked: Boolean = true,
    killsTask: Boolean = false)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType

  override protected def nullSafeEval(input: Any): Any = {
    if (input == bad) {
      // A kill arriving while the expression runs, whose interrupt the expression wraps.
      if (killsTask) TaskContext.get().markInterrupted("killed by the test")
      val cause = if (fatalCause) new OutOfMemoryError("wrapped") else null
      val message = s"the key expression refuses $bad"
      throw (if (checked) new SparkException(message, cause)
        else new IllegalStateException(message, cause))
    }
    input
  }

  override protected def withNewChildInternal(newChild: Expression): ThrowsCheckedOn =
    copy(child = newChild)
}

/**
 * Returns its child's value, and marks the running task killed when that value is `at`, the way a
 * kill arrives in the middle of a read.
 */
private case class MarksKilledOn(child: Expression, at: Long)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType

  override protected def nullSafeEval(input: Any): Any = {
    if (input == at) TaskContext.get().markInterrupted("killed by the test")
    input
  }

  override protected def withNewChildInternal(newChild: Expression): MarksKilledOn =
    copy(child = newChild)
}

/**
 * A [[FileFormat]] that does not override `buildReaderWithStorageFilters`, so it exercises the
 * default body, which answers `None`.
 */
private class NoStorageFilterFileFormat extends FileFormat {
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = None

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw new UnsupportedOperationException("write is not supported by this test format")
}

/**
 * A local filesystem under its own scheme that counts the bytes every read hands back, so a test
 * can compare the IO of two runs. The wrapper does not implement `ByteBufferReadable`, so reads go
 * through the byte-array path, which is fine as long as both runs use this same filesystem.
 */
class CountingLocalFileSystem extends RawLocalFileSystem {
  override def getUri: URI = URI.create(s"${CountingLocalFileSystem.scheme}:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    new FSDataInputStream(new CountingLocalFileSystem.CountingStream(super.open(f, bufferSize)))
}

object CountingLocalFileSystem {
  val scheme = "countingfile"

  private val counter = new AtomicLong(0L)

  def reset(): Unit = counter.set(0L)

  def bytesRead(): Long = counter.get()

  private class CountingStream(in: FSDataInputStream) extends FSInputStream {
    override def seek(pos: Long): Unit = in.seek(pos)

    override def getPos: Long = in.getPos

    override def seekToNewSource(targetPos: Long): Boolean = in.seekToNewSource(targetPos)

    override def read(): Int = {
      val b = in.read()
      if (b >= 0) counter.incrementAndGet()
      b
    }

    override def read(buf: Array[Byte], off: Int, len: Int): Int = {
      val n = in.read(buf, off, len)
      if (n > 0) counter.addAndGet(n)
      n
    }

    override def close(): Unit = in.close()
  }
}
