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

import java.io.{ByteArrayOutputStream, File}
import java.net.URI
import java.time.LocalTime
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, FSInputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{sources, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BloomFilterMightContain, BoundReference, Coalesce, Expression, GreaterThanOrEqual, IsNull, LessThanOrEqual, Literal, Or, Predicate, Rand, XxHash64}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter}
import org.apache.spark.sql.execution.{CollapseCodegenStages, ColumnarToRowExec, FileSourceScanExec, FileSourceScanLike, FilterExec, LocalLimitExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, FileSourceStrategy, OutputWriterFactory, PartitionedFile, UnsupportedFileReadException}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils
import org.apache.spark.util.sketch.BloomFilter

/**
 * Tests the late-materialization path of [[VectorizedParquetRecordReader]] driven by a
 * [[ParquetStorageFilter]]. Writes small multi-row-group parquet files, wires a hand-built filter
 * into the reader, and asserts correctness + the two storage-filter metrics.
 */
class ParquetStorageFilterSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // Writes a parquet file with the given rows and row-group size; returns the path.
  private def writeParquetFile(
      dir: File,
      rows: Seq[(Long, String)],
      rowGroupSize: Long = 1024L,
      pageSize: Option[Long] = None): String = {
    val outDir = new File(dir, s"test-${System.nanoTime()}").getAbsolutePath
    val writer = rows.toDF("k", "v")
      .repartition(1)
      .write
      .option(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize)
      // Dictionary encoding off keeps row-group sizing predictable. The column index is still
      // written either way.
      .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
    // A small page size gives each row group several pages per column, which is what lets
    // column-index filtering produce a row range narrower than the whole row group.
    pageSize.foreach(size => writer.option(ParquetOutputFormat.PAGE_SIZE, size))
    writer.parquet(outDir)
    val files = new File(outDir).listFiles((_, name) => name.endsWith(".parquet"))
    assert(files != null && files.length == 1, s"expected exactly one parquet file under $outDir")
    files(0).getAbsolutePath
  }

  // Collects all rows from a reader initialized with the given storage filter.
  //
  // `tryInitializeResource` closes the reader if anything inside throws and leaves it open
  // otherwise, which is the contract these helpers need: the caller closes it once its assertions
  // pass. Without it a failure in the read loop -- what these tests are looking for -- would leak
  // the reader, its input stream and its off-heap vectors for the rest of the JVM, and can cascade
  // into unrelated failures in the same suite. `initialize` throws too, so the wrap starts at
  // construction.
  private def readAll(
      filePath: String,
      storageFilter: ParquetStorageFilter): (Seq[(Long, String)], VectorizedParquetRecordReader) = {
    Utils.tryInitializeResource {
      new VectorizedParquetRecordReader(false, 4096)
    } { reader =>
      reader.setStorageFilter(storageFilter)
      reader.initialize(filePath, java.util.Arrays.asList("k", "v"))
      reader.initBatch(new StructType(), null)
      val collected = mutable.ArrayBuffer[(Long, String)]()
      while (reader.nextBatch()) {
        val batch = reader.resultBatch().asInstanceOf[ColumnarBatch]
        val n = batch.numRows()
        val kVec = batch.column(0)
        val vVec = batch.column(1)
        var i = 0
        while (i < n) {
          collected += ((kVec.getLong(i), vVec.getUTF8String(i).toString))
          i += 1
        }
      }
      (collected.toSeq, reader)
    }
  }

  // Builds a `k >= threshold` storage filter bound to position 0.
  private def keyAtLeastFilter(
      threshold: Long,
      metrics: StorageFilterMetrics = StorageFilterMetrics()): ParquetStorageFilter = {
    val expr = GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(threshold))
    val requested = StructType(Seq(
      StructField("k", LongType, nullable = false), StructField("v", StringType, nullable = false)))
    ParquetStorageFilter.create(Seq(expr), requested, metrics)
  }

  // Writes a single-column (just `k`) parquet file for the supplied key type via Spark's
  // {@code Encoder}.
  private def writeKeyOnlyParquetFile[T : org.apache.spark.sql.Encoder](
      dir: File,
      keys: Seq[T],
      rowGroupSize: Long = 1024L): String = {
    val outDir = new File(dir, s"test-${System.nanoTime()}").getAbsolutePath
    spark.createDataset(keys).toDF("k")
      .repartition(1)
      .write
      .option(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize)
      .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
      .parquet(outDir)
    val files = new File(outDir).listFiles((_, name) => name.endsWith(".parquet"))
    assert(files != null && files.length == 1, s"expected exactly one parquet file under $outDir")
    files(0).getAbsolutePath
  }

  // Reads a key-only file, returning the survivor keys and the reader. The {@code extract} function
  // pulls one value at a time from the batch's key column.
  private def readKeyOnlyAll[T](
      filePath: String,
      storageFilter: ParquetStorageFilter,
      extract: (org.apache.spark.sql.vectorized.ColumnVector, Int) => T,
      capacity: Int = 4096): (Seq[T], VectorizedParquetRecordReader) = {
    Utils.tryInitializeResource {
      new VectorizedParquetRecordReader(false, capacity)
    } { reader =>
      reader.setStorageFilter(storageFilter)
      reader.initialize(filePath, java.util.Arrays.asList("k"))
      reader.initBatch(new StructType(), null)
      val collected = mutable.ArrayBuffer[T]()
      while (reader.nextBatch()) {
        val batch = reader.resultBatch().asInstanceOf[ColumnarBatch]
        val n = batch.numRows()
        val kVec = batch.column(0)
        var i = 0
        while (i < n) {
          collected += extract(kVec, i)
          i += 1
        }
      }
      (collected.toSeq, reader)
    }
  }

  // Builds a `k >= threshold` storage filter bound to position 0 against a key-only schema of the
  // given key type.
  private def keyOnlyAtLeastFilter(
      threshold: Literal,
      keyType: DataType,
      metrics: StorageFilterMetrics = StorageFilterMetrics()): ParquetStorageFilter = {
    val expr = GreaterThanOrEqual(BoundReference(0, keyType, nullable = false), threshold)
    val requested = StructType(Seq(StructField("k", keyType, nullable = false)))
    ParquetStorageFilter.create(Seq(expr), requested, metrics)
  }

  test("rejects entire row group: no data-column IO, row-group-skipped metric incremented") {
    withTempDir { dir =>
      // 40 rows, ~small row group => >= 2 row groups.
      val rows = (1L to 40L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      val rgSkipped = SQLMetrics.createMetric(spark.sparkContext, "rowGroupsSkipped")
      val rowsExcludedRg = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedByRowGroup")
      val rowsExcludedPf = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedWithinRowGroup")
      val bytesAvoidedRg = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByRg")
      val bytesAvoidedPf = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByPf")
      val filter = keyAtLeastFilter(1000L, StorageFilterMetrics(
        rowGroupsSkipped = rgSkipped,
        rowsExcludedByRowGroup = rowsExcludedRg,
        rowsExcludedWithinRowGroup = rowsExcludedPf,
        bytesAvoidedByRowGroup = bytesAvoidedRg,
        bytesAvoidedByPageFiltering = bytesAvoidedPf))
      val (result, reader) = readAll(path, filter)
      try {
        assert(result.isEmpty, "filter rejects all rows; no rows should be emitted")
        assert(rgSkipped.value > 0,
          s"expected at least one row group skipped; got ${rgSkipped.value}")
        assert(rowsExcludedRg.value > 0,
          s"expected rows excluded by whole-rowgroup skip; got ${rowsExcludedRg.value}")
        assert(rowsExcludedPf.value == 0,
          s"no partial-row-group filtering expected; got ${rowsExcludedPf.value}")
        // Schema is (k: Long, v: String). Skipping a row group avoids the v-column bytes the
        // no-storage-filter path would have read; phase 1 still pays for k. So avoided > 0.
        assert(bytesAvoidedRg.value > 0,
          s"expected non-key bytes avoided by whole row groups; got ${bytesAvoidedRg.value}")
        assert(bytesAvoidedPf.value == 0,
          s"no page-filtering bytes expected when all groups skipped; got ${bytesAvoidedPf.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("all rows survive: no skipping and no filtering") {
    withTempDir { dir =>
      val rows = (1L to 40L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      val rgSkipped = SQLMetrics.createMetric(spark.sparkContext, "rowGroupsSkipped")
      val rowsExcludedPf = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedWithinRowGroup")
      val filter = keyAtLeastFilter(0L, StorageFilterMetrics(
        rowGroupsSkipped = rgSkipped, rowsExcludedWithinRowGroup = rowsExcludedPf))
      val (result, reader) = readAll(path, filter)
      try {
        assert(result.toSet == rows.toSet, s"all rows should round-trip; got ${result.size} rows")
        assert(rgSkipped.value == 0, s"nothing should be skipped; got ${rgSkipped.value}")
        assert(rowsExcludedPf.value == 0,
          s"nothing should be filtered; got ${rowsExcludedPf.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("mixed: some row groups skipped, others partially kept") {
    withTempDir { dir =>
      // Many rows + small row groups => guaranteed multiple row groups.
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      val rgSkipped = SQLMetrics.createMetric(spark.sparkContext, "rowGroupsSkipped")
      val rowsExcludedPf = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedWithinRowGroup")
      // k >= 195 keeps only the last 6 rows; earlier row groups should be skipped.
      val filter = keyAtLeastFilter(195L, StorageFilterMetrics(
        rowGroupsSkipped = rgSkipped, rowsExcludedWithinRowGroup = rowsExcludedPf))
      val (result, reader) = readAll(path, filter)
      try {
        // Output is exact: VectorizedColumnReader uses PageReadStore.getRowIndexes (driven by
        // our finalRanges) to skip rows within partial pages, so emitted rows == survivors.
        val expected = rows.filter(_._1 >= 195L).toSet
        assert(result.toSet == expected,
          s"expected exact filtering; got ${result.map(_._1).sorted}, " +
            s"expected ${expected.map(_._1).toSeq.sorted}")
        assert(rgSkipped.value >= 1, s"expected row groups skipped; got ${rgSkipped.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("key-only projection: phase 2 is skipped and both byte-avoided metrics are zero") {
    // When the projected schema contains only the bloom key, phase 2 is skipped entirely
    // (`nonKeyColumns == null` in the reader). All output rows come from the per-key-column
    // queues populated in phase 1. Total bytes read match the no-storage-filter path (phase 1 reads
    // the key column once instead of phase 2 re-reading it), so both `avoided` metrics are zero:
    // there are no non-key bytes to skip.
    withTempDir { dir =>
      val keys = (1L to 200L)
      val path = writeKeyOnlyParquetFile(dir, keys, rowGroupSize = 256L)

      val rgSkipped = SQLMetrics.createMetric(spark.sparkContext, "rowGroupsSkipped")
      val rowsExcludedPf = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedWithinRowGroup")
      val bytesAvoidedRg = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByRg")
      val bytesAvoidedPf = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByPf")
      // k >= 195 -> last 6 keys survive; preceding row groups skipped or page-pruned.
      val filter = keyOnlyAtLeastFilter(Literal(195L), LongType, StorageFilterMetrics(
        rowGroupsSkipped = rgSkipped,
        rowsExcludedWithinRowGroup = rowsExcludedPf,
        bytesAvoidedByRowGroup = bytesAvoidedRg,
        bytesAvoidedByPageFiltering = bytesAvoidedPf))
      val (result, reader) = readKeyOnlyAll(path, filter, (vec, i) => vec.getLong(i))
      try {
        val expected = keys.filter(_ >= 195L).toSet
        assert(result.toSet == expected,
          s"expected exact survivor keys; got ${result.sorted}, expected ${expected.toSeq.sorted}")
        assert(rgSkipped.value >= 1, s"expected row groups skipped; got ${rgSkipped.value}")
        // For an all-keys projection, the no-storage-filter path would have read the same key
        // column phase 1 reads. There are no non-key bytes to avoid; both metrics are 0.
        assert(bytesAvoidedRg.value == 0,
          s"expected no non-key bytes to avoid on all-keys projection; got ${bytesAvoidedRg.value}")
        assert(bytesAvoidedPf.value == 0,
          s"expected no non-key bytes to avoid on all-keys projection; got ${bytesAvoidedPf.value}")
      } finally {
        reader.close()
      }
    }
  }

  test("multi-batch emit: survivor count exceeds capacity") {
    // Drive the reader at capacity = 16 with a row group of 100 surviving rows. Exercises:
    //   - The per-key-column queue holding multiple full-capacity vectors plus a partial tail.
    //   - pendingCloseKeyVectors getting closed at the start of every subsequent emit.
    //   - The per-emit ColumnarBatch reconstruction running ceil(100/16) = 7 times.
    withTempDir { dir =>
      val keys = (1L to 100L)
      // Big rowGroupSize so all 100 rows fit in one row group.
      val path = writeKeyOnlyParquetFile(dir, keys, rowGroupSize = 64 * 1024L)
      // Filter accepts every row so the queue is fully populated.
      val filter = keyOnlyAtLeastFilter(Literal(0L), LongType)
      val (result, reader) =
        readKeyOnlyAll(path, filter, (vec, i) => vec.getLong(i), capacity = 16)
      try {
        assert(result == keys.toSeq,
          s"expected all keys returned in order across multiple batches; got ${result.size} rows")
      } finally {
        reader.close()
      }
    }
  }

  test("int key column: filter survivors round-trip through phase 1 accumulators") {
    // Covers the IntegerType branch of ValueCopier.
    withTempDir { dir =>
      val keys = (1 to 100)
      val path = writeKeyOnlyParquetFile(dir, keys, rowGroupSize = 256L)
      val filter = keyOnlyAtLeastFilter(Literal(90), IntegerType)
      val (result, reader) = readKeyOnlyAll(path, filter, (vec, i) => vec.getInt(i))
      try {
        assert(result.toSet == keys.filter(_ >= 90).toSet,
          s"expected int keys >= 90; got ${result.sorted}")
      } finally {
        reader.close()
      }
    }
  }

  test("string key column: filter survivors round-trip through phase 1 accumulators") {
    // Covers the StringType branch of ValueCopier (variable-length byte copy via putByteArray).
    withTempDir { dir =>
      val keys = (1 to 20).map(i => f"k$i%03d")
      val path = writeKeyOnlyParquetFile(dir, keys, rowGroupSize = 256L)
      val filter = keyOnlyAtLeastFilter(
        Literal.create("k015", StringType), StringType)
      val (result, reader) =
        readKeyOnlyAll(path, filter, (vec, i) => vec.getUTF8String(i).toString)
      try {
        assert(result.toSet == keys.filter(_ >= "k015").toSet,
          s"expected string keys >= 'k015'; got ${result.sorted}")
      } finally {
        reader.close()
      }
    }
  }

  test("ParquetStorageFilter.create rejects a filter that violates a planner precondition") {
    // These are all planner bugs by construction: extractStorageFilters pre-checks each one, and
    // by the time create runs the conjunct is gone from the post-scan Filter, so a soft rejection
    // would silently return rows the filter excludes. create fails instead.
    val requested = StructType(Seq(
      StructField("k", LongType, nullable = false),
      StructField("v", StringType, nullable = false)))

    // Nothing to push: the caller is supposed to check this before calling.
    val empty = intercept[IllegalArgumentException] {
      ParquetStorageFilter.create(Seq.empty, requested)
    }
    assert(empty.getMessage.contains("must be non-empty"), empty.getMessage)

    // Ordinal 5 is out of range for a two-field requested schema.
    val outOfRange = intercept[IllegalArgumentException] {
      ParquetStorageFilter.create(
        Seq(GreaterThanOrEqual(BoundReference(5, LongType, nullable = false), Literal(0L))),
        requested)
    }
    assert(outOfRange.getMessage.contains("outside the 2 fields"), outOfRange.getMessage)

    // No bound reference at all, so there is no key column to read in phase 1.
    val noRefs = intercept[IllegalArgumentException] {
      ParquetStorageFilter.create(Seq(GreaterThanOrEqual(Literal(1L), Literal(0L))), requested)
    }
    assert(noRefs.getMessage.contains("no bound reference"), noRefs.getMessage)

    // A key type the reader has no value copier for.
    val variantSchema = StructType(Seq(StructField("k", VariantType, nullable = true)))
    val badType = intercept[IllegalArgumentException] {
      ParquetStorageFilter.create(
        Seq(IsNull(BoundReference(0, VariantType, nullable = true))), variantSchema)
    }
    assert(badType.getMessage.contains("isSupportedKeyType"), badType.getMessage)
  }

  // Serializes a [[BloomFilter]] to bytes suitable for a [[Literal]].
  private def bloomBytes(bf: BloomFilter): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    bf.writeTo(out)
    out.toByteArray
  }

  // Computes `XxHash64(v)` using the same seed Spark uses for runtime bloom filters.
  private def xxHash64(v: Any, dt: DataType): Long = {
    new XxHash64(Seq(Literal(v, dt))).eval(InternalRow.empty).asInstanceOf[Long]
  }

  test("rewriteForMissingKeys: regular equi-join bloom probes the null key's hash") {
    // Regular equi-join: the runtime bloom is BloomFilterMightContain(bloom, XxHash64(key)).
    // XxHash64 is a HashExpression, so it is nullable = false and hashes a null input to its SEED
    // rather than producing null. Substituting Literal(null) therefore leaves a concrete probe for
    // `xxHash64(null)`, and whether the file is kept depends on whether that hash is in the bloom.
    // Both directions are asserted so the outcome does not hinge on a lucky bloom miss.
    val nullKeyHash = xxHash64(null, LongType)
    val requested = StructType(Seq(StructField("k", LongType, nullable = true)))

    def rewriteWithBloomContaining(hashes: Long*): ParquetStorageFilter = {
      val bf = BloomFilter.create(10, 128)
      hashes.foreach(bf.putLong)
      val expr = BloomFilterMightContain(
        Literal(bloomBytes(bf), BinaryType),
        new XxHash64(Seq(BoundReference(0, LongType, nullable = true))))
      val filter = ParquetStorageFilter.create(Seq(expr), requested)
      filter.rewriteForMissingKeys(Array(0), Array(null))
    }

    val dropped = rewriteWithBloomContaining(xxHash64(42L, LongType))
    assert(dropped.keyColumnIndices.isEmpty,
      "all key positions are missing; keyColumnIndices should be empty")
    assert(!dropped.evalAllMissing(),
      "the null key's hash is not in the bloom, so evalAllMissing must return false " +
        "(skip the file)")

    val kept = rewriteWithBloomContaining(nullKeyHash)
    assert(kept.evalAllMissing(),
      "the null key's hash IS in the bloom, so evalAllMissing must return true (keep the file)")
  }

  test("rewriteForMissingKeys: missing key with an existence DEFAULT probes the default's hash") {
    // A missing column that has a non-null existence DEFAULT is materialized by
    // ParquetColumnVector as that default, not as null. The predicate must therefore be evaluated
    // against the default -- evaluating against null could skip a whole file whose rows all match.
    val defaultValue = 7L
    val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
    val bf = BloomFilter.create(10, 128)
    bf.putLong(xxHash64(defaultValue, LongType))
    val expr = BloomFilterMightContain(
      Literal(bloomBytes(bf), BinaryType),
      new XxHash64(Seq(BoundReference(0, LongType, nullable = true))))
    val filter = ParquetStorageFilter.create(Seq(expr), requested)

    // Substituting the default keeps the file, because the default's hash is in the bloom.
    assert(filter.rewriteForMissingKeys(Array(0), Array(defaultValue)).evalAllMissing(),
      "substituting the existence default must probe the default's hash and keep the file")
    // Substituting null instead would drop it -- the bug this guards against.
    assert(!filter.rewriteForMissingKeys(Array(0), Array(null)).evalAllMissing(),
      "sanity check: substituting null probes a different hash and would drop the file")
  }

  test("rewriteForMissingKeys: null-safe equi-join bloom keeps the file") {
    // Null-safe equi-join: ExtractEquiJoinKeys rewrites `a <=> b` so the join key becomes
    // Coalesce(key, default). After rewriteForMissingKeys substitutes null for the
    // BoundReference, the coalesce produces `default` (0L here) and the bloom probe checks
    // whether the default's hash is in the bloom (which it is, matching what the creation side
    // would have inserted for its own null rows). Expected result: keep the file.
    val defaultHash = xxHash64(0L, LongType)
    val bf = BloomFilter.create(10, 128)
    bf.putLong(defaultHash)
    val bloomLit = Literal(bloomBytes(bf), BinaryType)
    val expr = BloomFilterMightContain(
      bloomLit,
      new XxHash64(Seq(Coalesce(Seq(
        BoundReference(0, LongType, nullable = true),
        Literal.default(LongType))))))

    val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
    val filter = ParquetStorageFilter.create(Seq(expr), requested)

    val rewritten = filter.rewriteForMissingKeys(Array(0), Array(null))
    assert(rewritten.keyColumnIndices.isEmpty,
      "all key positions are missing; keyColumnIndices should be empty")
    assert(rewritten.evalAllMissing(),
      "Coalesce(null, default) yields default; bloom hit, so evalAllMissing must return true")
  }

  test("rewriteForMissingKeys: partial-missing keys narrow keyColumnIndices and renumber") {
    // Two keys, one missing, one present. Verify the present key's BoundReference is renumbered to
    // position 0 in the new layout, and the missing one is substituted with Literal(null). We don't
    // run the predicate here, only the rewrite's structure.
    val bf = BloomFilter.create(10, 128)
    bf.putLong(xxHash64(1L, LongType))
    val bloomLit = Literal(bloomBytes(bf), BinaryType)
    // Two conjuncts: one on ordinal 0 (missing), one on ordinal 1 (present).
    val exprs = Seq(
      BloomFilterMightContain(
        bloomLit, new XxHash64(Seq(BoundReference(0, LongType, nullable = true)))),
      BloomFilterMightContain(
        bloomLit, new XxHash64(Seq(BoundReference(1, LongType, nullable = true)))))

    val requested = StructType(Seq(
      StructField("a", LongType, nullable = true),
      StructField("b", LongType, nullable = true)))
    val filter = ParquetStorageFilter.create(exprs, requested)
    assert(filter.keyColumnIndices.toSeq == Seq(0, 1))

    val rewritten = filter.rewriteForMissingKeys(Array(0), Array(null))
    assert(rewritten.keyColumnIndices.toSeq == Seq(1),
      "only the present key column should remain in keyColumnIndices")
    val ordinals = rewritten.boundExpression.collect {
      case b: BoundReference => b.ordinal
    }
    assert(ordinals == Seq(0),
      "the remaining BoundReference (for ordinal 1 in requested schema) should be " +
        s"renumbered to local position 0; got $ordinals")
  }

  // Returns the FileSourceScanExec for a parquet read, with `storageFilters` attached. Goes through
  // Spark's normal planning/execution machinery (not the test-only reader init), so it exercises
  // preparedStorageFilters' subquery materialization + bind, the SQL conf check,
  // ParquetFileFormat.buildReaderWithStorageFilters, and metric propagation.
  private def scanWithStorageFilter(
      path: String,
      keyName: String,
      threshold: Long): FileSourceScanExec = {
    val df = spark.read.parquet(path).select("k", "v")
    val plan = df.queryExecution.executedPlan
    val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
      .getOrElse(fail(s"No FileSourceScanExec found in plan: $plan"))
    val keyAttr = scan.output.find(_.name == keyName).getOrElse(fail(s"No $keyName in scan output"))
    val expr = GreaterThanOrEqual(keyAttr, Literal(threshold))
    scan.copy(storageFilters = Seq(expr))
  }

  // Executes a SparkPlan that may produce columnar batches. When the plan supports columnar output
  // (typical for parquet scans with WSCG enabled), Spark's planner normally inserts a
  // ColumnarToRowExec; since these tests bypass the planner, we wrap manually.
  private def executePlanCollect(plan: SparkPlan): Array[(Long, String)] = {
    val rowPlan = if (plan.supportsColumnar) ColumnarToRowExec(plan) else plan
    rowPlan.executeCollect().map(r => (r.getLong(0), r.getString(1)))
  }

  test("end-to-end via FileSourceScanExec: conf on, filter applied, metrics populated") {
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, "k", threshold = 195L)
        val collected = executePlanCollect(scan).toSet
        val expected = rows.filter(_._1 >= 195L).toSet
        assert(collected == expected, s"got ${collected.toSeq.sortBy(_._1)}; expected $expected")

        val rgSkipped = scan.metrics(FileSourceScanLike.STORAGE_FILTER_ROW_GROUPS_SKIPPED)
        assert(rgSkipped.value >= 1,
          s"expected at least one row group skipped via storage filter; got ${rgSkipped.value}")
      }
    }
  }

  test("pushed data filter on a non-key column + storage filter on key: cross-propagation works") {
    // A pushed data filter on a non-key column and a storage filter on the key column have to
    // compose: phase 0 derives its row ranges from the pushed filter, phase 1 narrows them with the
    // storage filter, and phase 2 reads the non-key columns under the intersection.
    //
    // Note the predicate deliberately uses `>` rather than `!=`. ColumnIndexFilter substitutes
    // `rangesForMissingColumns` for a predicate over a column outside its path set, and that is
    // EMPTY for Gt/GtEq/Lt/LtEq/Eq but allRows for NotEq -- so a `!=` predicate here would be
    // satisfied by a phase 0 that saw the wrong schema, and would prove nothing.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, f"v_$i%03d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).select("k", "v").filter("v > 'v_000'")
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        assert(scan.simpleString(200).contains("GreaterThan(v,"),
          s"the data filter must actually be pushed for this test to mean anything: " +
            scan.simpleString(200))
        // Storage filter on `k` (key column).
        val keyAttr = scan.output.find(_.name == "k").get
        val withSF = scan.copy(
          storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(100L))))

        val collected = executePlanCollect(withSF).toSet
        // Every row satisfies v > 'v_000', so the storage filter alone decides the result.
        val expected = rows.filter(_._1 >= 100L).toSet
        assert(collected == expected,
          s"got ${collected.toSeq.sortBy(_._1)}; expected ${expected.toSeq.sortBy(_._1)}")
      }
    }
  }

  // ----- FileSourceStrategy bloom-filter extraction -----

  // Counts BloomFilterMightContain expressions inside FilterExec nodes of a physical plan.
  private def countBloomFiltersInPostScanFilters(plan: SparkPlan): Int = {
    plan.collect {
      case f: FilterExec =>
        f.condition.collect { case _: BloomFilterMightContain => 1 }.sum
    }.sum
  }

  // Counts BloomFilterMightContain expressions inside FileSourceScanExec.storageFilters.
  private def countBloomFiltersInStorageFilters(plan: SparkPlan): Int = {
    plan.collect {
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

  // Creates two parquet tables (bf1: large, bf2: small with selective filter) for join tests.
  private def withBloomFilterTables(body: => Unit): Unit = {
    withTable("bf1", "bf2") {
      // bf1 = "large" application side: 600 rows.
      spark.range(600).selectExpr("id AS k", "id AS v").write.format("parquet").saveAsTable("bf1")
      // bf2 = "small" creation side: 30 rows, with a selective filter (v = 5 keeps 1 row).
      spark.range(30).selectExpr("id AS k", "id AS v").write.format("parquet").saveAsTable("bf2")
      body
    }
  }

  test("FileSourceStrategy extracts bloom filter into scan.storageFilters when conf is on") {
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val (plan, _) = runBloomFilterJoin()
        val storageBlooms = countBloomFiltersInStorageFilters(plan)
        val postScanBlooms = countBloomFiltersInPostScanFilters(plan)
        assert(storageBlooms >= 1,
          s"expected >= 1 bloom filter on scan.storageFilters; got $storageBlooms.\n" +
            s"Plan:\n$plan")
        assert(postScanBlooms == 0,
          s"expected no bloom filter in any post-scan FilterExec; got $postScanBlooms.\n" +
            s"Plan:\n$plan")
      }
    }
  }

  test("FileSourceStrategy leaves the bloom behind when every projected column is a key column") {
    // Nothing is left for phase 2 to prune: the reader would read the same column for the same rows
    // as a plain scan, since it has to read a key column to evaluate the filter on it, and would
    // add only the cost of evaluating the predicate outside the generated code.
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
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
        assert(df.collect().map(_.getLong(0)).toSet == Set(5L),
          "and the query must still return the joined key")
      }
    }
  }

  test("FileSourceStrategy leaves bloom filter as post-scan FilterExec when conf is off") {
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "false",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val (plan, _) = runBloomFilterJoin()
        val storageBlooms = countBloomFiltersInStorageFilters(plan)
        val postScanBlooms = countBloomFiltersInPostScanFilters(plan)
        assert(storageBlooms == 0,
          s"expected no bloom on scan.storageFilters when conf is off; got $storageBlooms")
        assert(postScanBlooms >= 1,
          s"expected bloom in post-scan FilterExec when conf is off; got $postScanBlooms.\n" +
            s"Plan:\n$plan")
      }
    }
  }

  test("ignoreCorruptFiles does not swallow a failure to honor a storage filter") {
    // The reader has to fail when a file cannot support late materialization, because the planner
    // removed the conjunct from the post-scan Filter. FileScanRDD and FilePartitionReader would
    // skip the rest of such a file under ignoreCorruptFiles, silently dropping readable rows, so
    // the exception the reader throws is excluded from that.
    assert(!DataSourceUtils.shouldIgnoreCorruptFileException(
      new UnsupportedFileReadException("cannot honor a storage filter")))
    // The generic reader failures it is carved out of stay swallowed.
    assert(DataSourceUtils.shouldIgnoreCorruptFileException(new IllegalStateException("corrupt")))
    assert(DataSourceUtils.shouldIgnoreCorruptFileException(new java.io.IOException("truncated")))
  }

  test("the feature still engages when ignoreCorruptFiles is on") {
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.IGNORE_CORRUPT_FILES.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val (plan, rows) = runBloomFilterJoin()
        val storageBlooms = countBloomFiltersInStorageFilters(plan)
        assert(storageBlooms == 1,
          s"expected the bloom on scan.storageFilters; got $storageBlooms.\nPlan:\n$plan")
        assert(countBloomFiltersInPostScanFilters(plan) == 0,
          s"expected no bloom left above the scan.\nPlan:\n$plan")
        assert(rows.map(r => (r.getLong(0), r.getLong(1))).toSet == Set((5L, 5L)),
          s"and the query must still return the joined row; got ${rows.mkString(", ")}")
      }
    }
  }

  test("FileSourceStrategy extraction preserves query results") {
    withBloomFilterTables {
      val baseConf = Map(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")
      val resultConfOff = withSQLConf(
          (baseConf + (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "false")).toSeq: _*) {
        runBloomFilterJoin()._2.map(r => (r.getLong(0), r.getLong(1))).toSet
      }
      val resultConfOn = withSQLConf(
          (baseConf + (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true")).toSeq: _*) {
        runBloomFilterJoin()._2.map(r => (r.getLong(0), r.getLong(1))).toSet
      }
      assert(resultConfOn == resultConfOff,
        s"results differ between conf-on and conf-off: on=$resultConfOn off=$resultConfOff")
    }
  }

  Seq(Seq("k", "v"), Seq("k")).foreach { projection =>
    test("a row group over the splice cap is read the plain way, same rows, more bytes " +
      s"(projection ${projection.mkString(",")})") {
      // Past the cap a row group is read the plain way: phase 2 takes every projected column, key
      // columns included, so nothing is buffered. Two things have to hold. The rows must not
      // change, since a fallback that quietly dropped the predicate would return extra rows. And
      // the fallback must actually have happened, which is observable: it reads the key column a
      // second time, so it transfers strictly more bytes. Without that second assertion the test
      // would pass even if the cap never reached the reader.
      //
      // The batch size is what makes the cap reachable at all: the reader examines the count only
      // once a batch worth of survivors per key column has been buffered, so 51 survivors have to
      // cross that line more than once.
      //
      // The `k` projection is the interesting one: an all-keys projection normally skips phase 2
      // entirely, so past the cap it has to read the key column there like any other column.
      withTempDir { dir =>
        val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
        val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))
        val fileSchema = StructType(Seq(
          StructField("k", LongType, nullable = true),
          StructField("v", StringType, nullable = true)))
        val readSchema = StructType(projection.map(fileSchema(_)))
        val storageFilters =
          Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(350L)))

        def run(maxSplicedBytes: String): (Int, Long) = {
          withSQLConf(
              SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_MAX_SPLICED_ROW_GROUP_BYTES.key ->
                maxSplicedBytes,
              SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "16") {
            val hadoopConf = spark.sessionState.newHadoopConf()
            hadoopConf.set(s"fs.${CountingLocalFileSystem.scheme}.impl",
              classOf[CountingLocalFileSystem].getName)
            hadoopConf.setBoolean(s"fs.${CountingLocalFileSystem.scheme}.impl.disable.cache", true)
            val readerFn = new ParquetFileFormat().buildReaderWithStorageFilters(
              spark, fileSchema, new StructType(), readSchema, Nil, storageFilters,
              Map(FileFormat.OPTION_RETURNING_BATCH -> "true"), hadoopConf, Map.empty)
            val file = PartitionedFile(
              InternalRow.empty,
              SparkPath.fromUrlString(s"${CountingLocalFileSystem.scheme}://$path"),
              0,
              new File(path).length())
            CountingLocalFileSystem.reset()
            val emitted = readerFn(file).asInstanceOf[Iterator[Object]].map {
              case batch: ColumnarBatch => batch.numRows()
              case _ => 1
            }.sum
            (emitted, CountingLocalFileSystem.bytesRead())
          }
        }

        val (splicedRows, splicedBytes) = run("64MB")
        val (plainRows, plainBytes) = run("1b")
        assert(splicedRows == 51, s"the filter keeps keys 350..400; got $splicedRows")
        assert(plainRows == splicedRows,
          s"rows differ past the cap: plain=$plainRows spliced=$splicedRows")
        assert(plainBytes > splicedBytes,
          s"past the cap the key column is read twice, so the read must be larger; " +
            s"plain=$plainBytes spliced=$splicedBytes")
      }
    }
  }

  test("FileSourceStrategy leaves a non-deterministic bloom in the post-scan Filter") {
    // `ParquetStorageFilter.test` evaluates the predicate without calling
    // `BasePredicate.initialize(partitionIndex)`, which `GeneratePredicate` emits for a
    // `Nondeterministic` expression, so a non-deterministic conjunct has to stay behind. No
    // producer builds one today, hence the hand-built plan: `InjectRuntimeFilter`'s blooms hash
    // join keys, which are deterministic.
    withTempDir { dir =>
      val rows = (1L to 50L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val bf = BloomFilter.create(10, 128)
        bf.putLong(xxHash64(42L, LongType))
        val bloomLit = Literal(bloomBytes(bf), BinaryType)
        val relation = spark.read.parquet(path).select("k", "v").queryExecution.optimizedPlan
        val k = relation.output.find(_.name == "k").getOrElse(fail("no k in the relation output"))

        def extractedBlooms(valueExpr: Expression): (Int, Int) = {
          val logical = LogicalFilter(BloomFilterMightContain(bloomLit, valueExpr), relation)
          val physical = FileSourceStrategy(logical).headOption
            .getOrElse(fail(s"FileSourceStrategy did not plan $logical"))
          (countBloomFiltersInStorageFilters(physical),
            countBloomFiltersInPostScanFilters(physical))
        }

        // Control: the same bloom over the key alone is extracted, so the arms differ in exactly
        // one thing.
        val (deterministicInScan, deterministicPostScan) =
          extractedBlooms(new XxHash64(Seq(k)))
        assert(deterministicInScan == 1 && deterministicPostScan == 0,
          s"a deterministic bloom must be extracted; got scan=$deterministicInScan " +
            s"postScan=$deterministicPostScan")

        // `Rand` contributes no reference, so `k` is still the only key column and only the
        // determinism gate can reject this one.
        val nonDeterministic = new XxHash64(Seq(k, Rand(Literal(1L))))
        assert(!nonDeterministic.deterministic, "the value expression must be non-deterministic")
        val (inScan, postScan) = extractedBlooms(nonDeterministic)
        assert(inScan == 0, s"a non-deterministic bloom must not be extracted; got $inScan")
        assert(postScan == 1, s"it must stay in the post-scan Filter; got $postScan")
      }
    }
  }

  // ----- Generic reader plumbing for the coverage tests below -----

  // Writes a single-column (`k`) parquet file from a SQL expression over `id`, avoiding the need
  // for an Encoder per key type. `keyExpr` is evaluated over `spark.range(1, n + 1)`.
  private def writeKeyOnlyParquetFileFromSql(
      dir: File,
      keyExpr: String,
      n: Long = 100L,
      rowGroupSize: Long = 256L,
      dictionary: Boolean = false): String = {
    val outDir = new File(dir, s"test-${System.nanoTime()}").getAbsolutePath
    spark.range(1, n + 1).selectExpr(s"$keyExpr AS k")
      .repartition(1)
      .write
      .option(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize)
      .option(ParquetOutputFormat.ENABLE_DICTIONARY, dictionary.toString)
      .parquet(outDir)
    val files = new File(outDir).listFiles((_, name) => name.endsWith(".parquet"))
    assert(files != null && files.length == 1, s"expected exactly one parquet file under $outDir")
    files(0).getAbsolutePath
  }

  // Reads every batch, projecting each row through `extract`. `storageFilter` may be null, which
  // selects the plain (non-splicing) vectorized path.
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
      new VectorizedParquetRecordReader(useOffHeap, capacity)
    } { reader =>
      reader.setStorageFilter(storageFilter)
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

  // Renders one column value as a string using its *internal* representation, so the splicing path
  // and the plain path can be compared without going through external type conversion.
  private def renderValue(vec: ColumnVector, i: Int, dt: DataType): String = {
    if (vec.isNullAt(i)) {
      "null"
    } else {
      dt match {
        case BooleanType => vec.getBoolean(i).toString
        case ByteType => vec.getByte(i).toString
        case ShortType => vec.getShort(i).toString
        case IntegerType | DateType | _: YearMonthIntervalType => vec.getInt(i).toString
        case LongType | TimestampType | TimestampNTZType | _: TimeType |
            _: DayTimeIntervalType => vec.getLong(i).toString
        case FloatType => vec.getFloat(i).toString
        case DoubleType => vec.getDouble(i).toString
        case d: DecimalType => vec.getDecimal(i, d.precision, d.scale).toString
        case _: StringType => vec.getUTF8String(i).toString
        case BinaryType => vec.getBinary(i).mkString(",")
        case other => fail(s"renderValue does not handle $other")
      }
    }
  }

  // Reads a key-only file through the plain (no storage filter) path and keeps the rows the given
  // bound predicate accepts. This is the oracle for the splicing path: whatever the plain reader
  // returns, filtered in Scala, is exactly what splicing must produce.
  private def survivorsViaPlainPath(
      filePath: String,
      dt: DataType,
      boundPredicate: org.apache.spark.sql.catalyst.expressions.Expression): Seq[String] = {
    val predicate = Predicate.create(boundPredicate)
    val (rows, reader) = readAllWith(
      filePath, Seq("k"), null,
      (b, i) => (renderValue(b.column(0), i, dt), b.getRow(i).copy()))
    try {
      rows.collect { case (rendered, row) if predicate.eval(row) => rendered }
    } finally {
      reader.close()
    }
  }

  // ----- Key-type coverage: one case per ValueCopier branch -----

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
    dictionary <- Seq(false, true)
  } {
    val encoding = if (dictionary) "dictionary-encoded" else "plain-encoded"
    test(s"key type $name ($encoding): survivors round-trip through the phase 1 accumulators") {
      // TIMESTAMP_MICROS rather than Spark's default INT96, which the reader only accepts with
      // int96AsTimestamp and which is not the INT64 copier branch we want to cover here.
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
        withTempDir { dir =>
          val path = writeKeyOnlyParquetFileFromSql(dir, keyExpr, dictionary = dictionary)
          val bound = GreaterThanOrEqual(
            BoundReference(0, dt, nullable = true), Literal.create(threshold, dt))
          val expected = survivorsViaPlainPath(path, dt, bound)
          assert(expected.nonEmpty && expected.size < 100,
            s"the $name case should keep some but not all rows; kept ${expected.size}")

          val requested = StructType(Seq(StructField("k", dt, nullable = true)))
          val filter = ParquetStorageFilter.create(Seq(bound), requested)
          val (result, reader) = readAllWith(
            path, Seq("k"), filter, (b, i) => renderValue(b.column(0), i, dt))
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
    // Regression test: TimeType is an AtomicType that passes every planning-time gate (it is
    // batch-readable and XxHash64 hashes it, so InjectRuntimeFilter will build a bloom on a TIME
    // join key), so a key-type whitelist that omitted it would fail the task at reader init.
    assert(ParquetStorageFilter.isSupportedKeyType(TimeType(6)),
      "TIME must be an eligible storage-filter key type")
    withTempDir { dir =>
      val times = (1 to 100).map(i => LocalTime.ofSecondOfDay(i.toLong))
      val path = writeKeyOnlyParquetFile(dir, times, rowGroupSize = 256L)
      val dt = TimeType(6)
      val bound = GreaterThanOrEqual(
        BoundReference(0, dt, nullable = true),
        Literal.create(LocalTime.ofSecondOfDay(90L), dt))
      val expected = survivorsViaPlainPath(path, dt, bound)
      assert(expected.size == 11, s"expected the last 11 of 100 TIME keys; got ${expected.size}")

      val requested = StructType(Seq(StructField("k", dt, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested)
      val (result, reader) =
        readAllWith(path, Seq("k"), filter, (b, i) => renderValue(b.column(0), i, dt))
      try {
        assert(result == expected, s"got $result; expected $expected")
      } finally {
        reader.close()
      }
    }
  }

  test("isSupportedKeyType covers exactly the types the reader can copy") {
    // This is the contract that keeps FileSourceStrategy, ParquetStorageFilter.create and
    // VectorizedParquetRecordReader.copierFor in lockstep. A type admitted here but missing from
    // copierFor turns a planning-time rejection into a task failure.
    val supported: Seq[DataType] = Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      DecimalType(9, 2), DecimalType(18, 2), DecimalType(30, 2), DateType, TimestampType,
      TimestampNTZType, TimeType(6), YearMonthIntervalType(), DayTimeIntervalType(),
      StringType, VarcharType(10), CharType(10), BinaryType)
    supported.foreach { dt =>
      assert(ParquetStorageFilter.isSupportedKeyType(dt), s"$dt should be a supported key type")
    }
    // Atomic but with no primitive Parquet leaf to accumulate into, plus the non-atomic types.
    val unsupported: Seq[DataType] = Seq(
      VariantType, NullType, ArrayType(IntegerType), MapType(IntegerType, IntegerType),
      new StructType().add("a", IntegerType))
    unsupported.foreach { dt =>
      assert(!ParquetStorageFilter.isSupportedKeyType(dt),
        s"$dt should NOT be a supported key type")
    }
  }

  // ----- Null keys, multiple keys, partition columns, off-heap, row-at-a-time -----

  test("nullable key column: surviving null keys are copied through as nulls") {
    // The predicate deliberately accepts nulls, so appendSurvivorRowToAccumulators must take its
    // dst.putNull branch. Every other test uses a non-nullable key, leaving that branch dead.
    withTempDir { dir =>
      val path = writeKeyOnlyParquetFileFromSql(
        dir, "CASE WHEN id % 10 = 0 THEN NULL ELSE id END")
      val ref = BoundReference(0, LongType, nullable = true)
      val bound = Or(IsNull(ref), GreaterThanOrEqual(ref, Literal(90L)))
      val expected = survivorsViaPlainPath(path, LongType, bound)
      assert(expected.count(_ == "null") == 10, s"expected 10 null keys; got $expected")

      val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested)
      val (result, reader) =
        readAllWith(path, Seq("k"), filter, (b, i) => renderValue(b.column(0), i, LongType))
      try {
        assert(result == expected, s"got $result; expected $expected")
      } finally {
        reader.close()
      }
    }
  }

  test("two key columns: both accumulators stay aligned with each other and with the data column") {
    withTempDir { dir =>
      val outDir = new File(dir, "twokeys").getAbsolutePath
      spark.range(1, 201).selectExpr("id AS a", "id * 2 AS b", "CONCAT('v_', id) AS c")
        .repartition(1)
        .write
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .parquet(outDir)
      val path = new File(outDir).listFiles((_, n) => n.endsWith(".parquet"))(0).getAbsolutePath

      // a >= 100 AND b <= 300  =>  a in [100, 150]
      val bound = And(
        GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(100L)),
        LessThanOrEqual(BoundReference(1, LongType, nullable = true), Literal(300L)))
      val requested = StructType(Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true),
        StructField("c", StringType, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested)
      assert(filter.keyColumnIndices.toSeq == Seq(0, 1), "both key columns should be recognized")

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
    // Exercises the `i < isKeyTopLevel.length` branch of the emit loop: the partition slot sits
    // past the end of isKeyTopLevel and must come from persistentBatchColumns, not the key queues.
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
      // Off-heap is where the close/free hazards actually bite: OffHeapColumnVector.close() frees
      // the native buffer, so a double close or a read after close is a crash rather than stale
      // data. capacity = 16 over 100 survivors forces 7 emits, each closing the previous emit's
      // dequeued key vectors.
      withTempDir { dir =>
        val path = writeKeyOnlyParquetFileFromSql(dir, "id", n = 100L, rowGroupSize = 64 * 1024L)
        val bound = GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(0L))
        val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
        val filter = ParquetStorageFilter.create(Seq(bound), requested)
        val (result, reader) = readAllWith(
          path, Seq("k"), filter, (b, i) => b.column(0).getLong(i),
          capacity = 16, useOffHeap = useOffHeap)
        try {
          assert(result == (1L to 100L), s"expected all 100 keys in order; got ${result.size} rows")
        } finally {
          reader.close()
        }
      }
    }

    test(s"$mode vectors: row-group skipping and page filtering over a mixed projection") {
      withTempDir { dir =>
        val rows = (1L to 200L).map(i => (i, s"v_$i"))
        val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
        val bytesAvoidedRg = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByRg")
        val bytesAvoidedPf = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByPf")
        val filter = keyAtLeastFilter(195L, StorageFilterMetrics(
          bytesAvoidedByRowGroup = bytesAvoidedRg,
          bytesAvoidedByPageFiltering = bytesAvoidedPf))
        val reader = new VectorizedParquetRecordReader(useOffHeap, 4096)
        reader.setStorageFilter(filter)
        reader.initialize(path, Seq("k", "v").asJava)
        reader.initBatch(new StructType(), null)
        val collected = mutable.ArrayBuffer[(Long, String)]()
        try {
          while (reader.nextBatch()) {
            val batch = reader.resultBatch()
            var i = 0
            while (i < batch.numRows()) {
              collected += ((batch.column(0).getLong(i), batch.column(1).getUTF8String(i).toString))
              i += 1
            }
          }
          assert(collected.toSeq == rows.filter(_._1 >= 195L),
            s"expected exact filtering; got ${collected.map(_._1)}")
          // A mixed projection has non-key bytes to avoid, in both the skipped row groups and the
          // partially kept one. Both counters must be non-negative, and together positive.
          assert(bytesAvoidedRg.value >= 0 && bytesAvoidedPf.value >= 0,
            s"avoided-byte metrics must never go negative; got rg=${bytesAvoidedRg.value} " +
              s"pf=${bytesAvoidedPf.value}")
          assert(bytesAvoidedRg.value + bytesAvoidedPf.value > 0,
            "a mixed projection with skipped row groups should avoid some non-key bytes")
        } finally {
          reader.close()
        }
      }
    }
  }

  test("row-at-a-time path: nextKeyValue re-fetches the spliced batch per row") {
    // The per-emit ColumnarBatch is replaced on every nextBatch(), so a consumer holding on to an
    // earlier getCurrentValue() would read the wrong vectors. Drives the non-columnar contract with
    // a capacity small enough to span several batches.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      val filter = keyAtLeastFilter(100L)
      val reader = new VectorizedParquetRecordReader(false, 8)
      try {
        reader.setStorageFilter(filter)
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

  // ----- Schema evolution: a column missing from the physical file -----

  test("non-key column missing from a file: byte-avoided metrics tolerate a missing offset index") {
    // ColumnIndexStore returns a null OffsetIndex for a column that is in the (clipped) requested
    // schema but absent from the row group, which is exactly what schema evolution produces. The
    // avoided-bytes walk runs over the full requested schema on every row group of every file, so
    // without a null guard this is an NPE on the first row group of the older file.
    withTempDir { dir =>
      val base = new File(dir, "merged").getAbsolutePath
      // Older file: (k, v). Newer file: (k, v, w).
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
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        val keyAttr = scan.output.find(_.name == "k").getOrElse(fail("no k in scan output"))
        // Keeps rows from BOTH files, so the older one (where `w` is missing) is really read, and
        // drops the leading row groups of the older file so the skip path is exercised there too.
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(150L))))

        val rowPlan = if (withSF.supportsColumnar) ColumnarToRowExec(withSF) else withSF
        val collected = rowPlan.executeCollect()
          .map(r => (r.getLong(0), r.getString(1), if (r.isNullAt(2)) None else Some(r.getLong(2))))
          .toSet
        val expected = ((150L to 200L).map(i => (i, s"v_$i", None)) ++
          (201L to 400L).map(i => (i, s"v_$i", Some(i * 2)))).toSet
        assert(collected == expected,
          s"expected ${expected.size} rows across both schemas; got ${collected.size}")

        // The avoided-bytes counters are what walk the offset index of the missing column. Their
        // being populated and non-negative is the evidence that the walk ran and coped.
        val bytesRg = withSF.metrics(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_ROW_GROUP)
        val bytesPf =
          withSF.metrics(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_PAGE_FILTERING)
        assert(bytesRg.value >= 0 && bytesPf.value >= 0,
          s"avoided-byte metrics must never go negative; got rg=${bytesRg.value} " +
            s"pf=${bytesPf.value}")
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
      val scan = spark.read.parquet(path).select("k", "v").queryExecution.executedPlan
        .collect { case s: FileSourceScanExec => s }.head
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
      buildFilter: Attribute => Expression): Set[(Long, Option[Long])] = {
    val df = spark.sql(s"SELECT id, k FROM $table")
    val plan = df.queryExecution.executedPlan
    val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
      .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
    val keyAttr = scan.output.find(_.name == "k").getOrElse(fail("no k in scan output"))
    val withSF = scan.copy(storageFilters = Seq(buildFilter(keyAttr)))
    val rowPlan = if (withSF.supportsColumnar) ColumnarToRowExec(withSF) else withSF
    // Executing the scan directly bypasses the Project that would reorder to the SELECT order, so
    // rows arrive in the scan's own order -- the relation's dataSchema order, not the SELECT's.
    // Resolve positions by name rather than assuming they line up.
    val idPos = scan.output.indexWhere(_.name == "id")
    val kPos = scan.output.indexWhere(_.name == "k")
    rowPlan.executeCollect()
      .map(r => (r.getLong(idPos), if (r.isNullAt(kPos)) None else Some(r.getLong(kPos))))
      .toSet
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
        val expected: Set[(Long, Option[Long])] =
          Set((1L, Some(7L)), (2L, Some(7L)), (3L, Some(7L)), (4L, Some(40L)), (5L, Some(50L)))
        assert(collected == expected,
          s"got ${collected.toSeq.sorted}; expected ${expected.toSeq.sorted}")
      }
    }
  }

  test("missing key column with an existence DEFAULT that fails the filter skips the older file") {
    // Mirror image of the previous test: the default does NOT satisfy the predicate, so the older
    // file must be skipped in full while the newer file is still filtered normally.
    withSQLConf(
        SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withEvolvedKeyTable("k BIGINT DEFAULT 7") { table =>
        val collected = collectWithStorageFilterOnKey(
          table, k => GreaterThanOrEqual(k, Literal(30L)))
        val expected: Set[(Long, Option[Long])] = Set((4L, Some(40L)), (5L, Some(50L)))
        assert(collected == expected,
          s"got ${collected.toSeq.sorted}; expected ${expected.toSeq.sorted}")
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
        assert(rejectsNull == Set((4L, Some(40L)), (5L, Some(50L))),
          s"a null-rejecting predicate should drop the older file; got ${rejectsNull.toSeq.sorted}")

        val acceptsNull = collectWithStorageFilterOnKey(
          table, k => Or(IsNull(k), GreaterThanOrEqual(k, Literal(45L))))
        val expected: Set[(Long, Option[Long])] =
          Set((1L, None), (2L, None), (3L, None), (5L, Some(50L)))
        assert(acceptsNull == expected,
          s"a null-accepting predicate should keep the older file; got ${acceptsNull.toSeq.sorted}")
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
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        val keyAttr = scan.output.find(_.name == "k").getOrElse(fail("no k in scan output"))
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(150L))))
        val rowPlan = if (withSF.supportsColumnar) ColumnarToRowExec(withSF) else withSF
        val collected = rowPlan.executeCollect().map(r => (r.getLong(0), r.getLong(2))).toSet
        // Rows were written in ascending k order in a single file, so row_index == k - 1.
        val expected = (150L to 200L).map(k => (k, k - 1)).toSet
        assert(collected == expected,
          s"row_index must be the absolute index in the file; got " +
            s"${collected.toSeq.sorted.take(5)}")
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
        val df = spark.read.parquet(outDir).select("k", "s")
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        val keyAttr = scan.output.find(_.name == "k").getOrElse(fail("no k in scan output"))
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(195L))))
        val rowPlan = if (withSF.supportsColumnar) ColumnarToRowExec(withSF) else withSF
        val collected = rowPlan.executeCollect()
          .map { r =>
            val s = r.getStruct(1, 2)
            (r.getLong(0), s.getInt(0), s.getString(1))
          }.toSet
        val expected = (195L to 200L).map(k => (k, k.toInt, s"s_$k")).toSet
        assert(collected == expected, s"got ${collected.toSeq.sorted}; expected $expected")
      }
    }
  }

  // ----- Planner gates and the lost-filter invariant -----

  test("supportsStorageFilter is what decides, and a subclass answers false") {
    // The planner asks the format rather than testing its class, so the expression shapes and the
    // column types a reader can evaluate stay in its own package. A ParquetFileFormat subclass
    // still answers false: it may customize reading by overriding buildReaderWithPartitionValues,
    // and a scan with storage filters routes through buildReaderWithStorageFilters instead, which
    // would bypass whatever the subclass does.
    val format = new ParquetFileFormat()
    val subclass = new ParquetFileFormat() {}
    val bloom = BloomFilterMightContain(
      Literal.create(null, BinaryType),
      XxHash64(Seq(AttributeReference("k", LongType)()), 42L))
    assert(format.supportsStorageFilter(bloom), "a plain bloom on a long key is supported")
    assert(!subclass.supportsStorageFilter(bloom), "a subclass must not claim support")
    // Not a bloom at all, and a bloom on a type the value copier has no branch for.
    assert(!format.supportsStorageFilter(Literal.TrueLiteral))
    val onVariant = BloomFilterMightContain(
      Literal.create(null, BinaryType),
      XxHash64(Seq(AttributeReference("v", VariantType)()), 42L))
    assert(!format.supportsStorageFilter(onVariant), "VariantType has no primitive Parquet leaf")
    // And the default is no support at all.
    assert(!new NoStorageFilterFileFormat().supportsStorageFilter(bloom))
  }


  test("bloom stays in the post-scan Filter when the vectorized reader is unavailable") {
    // The whole lost-filter safety argument rests on this gate: if the reader cannot do late
    // materialization, the planner must NOT move the bloom out of the post-scan Filter.
    withBloomFilterTables {
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val (plan, _) = runBloomFilterJoin()
        assert(countBloomFiltersInStorageFilters(plan) == 0,
          s"no bloom should be extracted when the vectorized reader is off.\nPlan:\n$plan")
        assert(countBloomFiltersInPostScanFilters(plan) >= 1,
          s"the bloom must remain as a post-scan FilterExec.\nPlan:\n$plan")
      }
    }
  }

  test("a scan with storage filters fails loudly if the vectorized reader is disabled later") {
    // preparedStorageFilters deliberately does not re-check the conf, because by then the bloom is
    // already gone from the post-scan Filter. So a vectorized-reader conf flipped between planning
    // and execution must fail rather than quietly return every row.
    withTempDir { dir =>
      val rows = (1L to 50L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows)
      val withSF = withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        scanWithStorageFilter(path, "k", threshold = 25L)
      }
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val e = intercept[Exception] {
          executePlanCollect(withSF)
        }
        val message = Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)
        assert(message != null && message.contains("Cannot honor storage filters"),
          s"expected a clear storage-filter failure; got: $message")
      }
    }
  }

  test("a file format without storage-filter support rejects a non-empty storageFilters") {
    // The default `FileFormat.buildReaderWithStorageFilters` body must not drop the filters it is
    // handed: extraction has already removed them from the post-scan Filter, so a reader that
    // ignores them returns rows the filter rejects. Only the planner's
    // `getClass == classOf[ParquetFileFormat]` gate keeps this unreachable today, and that gate
    // lives in another file.
    val storageFilters =
      Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = false), Literal(1L)))
    val e = intercept[IllegalArgumentException] {
      new NoStorageFilterFileFormat().buildReaderWithStorageFilters(
        spark, new StructType(), new StructType(), new StructType(), Nil, storageFilters,
        Map.empty, new Configuration())
    }
    assert(e.getMessage.contains("does not support storage-filter pushdown"), e.getMessage)
  }

  Seq(false, true).foreach { aqe =>
    test(s"FileSourceStrategy extraction preserves query results (AQE = $aqe)") {
      // AQE is on by default in production, and it is where the bloom subquery is planned by
      // PlanAdaptiveSubqueries rather than PlanSubqueries -- the path preparedStorageFilters'
      // ScalarSubquery materialization depends on.
      withBloomFilterTables {
        val baseConf = Map(
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString)
        def run(pushdown: Boolean): Set[(Long, Long)] = withSQLConf(
            (baseConf +
              (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> pushdown.toString)).toSeq: _*
          ) {
          runBloomFilterJoin()._2.map(r => (r.getLong(0), r.getLong(1))).toSet
        }
        val off = run(false)
        val on = run(true)
        assert(on == off, s"results differ between conf-on and conf-off: on=$on off=$off")
        assert(on.nonEmpty, "the join should return rows, otherwise this proves nothing")
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
        val withSF = scanWithStorageFilter(path, "k", threshold = 25L)
        val plain = withSF.copy(storageFilters = Nil)
        assert(withSF != plain, "case class equality must take storageFilters into account")
        assert(!withSF.sameResult(plain),
          s"a filtering scan must not be reusable as a plain one:\n${withSF.canonicalized}\n" +
            s"${plain.canonicalized}")
        // A second, independently planned scan of the same file with the same filter still
        // matches, so reuse is not disabled wholesale. Its key attribute carries a different
        // exprId, which is what canonicalization normalizes away.
        val sameSF = scanWithStorageFilter(path, "k", threshold = 25L)
        assert(withSF.sameResult(sameSF),
          s"two scans with the same storage filter must stay reusable:\n" +
            s"${withSF.canonicalized}\n${sameSF.canonicalized}")
        val otherSF = scanWithStorageFilter(path, "k", threshold = 30L)
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
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
      def run(pushdown: Boolean): (Set[(Long, Long)], Int, Boolean) = withSQLConf(
          (baseConf +
            (SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> pushdown.toString)).toSeq: _*
        ) {
        val (plan, result) = runBloomFilterJoin()
        val columnar = plan.collect { case s: FileSourceScanExec => s.supportsColumnar }
        (result.map(r => (r.getLong(0), r.getLong(1))).toSet,
          countBloomFiltersInStorageFilters(plan), columnar.forall(_ == false))
      }
      val (off, _, _) = run(false)
      val (on, storageBlooms, noColumnarScan) = run(true)
      assert(storageBlooms >= 1, s"the bloom must still be extracted with codegen off; " +
        s"got $storageBlooms")
      assert(noColumnarScan, "with codegen off no scan should output columnar batches")
      assert(on == off, s"results differ between conf-on and conf-off: on=$on off=$off")
      assert(on.nonEmpty, "the join should return rows, otherwise this proves nothing")
    }
  }

  test("off-heap column vectors through the planner: spliced values survive the free") {
    // Off-heap is where the vector lifecycle actually bites: the previous batch's key vectors are
    // freed at the next nextBatch(), so a stale reference reads released native memory rather than
    // old bytes. This drives it through the planner, where the batch also crosses
    // ColumnarToRowExec.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, "k", threshold = 150L)
        val collected = executePlanCollect(scan).toSet
        val expected = rows.filter(_._1 >= 150L).toSet
        assert(collected == expected,
          s"got ${collected.size} rows; expected ${expected.size}. " +
            s"first few: ${collected.toSeq.sortBy(_._1).take(3)}")
      }
    }
  }

  // ----- Page-level pushedFilterRanges (a strict subset of the row group) -----

  test("pushed data filter narrows to a page subset: phase 1 stays aligned with the row indexes") {
    // Every other fixture writes one page per column per row group, so column-index filtering can
    // only ever drop whole row groups and `pushedFilterRanges` is always the entire block. That
    // makes the phase 1 alignment -- the r-th row readBatch delivers must pair with
    // rowIndexIter.nextLong() -- hold trivially. With a small page size the data filter narrows
    // to a page subset, so the two sequences only agree if the pairing is actually correct.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        // `v` is correlated with `k`, so a range predicate on v prunes pages, not whole row groups.
        val df = spark.read.parquet(path).select("k", "v").filter("v >= 'v_0300'")
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        assert(scan.simpleString(200).contains("GreaterThanOrEqual(v,"),
          s"the data filter must be pushed: ${scan.simpleString(200)}")
        val keyAttr = scan.output.find(_.name == "k").get
        // Storage filter keeps a band that starts inside the data filter's surviving range.
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(350L))))

        val collected = executePlanCollect(withSF).toSet
        val expected = rows.filter(r => r._2 >= "v_0300" && r._1 >= 350L).toSet
        assert(collected == expected,
          s"got ${collected.size} rows; expected ${expected.size}. " +
            s"first few: ${collected.toSeq.sortBy(_._1).take(3)}")
      }
    }
  }

  test("page-subset ranges keep the avoided-byte metrics non-negative") {
    // The strict-subset branch of compressedBytesForRowRanges (offset index + dictionary page) only
    // runs when pushedFilterRanges is narrower than the block, which needs a multi-page row group.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))

      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).select("k", "v").filter("v >= 'v_0100'")
        val scan = df.queryExecution.executedPlan
          .collect { case s: FileSourceScanExec => s }.head
        val keyAttr = scan.output.find(_.name == "k").get
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(390L))))
        val collected = executePlanCollect(withSF).toSet
        assert(collected == rows.filter(_._1 >= 390L).toSet, s"got ${collected.size} rows")

        val bytesRg = withSF.metrics(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_ROW_GROUP)
        val bytesPf =
          withSF.metrics(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_PAGE_FILTERING)
        assert(bytesRg.value >= 0 && bytesPf.value >= 0,
          s"avoided-byte metrics must never go negative; rg=${bytesRg.value} pf=${bytesPf.value}")
      }
    }
  }

  test("column-index filtering off: phase 0 takes the whole row group") {
    // Phase 0 asks parquet for row ranges only when column-index filtering is on, because
    // ParquetFileReader.getRowRanges checks whether a filter is pushed and NOT whether the user
    // enabled the column index. That branch is the escape hatch for a file whose column index is
    // wrong -- trusting one here would drop rows for good, since the post-scan Filter no longer
    // holds the predicate -- and nothing exercised it.
    //
    // The row accounting is what tells the two arms apart. Everything is scoped to the rows the
    // pushed data filter left, so with the column index on, the rows it prunes at page level never
    // reach phase 1 and are never counted. With it off, every row of the block does, so emitted
    // plus excluded covers the whole file. One row group with many pages keeps statistics-level
    // row-group filtering out of it, which happens either way.
    withTempDir { dir =>
      val rows = (1L to 400L).map(i => (i, f"v_$i%04d"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 64 * 1024L, pageSize = Some(512L))

      def run(columnIndex: Boolean): (Set[(Long, String)], Long) = withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED -> columnIndex.toString) {
        val df = spark.read.parquet(path).select("k", "v").filter("k >= 350")
        val scan = df.queryExecution.executedPlan
          .collect { case s: FileSourceScanExec => s }.head
        assert(scan.simpleString(200).contains("GreaterThanOrEqual(k,"),
          s"the data filter must be pushed for this test to mean anything: " +
            scan.simpleString(200))
        val keyAttr = scan.output.find(_.name == "k").get
        val withSF = scan.copy(storageFilters = Seq(GreaterThanOrEqual(keyAttr, Literal(350L))))
        val collected = executePlanCollect(withSF).toSet
        val accounted = collected.size +
          withSF.metrics(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_BY_ROW_GROUP).value +
          withSF.metrics(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_WITHIN_ROW_GROUP).value
        (collected, accounted)
      }

      val expected = rows.filter(_._1 >= 350L).toSet
      val (rowsOff, accountedOff) = run(columnIndex = false)
      val (rowsOn, accountedOn) = run(columnIndex = true)
      assert(rowsOff == expected,
        s"with the column index off, got ${rowsOff.size} rows; expected ${expected.size}")
      assert(rowsOn == expected,
        s"with the column index on, got ${rowsOn.size} rows; expected ${expected.size}")
      assert(accountedOff == rows.size,
        s"with the column index off every row of the file must be emitted or excluded; " +
          s"accounted $accountedOff of ${rows.size}")
      assert(accountedOn < rows.size,
        s"with the column index on the pruned pages must not reach phase 1; " +
          s"accounted $accountedOn of ${rows.size}")
    }
  }

  Seq(true, false).foreach { pushDataFilter =>
    test("the byte metrics cost no extra IO " +
      s"(pushed data filter narrowing the row group = $pushDataFilter)") {
      // The whole point of the byte metrics is to report IO that did not happen, so they must not
      // cause any. Two arms of the same read, one with all five metrics wired and one with none,
      // over a filesystem that counts every byte a read hands back. `needBytes` is false in the
      // second arm, so it skips the walks entirely, and any difference in bytes read is the walks'.
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
        val schema = StructType(Seq(
          StructField("k", LongType, nullable = true),
          StructField("v", StringType, nullable = true)))
        val storageFilters =
          Seq(GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(350L)))
        val pushedFilters =
          if (pushDataFilter) Seq(sources.GreaterThan("v", "v_0100")) else Nil

        def run(metrics: Map[String, SQLMetric]): (Int, Long) = {
          val hadoopConf = spark.sessionState.newHadoopConf()
          hadoopConf.set(s"fs.${CountingLocalFileSystem.scheme}.impl",
            classOf[CountingLocalFileSystem].getName)
          hadoopConf.setBoolean(s"fs.${CountingLocalFileSystem.scheme}.impl.disable.cache", true)
          val readerFn = new ParquetFileFormat().buildReaderWithStorageFilters(
            spark, schema, new StructType(), schema, pushedFilters, storageFilters,
            Map(FileFormat.OPTION_RETURNING_BATCH -> "true"), hadoopConf, metrics)
          val file = PartitionedFile(
            InternalRow.empty,
            SparkPath.fromUrlString(s"${CountingLocalFileSystem.scheme}://$path"),
            0,
            new File(path).length())
          CountingLocalFileSystem.reset()
          val emitted = readerFn(file).asInstanceOf[Iterator[Object]].map {
            case batch: ColumnarBatch => batch.numRows()
            case _ => 1
          }.sum
          (emitted, CountingLocalFileSystem.bytesRead())
        }

        val wired = Map(
          FileSourceScanLike.STORAGE_FILTER_ROW_GROUPS_SKIPPED ->
            SQLMetrics.createMetric(spark.sparkContext, "rowGroupsSkipped"),
          FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_BY_ROW_GROUP ->
            SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedByRowGroup"),
          FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_WITHIN_ROW_GROUP ->
            SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedWithinRowGroup"),
          FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_ROW_GROUP ->
            SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByRowGroup"),
          FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_PAGE_FILTERING ->
            SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByPageFiltering"))

        val (emittedOff, bytesOff) = run(Map.empty)
        val (emittedOn, bytesOn) = run(wired)

        assert(emittedOn == emittedOff && emittedOn == 51,
          s"both arms must emit keys 350..400; got on=$emittedOn off=$emittedOff")
        assert(bytesOn == bytesOff,
          s"wiring the byte metrics must not read a single extra byte; " +
            s"with metrics $bytesOn, without $bytesOff")
        assert(bytesOff > 0, "the counting filesystem must have seen the read at all")

        // The walks really ran, and on the shape each arm is meant to exercise.
        val accounted = emittedOn +
          wired(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_BY_ROW_GROUP).value +
          wired(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_WITHIN_ROW_GROUP).value
        if (pushDataFilter) {
          assert(accounted < rows.size,
            s"the pushed filter must narrow the ranges below the block, so the offset-index " +
              s"branch is the one measured; accounted $accounted of ${rows.size}")
        } else {
          assert(accounted == rows.size,
            s"with no pushed filter every row reaches phase 1, so the footer branch is the one " +
              s"measured; accounted $accounted of ${rows.size}")
        }
        assert(wired(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_ROW_GROUP).value > 0 ||
          wired(FileSourceScanLike.STORAGE_FILTER_BYTES_AVOIDED_BY_PAGE_FILTERING).value > 0,
          "at least one byte metric must be non-zero, otherwise the walk answered nothing")
      }
    }
  }

  // ----- Metric arithmetic -----

  test("row metrics account for every row of the file") {
    // Ties the three count metrics to the file: whatever is not emitted must have been avoided
    // either by a whole-row-group skip or by page filtering. A sign flip or a mis-scoped schema in
    // the accounting shows up here, which a `>= 0` assertion cannot catch.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, "k", threshold = 150L)
        val emitted = executePlanCollect(scan).length
        val avoidedRg = scan.metrics(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_BY_ROW_GROUP)
        val avoidedPf =
          scan.metrics(FileSourceScanLike.STORAGE_FILTER_ROWS_EXCLUDED_WITHIN_ROW_GROUP)
        assert(emitted == 51, s"expected keys 150..200; got $emitted")
        assert(emitted + avoidedRg.value + avoidedPf.value == rows.size,
          s"emitted ($emitted) + avoided by row group (${avoidedRg.value}) + avoided by page " +
            s"filtering (${avoidedPf.value}) should equal ${rows.size}")
      }
    }
  }

  test("all-key projection: byte metrics are zero and no offset index work is needed") {
    // With every projected column a key, phase 2 never runs, so baseline == phase1 and both byte
    // metrics are 0 by construction. This is the shape the design notes call the biggest win, so it
    // must not be the shape that pays for metrics.
    withTempDir { dir =>
      val path = writeKeyOnlyParquetFileFromSql(dir, "id", n = 200L)
      val bytesRg = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByRg")
      val bytesPf = SQLMetrics.createSizeMetric(spark.sparkContext, "bytesAvoidedByPf")
      val rowsRg = SQLMetrics.createMetric(spark.sparkContext, "rowsExcludedByRowGroup")
      val bound = GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(190L))
      val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested, StorageFilterMetrics(
        rowsExcludedByRowGroup = rowsRg,
        bytesAvoidedByRowGroup = bytesRg,
        bytesAvoidedByPageFiltering = bytesPf))
      val (result, reader) =
        readAllWith(path, Seq("k"), filter, (b, i) => b.column(0).getLong(i))
      try {
        assert(result == (190L to 200L), s"expected keys 190..200; got $result")
        assert(bytesRg.value == 0 && bytesPf.value == 0,
          s"an all-key projection has no non-key bytes to avoid; got rg=${bytesRg.value} " +
            s"pf=${bytesPf.value}")
        assert(rowsRg.value > 0, "row groups should still be skipped, and counted in rows")
      } finally {
        reader.close()
      }
    }
  }

  // ----- Projection order and batch boundaries -----

  test("non-key column before the key column: emit maps queues to the right batch slots") {
    // The emit loop walks batch slots in ascending order and pulls survivor queues in order, so it
    // relies on keyColumnIndices being sorted. Every other test puts the keys in the leading slots,
    // where an off-by-one in that pairing is invisible.
    withTempDir { dir =>
      val outDir = new File(dir, "vk").getAbsolutePath
      spark.range(1, 201).selectExpr("CONCAT('v_', id) AS v", "id AS k", "id * 10 AS w")
        .repartition(1)
        .write
        .option(ParquetOutputFormat.BLOCK_SIZE, 256L)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
        .parquet(outDir)
      val path = new File(outDir).listFiles((_, n) => n.endsWith(".parquet"))(0).getAbsolutePath

      // Key is `k`, at slot 1 of the (v, k, w) projection.
      val bound = GreaterThanOrEqual(BoundReference(1, LongType, nullable = true), Literal(195L))
      val requested = StructType(Seq(
        StructField("v", StringType, nullable = true),
        StructField("k", LongType, nullable = true),
        StructField("w", LongType, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested)
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
    // finalizePartialAccumulators' early return only runs when the last accumulator is exactly
    // full. 64 survivors at capacity 16 hits it; the multi-batch tests use 100, which does not.
    withTempDir { dir =>
      val path = writeKeyOnlyParquetFileFromSql(dir, "id", n = 64L, rowGroupSize = 64 * 1024L)
      val bound = GreaterThanOrEqual(BoundReference(0, LongType, nullable = true), Literal(1L))
      val requested = StructType(Seq(StructField("k", LongType, nullable = true)))
      val filter = ParquetStorageFilter.create(Seq(bound), requested)
      val (result, reader) = readAllWith(
        path, Seq("k"), filter, (b, i) => b.column(0).getLong(i), capacity = 16)
      try {
        assert(result == (1L to 64L), s"expected all 64 keys in order; got ${result.size}")
      } finally {
        reader.close()
      }
    }
  }

  test("early termination: the reader stops without draining the file") {
    // executeTake on a bare ColumnarToRowExec goes through ColumnarToRowEvaluatorFactory, not
    // through the generated code, so nothing closes the batch from outside here. What this covers
    // is abandoning the reader mid-file: the survivor queues still hold vectors, and
    // RecordReaderIterator closes the reader on task completion. The external close is the test
    // below.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, "k", threshold = 50L)
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
    // is the limit check, which needs a limit inside the same codegen stage. So the plan is built
    // with LocalLimitExec and handed to CollapseCodegenStages, and the generated source is asserted
    // to contain the close -- without that, this test would pass for the wrong reason.
    //
    // What it exercises: the spliced batch's columns are closed from outside while the reader is
    // still open, and the reader's own close() then runs over the same vectors.
    withTempDir { dir =>
      val rows = (1L to 200L).map(i => (i, s"v_$i"))
      val path = writeParquetFile(dir, rows, rowGroupSize = 256L)
      withSQLConf(
          SQLConf.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        val scan = scanWithStorageFilter(path, "k", threshold = 50L)
        val planned =
          CollapseCodegenStages().apply(LocalLimitExec(5, ColumnarToRowExec(scan)))
        val stage = planned match {
          case w: WholeStageCodegenExec => w
          case other => fail(s"expected a whole-stage codegen plan, got $other")
        }
        val source = stage.doCodeGen()._2.body
        assert(source.contains(".close();"),
          s"the generated code must close the batch on the limit exit; source:\n$source")

        val limited = stage.executeCollect()
        assert(limited.length == 5, s"expected 5 rows from the limit; got ${limited.length}")
        assert(limited.forall(_.getLong(0) >= 50L),
          s"every row must satisfy the storage filter; got ${limited.map(_.getLong(0)).toSeq}")
      }
    }
  }

  // ----- Partially-missing key columns, end to end through the reader -----

  test("one of two key columns missing from a file: splicing runs with the rewritten predicate") {
    // The most intricate branch of initializeLateMaterialization: splicing engages with a predicate
    // that has one Literal substituted and one BoundReference renumbered, the missing key's field
    // lands among the non-key columns, and its output slot is filled by ParquetColumnVector.
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
        val plan = df.queryExecution.executedPlan
        val scan = plan.collect { case s: FileSourceScanExec => s }.headOption
          .getOrElse(fail(s"No FileSourceScanExec in plan: $plan"))
        val a = scan.output.find(_.name == "a").getOrElse(fail("no a"))
        val b = scan.output.find(_.name == "b").getOrElse(fail("no b"))
        // Two key columns. In the older file `b` is missing and reads as its default 7, so the
        // predicate must be evaluated with 7 substituted for it -- and `a >= 2` still filters.
        val withSF = scan.copy(storageFilters = Seq(
          GreaterThanOrEqual(a, Literal(2L)), GreaterThanOrEqual(b, Literal(5L))))

        val rowPlan = if (withSF.supportsColumnar) ColumnarToRowExec(withSF) else withSF
        // The scan emits its own order (a, c, b here), not the SELECT's (a, b, c), because
        // executing it directly skips the reordering Project. Resolve positions by name.
        val aPos = withSF.output.indexWhere(_.name == "a")
        val bPos = withSF.output.indexWhere(_.name == "b")
        val cPos = withSF.output.indexWhere(_.name == "c")
        val collected = rowPlan.executeCollect()
          .map(r => (r.getLong(aPos), r.getString(cPos), r.getLong(bPos))).toSet
        // Older file: a in {2,3} pass a>=2, and b=7 passes b>=5. Newer file: 40 and 50 both pass.
        val expected = Set((2L, "y", 7L), (3L, "z", 7L), (4L, "p", 40L), (5L, "q", 50L))
        // Compare against the plain read too, so a failure here is unambiguously the storage-filter
        // path rather than a wrong expectation. df.collect() goes through the Project, so it is in
        // the SELECT order (a, b, c).
        val baseline = df.collect().map(r => (r.getLong(0), r.getString(2), r.getLong(1))).toSet
        assert(baseline == expected + ((1L, "x", 7L)),
          s"the plain read is already wrong, so the expectation is: ${baseline.toSeq.sorted}")
        assert(collected == expected,
          s"got ${collected.toSeq.sorted}; expected ${expected.toSeq.sorted}")
      }
    }
  }
}

/**
 * A [[FileFormat]] that does not override `buildReaderWithStorageFilters`, so it exercises the
 * default body's rejection of storage filters it cannot honor.
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
