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

package org.apache.spark.util.kvstore

import java.io.File
import java.nio.file.Files

import scala.concurrent.duration._
import scala.util.Random

import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompressionType, FlushOptions, Options, WriteOptions}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.Utils

/**
 * Benchmark comparing the RocksDB tuning used by the live UI store (block size, bottommost
 * compression, write buffer sizing) against the settings used before that tuning was applied.
 *
 * This drives the raw `org.rocksdb.RocksDB` engine directly with two fixed configurations,
 * "Old config" and "New config", mirroring exactly what
 * `org.apache.spark.util.kvstore.RocksDB` used before and after the change. Those fields are
 * private in that class, so the two configurations are reproduced here by hand and must be kept
 * in sync if that class's settings change again.
 *
 * Each scenario isolates one of the three knobs that changed:
 *
 * - Bulk writes relying on auto-flush: isolates the write buffer size/count change.
 * - Explicit compaction after loading several SST files: isolates bottommost compression
 *   (ZSTD vs LZ4), also reporting on-disk size as the compression-ratio trade-off.
 * - Random point lookups after compaction: isolates the larger block size's decompression cost.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RocksDBConfigBenchmark-results.txt".
 * }}}
 */
object RocksDBConfigBenchmark extends BenchmarkBase {

  org.rocksdb.RocksDB.loadLibrary()

  /** Serialized-record size representative of this store's TaskMetrics/StageInfo entries. */
  private val VALUE_SIZE = 200

  /**
   * Size of the per-record unique portion of a generated value; the rest is a fixed shared
   * template. This mimics a real serialized record (a few varying fields over a mostly
   * repeated structure) well enough to avoid the two extremes that would skew this benchmark:
   * fully-identical values (compress away regardless of codec, hiding the ZSTD/LZ4 ratio
   * difference and overstating the effect of block size) and fully-random values (incompressible
   * under any codec, also hiding the ratio difference).
   */
  private val UNIQUE_PREFIX_SIZE = 160

  /** Fixed seed so "Old config" and "New config" write byte-identical data per run. */
  private val VALUE_SEED = 20261121L

  /** Large enough to force several flushes under the old 64MB default write buffer. */
  private val WRITE_COUNT = 800000

  private val FLUSH_BATCH_COUNT = 50000
  private val FLUSH_BATCHES = 6
  private val LOADED_COUNT = FLUSH_BATCH_COUNT * FLUSH_BATCHES

  private val LOOKUP_COUNT = 20000

  // 10.0D is BloomFilter.DEFAULT_BITS_PER_KEY.
  private val fullFilter = new BloomFilter(10.0D, false)

  private val writeOptions = new WriteOptions().setSync(false)

  private val configs =
    Seq("Old config" -> (() => oldOptions()), "New config" -> (() => newOptions()))

  private def tableFormatConfig(blockSize: Int): BlockBasedTableConfig = {
    new BlockBasedTableConfig()
      .setFilterPolicy(fullFilter)
      .setEnableIndexCompression(false)
      .setIndexBlockRestartInterval(8)
      .setFormatVersion(5)
      .setBlockSize(blockSize)
  }

  /** Settings used before the tuning change: 4KB block, ZSTD bottommost, default write buffer. */
  private def oldOptions(): Options = {
    new Options()
      .setCreateIfMissing(true)
      .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setTableFormatConfig(tableFormatConfig(4 * 1024))
  }

  /** Settings used after the tuning change: 8KB block, LZ4 bottommost, larger write buffer. */
  private def newOptions(): Options = {
    new Options()
      .setCreateIfMissing(true)
      .setBottommostCompressionType(CompressionType.LZ4_COMPRESSION)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setTableFormatConfig(tableFormatConfig(8 * 1024))
      .setWriteBufferSize(128 * 1024 * 1024)
      .setMaxWriteBufferNumber(4)
      .setMinWriteBufferNumberToMerge(2)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Bulk Write (Write Buffer Behavior)") {
      bulkWrite()
    }

    runBenchmark("Bottommost Compaction") {
      compaction()
    }

    runBenchmark("Point Lookups After Compaction") {
      pointLookups()
    }
  }

  private def bulkWrite(): Unit = {
    val benchmark = singleIterationBenchmark("Bulk Write", WRITE_COUNT)
    configs.foreach { case (label, buildOptions) =>
      benchmark.addTimerCase(label) { timer =>
        val (db, path) = openDb(buildOptions())
        try {
          val values = new ValueGenerator(VALUE_SEED)
          timer.startTiming()
          var i = 0
          while (i < WRITE_COUNT) {
            db.put(writeOptions, key(i), values.next())
            i += 1
          }
          timer.stopTiming()
        } finally {
          closeDb(db, path)
        }
      }
    }
    benchmark.run()
  }

  private def compaction(): Unit = {
    val benchmark = singleIterationBenchmark("Compact Range", LOADED_COUNT)
    configs.foreach { case (label, buildOptions) =>
      benchmark.addTimerCase(label) { timer =>
        val (db, path) = openDb(buildOptions())
        try {
          loadWithFlushes(db)
          timer.startTiming()
          db.compactRange()
          timer.stopTiming()
          // scalastyle:off println
          println(s"$label: on-disk size after compaction = ${dirSize(path)} bytes")
          // scalastyle:on println
        } finally {
          closeDb(db, path)
        }
      }
    }
    benchmark.run()
  }

  private def pointLookups(): Unit = {
    val benchmark = singleIterationBenchmark("Random Get", LOOKUP_COUNT)
    configs.foreach { case (label, buildOptions) =>
      benchmark.addTimerCase(label) { timer =>
        val (db, path) = openDb(buildOptions())
        try {
          loadWithFlushes(db)
          db.compactRange()
          val rnd = new Random(42)
          timer.startTiming()
          var i = 0
          while (i < LOOKUP_COUNT) {
            db.get(key(rnd.nextInt(LOADED_COUNT)))
            i += 1
          }
          timer.stopTiming()
        } finally {
          closeDb(db, path)
        }
      }
    }
    benchmark.run()
  }

  /** Writes [[LOADED_COUNT]] records as [[FLUSH_BATCHES]] flushed batches, forcing several SSTs. */
  private def loadWithFlushes(db: org.rocksdb.RocksDB): Unit = {
    val values = new ValueGenerator(VALUE_SEED)
    val flushOptions = new FlushOptions().setWaitForFlush(true)
    var batch = 0
    while (batch < FLUSH_BATCHES) {
      val base = batch * FLUSH_BATCH_COUNT
      var i = 0
      while (i < FLUSH_BATCH_COUNT) {
        db.put(writeOptions, key(base + i), values.next())
        i += 1
      }
      db.flush(flushOptions)
      batch += 1
    }
  }

  /**
   * Produces record values that share a fixed template but differ in a small unique prefix,
   * approximating the redundancy of a real serialized record. See [[UNIQUE_PREFIX_SIZE]].
   */
  private class ValueGenerator(seed: Long) {
    private val template = new Array[Byte](VALUE_SIZE)
    new Random(seed + 1).nextBytes(template)

    private val rnd = new Random(seed)
    private val prefix = new Array[Byte](UNIQUE_PREFIX_SIZE)
    private val value = template.clone()

    def next(): Array[Byte] = {
      rnd.nextBytes(prefix)
      System.arraycopy(prefix, 0, value, 0, UNIQUE_PREFIX_SIZE)
      value
    }
  }

  /** A [[Benchmark]] that runs each case exactly once; these cases are expensive bulk I/O. */
  private def singleIterationBenchmark(name: String, valuesPerIteration: Long): Benchmark = {
    new Benchmark(
      name,
      valuesPerIteration,
      minNumIters = 1,
      warmupTime = 0.seconds,
      minTime = 0.seconds,
      output = output)
  }

  private def openDb(options: Options): (org.rocksdb.RocksDB, File) = {
    val path = Files.createTempDirectory("rocksdb-config-benchmark").toFile
    path.delete()
    (org.rocksdb.RocksDB.open(options, path.toString), path)
  }

  private def closeDb(db: org.rocksdb.RocksDB, path: File): Unit = {
    db.close()
    Utils.deleteRecursively(path)
  }

  private def key(i: Int): Array[Byte] = f"key-$i%09d".getBytes("UTF-8")

  private def dirSize(dir: File): Long = {
    val files = dir.listFiles()
    if (files == null) {
      0L
    } else {
      files.map(f => if (f.isDirectory) dirSize(f) else f.length()).sum
    }
  }
}
