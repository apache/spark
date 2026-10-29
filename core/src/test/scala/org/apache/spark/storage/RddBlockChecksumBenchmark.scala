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

package org.apache.spark.storage

import java.io.OutputStream

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}

/**
 * Benchmark for the store-time overhead of the local-checkpoint RDD-block content checksum
 * (`spark.checkpoint.local.verifyChecksum.enabled`). The feature folds a JDK checksum
 * over the serialized+compressed plaintext of each cache block as it is written, by inserting a
 * `MutableCheckedOutputStream` into the serialize sink chain
 * (`ser.serializeStream(wrapForCompression(blockId, wrapForChecksum(checksum, sink)))`, exactly as
 * `BlockManager` composes it). This measures the cost of that inserted layer on the block store
 * path: for each data shape it times the exact serialize chain with and without the checksum
 * wrapper, so the delta is the per-block store-time tax the feature adds. A final case times the
 * raw checksum algorithms over a fixed buffer for reference.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RddBlockChecksumBenchmark-results.txt".
 * }}}
 */
object RddBlockChecksumBenchmark extends BenchmarkBase {

  // Number of times each fixed-size block is serialized per iteration, so the reported time is
  // per-block-batch rather than dominated by per-call setup.
  private val numBlocksPerIteration = 200

  // Representative cache-block payloads. `longRows` are compact primitive records; `byteRecords`
  // are wider opaque byte payloads (the shape of a materialized DML source row).
  private def longRows(numRows: Int): Iterator[Long] = (0 until numRows).iterator.map(_.toLong)
  private def byteRecords(numRecords: Int, recordSize: Int): Iterator[Array[Byte]] =
    (0 until numRecords).iterator.map { i =>
      val a = new Array[Byte](recordSize)
      var j = 0
      while (j < recordSize) {
        a(j) = (i + j).toByte
        j += 1
      }
      a
    }

  /**
   * Serialize `values` for `blockId` through the exact store-time chain, optionally inserting the
   * checksum wrapper the feature adds. Mirrors `SerializerManager.blockSerializationStream` and
   * `BlockManager`'s `checksumOpt.map(wrapForChecksum(_, sink)).getOrElse(sink)` composition
   * (compression outside, checksum inside, sink at the bottom). `classTag` + `autoPick = true`
   * select the serializer exactly as the real store path does (a primitive / byte-array block goes
   * to Kryo); values are written as `Any` because the serializer instance, not the static type,
   * drives the work.
   */
  private def serializeBlock(
      serManager: SerializerManager,
      blockId: BlockId,
      values: Iterator[Any],
      withChecksum: Boolean,
      algorithm: String,
      classTag: ClassTag[_]): Unit = {
    val sink: OutputStream = OutputStream.nullOutputStream()
    val checksummed =
      if (withChecksum) {
        serManager.wrapForChecksum(ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm), sink)
      } else {
        sink
      }
    val compressed = serManager.wrapForCompression(blockId, checksummed)
    val ser = serManager.getSerializer(classTag, autoPick = true).newInstance()
    ser.serializeStream(compressed).writeAll(values).close()
  }

  private def newSerializerManager(compressRdds: Boolean): SerializerManager = {
    val conf = new SparkConf(false)
      .set(config.RDD_COMPRESS, compressRdds)
    // Kryo is the serializer DML source materialization uses in practice (autoPick routes
    // primitive / byte-array blocks to it), so match it here. Kryo's FieldSerializer reflects into
    // java.lang.invoke.SerializedLambda, which needs the `--add-opens java.base/java.lang.invoke`
    // JVM flag - supplied by the scala_binary target's jvm_flags (Spark's COMMON_JDK17_FLAGS).
    new SerializerManager(new KryoSerializer(conf), conf)
  }

  private def runStoreOverheadCase(
      caseLabel: String,
      compressRdds: Boolean,
      makeValues: () => Iterator[Any],
      classTag: ClassTag[_]): Unit = {
    val serManager = newSerializerManager(compressRdds)
    val blockId = RDDBlockId(0, 0)
    val algorithm = "CRC32C"
    val benchmark =
      new Benchmark(caseLabel, numBlocksPerIteration.toLong, minNumIters = 5, output = output)
    benchmark.addCase("serialize only (feature off)") { _ =>
      var i = 0
      while (i < numBlocksPerIteration) {
        serializeBlock(serManager, blockId, makeValues(), withChecksum = false, algorithm, classTag)
        i += 1
      }
    }
    benchmark.addCase("serialize + CRC32C checksum (feature on)") { _ =>
      var i = 0
      while (i < numBlocksPerIteration) {
        serializeBlock(serManager, blockId, makeValues(), withChecksum = true, algorithm, classTag)
        i += 1
      }
    }
    // relativeTime shows the checksum case as a multiple of the serialize-only baseline, i.e. the
    // proportional store-time overhead the feature adds.
    benchmark.run(relativeTime = true)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("RDD block store-time checksum overhead") {
      // 64k long rows (~512 KiB serialized) - a compact-record block.
      runStoreOverheadCase(
        "64k long rows, spark.rdd.compress=false",
        compressRdds = false,
        makeValues = () => longRows(64 * 1024),
        classTag = implicitly[ClassTag[Long]])
      runStoreOverheadCase(
        "64k long rows, spark.rdd.compress=true",
        compressRdds = true,
        makeValues = () => longRows(64 * 1024),
        classTag = implicitly[ClassTag[Long]])
      // 8k x 128-byte records (~1 MiB serialized) - a wider-record block.
      runStoreOverheadCase(
        "8k x 128B records, spark.rdd.compress=false",
        compressRdds = false,
        makeValues = () => byteRecords(8 * 1024, 128),
        classTag = implicitly[ClassTag[Array[Byte]]])
      runStoreOverheadCase(
        "8k x 128B records, spark.rdd.compress=true",
        compressRdds = true,
        makeValues = () => byteRecords(8 * 1024, 128),
        classTag = implicitly[ClassTag[Array[Byte]]])
    }

    runBenchmark("Raw checksum algorithm over 4 MiB (reference)") {
      val data: Array[Byte] = (0 until 4 * 1024 * 1024).map(_.toByte).toArray
      val n = 256
      val benchmark =
        new Benchmark("Checksum algorithm", n.toLong, minNumIters = 5, output = output)
      Seq("ADLER32", "CRC32", "CRC32C").foreach { algorithm =>
        benchmark.addCase(algorithm) { _ =>
          var i = 0
          while (i < n) {
            val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
            checksum.update(data, 0, data.length)
            i += 1
          }
        }
      }
      benchmark.run()
    }
  }
}
