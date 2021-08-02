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

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.config
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.collection.{PartitionedPairBuffer, WritablePartitionedIterator}

/**
 * Benchmark for `close` vs `revertPartialWritesAndClose` method in `DiskBlockObjectWriter`.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to
 *      "benchmarks/DiskBlockObjectWriterCloseMethodBenchmark-results.txt".
 * }}}
 */
object DiskBlockObjectWriterCloseMethodBenchmark extends BenchmarkBase {

  val sc = new SparkContext(master = "local", appName = "test")

  private[this] def spillWithClose(
      iterator: WritablePartitionedIterator[Int, Int],
      blockManager: BlockManager,
      serInstance: SerializerInstance,
      fileBufferSize: Int,
      serializerBatchSize: Long,
      revertAndClose: Boolean): Unit = {
    val (blockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()

    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    def flush(): Unit = {
      writer.commitAndGet()
      objectsWritten = 0
    }

    while (iterator.hasNext) {
      iterator.writeNext(writer)
      objectsWritten += 1

      if (objectsWritten == serializerBatchSize) {
        flush()
      }
    }
    if (objectsWritten > 0) {
      flush()
      writer.close()
    } else if (revertAndClose) {
      writer.revertPartialWritesAndClose()
    } else {
      writer.close()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val blockManager = SparkEnv.get.blockManager
    val serInstance = SparkEnv.get.serializer.newInstance()
    val conf = SparkEnv.get.conf
    val bufferSize = conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024
    val batchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE)
    val numIters = 3


    runBenchmark("emptyDataBuffer") {
      val collection = new PartitionedPairBuffer[Int, Int]
      val iter = collection.destructiveSortedWritablePartitionedIterator(None)
      val benchmark = new Benchmark("emptyDataBuffer", 1, output = output)
      benchmark.addCase("use close", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = false)
      }
      benchmark.addCase("use revertPartialWritesAndClose", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = true)
      }
      benchmark.run()
    }

    runBenchmark("batchSize * 2") {
      val collection = new PartitionedPairBuffer[Int, Int]
      (0 until (batchSize * 2).toInt).foreach(i => collection.insert(0, 0, i))
      val iter = collection.destructiveSortedWritablePartitionedIterator(None)
      val benchmark = new Benchmark("batchSize * 2", 1, output = output)
      benchmark.addCase("use close", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = false)
      }
      benchmark.addCase("use revertPartialWritesAndClose", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = true)
      }
      benchmark.run()
    }

    runBenchmark("batchSize * 5") {
      val collection = new PartitionedPairBuffer[Int, Int]
      (0 until (batchSize * 5).toInt).foreach(i => collection.insert(0, 0, i))
      val iter = collection.destructiveSortedWritablePartitionedIterator(None)
      val benchmark = new Benchmark("batchSize * 5", 1, output = output)
      benchmark.addCase("use close", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = false)
      }
      benchmark.addCase("use revertPartialWritesAndClose", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = true)
      }
      benchmark.run()
    }

    runBenchmark("batchSize * 10") {
      val collection = new PartitionedPairBuffer[Int, Int]
      (0 until (batchSize * 5).toInt).foreach(i => collection.insert(0, 0, i))
      val iter = collection.destructiveSortedWritablePartitionedIterator(None)
      val benchmark = new Benchmark("batchSize * 10", 1, output = output)
      benchmark.addCase("use close", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = false)
      }
      benchmark.addCase("use revertPartialWritesAndClose", numIters) { _ =>
        spillWithClose(
          iter, blockManager, serInstance, bufferSize, batchSize, revertAndClose = true)
      }
      benchmark.run()
    }
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }
}
