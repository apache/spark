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

package org.apache.spark.tools

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.util.Utils

/**
 * Internal utility for micro-benchmarking shuffle write performance.
 *
 * Writes simulated shuffle output from several threads and records the observed throughput.
 */
object StoragePerfTester {
  def main(args: Array[String]) = {
    /** Total amount of data to generate. Distributed evenly amongst maps and reduce splits. */
    val dataSizeMb = Utils.memoryStringToMb(sys.env.getOrElse("OUTPUT_DATA", "1g"))

    /** Number of map tasks. All tasks execute concurrently. */
    val numMaps = sys.env.get("NUM_MAPS").map(_.toInt).getOrElse(8)

    /** Number of reduce splits for each map task. */
    val numOutputSplits = sys.env.get("NUM_REDUCERS").map(_.toInt).getOrElse(500)

    val recordLength = 1000 // ~1KB records
    val totalRecords = dataSizeMb * 1000
    val recordsPerMap = totalRecords / numMaps

    val writeData = "1" * recordLength
    val executor = Executors.newFixedThreadPool(numMaps)

    System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.shuffle.sync", "true")
    System.setProperty("spark.shuffle.manager",
      "org.apache.spark.shuffle.hash.HashShuffleManager")

    // This is only used to instantiate a BlockManager. All thread scheduling is done manually.
    val sc = new SparkContext("local[4]", "Write Tester")
    val hashShuffleManager = sc.env.shuffleManager.asInstanceOf[HashShuffleManager]

    def writeOutputBytes(mapId: Int, total: AtomicLong) = {
      val shuffle = hashShuffleManager.shuffleBlockManager.forMapTask(1, mapId, numOutputSplits,
        new KryoSerializer(sc.conf), new ShuffleWriteMetrics())
      val writers = shuffle.writers
      for (i <- 1 to recordsPerMap) {
        writers(i % numOutputSplits).write(writeData)
      }
      writers.map { w =>
        w.commitAndClose()
        total.addAndGet(w.fileSegment().length)
      }

      shuffle.releaseWriters(true)
    }

    val start = System.currentTimeMillis()
    val latch = new CountDownLatch(numMaps)
    val totalBytes = new AtomicLong()
    for (task <- 1 to numMaps) {
      executor.submit(new Runnable() {
        override def run() = {
          try {
            writeOutputBytes(task, totalBytes)
            latch.countDown()
          } catch {
            case e: Exception =>
              println("Exception in child thread: " + e + " " + e.getMessage)
              System.exit(1)
          }
        }
      })
    }
    latch.await()
    val end = System.currentTimeMillis()
    val time = (end - start) / 1000.0
    val bytesPerSecond = totalBytes.get() / time
    val bytesPerFile = (totalBytes.get() / (numOutputSplits * numMaps.toDouble)).toLong

    System.err.println("files_total\t\t%s".format(numMaps * numOutputSplits))
    System.err.println("bytes_per_file\t\t%s".format(Utils.bytesToString(bytesPerFile)))
    System.err.println("agg_throughput\t\t%s/s".format(Utils.bytesToString(bytesPerSecond.toLong)))

    executor.shutdown()
    sc.stop()
  }
}
