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

package org.apache.spark.unsafe

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for o.a.s.unsafe.Platform.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/PlatformBenchmark-results.txt".
 * }}}
 */
object PlatformBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val count4k = 4 * 1024 // 4k elements
    val str4k = "4k"
    val count1m = 1024 * 1024 // 1M elements
    val count16k = 16 * 1024 // 8k elements
    val str16k = "16k"
    val count256k = 256 * 1024 // 256k elements
    val str256k = "256k"
    val str1m = "1m"
    val count4m = 4 * 1024 * 1024 // 4M elements
    val str8m = "8m"
    val count16m = 16 * 1024 * 1024 // 16M elements
    val str32m = "32m"
    val iterations = 100000000L // 100M ops

    runByteAccess(count8m, iterations)
    runShortAccess(count8m, iterations)
    runIntAccess(count8m, iterations)
    runLongAccess(count8m, iterations)
    runFloatAccess(count8m, iterations)
    runDoubleAccess(count8m, iterations)
    runBooleanAccess(count8m, iterations)

    val counts = Seq((count4k, str4k), (count16k, str16k), (count256k, str256k),
      (count1m, str1m), (count8m, str8m), (count32m, str32m))
    counts.foreach { case (count, str) =>
      runBulkOperations(count, str)
    }
    counts.foreach { case (count, str) =>
      runMemoryAllocation(count, str)
    }
  }

  private def runByteAccess(count: Long, iterations: Long): Unit = {
    val size = count * 1
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Byte](count.toInt)
    val onHeapOffset = Platform.BYTE_ARRAY_OFFSET
    try {
      runBenchmark("Platform Byte Access") {
        val benchmark = new Benchmark("Byte Access", iterations, output = output)

        benchmark.addCase("putByte: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putByte(onHeapArray, onHeapOffset + (i & mask), i.toByte)
            i += 1
          }
        }

        benchmark.addCase("putByte: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putByte(null, address + (i & mask), i.toByte)
            i += 1
          }
        }

        benchmark.addCase("getByte: On-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getByte(onHeapArray, onHeapOffset + (i & mask))
            i += 1
          }
        }

        benchmark.addCase("getByte: Off-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getByte(null, address + (i & mask))
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runShortAccess(count: Long, iterations: Long): Unit = {
    val size = count * 2
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Short](count.toInt)
    val onHeapOffset = Platform.SHORT_ARRAY_OFFSET
    try {
      runBenchmark("Platform Short Access") {
        val benchmark = new Benchmark("Short Access", iterations, output = output)

        benchmark.addCase("putShort: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putShort(onHeapArray, onHeapOffset + (i & mask) * 2, i.toShort)
            i += 1
          }
        }

        benchmark.addCase("putShort: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putShort(null, address + (i & mask) * 2, i.toShort)
            i += 1
          }
        }

        benchmark.addCase("getShort: On-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getShort(onHeapArray, onHeapOffset + (i & mask) * 2)
            i += 1
          }
        }

        benchmark.addCase("getShort: Off-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getShort(null, address + (i & mask) * 2)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runIntAccess(count: Long, iterations: Long): Unit = {
    val size = count * 4
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Int](count.toInt)
    val onHeapOffset = Platform.INT_ARRAY_OFFSET
    try {
      runBenchmark("Platform Int Access") {
        val benchmark = new Benchmark("Int Access", iterations, output = output)

        benchmark.addCase("putInt: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putInt(onHeapArray, onHeapOffset + (i & mask) * 4, i.toInt)
            i += 1
          }
        }

        benchmark.addCase("putInt: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putInt(null, address + (i & mask) * 4, i.toInt)
            i += 1
          }
        }

        benchmark.addCase("getInt: On-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getInt(onHeapArray, onHeapOffset + (i & mask) * 4)
            i += 1
          }
        }

        benchmark.addCase("getInt: Off-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getInt(null, address + (i & mask) * 4)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runLongAccess(count: Long, iterations: Long): Unit = {
    val size = count * 8
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Long](count.toInt)
    val onHeapOffset = Platform.LONG_ARRAY_OFFSET
    try {
      runBenchmark("Platform Long Access") {
        val benchmark = new Benchmark("Long Access", iterations, output = output)

        benchmark.addCase("putLong: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putLong(onHeapArray, onHeapOffset + (i & mask) * 8, i)
            i += 1
          }
        }

        benchmark.addCase("putLong: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putLong(null, address + (i & mask) * 8, i)
            i += 1
          }
        }

        benchmark.addCase("getLong: On-heap") { _ =>
          var i = 0L
          var sum = 0L
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getLong(onHeapArray, onHeapOffset + (i & mask) * 8)
            i += 1
          }
        }

        benchmark.addCase("getLong: Off-heap") { _ =>
          var i = 0L
          var sum = 0L
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getLong(null, address + (i & mask) * 8)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runFloatAccess(count: Long, iterations: Long): Unit = {
    val size = count * 4
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Float](count.toInt)
    val onHeapOffset = Platform.FLOAT_ARRAY_OFFSET
    try {
      runBenchmark("Platform Float Access") {
        val benchmark = new Benchmark("Float Access", iterations, output = output)

        benchmark.addCase("putFloat: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putFloat(onHeapArray, onHeapOffset + (i & mask) * 4, i.toFloat)
            i += 1
          }
        }

        benchmark.addCase("putFloat: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putFloat(null, address + (i & mask) * 4, i.toFloat)
            i += 1
          }
        }

        benchmark.addCase("getFloat: On-heap") { _ =>
          var i = 0L
          var sum = 0.0f
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getFloat(onHeapArray, onHeapOffset + (i & mask) * 4)
            i += 1
          }
        }

        benchmark.addCase("getFloat: Off-heap") { _ =>
          var i = 0L
          var sum = 0.0f
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getFloat(null, address + (i & mask) * 4)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runDoubleAccess(count: Long, iterations: Long): Unit = {
    val size = count * 8
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Double](count.toInt)
    val onHeapOffset = Platform.DOUBLE_ARRAY_OFFSET
    try {
      runBenchmark("Platform Double Access") {
        val benchmark = new Benchmark("Double Access", iterations, output = output)

        benchmark.addCase("putDouble: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putDouble(onHeapArray, onHeapOffset + (i & mask) * 8, i.toDouble)
            i += 1
          }
        }

        benchmark.addCase("putDouble: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putDouble(null, address + (i & mask) * 8, i.toDouble)
            i += 1
          }
        }

        benchmark.addCase("getDouble: On-heap") { _ =>
          var i = 0L
          var sum = 0.0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getDouble(onHeapArray, onHeapOffset + (i & mask) * 8)
            i += 1
          }
        }

        benchmark.addCase("getDouble: Off-heap") { _ =>
          var i = 0L
          var sum = 0.0
          val mask = count - 1
          while (i < iterations) {
            sum += Platform.getDouble(null, address + (i & mask) * 8)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runBooleanAccess(count: Long, iterations: Long): Unit = {
    val size = count * 1
    val address = Platform.allocateMemory(size)
    val onHeapArray = new Array[Boolean](count.toInt)
    val onHeapOffset = Platform.BOOLEAN_ARRAY_OFFSET
    try {
      runBenchmark("Platform Boolean Access") {
        val benchmark = new Benchmark("Boolean Access", iterations, output = output)

        benchmark.addCase("putBoolean: On-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putBoolean(onHeapArray, onHeapOffset + (i & mask), (i & 1) == 0)
            i += 1
          }
        }

        benchmark.addCase("putBoolean: Off-heap") { _ =>
          var i = 0L
          val mask = count - 1
          while (i < iterations) {
            Platform.putBoolean(null, address + (i & mask), (i & 1) == 0)
            i += 1
          }
        }

        benchmark.addCase("getBoolean: On-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            if (Platform.getBoolean(onHeapArray, onHeapOffset + (i & mask))) {
              sum += 1
            }
            i += 1
          }
        }

        benchmark.addCase("getBoolean: Off-heap") { _ =>
          var i = 0L
          var sum = 0
          val mask = count - 1
          while (i < iterations) {
            if (Platform.getBoolean(null, address + (i & mask))) {
              sum += 1
            }
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runBulkOperations(count: Long, countStr: String): Unit = {
    val size = count * 4
    val address = Platform.allocateMemory(size)
    val srcArray = new Array[Int](count.toInt)
    val dstArray = new Array[Int](count.toInt)
    val srcByteArray = new Array[Byte](size.toInt)
    var j = 0
    while (j < count) {
      srcArray(j) = j.toInt
      j += 1
    }
    Platform.copyMemory(srcArray, Platform.INT_ARRAY_OFFSET, srcByteArray,
      Platform.BYTE_ARRAY_OFFSET, size)
    try {
      runBenchmark(s"Platform Bulk Operations $countStr Ints") {
        val benchmark = new Benchmark(s"Bulk Operations $countStr Ints", 10, output = output)

        benchmark.addCase("copyMemory: Off-heap -> Off-heap") { _ =>
          val dst = Platform.allocateMemory(size)
          var i = 0L
          while (i < 10) {
            Platform.copyMemory(null, address, null, dst, size)
            i += 1
          }
          Platform.freeMemory(dst)
        }

        benchmark.addCase("copyMemory: Heap -> Off-heap") { _ =>
          var i = 0L
          while (i < 10) {
            Platform.copyMemory(srcArray, Platform.INT_ARRAY_OFFSET, null, address, size)
            i += 1
          }
        }

        benchmark.addCase("copyMemory: Off-heap -> Heap") { _ =>
          var i = 0L
          while (i < 10) {
            Platform.copyMemory(null, address, dstArray, Platform.INT_ARRAY_OFFSET, size)
            i += 1
          }
        }

        benchmark.addCase("copyMemory: Heap -> Heap") { _ =>
          var i = 0L
          while (i < 10) {
            Platform.copyMemory(
              srcByteArray, Platform.BYTE_ARRAY_OFFSET, dstArray, Platform.INT_ARRAY_OFFSET, size)
            i += 1
          }
        }

        benchmark.addCase("manual: Heap -> Heap") { _ =>
          var i = 0L
          while (i < 10) {
            PlatformBenchmarkTestUtils.copyToIntArrayManual(
              srcArray, Platform.INT_ARRAY_OFFSET, dstArray, count.toInt)
            i += 1
          }
        }

        benchmark.addCase("manual: Off-heap -> Heap") { _ =>
          var i = 0L
          while (i < 10) {
            PlatformBenchmarkTestUtils.copyToIntArrayManual(
              null, address, dstArray, count.toInt)
            i += 1
          }
        }

        benchmark.addCase("setMemory: Off-heap") { _ =>
          var i = 0L
          while (i < 10) {
            Platform.setMemory(address, 0, size)
            i += 1
          }
        }
        benchmark.run()
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  private def runMemoryAllocation(count: Int, countStr: String): Unit = {
    val size = count * 4
    runBenchmark(s"Platform Memory Allocation $countStr Ints") {
      val benchmark = new Benchmark(s"Memory Allocation $countStr Ints", 2000, output = output)

      benchmark.addTimerCase(s"allocateMemory") { timer =>
        timer.startTiming()
        val addr = Platform.allocateMemory(size)
        timer.stopTiming()
        Platform.freeMemory(addr)
      }

      benchmark.addTimerCase(s"freeMemory") { timer =>
        val addr = Platform.allocateMemory(size)
        timer.startTiming()
        Platform.freeMemory(addr)
        timer.stopTiming()
      }

      benchmark.addTimerCase(s"reallocateMemory: double in size") { timer =>
        var addr = Platform.allocateMemory(size)
        timer.startTiming()
        addr = Platform.reallocateMemory(addr, size, size * 2)
        timer.stopTiming()
        Platform.freeMemory(addr)
      }
      benchmark.run()
    }
  }
}
