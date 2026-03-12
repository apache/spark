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
package org.apache.spark.sql.execution.vectorized

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet

/**
 * Benchmark to low level memory access using different ways to manage buffers.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ColumnarBatchBenchmark-results.txt".
 * }}}
 */
object ColumnarBatchBenchmark extends BenchmarkBase {
  // This benchmark reads and writes an array of ints.
  // TODO: there is a big (2x) penalty for a random access API for off heap.
  // Note: carefully if modifying this code. It's hard to reason about the JIT.
  def intAccess(iters: Long): Unit = {
    val count = 8 * 1000

    // Accessing a java array.
    val javaArray = { i: Int =>
      val data = new Array[Int](count)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data(i) = i
          i += 1
        }
        i = 0
        while (i < count) {
          sum += data(i)
          i += 1
        }
      }
    }

    // Accessing ByteBuffers
    val byteBufferUnsafe = { i: Int =>
      val data = ByteBuffer.allocate(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          Platform.putInt(data.array(), Platform.BYTE_ARRAY_OFFSET + i * 4, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += Platform.getInt(data.array(), Platform.BYTE_ARRAY_OFFSET + i * 4)
          i += 1
        }
      }
    }

    // Accessing offheap byte buffers
    val directByteBuffer = { i: Int =>
      val data = ByteBuffer.allocateDirect(count * 4).asIntBuffer()
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data.put(i)
          i += 1
        }
        data.rewind()
        i = 0
        while (i < count) {
          sum += data.get()
          i += 1
        }
        data.rewind()
      }
    }

    // Accessing ByteBuffer using the typed APIs
    val byteBufferApi = { i: Int =>
      val data = ByteBuffer.allocate(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data.putInt(i)
          i += 1
        }
        data.rewind()
        i = 0
        while (i < count) {
          sum += data.getInt()
          i += 1
        }
        data.rewind()
      }
    }

    // Using unsafe memory
    val unsafeBuffer = { i: Int =>
      val data: Long = Platform.allocateMemory(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var ptr = data
        var i = 0
        while (i < count) {
          Platform.putInt(null, ptr, i)
          ptr += 4
          i += 1
        }
        ptr = data
        i = 0
        while (i < count) {
          sum += Platform.getInt(null, ptr)
          ptr += 4
          i += 1
        }
      }
    }

    // Access through the column API with on heap memory
    val columnOnHeap = { i: Int =>
      val col = new OnHeapColumnVector(count, IntegerType)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          col.putInt(i, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += col.getInt(i)
          i += 1
        }
      }
      col.close
    }

    // Access through the column API with off heap memory
    def columnOffHeap = { i: Int => {
      val col = new OffHeapColumnVector(count, IntegerType)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          col.putInt(i, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += col.getInt(i)
          i += 1
        }
      }
      col.close
    }}

    // Access by directly getting the buffer backing the column.
    val columnOffheapDirect = { i: Int =>
      val col = new OffHeapColumnVector(count, IntegerType)
      var sum = 0L
      for (n <- 0L until iters) {
        var addr = col.valuesNativeAddress()
        var i = 0
        while (i < count) {
          Platform.putInt(null, addr, i)
          addr += 4
          i += 1
        }
        i = 0
        addr = col.valuesNativeAddress()
        while (i < count) {
          sum += Platform.getInt(null, addr)
          addr += 4
          i += 1
        }
      }
      col.close
    }

    // Access by going through a batch of unsafe rows.
    val unsafeRowOnheap = { i: Int =>
      val buffer = new Array[Byte](count * 16)
      var sum = 0L
      for (n <- 0L until iters) {
        val row = new UnsafeRow(1)
        var i = 0
        while (i < count) {
          row.pointTo(buffer, Platform.BYTE_ARRAY_OFFSET + i * 16, 16)
          row.setInt(0, i)
          i += 1
        }
        i = 0
        while (i < count) {
          row.pointTo(buffer, Platform.BYTE_ARRAY_OFFSET + i * 16, 16)
          sum += row.getInt(0)
          i += 1
        }
      }
    }

    // Access by going through a batch of unsafe rows.
    val unsafeRowOffheap = { i: Int =>
      val buffer = Platform.allocateMemory(count * 16)
      var sum = 0L
      for (n <- 0L until iters) {
        val row = new UnsafeRow(1)
        var i = 0
        while (i < count) {
          row.pointTo(null, buffer + i * 16, 16)
          row.setInt(0, i)
          i += 1
        }
        i = 0
        while (i < count) {
          row.pointTo(null, buffer + i * 16, 16)
          sum += row.getInt(0)
          i += 1
        }
      }
      Platform.freeMemory(buffer)
    }

    // Adding values by appending, instead of putting.
    val onHeapAppend = { i: Int =>
      val col = new OnHeapColumnVector(count, IntegerType)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          col.appendInt(i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += col.getInt(i)
          i += 1
        }
        col.reset()
      }
      col.close
    }

    val benchmark = new Benchmark("Int Read/Write", count * iters, output = output)
    benchmark.addCase("Java Array")(javaArray)
    benchmark.addCase("ByteBuffer Unsafe")(byteBufferUnsafe)
    benchmark.addCase("ByteBuffer API")(byteBufferApi)
    benchmark.addCase("DirectByteBuffer")(directByteBuffer)
    benchmark.addCase("Unsafe Buffer")(unsafeBuffer)
    benchmark.addCase("Column(on heap)")(columnOnHeap)
    benchmark.addCase("Column(off heap)")(columnOffHeap)
    benchmark.addCase("Column(off heap direct)")(columnOffheapDirect)
    benchmark.addCase("UnsafeRow (on heap)")(unsafeRowOnheap)
    benchmark.addCase("UnsafeRow (off heap)")(unsafeRowOffheap)
    benchmark.addCase("Column On Heap Append")(onHeapAppend)
    benchmark.run()
  }

  def booleanAccess(iters: Int): Unit = {
    val count = 8 * 1024
    val benchmark = new Benchmark("Boolean Read/Write", iters * count.toLong, output = output)
    benchmark.addCase("Bitset") { i: Int => {
      val b = new BitSet(count)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          if (i % 2 == 0) b.set(i)
          i += 1
        }
        i = 0
        while (i < count) {
          if (b.get(i)) sum += 1
          i += 1
        }
      }
    }}

    benchmark.addCase("Byte Array") { i: Int => {
      val b = new Array[Byte](count)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          if (i % 2 == 0) b(i) = 1
          i += 1
        }
        i = 0
        while (i < count) {
          if (b(i) == 1) sum += 1
          i += 1
        }
      }
    }}
    benchmark.run()
  }

  def stringAccess(iters: Long): Unit = {
    val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    val random = new Random(0)

    def randomString(min: Int, max: Int): String = {
      val len = random.nextInt(max - min) + min
      val sb = new StringBuilder(len)
      var i = 0
      while (i < len) {
        sb.append(chars.charAt(random.nextInt(chars.length())))
        i += 1
      }
      sb.toString
    }

    val minString = 3
    val maxString = 32
    val count = 4 * 1000

    val data = Seq.fill(count)(randomString(minString, maxString))
      .map(_.getBytes(StandardCharsets.UTF_8)).toArray

    def column(memoryMode: MemoryMode) = { i: Int =>
      val column = if (memoryMode == MemoryMode.OFF_HEAP) {
        new OffHeapColumnVector(count, BinaryType)
      } else {
        new OnHeapColumnVector(count, BinaryType)
      }

      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          column.putByteArray(i, data(i))
          i += 1
        }
        i = 0
        while (i < count) {
          sum += column.getUTF8String(i).numBytes()
          i += 1
        }
        column.reset()
      }
    }

    val benchmark = new Benchmark("String Read/Write", count * iters, output = output)
    benchmark.addCase("On Heap")(column(MemoryMode.ON_HEAP))
    benchmark.addCase("Off Heap")(column(MemoryMode.OFF_HEAP))
    benchmark.run()
  }

  def arrayAccess(iters: Int): Unit = {
    val random = new Random(0)
    val count = 4 * 1000

    val onHeapVector = new OnHeapColumnVector(count, ArrayType(IntegerType))
    val offHeapVector = new OffHeapColumnVector(count, ArrayType(IntegerType))

    val minSize = 3
    val maxSize = 32
    var arraysCount = 0
    var elementsCount = 0
    while (arraysCount < count) {
      val size = random.nextInt(maxSize - minSize) + minSize
      val onHeapArrayData = onHeapVector.arrayData()
      val offHeapArrayData = offHeapVector.arrayData()

      var i = 0
      while (i < size) {
        val value = random.nextInt()
        onHeapArrayData.appendInt(value)
        offHeapArrayData.appendInt(value)
        i += 1
      }

      onHeapVector.putArray(arraysCount, elementsCount, size)
      offHeapVector.putArray(arraysCount, elementsCount, size)
      elementsCount += size
      arraysCount += 1
    }

    def readArrays(onHeap: Boolean): Unit = {
      val vector = if (onHeap) onHeapVector else offHeapVector

      var sum = 0L
      for (_ <- 0 until iters) {
        var i = 0
        while (i < count) {
          sum += vector.getArray(i).numElements()
          i += 1
        }
      }
    }

    def readArrayElements(onHeap: Boolean): Unit = {
      val vector = if (onHeap) onHeapVector else offHeapVector

      var sum = 0L
      for (_ <- 0 until iters) {
        var i = 0
        while (i < count) {
          val array = vector.getArray(i)
          val size = array.numElements()
          var j = 0
          while (j < size) {
            sum += array.getInt(j)
            j += 1
          }
          i += 1
        }
      }
    }

    val benchmark = new Benchmark("Array Vector Read", count * iters, output = output)
    benchmark.addCase("On Heap Read Size Only") { _ => readArrays(true) }
    benchmark.addCase("Off Heap Read Size Only") { _ => readArrays(false) }
    benchmark.addCase("On Heap Read Elements") { _ => readArrayElements(true) }
    benchmark.addCase("Off Heap Read Elements") { _ => readArrayElements(false) }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Int Read/Write") {
      intAccess(1024 * 40)
    }
    runBenchmark("Boolean Read/Write") {
      booleanAccess(1024 * 40)
    }
    runBenchmark("String Read/Write") {
      stringAccess(1024 * 4)
    }
    runBenchmark("Array Vector Read") {
      arrayAccess(1024 * 40)
    }
  }
}
