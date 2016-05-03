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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.sql.types.{BinaryType, IntegerType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Benchmark
import org.apache.spark.util.collection.BitSet

/**
 * Benchmark to low level memory access using different ways to manage buffers.
 */
object ColumnarBatchBenchmark {

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
      val col = ColumnVector.allocate(count, IntegerType, MemoryMode.ON_HEAP)
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
      val col = ColumnVector.allocate(count, IntegerType, MemoryMode.OFF_HEAP)
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
      val col = ColumnVector.allocate(count, IntegerType, MemoryMode.OFF_HEAP)
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
      val col = ColumnVector.allocate(count, IntegerType, MemoryMode.ON_HEAP)
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

    /*
    Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
    Int Read/Write:              Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------
    Java Array                          248.8          1317.04         1.00 X
    ByteBuffer Unsafe                   435.6           752.25         0.57 X
    ByteBuffer API                     1752.0           187.03         0.14 X
    DirectByteBuffer                    595.4           550.35         0.42 X
    Unsafe Buffer                       235.2          1393.20         1.06 X
    Column(on heap)                     189.8          1726.45         1.31 X
    Column(off heap)                    408.4           802.35         0.61 X
    Column(off heap direct)             237.6          1379.12         1.05 X
    UnsafeRow (on heap)                 414.6           790.35         0.60 X
    UnsafeRow (off heap)                487.2           672.58         0.51 X
    Column On Heap Append               530.1           618.14         0.59 X
    */
    val benchmark = new Benchmark("Int Read/Write", count * iters)
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
    val benchmark = new Benchmark("Boolean Read/Write", iters * count)
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
          if (i % 2 == 0) b(i) = 1;
          i += 1
        }
        i = 0
        while (i < count) {
          if (b(i) == 1) sum += 1
          i += 1
        }
      }
    }}
    /*
    Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
    Boolean Read/Write:          Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------
    Bitset                             895.88           374.54         1.00 X
    Byte Array                         578.96           579.56         1.55 X
    */
    benchmark.run()
  }

  def stringAccess(iters: Long): Unit = {
    val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    val random = new Random(0)

    def randomString(min: Int, max: Int): String = {
      val len = random.nextInt(max - min) + min
      val sb = new StringBuilder(len)
      var i = 0
      while (i < len) {
        sb.append(chars.charAt(random.nextInt(chars.length())));
        i += 1
      }
      return sb.toString
    }

    val minString = 3
    val maxString = 32
    val count = 4 * 1000

    val data = Seq.fill(count)(randomString(minString, maxString))
      .map(_.getBytes(StandardCharsets.UTF_8)).toArray

    def column(memoryMode: MemoryMode) = { i: Int =>
      val column = ColumnVector.allocate(count, BinaryType, memoryMode)
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

    /*
    String Read/Write:                       Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------------
    On Heap                                         457.0            35.85         1.00 X
    Off Heap                                       1206.0            13.59         0.38 X
    */
    val benchmark = new Benchmark("String Read/Write", count * iters)
    benchmark.addCase("On Heap")(column(MemoryMode.ON_HEAP))
    benchmark.addCase("Off Heap")(column(MemoryMode.OFF_HEAP))
    benchmark.run
  }

  def main(args: Array[String]): Unit = {
    intAccess(1024 * 40)
    booleanAccess(1024 * 40)
    stringAccess(1024 * 4)
  }
}
