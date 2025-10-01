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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector

object ColumnVectorBenchmark extends BenchmarkBase {
  private def populateSingleValue(
      col: WritableColumnVector, batchSize: Int, row: InternalRow, fieldIdx: Int): Unit = {
    col.dataType() match {
      case IntegerType => col.putInts(0, batchSize, row.getInt(fieldIdx))
      case LongType => col.putLongs(0, batchSize, row.getLong(fieldIdx))
      case FloatType => col.putFloats(0, batchSize, row.getFloat(fieldIdx))
      case DoubleType => col.putDoubles(0, batchSize, row.getDouble(fieldIdx))
      case _ =>
    }
  }

  private def populateFromArray(
      col: WritableColumnVector, batchSize: Int, dataType: DataType): Unit = {
    dataType match {
      case IntegerType =>
        val src = Array.tabulate(batchSize)(i => i)
        col.putInts(0, batchSize, src, 0)
      case LongType =>
        val src = Array.tabulate(batchSize)(i => i.toLong)
        col.putLongs(0, batchSize, src, 0)
      case FloatType =>
        val src = Array.tabulate(batchSize)(i => i.toFloat)
        col.putFloats(0, batchSize, src, 0)
      case DoubleType =>
        val src = Array.tabulate(batchSize)(i => i.toDouble)
        col.putDoubles(0, batchSize, src, 0)
      case _ =>
    }
  }

  private def populateFromByteArrayLE(
      col: WritableColumnVector, batchSize: Int, dataType: DataType): Unit = {
    dataType match {
      case IntegerType =>
        val buf = java.nio.ByteBuffer.allocate(
          batchSize * 4).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < batchSize) { buf.putInt(i * 4, i); i += 1 }
        col.putIntsLittleEndian(0, batchSize, buf.array(), 0)
      case LongType =>
        val buf = java.nio.ByteBuffer.allocate(
          batchSize * 8).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < batchSize) { buf.putLong(i * 8, i.toLong); i += 1 }
        col.putLongsLittleEndian(0, batchSize, buf.array(), 0)
      case FloatType =>
        val buf = java.nio.ByteBuffer.allocate(
          batchSize * 4).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < batchSize) { buf.putFloat(i * 4, i.toFloat); i += 1 }
        col.putFloatsLittleEndian(0, batchSize, buf.array(), 0)
      case DoubleType =>
        val buf = java.nio.ByteBuffer.allocate(
          batchSize * 8).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < batchSize) { buf.putDouble(i * 8, i.toDouble); i += 1 }
        col.putDoublesLittleEndian(0, batchSize, buf.array(), 0)
      case _ =>
    }
  }

  private def readValues(dataType: DataType, batchSize: Int, vector: ColumnVector): Unit = {
    dataType match {
      case IntegerType =>
        var i = 0
        while (i < batchSize) {
          vector.getInt(i)
          i += 1
        }
      case LongType =>
        var i = 0
        while (i < batchSize) {
          vector.getLong(i)
          i += 1
        }
      case FloatType =>
        var i = 0
        while (i < batchSize) {
          vector.getFloat(i)
          i += 1
        }
      case DoubleType =>
        var i = 0
        while (i < batchSize) {
          vector.getDouble(i)
          i += 1
        }
      case StringType =>
        var i = 0
        while (i < batchSize) {
          vector.getUTF8String(i)
          i += 1
        }
    }
  }

  private def readBatch(dataType: DataType, batchSize: Int, vector: ColumnVector): Unit = {
    dataType match {
      case IntegerType => vector.asInstanceOf[WritableColumnVector].getInts(0, batchSize)
      case LongType => vector.asInstanceOf[WritableColumnVector].getLongs(0, batchSize)
      case FloatType => vector.asInstanceOf[WritableColumnVector].getFloats(0, batchSize)
      case DoubleType => vector.asInstanceOf[WritableColumnVector].getDoubles(0, batchSize)
      case _ => // ignore
    }
  }

  private def testWrite(
      valuesPerIteration: Int, batchSize: Int, dataType: DataType, row: InternalRow): Unit = {
    val vectors = Seq(
      new OnHeapColumnVector(batchSize, dataType),
      new OffHeapColumnVector(batchSize, dataType),
      new ArrowWritableColumnVector(batchSize, dataType)
    )
    vectors.foreach(_.setNum(batchSize))

    val benchmark = new Benchmark(
      s"Write benchmark for $dataType",
      valuesPerIteration * batchSize,
      output = output)

    for (vector <- vectors) {
      val name = vector.getClass.getSimpleName

      benchmark.addCase(s"$name.putSingleValue") { _: Int =>
        for (_ <- 0 until valuesPerIteration) {
          vector.reset()
          populateSingleValue(vector, batchSize, row, 0)
        }
      }

      benchmark.addCase(s"$name.putFromArray") { _: Int =>
        for (_ <- 0 until valuesPerIteration) {
          vector.reset()
          populateFromArray(vector, batchSize, dataType)
        }
      }

      benchmark.addCase(s"$name.putLittleEndian") { _: Int =>
        for (_ <- 0 until valuesPerIteration) {
          vector.reset()
          populateFromByteArrayLE(vector, batchSize, dataType)
        }
      }
    }

    benchmark.run()
    vectors.foreach(_.close())
  }

  private def testRead(
      valuesPerIteration: Int, batchSize: Int, dataType: DataType, row: InternalRow): Unit = {
    val onHeap = new OnHeapColumnVector(batchSize, dataType)
    val offHeap = new OffHeapColumnVector(batchSize, dataType)
    val arrow = new ArrowWritableColumnVector(batchSize, dataType)
    arrow.setNum(batchSize)

    onHeap.reset()
    offHeap.reset()
    arrow.reset()

    populateFromArray(onHeap, batchSize, dataType)
    populateFromArray(offHeap, batchSize, dataType)
    populateFromArray(arrow, batchSize, dataType)

    val benchmark = new Benchmark(
      s"Read benchmark for $dataType",
      valuesPerIteration * batchSize,
      output = output)

    benchmark.addCase("OnHeapColumnVector (single)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readValues(dataType, batchSize, onHeap)
    }
    benchmark.addCase("OnHeapColumnVector (batch)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readBatch(dataType, batchSize, onHeap)
    }
    benchmark.addCase("OffHeapColumnVector (single)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readValues(dataType, batchSize, offHeap)
    }
    benchmark.addCase("OffHeapColumnVector (batch)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readBatch(dataType, batchSize, offHeap)
    }
    benchmark.addCase("ArrowWritableColumnVector (single)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readValues(dataType, batchSize, arrow)
    }
    benchmark.addCase("ArrowWritableColumnVector (batch)") { _: Int =>
      for (_ <- 0 until valuesPerIteration) readBatch(dataType, batchSize, arrow)
    }

    benchmark.run()
    Seq(onHeap, offHeap, arrow).foreach(_.close())
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    val batchSize = 4096

    val testData = Seq(
      (IntegerType, InternalRow(100)),
      (LongType, InternalRow(100L)),
      (FloatType, InternalRow(100F)),
      (DoubleType, InternalRow(100D))
    )

    for ((dtype, row) <- testData) {
      testWrite(valuesPerIteration, batchSize, dtype, row)
      testRead(valuesPerIteration, batchSize, dtype, row)
    }
  }
}
