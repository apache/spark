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

import org.apache.commons.lang3.RandomStringUtils

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, ConstantColumnVector, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.UTF8StringBuilder

/**
 * Benchmark for constant ColumnVector read and write,
 * include `ConstantColumnVector`, `OnHeapColumnVector` and `OffHeapColumnVector`
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/ConstantColumnVectorBenchmark-results.txt".
 * }}}
 */
object ConstantColumnVectorBenchmark extends BenchmarkBase {

  private def populate(
      col: WritableColumnVector, batchSize: Int, row: InternalRow, fieldIdx: Int): Unit = {
    col.dataType() match {
      case IntegerType => col.putInts(0, batchSize, row.getInt(fieldIdx))
      case LongType => col.putLongs(0, batchSize, row.getLong(fieldIdx))
      case FloatType => col.putFloats(0, batchSize, row.getFloat(fieldIdx))
      case DoubleType => col.putDoubles(0, batchSize, row.getDouble(fieldIdx))
      case StringType =>
        val v = row.getUTF8String(fieldIdx)
        val bytes = v.getBytes
        (0 until batchSize).foreach { i =>
          col.putByteArray(i, bytes)
        }
    }
  }

  private def readValues(dataType: DataType, batchSize: Int, vector: ColumnVector): Unit = {
    dataType match {
      case IntegerType =>
        (0 until batchSize).foreach(i => vector.getInt(i))
      case LongType =>
        (0 until batchSize).foreach(i => vector.getLong(i))
      case FloatType =>
        (0 until batchSize).foreach(i => vector.getFloat(i))
      case DoubleType =>
        (0 until batchSize).foreach(i => vector.getDouble(i))
      case StringType =>
        (0 until batchSize).foreach(i => vector.getUTF8String(i))
    }
  }

  def testWrite(
      valuesPerIteration: Int,
      batchSize: Int,
      dataType: DataType,
      row: InternalRow): Unit = {

    val onHeapColumnVector = new OnHeapColumnVector(batchSize, dataType)
    val offHeapColumnVector = new OffHeapColumnVector(batchSize, dataType)
    val constantColumnVector = new ConstantColumnVector(batchSize, dataType)

    val other = if (dataType == StringType) {
      s", row length = ${row.getUTF8String(0).toString.length}"
    } else {
      ""
    }

    val benchmark = new Benchmark(
      s"Test write with $dataType$other",
      valuesPerIteration * batchSize,
      output = output)

    benchmark.addCase("ConstantColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        ColumnVectorUtils.populate(constantColumnVector, row, 0)
      }
    }

    benchmark.addCase("OnHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        onHeapColumnVector.reset()
        populate(onHeapColumnVector, batchSize, row, 0)
      }
    }

    benchmark.addCase("OffHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        offHeapColumnVector.reset()
        populate(offHeapColumnVector, batchSize, row, 0)
      }
    }

    benchmark.run()
    onHeapColumnVector.close()
    offHeapColumnVector.close()
    constantColumnVector.close()
  }

  def testRead(
      valuesPerIteration: Int,
      batchSize: Int,
      dataType: DataType,
      row: InternalRow): Unit = {

    val onHeapColumnVector = new OnHeapColumnVector(batchSize, dataType)
    val offHeapColumnVector = new OffHeapColumnVector(batchSize, dataType)
    val constantColumnVector = new ConstantColumnVector(batchSize, dataType)

    onHeapColumnVector.reset()
    populate(onHeapColumnVector, batchSize, row, 0)
    offHeapColumnVector.reset()
    populate(offHeapColumnVector, batchSize, row, 0)
    ColumnVectorUtils.populate(constantColumnVector, row, 0)

    val other = if (dataType == StringType) {
      s", row length = ${row.getUTF8String(0).toString.length}"
    } else {
      ""
    }

    val benchmark = new Benchmark(
      s"Test read with $dataType$other",
      valuesPerIteration * batchSize,
      output = output)

    benchmark.addCase("ConstantColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, constantColumnVector)
      }
    }

    benchmark.addCase("OnHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, onHeapColumnVector)
      }
    }

    benchmark.addCase("OffHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, offHeapColumnVector)
      }
    }

    benchmark.run()
    onHeapColumnVector.close()
    offHeapColumnVector.close()
    constantColumnVector.close()
  }

  def testWriteAndRead(
      valuesPerIteration: Int,
      batchSize: Int,
      dataType: DataType,
      row: InternalRow): Unit = {

    val onHeapColumnVector = new OnHeapColumnVector(batchSize, dataType)
    val offHeapColumnVector = new OffHeapColumnVector(batchSize, dataType)
    val constantColumnVector = new ConstantColumnVector(batchSize, dataType)

    val other = if (dataType == StringType) {
      s", row length = ${row.getUTF8String(0).toString.length}"
    } else {
      ""
    }

    val benchmark = new Benchmark(
      s"Test write and read with $dataType$other",
      valuesPerIteration * batchSize,
      output = output)

    benchmark.addCase("ConstantColumnVector") { _: Int =>
      ColumnVectorUtils.populate(constantColumnVector, row, 0)
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, constantColumnVector)
      }
    }

    benchmark.addCase("OnHeapColumnVector") { _: Int =>
      onHeapColumnVector.reset()
      populate(onHeapColumnVector, batchSize, row, 0)
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, onHeapColumnVector)
      }
    }

    benchmark.addCase("OffHeapColumnVector") { _: Int =>
      offHeapColumnVector.reset()
      populate(offHeapColumnVector, batchSize, row, 0)
      for (_ <- 0 until valuesPerIteration) {
        readValues(dataType, batchSize, offHeapColumnVector)
      }
    }

    benchmark.run()
    onHeapColumnVector.close()
    offHeapColumnVector.close()
    constantColumnVector.close()
  }

  def testIsNull(
      valuesPerIteration: Int,
      batchSize: Int,
      dataType: DataType): Unit = {

    val onHeapColumnVector = new OnHeapColumnVector(batchSize, dataType)
    val offHeapColumnVector = new OffHeapColumnVector(batchSize, dataType)
    val constantColumnVector = new ConstantColumnVector(batchSize, dataType)

    onHeapColumnVector.putNulls(0, batchSize)
    offHeapColumnVector.putNulls(0, batchSize)
    constantColumnVector.setNull()

    val benchmark = new Benchmark(
      s"Test isNull with $dataType",
      valuesPerIteration * batchSize,
      output = output)

    benchmark.addCase("ConstantColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        (0 until batchSize).foreach(constantColumnVector.isNullAt)
      }
    }

    benchmark.addCase("OnHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        (0 until batchSize).foreach(onHeapColumnVector.isNullAt)
      }
    }

    benchmark.addCase("OffHeapColumnVector") { _: Int =>
      for (_ <- 0 until valuesPerIteration) {
        (0 until batchSize).foreach(offHeapColumnVector.isNullAt)
      }
    }

    benchmark.run()
    onHeapColumnVector.close()
    offHeapColumnVector.close()
    constantColumnVector.close()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    val batchSize = 4096

    Seq(1, 5, 10, 15, 20, 30).foreach { length =>
      val builder = new UTF8StringBuilder()
      builder.append(RandomStringUtils.random(length))
      val row = InternalRow(builder.build())
      testWrite(valuesPerIteration, batchSize, StringType, row)
    }

    testWrite(valuesPerIteration, batchSize, IntegerType, InternalRow(100))
    testWrite(valuesPerIteration, batchSize, LongType, InternalRow(100L))
    testWrite(valuesPerIteration, batchSize, FloatType, InternalRow(100F))
    testWrite(valuesPerIteration, batchSize, DoubleType, InternalRow(100D))


    Seq(1, 5, 10, 15, 20, 30).foreach { length =>
      val builder = new UTF8StringBuilder()
      builder.append(RandomStringUtils.random(length))
      val row = InternalRow(builder.build())
      testRead(valuesPerIteration, batchSize, StringType, row)
    }

    testRead(valuesPerIteration, batchSize, IntegerType, InternalRow(100))
    testRead(valuesPerIteration, batchSize, LongType, InternalRow(100L))
    testRead(valuesPerIteration, batchSize, FloatType, InternalRow(100F))
    testRead(valuesPerIteration, batchSize, DoubleType, InternalRow(100D))

    Seq(1, 5, 10, 15, 20, 30).foreach { length =>
      val builder = new UTF8StringBuilder()
      builder.append(RandomStringUtils.random(length))
      val row = InternalRow(builder.build())
      testWriteAndRead(valuesPerIteration, batchSize, StringType, row)
    }

    testWriteAndRead(valuesPerIteration, batchSize, IntegerType, InternalRow(100))
    testWriteAndRead(valuesPerIteration, batchSize, LongType, InternalRow(100L))
    testWriteAndRead(valuesPerIteration, batchSize, FloatType, InternalRow(100F))
    testWriteAndRead(valuesPerIteration, batchSize, DoubleType, InternalRow(100D))

    testIsNull(valuesPerIteration, batchSize, StringType)
    testIsNull(valuesPerIteration, batchSize, IntegerType)
    testIsNull(valuesPerIteration, batchSize, LongType)
    testIsNull(valuesPerIteration, batchSize, FloatType)
    testIsNull(valuesPerIteration, batchSize, DoubleType)
  }
}
