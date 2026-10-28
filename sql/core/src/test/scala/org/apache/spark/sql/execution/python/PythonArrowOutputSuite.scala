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

package org.apache.spark.sql.execution.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{BigIntVector, FieldVector, FixedSizeBinaryVector, LargeVarCharVector, NullVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.unsafe.types.UTF8String

class PythonArrowOutputSuite extends SparkFunSuite {

  private val schema = new StructType()
    .add("id", LongType)
    .add("s", StringType)
    .add("st", new StructType().add("a", IntegerType).add("b", StringType))

  private def row(i: Long): InternalRow = {
    val s = if (i % 3 == 0) null else UTF8String.fromString(s"str_$i")
    val st = if (i % 5 == 0) {
      null
    } else {
      InternalRow(i.toInt * 2, if (i % 2 == 0) null else UTF8String.fromString(s"b_$i"))
    }
    InternalRow(i, s, st)
  }

  /** Writes an Arrow IPC stream with one record batch per entry of `batchSizes`. */
  private def writeStream(batchSizes: Seq[Int]): Array[Byte] = {
    val writer = ArrowWriter.create(schema, "UTC")
    val out = new ByteArrayOutputStream()
    try {
      val streamWriter = new ArrowStreamWriter(writer.root, null, out)
      streamWriter.start()
      var next = 0L
      batchSizes.foreach { size =>
        (0 until size).foreach { _ =>
          writer.write(row(next))
          next += 1
        }
        writer.finish()
        streamWriter.writeBatch()
        writer.reset()
      }
      streamWriter.end()
    } finally {
      writer.root.close()
    }
    out.toByteArray
  }

  /**
   * Drives `processor` the way `PythonArrowOutput.newReaderIterator.read()` does, and returns
   * the rows and the buffer size in bytes, as `SliceBytesArrowOutputProcessorImpl` measures it,
   * of every output batch. Rows are copied before the next batch is produced, since the
   * processor releases the previous output batch then.
   */
  private def readAll(
      batchSizes: Seq[Int],
      createProcessor: (ArrowStreamReader, Map[String, SQLMetric]) => ArrowOutputProcessor)
    : Seq[(Seq[InternalRow], Long)] = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("reader", 0, Long.MaxValue)
    val reader = new ArrowStreamReader(new ByteArrayInputStream(writeStream(batchSizes)), allocator)
    val metrics = Map(
      "pythonNumRowsReceived" -> new SQLMetric("sum"),
      "pythonDataReceived" -> new SQLMetric("size"))
    val processor = createProcessor(reader, metrics)
    val output = ArrayBuffer.empty[(Seq[InternalRow], Long)]
    while (processor.loadBatch()) {
      val batch = processor.produceBatch()
      val bytes = (0 until batch.numCols()).map { i =>
        SliceBytesArrowOutputProcessorImpl.bufferSize(
          batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector)
      }.sum
      output += ((batch.rowIterator().asScala.map(_.copy()).toSeq, bytes))
    }
    processor.close()
    reader.close(false)
    // Fails if any slice made by the processor was leaked.
    allocator.close()
    // `PythonArrowOutput` closes the processor again in its task completion listener.
    processor.close()
    assert(metrics("pythonNumRowsReceived").value === batchSizes.sum)
    output.toSeq
  }

  private def checkAllRowsOnceInOrder(
      batchSizes: Seq[Int],
      output: Seq[(Seq[InternalRow], Long)]): Unit = {
    assert(output.flatMap(_._1) === (0L until batchSizes.sum).map(row))
  }

  Seq(
    (Seq(3, 3, 3), 2),
    (Seq(4, 4, 4), 2),
    (Seq(2, 5, 2, 7), 2),
    (Seq(1, 0, 6, 1, 3), 3),
    (Seq(10), 3)
  ).foreach { case (batchSizes, maxRecords) =>
    test(s"slice by records: batches $batchSizes, max $maxRecords records per batch") {
      val output = readAll(batchSizes, (reader, metrics) =>
        new SliceRecordsArrowOutputProcessorImpl(reader, metrics, maxRecords))
      checkAllRowsOnceInOrder(batchSizes, output)
      val expectedSizes = batchSizes.flatMap { n =>
        val sizes = Seq.fill(n / maxRecords)(maxRecords) ++ Seq(n % maxRecords).filter(_ > 0)
        if (sizes.isEmpty) Seq(0) else sizes
      }
      assert(output.map(_._1.size) === expectedSizes)
    }
  }

  test("SPARK-60627: slice by records: a batch that Arrow cannot slice is output whole") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("unsliceable", 0, Long.MaxValue)
    // An array column that reports 2^29 + 2^20 rows without allocating them, as if its offsets
    // took over 2 GiB, so that Arrow cannot slice it. Since a real overflow needs buffers over
    // 2 GiB, this checks only that the batch is output whole instead of sliced. The fixed-width
    // vectors that Spark reads, e.g. of an int64 column over 2 GiB, are final classes, so a list
    // stands in.
    val rows = (1 << 29) + (1 << 20)
    val list = new ListVector("l", allocator, FieldType.nullable(ArrowType.List.INSTANCE), null) {
      override def getValueCount: Int = rows
    }
    list.addOrGetVector[BigIntVector](FieldType.nullable(new ArrowType.Int(64, true)))
    // Like the output of mapInArrow, the column is in a struct column.
    val struct = new StructVector(
        "st", allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), null) {
      putChild("l", list)
    }
    val root = new VectorSchemaRoot(
      Seq(struct.getField).asJava, Seq[FieldVector](struct).asJava, rows)
    val reader = new ArrowStreamReader(
        new ByteArrayInputStream(Array.emptyByteArray), allocator) {
      private var loaded = false
      override def getVectorSchemaRoot: VectorSchemaRoot = root
      override def loadNextBatch(): Boolean = {
        val hasNext = !loaded
        loaded = true
        hasNext
      }
      override def bytesRead(): Long = 0L
    }
    val metrics = Map(
      "pythonNumRowsReceived" -> new SQLMetric("sum"),
      "pythonDataReceived" -> new SQLMetric("size"))
    val processor = new SliceRecordsArrowOutputProcessorImpl(reader, metrics, 1 << 20)
    try {
      assert(processor.loadBatch())
      assert(processor.produceBatch().numRows() === rows)
      assert(!processor.loadBatch())
    } finally {
      processor.close()
      allocator.close()
    }
  }

  test("SPARK-60627: canSlice rejects vectors whose slice positions overflow int") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("canSlice", 0, Long.MaxValue)
    // Vectors that report many values without allocating them.
    def fixed(rows: Int): FixedSizeBinaryVector =
      new FixedSizeBinaryVector("f", allocator, 8) {
        override def getValueCount: Int = rows
      }
    def list(rows: Int): ListVector = new ListVector(
        "l", allocator, FieldType.nullable(ArrowType.List.INSTANCE), null) {
      override def getValueCount: Int = rows
    }
    val vectors =
      Seq(fixed((1 << 28) - 1), fixed(1 << 28), list((1 << 29) - 2), list((1 << 29) - 1))
    try {
      // `splitAndTransfer` computes `startIndex * 8` and `length * 8` of an 8-byte wide column,
      // and the offset positions and `(length + 1) * 4` offset bytes of a list column, in int.
      assert(vectors.map(BaseSliceArrowOutputProcessor.canSlice) ===
        Seq(true, false, true, false))
    } finally {
      vectors.foreach(_.close())
      allocator.close()
    }
  }

  Seq(
    (Seq(1, 100, 1), 512),
    (Seq(100, 1, 100), 512),
    (Seq(1, 0, 57, 2, 33), 512),
    // Even a single row is larger than the limit, so every non-empty output batch has one row.
    (Seq(2, 0, 5, 1), 3)
  ).foreach { case (batchSizes, maxBytes) =>
    test(s"slice by bytes: batches $batchSizes, max $maxBytes bytes per batch") {
      val output = readAll(batchSizes, (reader, metrics) =>
        new SliceBytesArrowOutputProcessorImpl(reader, metrics, maxBytes))
      checkAllRowsOnceInOrder(batchSizes, output)
      // Guards the choice of cases: each has a batch that does not fit, which must be sliced.
      assert(output.size > batchSizes.size)
      assert(output.forall { case (rows, bytes) => rows.size <= 1 || bytes <= maxBytes })
    }
  }

  test("SPARK-60395: slice by bytes: a batch whose size equals the limit is not sliced") {
    val batchSizes = Seq(50, 50)
    val batchBytes = readAll(batchSizes, new ArrowOutputProcessorImpl(_, _)).map(_._2)
    assert(batchBytes.distinct.size === 2)
    val maxBytes = batchBytes.max.toInt
    val output = readAll(batchSizes, new SliceBytesArrowOutputProcessorImpl(_, _, maxBytes))
    checkAllRowsOnceInOrder(batchSizes, output)
    assert(output.map(_._1.size) === batchSizes)
    // One byte less, the larger batch is sliced.
    val sliced = readAll(batchSizes, new SliceBytesArrowOutputProcessorImpl(_, _, maxBytes - 1))
    checkAllRowsOnceInOrder(batchSizes, sliced)
    assert(sliced.size > batchSizes.size)
    assert(sliced.forall(_._2 <= maxBytes - 1))
  }

  test("SPARK-60395: slice by bytes: each search does not restart from all remaining rows") {
    val batchSizes = Seq(2000)
    val maxBytes = (readAll(batchSizes, new ArrowOutputProcessorImpl(_, _)).head._2 / 50).toInt
    var measured = 0
    val output = readAll(batchSizes, (reader, metrics) =>
      new SliceBytesArrowOutputProcessorImpl(reader, metrics, maxBytes) {
        override private[python] def batchBytes(root: VectorSchemaRoot): Long = {
          measured += 1
          super.batchBytes(root)
        }
      })
    checkAllRowsOnceInOrder(batchSizes, output)
    assert(output.forall { case (rows, bytes) => rows.size <= 1 || bytes <= maxBytes })
    assert(output.size >= 50)
    // The whole batch and the first search take a few measurements, and each later slice one,
    // or a few more where its rows are larger. Restarting every search from all the remaining
    // rows would take hundreds.
    assert(measured <= 2 * output.size, s"$measured measurements for ${output.size} slices")
  }

  /**
   * Slices `batchSizes` by bytes, where a row whose `id` is i takes `rowBytes(i)` bytes, and
   * returns the number of rows of every output batch and the number of size measurements.
   */
  private def sliceRowCountsAndMeasurements(batchSizes: Seq[Int], maxBytes: Int)(
      rowBytes: Long => Long): (Seq[Int], Int) = {
    var measured = 0
    val output = readAll(batchSizes, (reader, metrics) =>
      new SliceBytesArrowOutputProcessorImpl(reader, metrics, maxBytes) {
        override private[python] def batchBytes(root: VectorSchemaRoot): Long = {
          measured += 1
          val ids = root.getVector("id").asInstanceOf[BigIntVector]
          (0 until root.getRowCount).map(i => rowBytes(ids.get(i))).sum
        }
      })
    checkAllRowsOnceInOrder(batchSizes, output)
    (output.map(_._1.size), measured)
  }

  private def sliceRowCounts(batchSizes: Seq[Int], maxBytes: Int)(rowBytes: Long => Long)
    : Seq[Int] = sliceRowCountsAndMeasurements(batchSizes, maxBytes)(rowBytes)._1

  test("SPARK-60395: slice by bytes: number of rows of each slice") {
    // The first search starts from the rows that would fit at the bytes per row of the root, and
    // later searches from those of the last slice, here 10 rows, which fit exactly.
    assert(sliceRowCounts(Seq(50), 1000)(_ => 100) === Seq.fill(5)(10))
    // Row 0 alone is larger than the limit, which does not keep the other slices at one row. The
    // first search goes 66, 59, 53, ..., 2, 1 rows, and the next search starts from 66 rows.
    assert(sliceRowCounts(Seq(1000), 1000)(i => if (i == 0) 5000 else 10) ===
      Seq(1, 66) ++ Seq.fill(9)(100) ++ Seq(33))
    // Slices grow again after the large rows.
    assert(sliceRowCounts(Seq(1000), 1000)(i => if (i < 10) 500 else 10) ===
      Seq.fill(6)(2) ++ Seq.fill(9)(100) ++ Seq(88))
    // Every row is larger than the limit.
    assert(sliceRowCounts(Seq(3, 0, 2), 1000)(_ => 2000) === Seq(1, 1, 1, 0, 1, 1))
  }

  test("SPARK-60395: slice by bytes: rows larger than the limit are measured once each") {
    // 100 small rows make the search start from about 100 rows, then 900 rows are each larger
    // than the limit. Searching from there for each of them would take about 45 measurements.
    val (rowCounts, measured) = sliceRowCountsAndMeasurements(Seq(1000), 1000) { i =>
      if (i < 100) 10 else 2000
    }
    assert(rowCounts.takeRight(900) === Seq.fill(900)(1))
    assert(measured <= 2 * rowCounts.size, s"$measured measurements for $rowCounts")
  }

  test("SPARK-60395: slice by bytes: a batch that Arrow cannot slice is output whole") {
    val batchSizes = Seq(10, 20)
    val output = readAll(batchSizes, (reader, metrics) =>
      new SliceBytesArrowOutputProcessorImpl(reader, metrics, 1) {
        override private[python] def canSlice(root: VectorSchemaRoot): Boolean = false
      })
    checkAllRowsOnceInOrder(batchSizes, output)
    assert(output.map(_._1.size) === batchSizes)

    val allocator = ArrowUtils.rootAllocator.newChildAllocator("canSlice", 0, Long.MaxValue)
    // Vectors that report many values without allocating them.
    def fixed(rows: Int): FixedSizeBinaryVector =
      new FixedSizeBinaryVector("f", allocator, 8) {
        override def getValueCount: Int = rows
      }
    def list(rows: Int): ListVector = new ListVector(
        "l", allocator, FieldType.nullable(ArrowType.List.INSTANCE), null) {
      override def getValueCount: Int = rows
    }
    val vectors = Seq(fixed((1 << 28) - 1), fixed(1 << 28), list(1000), list(1 << 29))
    try {
      // `splitAndTransfer` computes `startIndex * 8` and `length * 8` of an int64 column, and
      // the offset positions of a list column, in int.
      assert(vectors.map(SliceBytesArrowOutputProcessorImpl.canSlice) ===
        Seq(true, false, true, false))
    } finally {
      vectors.foreach(_.close())
      allocator.close()
    }
  }

  test("SPARK-60395: bufferSize matches getBufferSize of vectors below 2 GiB") {
    val nestedSchema = new StructType()
      .add("b", BooleanType)
      .add("d", DecimalType(20, 2))
      .add("bin", BinaryType)
      .add("arr", ArrayType(new StructType().add("x", LongType).add("y", StringType)))
      .add("m", MapType(StringType, ArrayType(IntegerType)))
    val writer = ArrowWriter.create(nestedSchema, "UTC")
    try {
      (0 until 20).foreach { i =>
        val arr = new GenericArrayData((0 until i % 4).map { j =>
          InternalRow(j.toLong, if (j % 2 == 0) null else UTF8String.fromString(s"y_$j"))
        })
        val m = ArrayBasedMapData(
          Array[Any](UTF8String.fromString(s"k_$i")),
          Array[Any](new GenericArrayData((0 until i % 3).toArray[Any])))
        writer.write(InternalRow(i % 2 == 0, Decimal(i * 100L, 20, 2),
          Array.fill[Byte](i)(1), if (i % 5 == 0) null else arr, m))
      }
      writer.finish()
      val root = writer.root
      Seq((0, 20), (3, 7), (5, 1), (19, 1), (0, 13)).foreach { case (start, length) =>
        val slice = root.slice(start, length)
        try {
          (slice.getFieldVectors.asScala ++ root.getFieldVectors.asScala).foreach { v =>
            assert(SliceBytesArrowOutputProcessorImpl.bufferSize(v) === v.getBufferSize,
              s"${v.getField} in slice ($start, $length)")
          }
        } finally {
          slice.close()
        }
      }
    } finally {
      writer.root.close()
    }
  }

  test("SPARK-60395: bufferSize does not wrap around for a struct vector over 2 GiB") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    class TestStructVector extends StructVector(
        "st", allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), null) {
      def addChild(name: String, child: FieldVector): Unit = putChild(name, child)
    }
    val struct = new TestStructVector
    try {
      // Children that report 1 GiB each, like three int64 columns of 2^27 rows.
      (0 until 3).foreach { i =>
        struct.addChild(s"c$i", new NullVector(s"c$i") {
          override def getBufferSize: Int = 1 << 30
        })
      }
      // An int64 column of 2^28 rows, without allocating it, which the struct vector measures
      // only if it measures its children in Long too.
      struct.addChild("c3", new FixedSizeBinaryVector("c3", allocator, 8) {
        override def getValueCount: Int = 1 << 28
        override def setValueCount(valueCount: Int): Unit = {}
      })
      struct.setValueCount(1)
      // `StructVector.getBufferSize` wraps around to a negative size.
      assert(struct.getBufferSize < 0)
      val expected = 3L * (1 << 30) + (1L << 31) + (1 << 25) + 1
      assert(SliceBytesArrowOutputProcessorImpl.bufferSize(struct) === expected)
      val root = new VectorSchemaRoot(Seq[FieldVector](struct).asJava)
      assert(SliceBytesArrowOutputProcessorImpl.batchBytes(root) === expected)
      // Arrow cannot slice the int64 child of 2 GiB, which only the check of the children sees.
      assert(!SliceBytesArrowOutputProcessorImpl.canSlice(struct))
    } finally {
      struct.close()
      allocator.close()
    }
  }

  test("SPARK-60395: bufferSize does not wrap around for flat vectors over 2 GiB") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("flat", 0, Long.MaxValue)
    // An int64 column of 2^28 rows, without allocating it.
    val fixed = new FixedSizeBinaryVector("f", allocator, 8) {
      override def getValueCount: Int = 1 << 28
    }
    val varChar = new VarCharVector("v", allocator)
    val largeVarChar = new LargeVarCharVector("l", allocator)
    try {
      assert(SliceBytesArrowOutputProcessorImpl.bufferSize(fixed) === (1L << 31) + (1 << 25))

      // Variable-width vectors of 3 values whose end offset is then set to a large value.
      varChar.allocateNew()
      largeVarChar.allocateNew()
      Seq("a", "bb", "ccc").zipWithIndex.foreach { case (str, i) =>
        varChar.setSafe(i, str.getBytes("UTF-8"))
        largeVarChar.setSafe(i, str.getBytes("UTF-8"))
      }
      Seq(varChar, largeVarChar).foreach { v =>
        v.setValueCount(3)
        assert(SliceBytesArrowOutputProcessorImpl.bufferSize(v) === v.getBufferSize)
      }
      varChar.getOffsetBuffer.setInt(3 * 4, Int.MaxValue - 10)
      // `getBufferSize` wraps around to a negative size.
      assert(varChar.getBufferSize < 0)
      assert(SliceBytesArrowOutputProcessorImpl.bufferSize(varChar) ===
        1 + 4 * 4 + (Int.MaxValue - 10L))
      largeVarChar.getOffsetBuffer.setLong(3 * 8, 3L << 30)
      // `getBufferSize` caps the size at Int.MaxValue.
      assert(largeVarChar.getBufferSize === Int.MaxValue)
      assert(SliceBytesArrowOutputProcessorImpl.bufferSize(largeVarChar) ===
        1 + 4 * 8 + (3L << 30))
    } finally {
      fixed.close()
      varChar.close()
      largeVarChar.close()
      allocator.close()
    }
  }

  test("SPARK-60395: maxBytesPerOutputBatch below Int.MinValue means no limit") {
    val conf = new SQLConf
    Seq("-3g", "-4294967295", "-1", "0").foreach { value =>
      conf.setConfString(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_OUTPUT_BATCH.key, value)
      assert(conf.arrowMaxBytesPerOutputBatch === 0, value)
    }
    conf.setConfString(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_OUTPUT_BATCH.key, "256m")
    assert(conf.arrowMaxBytesPerOutputBatch === 256 * 1024 * 1024)
  }
}
