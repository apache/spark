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

import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
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
   * the rows and the buffer size in bytes of every output batch. Rows are copied before the
   * next batch is produced, since the processor releases the previous output batch then.
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
        batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector.getBufferSize.toLong
      }.sum
      output += ((batch.rowIterator().asScala.map(_.copy()).toSeq, bytes))
    }
    processor.close()
    reader.close(false)
    // Fails if any slice made by the processor was leaked.
    allocator.close()
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
      // The large input batches must have been sliced.
      assert(output.size > batchSizes.size)
      assert(output.forall { case (rows, bytes) => rows.size <= 1 || bytes < maxBytes })
    }
  }
}
