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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class PythonArrowOutputSuite extends SparkFunSuite {

  private val schema = new StructType()
    .add("id", LongType)
    .add("s", StringType)
    .add("st", new StructType().add("a", IntegerType).add("b", StringType))

  private type Value = (Long, String, Option[(Int, String)])

  private def value(i: Long): Value = {
    val s = if (i % 3 == 0) null else s"str_$i"
    val st = if (i % 5 == 0) None else Some((i.toInt * 2, if (i % 2 == 0) null else s"b_$i"))
    (i, s, st)
  }

  private def toRow(v: Value): InternalRow = {
    val st = v._3.map { case (a, b) => InternalRow(a, UTF8String.fromString(b)) }.orNull
    InternalRow(v._1, UTF8String.fromString(v._2), st)
  }

  private def fromRow(row: InternalRow): Value = {
    val s = if (row.isNullAt(1)) null else row.getUTF8String(1).toString
    val st = if (row.isNullAt(2)) {
      None
    } else {
      val struct = row.getStruct(2, 2)
      val b = if (struct.isNullAt(1)) null else struct.getUTF8String(1).toString
      Some((struct.getInt(0), b))
    }
    (row.getLong(0), s, st)
  }

  /** Writes an Arrow IPC stream with one record batch per entry of `batchSizes`. */
  private def writeStream(batchSizes: Seq[Int]): Array[Byte] = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC", true, false)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("writer", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    try {
      val writer = ArrowWriter.create(root)
      val streamWriter = new ArrowStreamWriter(root, null, out)
      streamWriter.start()
      var next = 0L
      batchSizes.foreach { size =>
        (0 until size).foreach { _ =>
          writer.write(toRow(value(next)))
          next += 1
        }
        writer.finish()
        streamWriter.writeBatch()
        writer.reset()
      }
      streamWriter.end()
    } finally {
      root.close()
      allocator.close()
    }
    out.toByteArray
  }

  /**
   * Drives `processor` the way `PythonArrowOutput.newReaderIterator.read()` does, and returns
   * the values of every output batch. Rows are read before the next batch is produced, since
   * the processor releases the previous output batch when it produces the next one.
   */
  private def readAll(
      batchSizes: Seq[Int],
      createProcessor: (ArrowStreamReader, Map[String, SQLMetric]) => ArrowOutputProcessor)
    : Seq[Seq[Value]] = {
    val allocator: BufferAllocator =
      ArrowUtils.rootAllocator.newChildAllocator("reader", 0, Long.MaxValue)
    val reader = new ArrowStreamReader(new ByteArrayInputStream(writeStream(batchSizes)), allocator)
    val metrics = Map(
      "pythonNumRowsReceived" -> new SQLMetric("sum"),
      "pythonDataReceived" -> new SQLMetric("size"))
    val processor = createProcessor(reader, metrics)
    val output = ArrayBuffer.empty[Seq[Value]]
    while (processor.loadBatch()) {
      val batch = processor.produceBatch()
      output += batch.rowIterator().asScala.map(fromRow).toSeq
    }
    processor.close()
    reader.close(false)
    // Fails if any root or slice handed out by the processor was leaked.
    allocator.close()
    assert(metrics("pythonNumRowsReceived").value === batchSizes.sum)
    output.toSeq
  }

  private def checkAllRowsOnceInOrder(batchSizes: Seq[Int], output: Seq[Seq[Value]]): Unit = {
    assert(output.flatten === (0L until batchSizes.sum).map(value))
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
      assert(output.forall(_.size <= maxRecords))
    }
  }

  Seq(
    Seq(1, 100, 1),
    Seq(100, 1, 100),
    Seq(1, 0, 57, 2, 33)
  ).foreach { batchSizes =>
    test(s"slice by bytes: batches $batchSizes") {
      val maxBytes = 512
      val output = readAll(batchSizes, (reader, metrics) =>
        new SliceBytesArrowOutputProcessorImpl(reader, metrics, maxBytes))
      checkAllRowsOnceInOrder(batchSizes, output)
      // The large input batches must have been sliced.
      assert(output.size > batchSizes.size)
    }
  }
}
