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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.ReusedColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class ArrowOutputProcessorSuite extends SparkFunSuite {

  private val schema = new StructType()
    .add("s", new StructType().add("a", IntegerType).add("b", StringType))
  private val numBatches = 3
  private val rowsPerBatch = 3

  /** Writes `numBatches` batches of a struct column as an Arrow stream, as a Python worker does. */
  private def arrowStream(): Array[Byte] = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("arrow-output-writer", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(
      ArrowUtils.toArrowSchema(schema, null, errorOnDuplicatedFieldNames = true,
        largeVarTypes = false),
      allocator)
    try {
      val out = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, out)
      val arrowWriter = ArrowWriter.create(root)
      writer.start()
      (0 until numBatches * rowsPerBatch).grouped(rowsPerBatch).foreach { values =>
        values.foreach { v =>
          arrowWriter.write(InternalRow(InternalRow(v, UTF8String.fromString(s"v$v"))))
        }
        arrowWriter.finish()
        writer.writeBatch()
        arrowWriter.reset()
      }
      writer.end()
      out.toByteArray
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("Struct output columns stay valid after a consumer releases each batch") {
    // The output processor loads every batch into the same vectors. A consumer releases each
    // batch with closeIfFreeable once it is done with it, which must leave the vectors, including
    // the child columns of a struct, to the processor.
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("arrow-output-reader", 0, Long.MaxValue)
    val reader = new ArrowStreamReader(new ByteArrayInputStream(arrowStream()), allocator)
    val processor = new ArrowOutputProcessorImpl(reader, metrics())
    try {
      val values = ArrayBuffer[(Int, String)]()
      while (processor.loadBatch()) {
        val batch = processor.produceBatch()
        val column = batch.column(0)
        assert(column.isInstanceOf[ReusedColumnVector])
        (0 until batch.numRows()).foreach { i =>
          val struct = column.getStruct(i)
          values += ((struct.getInt(0), struct.getUTF8String(1).toString))
        }
        batch.closeIfFreeable()
      }
      assert(values.toSeq == (0 until numBatches * rowsPerBatch).map(v => (v, s"v$v")))
    } finally {
      processor.close()
      reader.close()
      allocator.close()
    }
  }

  private def metrics(): Map[String, SQLMetric] = Map(
    "pythonNumRowsReceived" -> new SQLMetric("sum"),
    "pythonDataReceived" -> new SQLMetric("size"))
}
