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
package org.apache.spark.sql.execution.python.streaming

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter

class BaseStreamingArrowWriterSuite extends SparkFunSuite with BeforeAndAfterEach {
  // Setting the maximum number of records per batch to 2 to make test easier.
  val arrowMaxRecordsPerBatch = 2
  val arrowMaxBytesPerBatch = Int.MaxValue
  var transformWithStateInPySparkWriter: BaseStreamingArrowWriter = _
  var arrowWriter: ArrowWriter = _
  var writer: ArrowStreamWriter = _

  override def beforeEach(): Unit = {
    val root: VectorSchemaRoot = mock(classOf[VectorSchemaRoot])
    writer = mock(classOf[ArrowStreamWriter])
    arrowWriter = mock(classOf[ArrowWriter])
    transformWithStateInPySparkWriter = new BaseStreamingArrowWriter(
      root, writer, arrowMaxRecordsPerBatch, arrowMaxBytesPerBatch, arrowWriter)
  }

  test("test writeRow") {
    val dataRow = mock(classOf[InternalRow])
    // Write 2 rows first, batch is not finalized.
    transformWithStateInPySparkWriter.writeRow(dataRow)
    transformWithStateInPySparkWriter.writeRow(dataRow)
    verify(arrowWriter, times(2)).write(dataRow)
    verify(writer, never()).writeBatch()
    // Write a 3rd row, batch is finalized.
    transformWithStateInPySparkWriter.writeRow(dataRow)
    verify(arrowWriter, times(3)).write(dataRow)
    verify(writer).writeBatch()
    // Write 2 more rows, a new batch is finalized.
    transformWithStateInPySparkWriter.writeRow(dataRow)
    transformWithStateInPySparkWriter.writeRow(dataRow)
    verify(arrowWriter, times(5)).write(dataRow)
    verify(writer, times(2)).writeBatch()
  }

  test("test finalizeCurrentArrowBatch") {
    transformWithStateInPySparkWriter.finalizeCurrentArrowBatch()
    verify(arrowWriter).finish()
    verify(writer).writeBatch()
    verify(arrowWriter).reset()
  }

  test("test maxBytesPerBatch can work") {
    val root: VectorSchemaRoot = mock(classOf[VectorSchemaRoot])

    var sizeCounter = 0
    when(arrowWriter.write(any[InternalRow])).thenAnswer { _ =>
      sizeCounter += 1
      ()
    }

    when(arrowWriter.sizeInBytes()).thenAnswer { _ => sizeCounter }

    // Set arrowMaxBytesPerBatch to 1
    transformWithStateInPySparkWriter = new BaseStreamingArrowWriter(
      root, writer, arrowMaxRecordsPerBatch, 1, arrowWriter)
    val dataRow = mock(classOf[InternalRow])
    transformWithStateInPySparkWriter.writeRow(dataRow)
    verify(arrowWriter).write(dataRow)
    verify(writer, never()).writeBatch()
    transformWithStateInPySparkWriter.writeRow(dataRow)
    verify(arrowWriter, times(2)).write(dataRow)
    // Write batch is called since we reach arrowMaxBytesPerBatch
    verify(writer).writeBatch()
    transformWithStateInPySparkWriter.finalizeCurrentArrowBatch()
    verify(arrowWriter, times(2)).finish()
    // The second record would be written
    verify(writer, times(2)).writeBatch()
    verify(arrowWriter, times(2)).reset()
  }

  test("test negative or zero arrowMaxRecordsPerBatch is unlimited") {
    val root: VectorSchemaRoot = mock(classOf[VectorSchemaRoot])
    val dataRow = mock(classOf[InternalRow])

    // Test with negative value
    transformWithStateInPySparkWriter = new BaseStreamingArrowWriter(
      root, writer, -1, arrowMaxBytesPerBatch, arrowWriter)

    // Write many rows (more than typical batch size)
    for (_ <- 1 to 10) {
      transformWithStateInPySparkWriter.writeRow(dataRow)
    }

    // Verify all rows were written but batch was not finalized
    verify(arrowWriter, times(10)).write(dataRow)
    verify(writer, never()).writeBatch()

    // Only finalize when explicitly called
    transformWithStateInPySparkWriter.finalizeCurrentArrowBatch()
    verify(writer).writeBatch()

    // Test with zero value
    transformWithStateInPySparkWriter = new BaseStreamingArrowWriter(
      root, writer, 0, arrowMaxBytesPerBatch, arrowWriter)

    // Write many rows again
    for (_ <- 1 to 10) {
      transformWithStateInPySparkWriter.writeRow(dataRow)
    }

    // Verify rows were written but batch was not finalized
    verify(arrowWriter, times(20)).write(dataRow)
    verify(writer).writeBatch()  // still 1 from before

    // Only finalize when explicitly called
    transformWithStateInPySparkWriter.finalizeCurrentArrowBatch()
    verify(writer, times(2)).writeBatch()
  }
}
