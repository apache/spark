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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.mockito.Mockito.{mock, never, times, verify}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter

class BaseStreamingArrowWriterSuite extends SparkFunSuite with BeforeAndAfterEach {
  // Setting the maximum number of records per batch to 2 to make test easier.
  val arrowMaxRecordsPerBatch = 2
  var transformWithStateInPandasWriter: BaseStreamingArrowWriter = _
  var arrowWriter: ArrowWriter = _
  var writer: ArrowStreamWriter = _

  override def beforeEach(): Unit = {
    val root: VectorSchemaRoot = mock(classOf[VectorSchemaRoot])
    writer = mock(classOf[ArrowStreamWriter])
    arrowWriter = mock(classOf[ArrowWriter])
    transformWithStateInPandasWriter = new BaseStreamingArrowWriter(
      root, writer, arrowMaxRecordsPerBatch, arrowWriter)
  }

  test("test writeRow") {
    val dataRow = mock(classOf[InternalRow])
    // Write 2 rows first, batch is not finalized.
    transformWithStateInPandasWriter.writeRow(dataRow)
    transformWithStateInPandasWriter.writeRow(dataRow)
    verify(arrowWriter, times(2)).write(dataRow)
    verify(writer, never()).writeBatch()
    // Write a 3rd row, batch is finalized.
    transformWithStateInPandasWriter.writeRow(dataRow)
    verify(arrowWriter, times(3)).write(dataRow)
    verify(writer).writeBatch()
    // Write 2 more rows, a new batch is finalized.
    transformWithStateInPandasWriter.writeRow(dataRow)
    transformWithStateInPandasWriter.writeRow(dataRow)
    verify(arrowWriter, times(5)).write(dataRow)
    verify(writer, times(2)).writeBatch()
  }

  test("test finalizeCurrentArrowBatch") {
    transformWithStateInPandasWriter.finalizeCurrentArrowBatch()
    verify(arrowWriter).finish()
    verify(writer).writeBatch()
    verify(arrowWriter).reset()
  }
}
