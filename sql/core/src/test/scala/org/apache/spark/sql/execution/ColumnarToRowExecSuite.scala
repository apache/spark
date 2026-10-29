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

package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{Decimal, IntegerType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

class ColumnarToRowExecSuite extends SparkFunSuite {

  /** A minimal ColumnVector that records whether `closeIfFreeable`/`close` was called. */
  private class RecordingColumnVector extends ColumnVector(IntegerType) {
    var closeIfFreeableCalls = 0
    var closeCalls = 0

    override def closeIfFreeable(): Unit = closeIfFreeableCalls += 1
    override def close(): Unit = closeCalls += 1

    private def unused = throw new UnsupportedOperationException("not used by advanceBatch")
    override def hasNull: Boolean = unused
    override def numNulls(): Int = unused
    override def isNullAt(rowId: Int): Boolean = unused
    override def getBoolean(rowId: Int): Boolean = unused
    override def getByte(rowId: Int): Byte = unused
    override def getShort(rowId: Int): Short = unused
    override def getInt(rowId: Int): Int = unused
    override def getLong(rowId: Int): Long = unused
    override def getFloat(rowId: Int): Float = unused
    override def getDouble(rowId: Int): Double = unused
    override def getArray(rowId: Int): ColumnarArray = unused
    override def getMap(ordinal: Int): ColumnarMap = unused
    override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = unused
    override def getUTF8String(rowId: Int): UTF8String = unused
    override def getBinary(rowId: Int): Array[Byte] = unused
    override def getChild(ordinal: Int): ColumnVector = unused
  }

  private def newBatch(numRows: Int): (ColumnarBatch, RecordingColumnVector) = {
    val vector = new RecordingColumnVector
    val batch = new ColumnarBatch(Array[ColumnVector](vector), numRows)
    (batch, vector)
  }

  test("advanceBatch releases the current batch and fetches the next, bumping metrics") {
    val numInputBatches = new SQLMetric("sum")
    val numOutputRows = new SQLMetric("sum")
    val (current, currentVector) = newBatch(3)
    val (next, _) = newBatch(5)

    val result = ColumnarToRowExec.advanceBatch(
      Iterator(next), current, numInputBatches, numOutputRows)

    assert(result eq next)
    // The previous batch is released before the next is fetched.
    assert(currentVector.closeIfFreeableCalls == 1)
    assert(numInputBatches.value == 1)
    assert(numOutputRows.value == 5)
  }

  test("advanceBatch does not release when the current batch is null (first call)") {
    val numInputBatches = new SQLMetric("sum")
    val numOutputRows = new SQLMetric("sum")
    val (next, _) = newBatch(4)

    val result = ColumnarToRowExec.advanceBatch(
      Iterator(next), null, numInputBatches, numOutputRows)

    assert(result eq next)
    assert(numInputBatches.value == 1)
    assert(numOutputRows.value == 4)
  }

  test("advanceBatch returns null and bumps no metrics when the input is exhausted") {
    val numInputBatches = new SQLMetric("sum")
    val numOutputRows = new SQLMetric("sum")
    val (current, currentVector) = newBatch(3)

    val result = ColumnarToRowExec.advanceBatch(
      Iterator.empty, current, numInputBatches, numOutputRows)

    assert(result == null)
    // The current batch is still released even when there is no next batch.
    assert(currentVector.closeIfFreeableCalls == 1)
    assert(numInputBatches.value == 0)
    assert(numOutputRows.value == 0)
  }
}
