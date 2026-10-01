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

import java.util.Queue

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class BufferedRowIteratorSuite extends SparkFunSuite {

  // Mirrors the private SHRINK_BUFFER_THRESHOLD constant in BufferedRowIterator.
  private val shrinkThreshold = 1024

  /**
   * A minimal BufferedRowIterator whose `processNext()` emits one batch per entry in `batchSizes`,
   * appending that many (non-null) rows. Exposes the internal buffer so a test can observe whether
   * it is replaced across batches.
   */
  private class FixedBatchIterator(batchSizes: Int*) extends BufferedRowIterator {
    private val batches = batchSizes.iterator
    private val row: InternalRow = new GenericInternalRow(0)

    override def init(index: Int, iters: Array[Iterator[InternalRow]]): Unit = {}

    override protected def processNext(): Unit = {
      if (batches.hasNext) {
        var remaining = batches.next()
        while (remaining > 0) {
          append(row)
          remaining -= 1
        }
      }
    }

    def buffer: Queue[InternalRow] = currentRows
  }

  test("a batch above the shrink threshold is replaced with a fresh buffer on the next refill") {
    val iter = new FixedBatchIterator(shrinkThreshold + 1, 1)
    assert(iter.hasNext)
    val firstBuffer = iter.buffer
    assert(firstBuffer.size() == shrinkThreshold + 1)
    // Drain the batch directly (no hasNext, so it does not refill mid-drain).
    (0 until shrinkThreshold + 1).foreach(_ => iter.next())
    assert(firstBuffer.isEmpty)
    assert(iter.hasNext)
    assert(iter.buffer ne firstBuffer, "buffer should be replaced after a batch exceeds threshold")
  }

  test("a batch at the shrink threshold reuses the same buffer") {
    val iter = new FixedBatchIterator(shrinkThreshold, 1)
    assert(iter.hasNext)
    val firstBuffer = iter.buffer
    assert(firstBuffer.size() == shrinkThreshold)
    (0 until shrinkThreshold).foreach(_ => iter.next())
    assert(firstBuffer.isEmpty)
    assert(iter.hasNext)
    assert(iter.buffer eq firstBuffer, "buffer should be reused at the threshold")
  }
}
