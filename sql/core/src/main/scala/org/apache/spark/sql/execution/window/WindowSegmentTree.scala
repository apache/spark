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

package org.apache.spark.sql.execution.window

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, MutableProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

/**
 * Stub for TDD red phase. Real implementation lands in the subsequent commit.
 * See `the class documentation`.
 */
class WindowSegmentTree(
    functions: Array[DeclarativeAggregate],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    fanout: Int = WindowSegmentTree.DEFAULT_FANOUT,
    blockSize: Int = WindowSegmentTree.DEFAULT_BLOCK_SIZE,
    maxCachedBlocks: Int = -1)
  extends AutoCloseable {

  def build(rows: Iterator[InternalRow], numRowsHint: Int): Unit =
    throw new UnsupportedOperationException("WindowSegmentTree.build: stub (TDD red)")

  def query(lo: Int, hi: Int, outBuffer: InternalRow): Unit =
    throw new UnsupportedOperationException("WindowSegmentTree.query: stub (TDD red)")

  def size: Int = throw new UnsupportedOperationException("WindowSegmentTree.size: stub (TDD red)")

  override def close(): Unit = ()

  private[window] def peekBlockCount: Int =
    throw new UnsupportedOperationException("WindowSegmentTree.peekBlockCount: stub (TDD red)")

  private[window] def peekLevelSize(blockIdx: Int, level: Int): Int =
    throw new UnsupportedOperationException("WindowSegmentTree.peekLevelSize: stub (TDD red)")

  private[window] def peekLevelCount(blockIdx: Int): Int =
    throw new UnsupportedOperationException("WindowSegmentTree.peekLevelCount: stub (TDD red)")
}

object WindowSegmentTree {
  val DEFAULT_FANOUT: Int = 16
  val DEFAULT_BLOCK_SIZE: Int = 65536
}
