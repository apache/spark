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

package org.apache.spark.sql.execution.joins

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class SortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  @transient protected lazy val leftKeyGenerator = newProjection(leftKeys, left.output)
  @transient protected lazy val rightKeyGenerator = newProjection(rightKeys, right.output)

  protected[this] def isUnsafeMode: Boolean = {
    (codegenEnabled && unsafeEnabled
      && UnsafeProjection.canSupport(leftKeys)
      && UnsafeProjection.canSupport(schema))
  }

  override def outputsUnsafeRows: Boolean = isUnsafeMode
  override def canProcessUnsafeRows: Boolean = isUnsafeMode
  override def canProcessSafeRows: Boolean = !isUnsafeMode

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      new Iterator[InternalRow] {
        // An ordering that can be used to compare keys from both sides.
        private[this] val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
        private[this] var currentLeftRow: InternalRow = _
        private[this] var currentRightMatches: ArrayBuffer[InternalRow] = _
        private[this] var currentMatchIdx: Int = -1
        private[this] val smjScanner = new SortMergeJoinScanner(
          leftKeyGenerator,
          rightKeyGenerator,
          keyOrdering,
          RowIterator.fromScala(leftIter),
          RowIterator.fromScala(rightIter)
        )
        private[this] val joinRow = new JoinedRow
        private[this] val resultProjection: (InternalRow) => InternalRow = {
          if (isUnsafeMode) {
            UnsafeProjection.create(schema)
          } else {
            identity[InternalRow]
          }
        }

        override final def hasNext: Boolean =
          (currentMatchIdx != -1 && currentMatchIdx < currentRightMatches.length) || fetchNext()

        private[this] def fetchNext(): Boolean = {
          if (smjScanner.findNextInnerJoinRows()) {
            currentRightMatches = smjScanner.getBufferedMatches
            currentLeftRow = smjScanner.getStreamedRow
            currentMatchIdx = 0
            true
          } else {
            currentRightMatches = null
            currentLeftRow = null
            currentMatchIdx = -1
            false
          }
        }

        override def next(): InternalRow = {
          if (currentMatchIdx == -1 || currentMatchIdx == currentRightMatches.length) {
            fetchNext()
          }
          val joinedRow = joinRow(currentLeftRow, currentRightMatches(currentMatchIdx))
          currentMatchIdx += 1
          resultProjection(joinedRow)
        }
      }
    }
  }
}

/**
 * Helper class that is used to implement [[SortMergeJoin]] and [[SortMergeOuterJoin]].
 *
 * The streamed input is the left side of a left outer join or the right side of a right outer join.
 *
 * To perform an inner (outer) join, users of this class call [[findNextInnerJoinRows()]]
 * ([[findNextOuterJoinRows()]]), which returns `true` if a result has been produced and `false`
 * otherwise. If a result has been produced, then the caller may call [[getStreamedRow]] to return
 * the matching row from the streamed input and may call [[getBufferedMatches]] to return the
 * sequence of matching rows from the buffered input (in the case of an outer join, this will return
 * an empty sequence). For efficiency, both of these methods return mutable objects which are
 * re-used across calls to the `findNext*JoinRows()` methods.
 *
 * @param streamedKeyGenerator a projection that produces join keys from the streamed input.
 * @param bufferedKeyGenerator a projection that produces join keys from the buffered input.
 * @param keyOrdering an ordering which can be used to compare join keys.
 * @param streamedIter an input whose rows will be streamed.
 * @param bufferedIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 */
private[joins] class SortMergeJoinScanner(
    streamedKeyGenerator: Projection,
    bufferedKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    streamedIter: RowIterator,
    bufferedIter: RowIterator) {
  private[this] var streamedRow: InternalRow = _
  private[this] var streamedRowKey: InternalRow = _
  private[this] var bufferedRow: InternalRow = _
  private[this] var bufferedRowKey: InternalRow = _
  /**
   * The join key for the rows buffered in `bufferedMatches`, or null if `bufferedMatches` is empty
   */
  private[this] var matchJoinKey: InternalRow = _
  /** Buffered rows from the buffered side of the join. This is empty if there are no matches. */
  private[this] val bufferedMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // Initialization (note: do _not_ want to advance streamed here).
  advancedBuffered()

  // --- Public methods ---------------------------------------------------------------------------

  def getStreamedRow: InternalRow = streamedRow

  def getBufferedMatches: ArrayBuffer[InternalRow] = bufferedMatches

  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   * @return true if matching rows have been found and false otherwise. If this returns true, then
   *         [[getStreamedRow]] and [[getBufferedMatches]] can be called to construct the join
   *         results.
   */
  final def findNextInnerJoinRows(): Boolean = {
    while (advancedStreamed() && streamedRowKey.anyNull) {
      // Advance the streamed side of the join until we find the next row whose join key contains
      // no nulls or we hit the end of the streamed iterator.
    }
    if (streamedRow == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      false
    } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
      // The new streamed row has the same join key as the previous row, so return the same matches.
      true
    } else if (bufferedRow == null) {
      // The streamed row's join key does not match the current batch of buffered rows and there are
      // no more rows to read from the buffered iterator, so there can be no more matches.
      false
    } else {
      // Advance both the streamed and buffered iterators to find the next pair of matching rows.
      var comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
      do {
        if (streamedRowKey.anyNull) {
          advancedStreamed()
        } else if (bufferedRowKey.anyNull) {
          advancedBuffered()
        } else {
          comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          if (comp > 0) advancedBuffered()
          else if (comp < 0) advancedStreamed()
        }
      } while (streamedRow != null && bufferedRow != null && comp != 0)
      if (streamedRow == null || bufferedRow == null) {
        // We have either hit the end of one of the iterators, so there can be no more matches.
        false
      } else {
        // The streamed row's join key matches the current buffered row's join, so walk through the
        // buffered iterator to buffer the rest of the matching rows.
        assert(comp == 0)
        bufferMatchingRows()
        true
      }
    }
  }

  /**
   * Advances the streamed input iterator and buffers all rows from the buffered input that
   * have matching keys.
   * @return true if the streamed iterator returned a row, false otherwise. If this returns true,
   *         then [getStreamedRow and [[getBufferedMatches]] can be called to produce the outer
   *         join results.
   */
  final def findNextOuterJoinRows(): Boolean = {
    if (advancedStreamed()) {
      if (streamedRowKey.anyNull) {
        // Since at least one join column is null, the streamed row has no matches.
        matchJoinKey = null
        bufferedMatches.clear()
      } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
        // Matches the current group, so do nothing.
      } else {
        // The streamed row does not match the current group.
        matchJoinKey = null
        bufferedMatches.clear()
        if (bufferedRow != null) {
          // The buffered iterator could still contain matching rows, so we'll need to walk through
          // it until we either find matches or pass where they would be found.
          var comp =
            if (bufferedRowKey.anyNull) 1 else keyOrdering.compare(streamedRowKey, bufferedRowKey)
          while (comp > 0 && advancedBuffered()) {
            comp = if (bufferedRowKey.anyNull) {
              1
            } else {
              keyOrdering.compare(streamedRowKey, bufferedRowKey)
            }
          }
          if (comp == 0) {
            // We have found matches, so buffer them (this updates matchJoinKey)
            bufferMatchingRows()
          } else {
            // We have overshot the position where the row would be found, hence no matches.
          }
        }
      }
      // If there is a streamed input, then we always return true
      true
    } else {
      // End of streamed input, hence no more results.
      false
    }
  }


  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the streamed iterator and compute the new row's join key.
   * @return true if the streamed iterator returned a row and false otherwise.
   */
  private def advancedStreamed(): Boolean = {
    if (streamedIter.advanceNext()) {
      streamedRow = streamedIter.getRow
      streamedRowKey = streamedKeyGenerator(streamedRow)
      true
    } else {
      streamedRow = null
      streamedRowKey = null
      false
    }
  }

  /**
   * Advance the buffered iterator and compute the new row's join key.
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  private def advancedBuffered(): Boolean = {
    if (bufferedIter.advanceNext()) {
      bufferedRow = bufferedIter.getRow
      bufferedRowKey = bufferedKeyGenerator(bufferedRow)
      true
    } else {
      bufferedRow = null
      bufferedRowKey = null
      false
    }
  }

  /**
   * Called when the streamed and buffered join keys match in order to buffer the matching rows.
   */
  private def bufferMatchingRows(): Unit = {
    assert(streamedRowKey != null)
    assert(!streamedRowKey.anyNull)
    assert(bufferedRowKey != null)
    assert(!bufferedRowKey.anyNull)
    assert(keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
    // This join key may have been produced by a mutable projection, so we need to make a copy:
    matchJoinKey = streamedRowKey.copy()
    bufferedMatches.clear()
    do {
      bufferedMatches += bufferedRow.copy() // need to copy mutable rows before buffering them
      advancedBuffered()
    } while (
      bufferedRow != null &&
      !bufferedRowKey.anyNull &&
      keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0
    )
  }
}
