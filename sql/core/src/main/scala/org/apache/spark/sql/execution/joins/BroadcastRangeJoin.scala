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

import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.util.ThreadUtils

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * Performs an inner range join on two tables. A range join typically has the following form:
 *
 * SELECT A.*
 *        ,B.*
 * FROM   tableA A
 *        JOIN tableB B
 *         ON A.start <= B.end
 *          AND A.end > B.start
 *
 * The implementation builds a range index from the smaller build side, broadcasts this index
 * to all executors. The streaming side is then matched against the index. This reduces the number
 * of comparisons made by log(n) (n is the number of records in the build table) over the
 * typical solution (Nested Loop Join).
 *
 * TODO NaN values
 * TODO NULL values
 * TODO Outer joins? StreamSide is quite easy/BuildSide requires bookkeeping and
 */
@DeveloperApi
case class BroadcastRangeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    equality: Seq[Boolean],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode {

  private[this] lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  private[this] lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  @transient
  private[this] lazy val buildSideKeyGenerator: Projection =
    newProjection(buildKeys, buildPlan.output)

  @transient
  private[this] lazy val streamSideKeyGenerator: () => MutableProjection =
    newMutableProjection(streamedKeys, streamedPlan.output)

  private[this] val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  // Construct the range index.
  @transient
  private[this] val indexBroadcastFuture = future {
    // Deal with equality.
    val Seq(allowLowEqual: Boolean, allowHighEqual: Boolean) = buildSide match {
      case BuildLeft => equality
      case BuildRight => equality.reverse
    }

    // Get the ordering for the datatype.
    val ordering = TypeUtils.getOrdering(buildKeys.head.dataType)

    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    // TODO find out if the result of a sort and a collect is still sorted.
    val eventifier = RangeIndex.toRangeEvent(buildSideKeyGenerator, ordering)
    val events = buildPlan.execute().collect().flatMap(eventifier)

    // Create the index.
    val index = RangeIndex.build(ordering, events, allowLowEqual, allowHighEqual)

    // Broadcast the index.
    sparkContext.broadcast(index)
  }(BroadcastRangeJoin.broadcastRangeJoinExecutionContext)

  override def doExecute(): RDD[InternalRow] = {
    // Construct the range index.
    val indexBC = Await.result(indexBroadcastFuture, timeout)

    // Iterate over the streaming relation.
    streamedPlan.execute().mapPartitions { stream =>
      val index = indexBC.value
      val streamSideKeys = streamSideKeyGenerator()
      val join = new JoinedRow2 // TODO create our own join row...
      stream.flatMap { sRow =>
        // Get the bounds.
        val lowHigh = streamSideKeys(sRow)
        val low = lowHigh(0)
        val high = lowHigh(1)

        // Only allow non-null keys.
        if (low != null && high != null) {
          index.intersect(low, high).map { bRow =>
            buildSide match {
              case BuildRight => join(sRow, bRow)
              case BuildLeft => join(bRow, sRow)
            }
          }
        }
        else Iterator.empty
      }
    }
  }
}

private[joins] object BroadcastRangeJoin {
  private val broadcastRangeJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-range-join", 128))
}

private[joins] object RangeIndex {
  type RangeEvent = (Any, Int, InternalRow)

  /** Build a range index from an array of unsorted events. */
  def build(ordering: Ordering[Any], events: Array[RangeEvent],
      allowLowEqual: Boolean, allowHighEqual: Boolean): RangeIndex = {
    buildFromSorted(ordering, events.sortBy(_._1)(ordering), allowLowEqual, allowHighEqual)
  }

  /** Build a range index from an array of sorted events. */
  def buildFromSorted(ordering: Ordering[Any], events: Array[RangeEvent],
      allowLowEqual: Boolean, allowHighEqual: Boolean): RangeIndex = {
    // Persisted index components. A dummy null value is added to the array. This makes searching
    // easier and it allows us to deal gracefully with unbound keys.
    val keys = mutable.Buffer[Any](null)
    val activatedRows = mutable.Buffer(Array.empty[InternalRow])
    val activeRows = mutable.Buffer(Array.empty[InternalRow])

    // Current State of the iteration.
    var currentKey: Any = null
    var currentRowCount = 0
    val currentActivatedRows = mutable.Buffer.empty[InternalRow]
    val currentActiveRows = mutable.Buffer.empty[InternalRow]

    // Store the current key state in the final results.
    def finishKey = {
      if (currentRowCount > 0) {
        keys += currentKey
        activatedRows += currentActivatedRows.toArray
        activeRows += currentActiveRows.toArray
      }
    }
    events.foreach {
      case (key, flow, row) =>
        // Check if we have finished processing a key
        if (currentKey != key) {
          finishKey
          currentKey = key
          currentActivatedRows.clear()
          currentRowCount = 0
        }

        // Keep track of rows.
        flow match {
          case 1 =>
            currentActiveRows += row
            currentActivatedRows += row
          case -1 =>
            currentActiveRows -= row
        }
        currentRowCount += 1
    }

    // Store the final events.
    finishKey

    // Determine corrections based on equality
    val lowBoundEqualCorrection = if (allowLowEqual) 1 else 0
    val highBoundEqualCorrection = if (allowHighEqual) 0 else 1

    // Create the index.
    new RangeIndex(ordering, keys.toArray, activeRows.toArray, activatedRows.toArray,
      lowBoundEqualCorrection, highBoundEqualCorrection)
  }

  /** Create a function that turns a row into its respective range events. */
  def toRangeEvent(lowHighExtr: Projection, cmp: Ordering[Any]):
      (InternalRow => Seq[RangeEvent]) = {
    (row: InternalRow) => {
      val Row(low, high) = lowHighExtr(row)
      // Valid points and intervals.
      if (low != null && high != null && cmp.compare(low, high) <= 0) {
        val copy = row.copy()
        (low, 1, copy) ::(high, -1, copy) :: Nil
      }
      // Nulls
      else Nil
    }
  }
}

/**
 * A range index is an data structure which can be used to efficiently execute range queries upon. A
 * range query has a lower and an upper bound, the result of a range query is a iterator of rows
 * that match the given constraints.
 *
 * @param ordering used for sorting keys, comparing keys and values, and retrieving the rows in a
 *                 given interval.
 * @param keys which are used for finding the active and activated rows. A key is used when
 *             something changes, an event occurs (if you will), in the composition of the active
 *             or activated rows.
 * @param active contains the rows that are 'active' between the current key and the next key.
 *               This is only used for rows that span an interval.
 * @param activated contains the row that have been activated at the current key. This contains
 *                  both rows that span an interval or only exist at one point
 * @param lowBoundEqualCorrection correction to apply to the lower bound index, when the value
 *                                queried equals the key.
 * @param highBoundEqualCorrection correction to apply to the upper bound index, when the value
 *                                 queried equals the key.
 */
private[joins] class RangeIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val active: Array[Array[InternalRow]],
    private[this] val activated: Array[Array[InternalRow]],
    private[this] val lowBoundEqualCorrection: Int,
    private[this] val highBoundEqualCorrection: Int) extends Serializable {

  /**
   * Find the index of the closest key lower than or equal to the value given. When a value is
   * equal to the found key, the result is corrected.
   *
   * This method is tail recursive.
   *
   * @param value to find the closest lower or equal key index for.
   * @param equalCorrection to correct the result with in case of equality.
   * @param first index (inclusive) to start searching at.
   * @param last index (inclusive) to stop searching at.
   * @return the index of the closest upper bound.
   */
  @tailrec
  final def closestLowerKey(value: Any, equalCorrection: Int,
      first: Int = 0, last: Int = keys.length - 1): Int = {
    if (first < last) {
      val index = first + ((last - first) >> 1)
      val key = keys(index)
      val cmp = if (key == null) -1
      else ordering.compare(value, key)
      if (cmp == 0) index - equalCorrection
      else if (cmp < 0) closestLowerKey(value, equalCorrection, first, index)
      else closestLowerKey(value, equalCorrection, index + 1, last)
    } else first
  }

  /**
   * Calculate the intersection between the index and a given range.
   *
   * @param low point of the range. Note that a NULL value is currently interpreted as an unbound
   *            (negative infinite) value.
   * @param high point of the range to intersect with. Note that a NULL value is currently
   *            interpreted as an unbound (infinite) value.
   * @return an iterator containing all the rows that fall within the given range.
   */
  final def intersect(low: Any, high: Any): Iterator[InternalRow] = {
    // Find first index by searching for the last key lower than the low value.
    val first = if (low == null) 0
    else closestLowerKey(low, lowBoundEqualCorrection)

    // Find last index by searching for the last lower than the high value.
    val last = if (high == null || first == keys.length - 1) keys.length - 1
    else closestLowerKey(high, highBoundEqualCorrection, first)

    // Return the iterator.
    new Iterator[InternalRow] {
      var index = first
      var rowIndex = 0
      var rows = active(index)

      def hasNext: Boolean = {
        var result = rows != null && rowIndex < rows.length
        while (!result && index < last) {
          index += 1
          rowIndex = 0
          rows = activated(index)
          result = rows != null && rowIndex < rows.length
        }
        result
      }

      def next() = {
        val row = rows(rowIndex)
        rowIndex += 1
        row
      }
    }
  }
}

