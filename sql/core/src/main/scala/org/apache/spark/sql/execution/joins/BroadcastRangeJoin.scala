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
 * TODO This join will maintain sort order. The build side rows will also be added in a lower
 *      bound sorted fashion.
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
      case BuildLeft => equality.reverse
      case BuildRight => equality
    }

    // Get the ordering for the datatype.
    val ordering = TypeUtils.getOrdering(buildKeys.head.dataType)

    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    // TODO find out if the result of a sort and a collect is still sorted.
    val eventifier = RangeIndex.toRangeEvent(buildSideKeyGenerator, ordering)
    val events = buildPlan.execute().map(_.copy()).collect().flatMap(eventifier)

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
      new Iterator[InternalRow] {
        private[this] val index = indexBC.value
        private[this] val streamSideKeys = streamSideKeyGenerator()
        private[this] val join = new JoinedRow2 // TODO create our own join row...
        private[this] var row: InternalRow = EmptyRow
        private[this] var iterator: Iterator[InternalRow] = Iterator.empty

        override final def hasNext: Boolean = {
          var result = iterator.hasNext
          while (!result && stream.hasNext) {
            row = stream.next()
            val lowHigh = streamSideKeys(row)
            val low = lowHigh(0)
            val high = lowHigh(1)
            if (low != null && high != null) {
              iterator = index.intersect(low, high)
            }
            result = iterator.hasNext
          }
          result
        }

        override final def next(): InternalRow = {
          buildSide match {
            case BuildRight => join(row, iterator.next())
            case BuildLeft => join(iterator.next(), row)
          }
        }
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
    val eventOrdering = Ordering.Tuple2(ordering, Ordering.Int)
    val sortedEvents = events.sortBy(e => (e._1, e._2))(eventOrdering)
    buildFromSorted(ordering, sortedEvents, allowLowEqual, allowHighEqual)
  }

  /** Build a range index from an array of sorted events. */
  def buildFromSorted(ordering: Ordering[Any], events: Array[RangeEvent],
      allowLowEqual: Boolean, allowHighEqual: Boolean): RangeIndex = {
    // Persisted index components. A dummy null value is added to the array. This makes searching
    // easier and it allows us to deal gracefully with unbound keys.
    val empty = Array.empty[InternalRow]
    val keys = mutable.Buffer[Any](null)
    val offsets = mutable.Buffer[Int](0)
    val activatedRows = mutable.Buffer.empty[InternalRow]
    val activeNewOffsets = mutable.Buffer.empty[Int]
    val activeRows = mutable.Buffer.empty[Array[InternalRow]]

    // Current State of the iteration.
    var currentKey: Any = null
    var currentActiveNewOffset: Int = -1
    val currentActiveRows = mutable.Buffer.empty[InternalRow]

    // Store the currently active rows.
    def writeActiveRows = {
      activeNewOffsets += currentActiveNewOffset
      if (currentActiveRows.isEmpty) activeRows += empty
      else activeRows += currentActiveRows.toArray
    }
    events.foreach {
      case (key, flow, row) =>
        // Check if we have finished processing a key
        if (currentKey != key) {
          writeActiveRows
          currentKey = key
          currentActiveNewOffset = -1
          keys += key
          offsets += activatedRows.size
        }

        // Store the offset at which we are starting to add rows to the 'active' buffer.
        if (flow >= 0 && currentActiveNewOffset == -1) {
          currentActiveNewOffset = currentActiveRows.size
        }

        // Keep track of rows.
        flow match {
          case 1 =>
            activatedRows += row
            currentActiveRows += row
          case 0 =>
            activatedRows += row
          case -1 =>
            currentActiveRows -= row
        }
    }

    // Store the final array of activate rows.
    writeActiveRows

    // Determine corrections based on equality
    val lowBoundEqualCorrection = if (allowLowEqual) 1 else 0
    val highBoundEqualCorrection = if (allowHighEqual) 0 else 1

    // Create the index.
    new RangeIndex(ordering, keys.toArray, offsets.toArray, activeNewOffsets.toArray,
      activeRows.toArray, activatedRows.toArray, lowBoundEqualCorrection, highBoundEqualCorrection)
  }

  /** Create a function that turns a row into its respective range events. */
  def toRangeEvent(lowHighExtr: Projection, cmp: Ordering[Any]):
      (InternalRow => Seq[RangeEvent]) = {
    (row: InternalRow) => {
      val Row(low, high) = lowHighExtr(row)
      // Valid points and intervals.
      if (low != null && high != null) {
        val result = cmp.compare(low, high)
        // Point
        if (result == 0) {
          (low, 0, row) :: Nil
        }
        // Interval
        else if (result < 0) {
          (low, 1, row) ::(high, -1, row) :: Nil
        }
        // Reversed Interval (low > high) - Cannot join on this record.
        else Nil
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
 * @param activeNewOffsets array contains the index at which the rows in the active array are new.
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
    private[this] val offsets: Array[Int],
    private[this] val activeNewOffsets: Array[Int],
    private[this] val active: Array[Array[InternalRow]],
    private[this] val activated: Array[InternalRow],
    private[this] val lowBoundEqualCorrection: Int,
    private[this] val highBoundEqualCorrection: Int) extends Serializable {

  private[this] val maxKeyIndex = keys.length - 1

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
      first: Int = 0, last: Int = maxKeyIndex): Int = {
    // Determine the mid point.
    val mid = first + ((last - first + 1) >>> 1)

    // Compare the value with the key at the mid point.
    // Note that a value is always larger than NULL.
    val key = keys(mid)
    val cmp = if (key == null) 1
    else ordering.compare(value, key)

    // Value == Key. Keys are unique so we can stop.
    if (cmp == 0) mid - equalCorrection
    // No more elements left to search.
    else if (first == last) mid
    // Value > Key: Search the top half of the key array.
    else if (cmp > 0) closestLowerKey(value, equalCorrection, mid, last)
    // Value < Key: Search the lower half of the array.
    else closestLowerKey(value, equalCorrection, first, mid - 1)
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
    val last = if (high == null || first == maxKeyIndex) maxKeyIndex
    else closestLowerKey(high, highBoundEqualCorrection, first)

    new Iterator[InternalRow] {
      var activatedAvailable = first < last
      var rowIndex = 0
      var rows = active(first)
      var rowLength = if (first <= last) rows.length
      else activeNewOffsets(first)

      override final def hasNext: Boolean = {
        var result = rowIndex < rowLength
        if (!result && activatedAvailable) {
          activatedAvailable = false
          rows = activated
          rowIndex = offsets(first + 1)
          rowLength = if (last == maxKeyIndex) activated.length
          else offsets(last + 1)
          result = rowIndex < rowLength
        }
        result
      }

      override final def next(): InternalRow = {
        val row = rows(rowIndex)
        rowIndex += 1
        row
      }
    }
  }

  /**
   * Create a textual representation of the index for debugging purposes.
   *
   * @param maxKeys maximum number of keys shows in the string.
   * @return a textual representation of the index for debugging purposes.
   */
  def toDebugString(maxKeys: Int = Int.MaxValue): String = {
    val builder = new StringBuilder
    builder.append("Index[lowBoundEqualCorrection = ")
    builder.append(lowBoundEqualCorrection)
    builder.append(", highBoundEqualCorrection = ")
    builder.append(highBoundEqualCorrection)
    builder.append("]")
    val keysShown = math.min(keys.length, maxKeys)
    val keysLeft = keys.length - keysShown
    for (i <- 0 until keysShown) {
      builder.append("\n  +[")
      builder.append(keys(i))
      builder.append("]@")
      builder.append(offsets(i))
      builder.append("\n  | Active: ")
      builder.append(active(i).mkString(","))
      builder.append("\n  | Activated: ")
      val nextOffset = if (i == maxKeyIndex) activated.length
      else offsets(i + 1)
      builder.append(activated.slice(offsets(i), nextOffset).mkString(","))
    }
    if (keysLeft > 0) {
      builder.append("\n  (")
      builder.append(keysLeft)
      builder.append(" keys left)")
    }
    builder.toString
  }

  override def toString: String = toDebugString(10)
}

