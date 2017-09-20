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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, Literal, NamedExpression, PreciseTimestampConversion, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}


/**
 * Performs stream-stream join using symmetric hash join algorithm. It works as follows.
 *
 *                             /-----------------------\
 *   left side input --------->|    left side state    |------\
 *                             \-----------------------/      |
 *                                                            |--------> joined output
 *                             /-----------------------\      |
 *   right side input -------->|    right side state   |------/
 *                             \-----------------------/
 *
 * Each join side buffers past input rows as streaming state so that the past input can be joined
 * with future input on the other side. This buffer state is effectively a multi-map:
 *    equi-join key -> list of past input rows received with the join key
 *
 * For each input row in each side, the following operations take place.
 * - Calculate join key from the row.
 * - Use the join key to append the row to the buffer state of the side that the row came from.
 * - Find past buffered values for the key from the other side. For each such value, emit the
 *   "joined row" (left-row, right-row)
 * - Apply the optional condition to filter the joined rows as the final output.
 *
 * If a timestamp column with event time watermark is present in the join keys or in the input
 * data, then the it uses the watermark figure out which rows in the buffer will not join with
 * and the new data, and therefore can be discarded. Depending on the provided query conditions, we
 * can define thresholds on both state key (i.e. joining keys) and state value (i.e. input rows).
 * There are three kinds of queries possible regarding this as explained below.
 * Assume that watermark has been defined on both `leftTime` and `rightTime` columns used below.
 *
 * 1. When timestamp/time-window + watermark is in the join keys. Example (pseudo-SQL):
 *
 *      SELECT * FROM leftTable, rightTable
 *      ON
 *        leftKey = rightKey AND
 *        window(leftTime, "1 hour") = window(rightTime, "1 hour")    // 1hr tumbling windows
 *
 *    In this case, this operator will join rows newer than watermark which fall in the same
 *    1 hour window. Say the event-time watermark is "12:34" (both left and right input).
 *    Then input rows can only have time > 12:34. Hence, they can only join with buffered rows
 *    where window >= 12:00 - 1:00 and all buffered rows with join window < 12:00 can be
 *    discarded. In other words, the operator will discard all state where
 *    window in state key (i.e. join key) < event time watermark. This threshold is called
 *    State Key Watermark.
 *
 * 2. When timestamp range conditions are provided (no time/window + watermark in join keys). E.g.
 *
 *      SELECT * FROM leftTable, rightTable
 *      ON
 *        leftKey = rightKey AND
 *        leftTime > rightTime - INTERVAL 8 MINUTES AND leftTime < rightTime + INTERVAL 1 HOUR
 *
 *   In this case, the event-time watermark and the BETWEEN condition can be used to calculate a
 *   state watermark, i.e., time threshold for the state rows that can be discarded.
 *   For example, say each join side has a time column, named "leftTime" and
 *   "rightTime", and there is a join condition "leftTime > rightTime - 8 min".
 *   While processing, say the watermark on right input is "12:34". This means that from henceforth,
 *   only right inputs rows with "rightTime > 12:34" will be processed, and any older rows will be
 *   considered as "too late" and therefore dropped. Then, the left side buffer only needs
 *   to keep rows where "leftTime > rightTime - 8 min > 12:34 - 8m > 12:26".
 *   That is, the left state watermark is 12:26, and any rows older than that can be dropped from
 *   the state. In other words, the operator will discard all state where
 *   timestamp in state value (input rows) < state watermark. This threshold is called
 *   State Value Watermark (to distinguish from the state key watermark).
 *
 *   Note:
 *   - The event watermark value of one side is used to calculate the
 *     state watermark of the other side. That is, a condition ~ "leftTime > rightTime + X" with
 *     right side event watermark is used to calculate the left side state watermark. Conversely,
 *     a condition ~ "left < rightTime + Y" with left side event watermark is used to calculate
 *     right side state watermark.
 *   - Depending on the conditions, the state watermark maybe different for the left and right
 *     side. In the above example, leftTime > 12:26 AND rightTime > 12:34 - 1 hour = 11:34.
 *   - State can be dropped from BOTH sides only when there are conditions of the above forms that
 *     define time bounds on timestamp in both directions.
 *
 * 3. When both window in join key and time range conditions are present, case 1 + 2.
 *    In this case, since window equality is a stricter condition than the time range, we can
 *    use the the State Key Watermark = event time watermark to discard state (similar to case 1).
 *
 * @param leftKeys  Expression to generate key rows for joining from left input
 * @param rightKeys Expression to generate key rows for joining from right input
 * @param joinType  Type of join (inner, left outer, etc.)
 * @param condition Optional, additional condition to filter output of the equi-join
 * @param stateInfo Version information required to read join state (buffered rows)
 * @param eventTimeWatermark Watermark of input event, same for both sides
 * @param stateWatermarkPredicates Predicates for removal of state, see
 *                                 [[JoinStateWatermarkPredicates]]
 * @param left      Left child plan
 * @param right     Right child plan
 */
case class StreamingSymmetricHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermark: Option[Long],
    stateWatermarkPredicates: JoinStateWatermarkPredicates,
    left: SparkPlan,
    right: SparkPlan) extends SparkPlan with BinaryExecNode with StateStoreWriter {

  def this(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan) = {
    this(
      leftKeys, rightKeys, joinType, condition, stateInfo = None, eventTimeWatermark = None,
      stateWatermarkPredicates = JoinStateWatermarkPredicates(), left, right)
  }

  require(joinType == Inner, s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType))

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator
    val stateStoreNames = SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)
    left.execute().stateStoreAwareZipPartitions(
      right.execute(), stateInfo.get, stateStoreNames, stateStoreCoord)(processPartitions)
  }

  private def processPartitions(
      leftInputIter: Iterator[InternalRow],
      rightInputIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (stateInfo.isEmpty) {
      throw new IllegalStateException(s"Cannot execute join as state info was not specified\n$this")
    }

    val numOutputRows = longMetric("numOutputRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")

    val updateStartTimeNs = System.nanoTime
    val joinedRow = new JoinedRow

    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter, stateWatermarkPredicates.left)
    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter, stateWatermarkPredicates.right)

    //  Join one side input using the other side's buffered/state rows. Here is how it is done.
    //
    //  - `leftJoiner.joinWith(rightJoiner)` generates all rows from matching new left input with
    //    stored right input, and also stores all the left input
    //
    //  - `rightJoiner.joinWith(leftJoiner)` generates all rows from matching new right input with
    //    stored left input, and also stores all the right input. It also generates all rows from
    //    matching new left input with new right input, since the new left input has become stored
    //    by that point. This tiny asymmetry is necessary to avoid duplication.
    val leftOutputIter = leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner) {
      (inputRow: UnsafeRow, matchedRow: UnsafeRow) =>
        joinedRow.withLeft(inputRow).withRight(matchedRow)
    }
    val rightOutputIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner) {
      (inputRow: UnsafeRow, matchedRow: UnsafeRow) =>
        joinedRow.withLeft(matchedRow).withRight(inputRow)
    }

    // Filter the joined rows based on the given condition.
    val outputFilterFunction =
      newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output).eval _
    val filteredOutputIter =
      (leftOutputIter ++ rightOutputIter).filter(outputFilterFunction).map { row =>
        numOutputRows += 1
        row
      }

    // Function to remove old state after all the input has been consumed and output generated
    def onOutputCompletion = {
      allUpdatesTimeMs += math.max(NANOSECONDS.toMillis(System.nanoTime - updateStartTimeNs), 0)

      // Remove old state if needed
      allRemovalsTimeMs += timeTakenMs {
        leftSideJoiner.removeOldState()
        rightSideJoiner.removeOldState()
      }

      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
        val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
        val combinedMetrics = StateStoreMetrics.combine(Seq(leftSideMetrics, rightSideMetrics))

        // Update SQL metrics
        numUpdatedStateRows +=
          (leftSideJoiner.numUpdatedStateRows + rightSideJoiner.numUpdatedStateRows)
        numTotalStateRows += combinedMetrics.numKeys
        stateMemory += combinedMetrics.memoryUsedBytes
        combinedMetrics.customMetrics.foreach { case (metric, value) =>
          longMetric(metric.name) += value
        }
      }
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](filteredOutputIter, onOutputCompletion)
  }

  /**
   * Internal helper class to consume input rows, generate join output rows using other sides
   * buffered state rows, and finally clean up this sides buffered state rows
   */
  private class OneSideHashJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate]) {

    private val joinStateManager = new SymmetricHashJoinStateManager(
      joinSide, inputAttributes, joinKeys, stateInfo, storeConf, hadoopConfBcast.value.value)
    private[this] val keyGenerator = UnsafeProjection.create(joinKeys, inputAttributes)

    private[this] val stateKeyWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateKeyWatermarkPredicate(expr)) =>
        // inputSchema can be empty as expr should only have BoundReferences and does not require
        // the schema to generated predicate. See [[StreamingSymmetricHashJoinHelper]].
        newPredicate(expr, Seq.empty).eval _
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _ // false = do not remove if no predicate
    }

    private[this] val stateValueWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateValueWatermarkPredicate(expr)) =>
        newPredicate(expr, inputAttributes).eval _
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _  // false = do not remove if no predicate
    }

    private[this] var updatedStateRowsCount = 0

    /**
     * Generate joined rows by consuming input from this side, and matching it with the buffered
     * rows (i.e. state) of the other side.
     * @param otherSideJoiner   Joiner of the other side
     * @param generateJoinedRow Function to generate the joined row from the
     *                          input row from this side and the matched row from the other side
     */
    def storeAndJoinWithOtherSide(
        otherSideJoiner: OneSideHashJoiner)(
        generateJoinedRow: (UnsafeRow, UnsafeRow) => JoinedRow): Iterator[InternalRow] = {

      val watermarkAttribute = inputAttributes.find(_.metadata.contains(delayKey))
      val nonLateRows =
        WatermarkSupport.watermarkExpression(watermarkAttribute, eventTimeWatermark) match {
          case Some(watermarkExpr) =>
            val predicate = newPredicate(watermarkExpr, inputAttributes)
            inputIter.filter { row => !predicate.eval(row) }
          case None =>
            inputIter
        }

      nonLateRows.flatMap { row =>
        val thisRow = row.asInstanceOf[UnsafeRow]
        val key = keyGenerator(thisRow)
        val outputIter = otherSideJoiner.joinStateManager.get(key).map { thatRow =>
          generateJoinedRow(thisRow, thatRow)
        }
        val shouldAddToState = // add only if both removal predicates do not match
          !stateKeyWatermarkPredicateFunc(key) && !stateValueWatermarkPredicateFunc(thisRow)
        if (shouldAddToState) {
          joinStateManager.append(key, thisRow)
          updatedStateRowsCount += 1
        }
        outputIter
      }
    }

    /** Remove old buffered state rows using watermarks for state keys and values */
    def removeOldState(): Unit = {
      stateWatermarkPredicate match {
        case Some(JoinStateKeyWatermarkPredicate(expr)) =>
          joinStateManager.removeByKeyCondition(stateKeyWatermarkPredicateFunc)
        case Some(JoinStateValueWatermarkPredicate(expr)) =>
          joinStateManager.removeByValueCondition(stateValueWatermarkPredicateFunc)
        case _ =>
      }
    }

    /** Commit changes to the buffer state and return the state store metrics */
    def commitStateAndGetMetrics(): StateStoreMetrics = {
      joinStateManager.commit()
      joinStateManager.metrics
    }

    def numUpdatedStateRows: Long = updatedStateRowsCount
  }
}
