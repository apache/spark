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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SessionState
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
 * @param condition Conditions to filter rows, split by left, right, and joined. See
 *                  [[JoinConditionSplitPredicates]]
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
    condition: JoinConditionSplitPredicates,
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
      leftKeys, rightKeys, joinType, JoinConditionSplitPredicates(condition, left, right),
      stateInfo = None, eventTimeWatermark = None,
      stateWatermarkPredicates = JoinStateWatermarkPredicates(), left, right)
  }

  private def throwBadJoinTypeException(): Nothing = {
    throw new IllegalArgumentException(
      s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  }

  require(
    joinType == Inner || joinType == LeftOuter || joinType == RightOuter,
    s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType))

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  val nullLeft = new GenericInternalRow(left.output.map(_.withNullability(true)).length)
  val nullRight = new GenericInternalRow(right.output.map(_.withNullability(true)).length)

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys, stateInfo.map(_.numPartitions)) ::
      HashClusteredDistribution(rightKeys, stateInfo.map(_.numPartitions)) :: Nil

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
    case _ => throwBadJoinTypeException()
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => PartitioningCollection(Seq(left.outputPartitioning))
    case RightOuter => PartitioningCollection(Seq(right.outputPartitioning))
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    val watermarkUsedForStateCleanup =
      stateWatermarkPredicates.left.nonEmpty || stateWatermarkPredicates.right.nonEmpty

    // Latest watermark value is more than that used in this previous executed plan
    val watermarkHasChanged =
      eventTimeWatermark.isDefined && newMetadata.batchWatermarkMs > eventTimeWatermark.get

    watermarkUsedForStateCleanup && watermarkHasChanged
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


    val postJoinFilter =
      newPredicate(condition.bothSides.getOrElse(Literal(true)), left.output ++ right.output).eval _
    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter,
      condition.leftSideOnly, postJoinFilter, stateWatermarkPredicates.left)
    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter,
      condition.rightSideOnly, postJoinFilter, stateWatermarkPredicates.right)

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
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(input).withRight(matched)
    }
    val rightOutputIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner) {
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(matched).withRight(input)
    }

    // We need to save the time that the inner join output iterator completes, since outer join
    // output counts as both update and removal time.
    var innerOutputCompletionTimeNs: Long = 0
    def onInnerOutputCompletion = {
      innerOutputCompletionTimeNs = System.nanoTime
    }
    // This is the iterator which produces the inner join rows. For outer joins, this will be
    // prepended to a second iterator producing outer join rows; for inner joins, this is the full
    // output.
    val innerOutputIter = CompletionIterator[InternalRow, Iterator[InternalRow]](
      (leftOutputIter ++ rightOutputIter), onInnerOutputCompletion)


    val outputIter: Iterator[InternalRow] = joinType match {
      case Inner =>
        innerOutputIter
      case LeftOuter =>
        // We generate the outer join input by:
        // * Getting an iterator over the rows that have aged out on the left side. These rows are
        //   candidates for being null joined. Note that to avoid doing two passes, this iterator
        //   removes the rows from the state manager as they're processed.
        // * Checking whether the current row matches a key in the right side state, and that key
        //   has any value which satisfies the filter function when joined. If it doesn't,
        //   we know we can join with null, since there was never (including this batch) a match
        //   within the watermark period. If it does, there must have been a match at some point, so
        //   we know we can't join with null.
        def matchesWithRightSideState(leftKeyValue: UnsafeRowPair) = {
          rightSideJoiner.get(leftKeyValue.key).exists { rightValue =>
            postJoinFilter(joinedRow.withLeft(leftKeyValue.value).withRight(rightValue))
          }
        }
        val removedRowIter = leftSideJoiner.removeOldState()
        val outerOutputIter = removedRowIter
          .filterNot(pair => matchesWithRightSideState(pair))
          .map(pair => joinedRow.withLeft(pair.value).withRight(nullRight))

        innerOutputIter ++ outerOutputIter
      case RightOuter =>
        // See comments for left outer case.
        def matchesWithLeftSideState(rightKeyValue: UnsafeRowPair) = {
          leftSideJoiner.get(rightKeyValue.key).exists { leftValue =>
            postJoinFilter(joinedRow.withLeft(leftValue).withRight(rightKeyValue.value))
          }
        }
        val removedRowIter = rightSideJoiner.removeOldState()
        val outerOutputIter = removedRowIter
          .filterNot(pair => matchesWithLeftSideState(pair))
          .map(pair => joinedRow.withLeft(nullLeft).withRight(pair.value))

        innerOutputIter ++ outerOutputIter
      case _ => throwBadJoinTypeException()
    }

    val outputProjection = UnsafeProjection.create(left.output ++ right.output, output)
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      outputProjection(row)
    }

    // Function to remove old state after all the input has been consumed and output generated
    def onOutputCompletion = {
      // All processing time counts as update time.
      allUpdatesTimeMs += math.max(NANOSECONDS.toMillis(System.nanoTime - updateStartTimeNs), 0)

      // Processing time between inner output completion and here comes from the outer portion of a
      // join, and thus counts as removal time as we remove old state from one side while iterating.
      if (innerOutputCompletionTimeNs != 0) {
        allRemovalsTimeMs +=
          math.max(NANOSECONDS.toMillis(System.nanoTime - innerOutputCompletionTimeNs), 0)
      }

      allRemovalsTimeMs += timeTakenMs {
        // Remove any remaining state rows which aren't needed because they're below the watermark.
        //
        // For inner joins, we have to remove unnecessary state rows from both sides if possible.
        // For outer joins, we have already removed unnecessary state rows from the outer side
        // (e.g., left side for left outer join) while generating the outer "null" outputs. Now, we
        // have to remove unnecessary state rows from the other side (e.g., right side for the left
        // outer join) if possible. In all cases, nothing needs to be outputted, hence the removal
        // needs to be done greedily by immediately consuming the returned iterator.
        val cleanupIter = joinType match {
          case Inner => leftSideJoiner.removeOldState() ++ rightSideJoiner.removeOldState()
          case LeftOuter => rightSideJoiner.removeOldState()
          case RightOuter => leftSideJoiner.removeOldState()
          case _ => throwBadJoinTypeException()
        }
        while (cleanupIter.hasNext) {
          cleanupIter.next()
        }
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

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      outputIterWithMetrics, onOutputCompletion)
  }

  /**
   * Internal helper class to consume input rows, generate join output rows using other sides
   * buffered state rows, and finally clean up this sides buffered state rows
   *
   * @param joinSide The JoinSide - either left or right.
   * @param inputAttributes The input attributes for this side of the join.
   * @param joinKeys The join keys.
   * @param inputIter The iterator of input rows on this side to be joined.
   * @param preJoinFilterExpr A filter over rows on this side. This filter rejects rows that could
   *                          never pass the overall join condition no matter what other side row
   *                          they're joined with.
   * @param postJoinFilter A filter over joined rows. This filter completes the application of
   *                       the overall join condition, assuming that preJoinFilter on both sides
   *                       of the join has already been passed.
   *                       Passed as a function rather than expression to avoid creating the
   *                       predicate twice; we also need this filter later on in the parent exec.
   * @param stateWatermarkPredicate The state watermark predicate. See
   *                                [[StreamingSymmetricHashJoinExec]] for further description of
   *                                state watermarks.
   */
  private class OneSideHashJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      preJoinFilterExpr: Option[Expression],
      postJoinFilter: (InternalRow) => Boolean,
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate]) {

    // Filter the joined rows based on the given condition.
    val preJoinFilter =
      newPredicate(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

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
        generateJoinedRow: (InternalRow, InternalRow) => JoinedRow):
    Iterator[InternalRow] = {
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
        // If this row fails the pre join filter, that means it can never satisfy the full join
        // condition no matter what other side row it's matched with. This allows us to avoid
        // adding it to the state, and generate an outer join row immediately (or do nothing in
        // the case of inner join).
        if (preJoinFilter(thisRow)) {
          val key = keyGenerator(thisRow)
          val outputIter = otherSideJoiner.joinStateManager.get(key).map { thatRow =>
            generateJoinedRow(thisRow, thatRow)
          }.filter(postJoinFilter)
          val shouldAddToState = // add only if both removal predicates do not match
            !stateKeyWatermarkPredicateFunc(key) && !stateValueWatermarkPredicateFunc(thisRow)
          if (shouldAddToState) {
            joinStateManager.append(key, thisRow)
            updatedStateRowsCount += 1
          }
          outputIter
        } else {
          joinSide match {
            case LeftSide if joinType == LeftOuter =>
              Iterator(generateJoinedRow(thisRow, nullRight))
            case RightSide if joinType == RightOuter =>
              Iterator(generateJoinedRow(thisRow, nullLeft))
            case _ => Iterator()
          }
        }
      }
    }

    /**
     * Get an iterator over the values stored in this joiner's state manager for the given key.
     *
     * Should not be interleaved with mutations.
     */
    def get(key: UnsafeRow): Iterator[UnsafeRow] = {
      joinStateManager.get(key)
    }

    /**
     * Builds an iterator over old state key-value pairs, removing them lazily as they're produced.
     *
     * @note This iterator must be consumed fully before any other operations are made
     * against this joiner's join state manager. For efficiency reasons, the intermediate states of
     * the iterator leave the state manager in an undefined state.
     *
     * We do this to avoid requiring either two passes or full materialization when
     * processing the rows for outer join.
     */
    def removeOldState(): Iterator[UnsafeRowPair] = {
      stateWatermarkPredicate match {
        case Some(JoinStateKeyWatermarkPredicate(expr)) =>
          joinStateManager.removeByKeyCondition(stateKeyWatermarkPredicateFunc)
        case Some(JoinStateValueWatermarkPredicate(expr)) =>
          joinStateManager.removeByValueCondition(stateValueWatermarkPredicateFunc)
        case _ => Iterator.empty
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
