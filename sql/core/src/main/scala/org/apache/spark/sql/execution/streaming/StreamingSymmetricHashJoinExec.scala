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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Literal, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.SymmetricHashJoinStateManager.KeyToValuePair
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
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
 * data, then it uses the watermark to figure out which rows in the buffer will not join with
 * the new data, and therefore can be discarded. Depending on the provided query conditions, we
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
 *    use the State Key Watermark = event time watermark to discard state (similar to case 1).
 *
 * @param leftKeys  Expression to generate key rows for joining from left input
 * @param rightKeys Expression to generate key rows for joining from right input
 * @param joinType  Type of join (inner, left outer, etc.)
 * @param condition Conditions to filter rows, split by left, right, and joined. See
 *                  [[JoinConditionSplitPredicates]]
 * @param stateInfo Version information required to read join state (buffered rows)
 * @param eventTimeWatermarkForLateEvents Watermark for filtering late events, same for both sides
 * @param eventTimeWatermarkForEviction Watermark for state eviction
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
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    stateWatermarkPredicates: JoinStateWatermarkPredicates,
    stateFormatVersion: Int,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with StateStoreWriter {

  def this(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      stateFormatVersion: Int,
      left: SparkPlan,
      right: SparkPlan) = {

    this(
      leftKeys, rightKeys, joinType, JoinConditionSplitPredicates(condition, left, right),
      stateInfo = None,
      eventTimeWatermarkForLateEvents = None, eventTimeWatermarkForEviction = None,
      stateWatermarkPredicates = JoinStateWatermarkPredicates(), stateFormatVersion, left, right)
  }

  if (stateFormatVersion < 2 && joinType != Inner) {
    throw new IllegalArgumentException(
      s"The query is using stream-stream $joinType join with state" +
      s" format version ${stateFormatVersion} - correctness issue is discovered. Please discard" +
      " the checkpoint and rerun the query. See SPARK-26154 for more details.")
  }

  private lazy val errorMessageForJoinType =
    s"${getClass.getSimpleName} should not take $joinType as the JoinType"

  private def throwBadJoinTypeException(): Nothing = {
    throw new IllegalArgumentException(errorMessageForJoinType)
  }

  private def throwBadStateFormatVersionException(): Nothing = {
    throw new IllegalStateException("Unexpected state format version! " +
      s"version $stateFormatVersion")
  }

  require(
    joinType == Inner || joinType == LeftOuter || joinType == RightOuter || joinType == FullOuter ||
    joinType == LeftSemi,
    errorMessageForJoinType)

  // The assertion against join keys is same as hash join for batch query.
  require(leftKeys.length == rightKeys.length &&
    leftKeys.map(_.dataType)
      .zip(rightKeys.map(_.dataType))
      .forall(types => DataTypeUtils.sameType(types._1, types._2)),
    "Join keys from two sides should have same length and types")

  private val storeConf = new StateStoreConf(conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, conf)))
  private val allowMultipleStatefulOperators =
    conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)

  val nullLeft = new GenericInternalRow(left.output.map(_.withNullability(true)).length)
  val nullRight = new GenericInternalRow(right.output.map(_.withNullability(true)).length)

  override def requiredChildDistribution: Seq[Distribution] =
    StatefulOpClusteredDistribution(leftKeys, getStateInfo.numPartitions) ::
      StatefulOpClusteredDistribution(rightKeys, getStateInfo.numPartitions) :: Nil

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter => (left.output ++ right.output).map(_.withNullability(true))
    case LeftSemi => left.output
    case _ => throwBadJoinTypeException()
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftSemi => left.outputPartitioning
    case _ => throwBadJoinTypeException()
  }

  override def shortName: String = "symmetricHashJoin"

  private val stateStoreNames =
    SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)

  override def operatorStateMetadata(
      stateSchemaPaths: List[List[String]] = List.empty): OperatorStateMetadata = {
    val info = getStateInfo
    val operatorInfo = OperatorInfoV1(info.operatorId, shortName)
    val stateStoreInfo =
      stateStoreNames.map(StateStoreMetadataV1(_, 0, info.numPartitions)).toArray
    OperatorStateMetadataV1(operatorInfo, stateStoreInfo)
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    val watermarkUsedForStateCleanup =
      stateWatermarkPredicates.left.nonEmpty || stateWatermarkPredicates.right.nonEmpty

    // Latest watermark value is more than that used in this previous executed plan
    val watermarkHasChanged =
      eventTimeWatermarkForEviction.isDefined &&
        newInputWatermark > eventTimeWatermarkForEviction.get

    watermarkUsedForStateCleanup && watermarkHasChanged
  }

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    var result: Map[String, (StructType, StructType)] = Map.empty
    // get state schema for state stores on left side of the join
    result ++= SymmetricHashJoinStateManager.getSchemaForStateStores(LeftSide,
      left.output, leftKeys, stateFormatVersion)

    // get state schema for state stores on right side of the join
    result ++= SymmetricHashJoinStateManager.getSchemaForStateStores(RightSide,
      right.output, rightKeys, stateFormatVersion)

    // validate and maybe evolve schema for all state stores across both sides of the join
    result.map { case (stateStoreName, (keySchema, valueSchema)) =>
      // we have to add the default column family schema because the RocksDBStateEncoder
      // expects this entry to be present in the stateSchemaProvider.
      val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME, 0,
        keySchema, 0, valueSchema))
      StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
        newStateSchema, session.sessionState, stateSchemaVersion, storeName = stateStoreName)
    }.toList
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val stateStoreCoord = session.sessionState.streamingQueryManager.stateStoreCoordinator
    val stateStoreNames = SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)
    metrics // initialize metrics
    left.execute().stateStoreAwareZipPartitions(
      right.execute(), stateInfo.get, stateStoreNames, stateStoreCoord)(processPartitions)
  }

  private def processPartitions(
      partitionId: Int,
      leftInputIter: Iterator[InternalRow],
      rightInputIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (stateInfo.isEmpty) {
      throw new IllegalStateException(s"Cannot execute join as state info was not specified\n$this")
    }

    val numOutputRows = longMetric("numOutputRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val numRemovedStateRows = longMetric("numRemovedStateRows")
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")
    val skippedNullValueCount = if (storeConf.skipNullsForStreamStreamJoins) {
      Some(longMetric("skippedNullValueCount"))
    } else {
      None
    }

    val updateStartTimeNs = System.nanoTime
    val joinedRow = new JoinedRow

    assert(stateInfo.isDefined, "State info not defined")
    val checkpointIds = SymmetricHashJoinStateManager.getStateStoreCheckpointIds(
      partitionId, stateInfo.get)

    val inputSchema = left.output ++ right.output
    val postJoinFilter =
      Predicate.create(condition.bothSides.getOrElse(Literal(true)), inputSchema).eval _
    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter,
      condition.leftSideOnly, postJoinFilter, stateWatermarkPredicates.left, partitionId,
      checkpointIds.left.keyToNumValues, checkpointIds.left.valueToNumKeys, skippedNullValueCount)
    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter,
      condition.rightSideOnly, postJoinFilter, stateWatermarkPredicates.right, partitionId,
      checkpointIds.right.keyToNumValues, checkpointIds.right.valueToNumKeys, skippedNullValueCount)

    //  Join one side input using the other side's buffered/state rows. Here is how it is done.
    //
    //  - `leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner)`
    //    - Inner, Left Outer, Right Outer, Full Outer Join: generates all rows from matching
    //      new left input with stored right input, and also stores all the left input.
    //    - Left Semi Join: generates all new left input rows from matching new left input with
    //      stored right input, and also stores all the non-matched left input.
    //
    //  - `rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner)`
    //    - Inner, Left Outer, Right Outer, Full Outer Join: generates all rows from matching
    //      new right input with stored left input, and also stores all the right input.
    //      It also generates all rows from matching new left input with new right input, since
    //      the new left input has become stored by that point. This tiny asymmetry is necessary
    //      to avoid duplication.
    //    - Left Semi Join: generates all stored left input rows, from matching new right input
    //      with stored left input, and also stores all the right input. Note only first-time
    //      matched left input rows will be generated, this is to guarantee left semi semantics.
    val leftOutputIter = leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner) {
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(input).withRight(matched)
    }
    val rightOutputIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner) {
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(matched).withRight(input)
    }

    // We need to save the time that the one side hash join output iterator completes, since
    // other join output counts as both update and removal time.
    var hashJoinOutputCompletionTimeNs: Long = 0
    def onHashJoinOutputCompletion(): Unit = {
      hashJoinOutputCompletionTimeNs = System.nanoTime
    }
    // This is the iterator which produces the inner and left semi join rows. For other joins,
    // this will be prepended to a second iterator producing other rows; for inner and left semi
    // joins, this is the full output.
    val hashJoinOutputIter = CompletionIterator[InternalRow, Iterator[InternalRow]](
      leftOutputIter ++ rightOutputIter, onHashJoinOutputCompletion())

    val outputIter: Iterator[InternalRow] = joinType match {
      case Inner | LeftSemi =>
        hashJoinOutputIter
      case LeftOuter =>
        // We generate the outer join input by:
        // * Getting an iterator over the rows that have aged out on the left side. These rows are
        //   candidates for being null joined. Note that to avoid doing two passes, this iterator
        //   removes the rows from the state manager as they're processed.
        // * (state format version 1) Checking whether the current row matches a key in the
        //   right side state, and that key has any value which satisfies the filter function when
        //   joined. If it doesn't, we know we can join with null, since there was never
        //   (including this batch) a match within the watermark period. If it does, there must have
        //   been a match at some point, so we know we can't join with null.
        // * (state format version 2) We found edge-case of above approach which brings correctness
        //   issue, and had to take another approach (see SPARK-26154); now Spark stores 'matched'
        //   flag along with row, which is set to true when there's any matching row on the right.

        def matchesWithRightSideState(leftKeyValue: UnsafeRowPair) = {
          rightSideJoiner.get(leftKeyValue.key).exists { rightValue =>
            postJoinFilter(joinedRow.withLeft(leftKeyValue.value).withRight(rightValue))
          }
        }

        val initIterFn = { () =>
          val removedRowIter = leftSideJoiner.removeOldState()
          removedRowIter.filterNot { kv =>
            stateFormatVersion match {
              case 1 => matchesWithRightSideState(new UnsafeRowPair(kv.key, kv.value))
              case 2 => kv.matched
              case _ => throwBadStateFormatVersionException()
            }
          }.map(pair => joinedRow.withLeft(pair.value).withRight(nullRight))
        }

        // NOTE: we need to make sure `outerOutputIter` is evaluated "after" exhausting all of
        // elements in `hashJoinOutputIter`, otherwise it may lead to out of sync according to
        // the interface contract on StateStore.iterator and end up with correctness issue.
        // Please refer SPARK-38684 for more details.
        val outerOutputIter = new LazilyInitializingJoinedRowIterator(initIterFn)

        hashJoinOutputIter ++ outerOutputIter
      case RightOuter =>
        // See comments for left outer case.
        def matchesWithLeftSideState(rightKeyValue: UnsafeRowPair) = {
          leftSideJoiner.get(rightKeyValue.key).exists { leftValue =>
            postJoinFilter(joinedRow.withLeft(leftValue).withRight(rightKeyValue.value))
          }
        }

        val initIterFn = { () =>
          val removedRowIter = rightSideJoiner.removeOldState()
          removedRowIter.filterNot { kv =>
            stateFormatVersion match {
              case 1 => matchesWithLeftSideState(new UnsafeRowPair(kv.key, kv.value))
              case 2 => kv.matched
              case _ => throwBadStateFormatVersionException()
            }
          }.map(pair => joinedRow.withLeft(nullLeft).withRight(pair.value))
        }

        // NOTE: we need to make sure `outerOutputIter` is evaluated "after" exhausting all of
        // elements in `hashJoinOutputIter`, otherwise it may lead to out of sync according to
        // the interface contract on StateStore.iterator and end up with correctness issue.
        // Please refer SPARK-38684 for more details.
        val outerOutputIter = new LazilyInitializingJoinedRowIterator(initIterFn)

        hashJoinOutputIter ++ outerOutputIter
      case FullOuter =>
        lazy val isKeyToValuePairMatched = (kv: KeyToValuePair) =>
          stateFormatVersion match {
            case 2 => kv.matched
            case _ => throwBadStateFormatVersionException()
          }

        val leftSideInitIterFn = { () =>
          val removedRowIter = leftSideJoiner.removeOldState()
          removedRowIter.filterNot(isKeyToValuePairMatched)
            .map(pair => joinedRow.withLeft(pair.value).withRight(nullRight))
        }

        val rightSideInitIterFn = { () =>
          val removedRowIter = rightSideJoiner.removeOldState()
          removedRowIter.filterNot(isKeyToValuePairMatched)
            .map(pair => joinedRow.withLeft(nullLeft).withRight(pair.value))
        }

        // NOTE: we need to make sure both `leftSideOutputIter` and `rightSideOutputIter` are
        // evaluated "after" exhausting all of elements in `hashJoinOutputIter`, otherwise it may
        // lead to out of sync according to the interface contract on StateStore.iterator and
        // end up with correctness issue. Please refer SPARK-38684 for more details.
        val leftSideOutputIter = new LazilyInitializingJoinedRowIterator(leftSideInitIterFn)
        val rightSideOutputIter = new LazilyInitializingJoinedRowIterator(rightSideInitIterFn)

        hashJoinOutputIter ++ leftSideOutputIter ++ rightSideOutputIter
      case _ => throwBadJoinTypeException()
    }

    val outputProjection = if (joinType == LeftSemi) {
      UnsafeProjection.create(output, output)
    } else {
      UnsafeProjection.create(left.output ++ right.output, output)
    }
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      outputProjection(row)
    }

    // Function to remove old state after all the input has been consumed and output generated
    def onOutputCompletion = {
      // All processing time counts as update time.
      allUpdatesTimeMs += math.max(NANOSECONDS.toMillis(System.nanoTime - updateStartTimeNs), 0)

      // Processing time between one side hash join output completion and here comes from the
      // outer portion of a join, and thus counts as removal time as we remove old state from
      // one side while iterating.
      if (hashJoinOutputCompletionTimeNs != 0) {
        allRemovalsTimeMs +=
          math.max(NANOSECONDS.toMillis(System.nanoTime - hashJoinOutputCompletionTimeNs), 0)
      }

      allRemovalsTimeMs += timeTakenMs {
        // Remove any remaining state rows which aren't needed because they're below the watermark.
        //
        // For inner and left semi joins, we have to remove unnecessary state rows from both sides
        // if possible.
        //
        // For left outer and right outer joins, we have already removed unnecessary state rows from
        // the outer side (e.g., left side for left outer join) while generating the outer "null"
        // outputs. Now, we have to remove unnecessary state rows from the other side (e.g., right
        // side for the left outer join) if possible. In all cases, nothing needs to be outputted,
        // hence the removal needs to be done greedily by immediately consuming the returned
        // iterator.
        //
        // For full outer joins, we have already removed unnecessary states from both sides, so
        // nothing needs to be outputted here.
        val cleanupIter = joinType match {
          case Inner | LeftSemi =>
            leftSideJoiner.removeOldState() ++ rightSideJoiner.removeOldState()
          case LeftOuter => rightSideJoiner.removeOldState()
          case RightOuter => leftSideJoiner.removeOldState()
          case FullOuter => Iterator.empty
          case _ => throwBadJoinTypeException()
        }
        while (cleanupIter.hasNext) {
          cleanupIter.next()
          numRemovedStateRows += 1
        }
      }

      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
        val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
        val combinedMetrics = StateStoreMetrics.combine(Seq(leftSideMetrics, rightSideMetrics))

        if (StatefulOperatorStateInfo.enableStateStoreCheckpointIds(conf)) {
          val checkpointInfo = SymmetricHashJoinStateManager.mergeStateStoreCheckpointInfo(
            JoinStateStoreCkptInfo(
              leftSideJoiner.getLatestCheckpointInfo(),
              rightSideJoiner.getLatestCheckpointInfo()
            )
          )
          setStateStoreCheckpointInfo(checkpointInfo)
        }

        // Update SQL metrics
        numUpdatedStateRows +=
          (leftSideJoiner.numUpdatedStateRows + rightSideJoiner.numUpdatedStateRows)
        numTotalStateRows += combinedMetrics.numKeys
        stateMemory += combinedMetrics.memoryUsedBytes
        combinedMetrics.customMetrics.foreach { case (metric, value) =>
          longMetric(metric.name) += value
        }
      }

      val stateStoreNames = SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide);
      setOperatorMetrics(numStateStoreInstances = stateStoreNames.length)
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
   * @param oneSideStateInfo  Reconstructed state info for this side
   * @param partitionId A partition ID of source RDD.
   */
  private class OneSideHashJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      preJoinFilterExpr: Option[Expression],
      postJoinFilter: (InternalRow) => Boolean,
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate],
      partitionId: Int,
      keyToNumValuesStateStoreCkptId: Option[String],
      keyWithIndexToValueStateStoreCkptId: Option[String],
      skippedNullValueCount: Option[SQLMetric]) {

    // Filter the joined rows based on the given condition.
    val preJoinFilter =
      Predicate.create(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

    private val joinStateManager = new SymmetricHashJoinStateManager(
      joinSide = joinSide,
      inputValueAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateInfo = stateInfo,
      storeConf = storeConf,
      hadoopConf = hadoopConfBcast.value.value,
      partitionId = partitionId,
      keyToNumValuesStateStoreCkptId = keyToNumValuesStateStoreCkptId,
      keyWithIndexToValueStateStoreCkptId = keyWithIndexToValueStateStoreCkptId,
      stateFormatVersion = stateFormatVersion,
      skippedNullValueCount = skippedNullValueCount)

    private[this] val keyGenerator = UnsafeProjection.create(joinKeys, inputAttributes)

    private[this] val stateKeyWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateKeyWatermarkPredicate(expr)) =>
        // inputSchema can be empty as expr should only have BoundReferences and does not require
        // the schema to generated predicate. See [[StreamingSymmetricHashJoinHelper]].
        Predicate.create(expr, Seq.empty).eval _
      case _ =>
        Predicate.create(Literal(false), Seq.empty).eval _ // false = do not remove if no predicate
    }

    private[this] val stateValueWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateValueWatermarkPredicate(expr)) =>
        Predicate.create(expr, inputAttributes).eval _
      case _ =>
        Predicate.create(Literal(false), Seq.empty).eval _  // false = do not remove if no predicate
    }

    private[this] var updatedStateRowsCount = 0
    private[this] val allowMultipleStatefulOperators: Boolean =
      conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)

    /**
     * Generate joined rows by consuming input from this side, and matching it with the buffered
     * rows (i.e. state) of the other side.
     * @param otherSideJoiner   Joiner of the other side
     * @param generateJoinedRow Function to generate the joined row from the
     *                          input row from this side and the matched row from the other side
     */
    def storeAndJoinWithOtherSide(
        otherSideJoiner: OneSideHashJoiner)(
        generateJoinedRow: (InternalRow, InternalRow) => JoinedRow)
      : Iterator[InternalRow] = {

      val watermarkAttribute = WatermarkSupport.findEventTimeColumn(inputAttributes,
        allowMultipleEventTimeColumns = !allowMultipleStatefulOperators)
      val nonLateRows =
        WatermarkSupport.watermarkExpression(
          watermarkAttribute, eventTimeWatermarkForLateEvents) match {
          case Some(watermarkExpr) =>
            val predicate = Predicate.create(watermarkExpr, inputAttributes)
            applyRemovingRowsOlderThanWatermark(inputIter, predicate)
          case None =>
            inputIter
        }

      val generateFilteredJoinedRow: InternalRow => Iterator[InternalRow] = joinSide match {
        case LeftSide if joinType == LeftOuter || joinType == FullOuter =>
          (row: InternalRow) => Iterator(generateJoinedRow(row, nullRight))
        case RightSide if joinType == RightOuter || joinType == FullOuter =>
          (row: InternalRow) => Iterator(generateJoinedRow(row, nullLeft))
        case _ => (_: InternalRow) => Iterator.empty
      }

      val excludeRowsAlreadyMatched = joinType == LeftSemi && joinSide == RightSide

      val generateOutputIter: (InternalRow, Iterator[JoinedRow]) => Iterator[InternalRow] =
        joinSide match {
          case LeftSide if joinType == LeftSemi =>
            (input: InternalRow, joinedRowIter: Iterator[JoinedRow]) =>
              // For left side of left semi join, generate one left row if there is matched
              // rows from right side. Otherwise, generate nothing.
              if (joinedRowIter.nonEmpty) {
                Iterator.single(input)
              } else {
                Iterator.empty
              }
          case RightSide if joinType == LeftSemi =>
            (_: InternalRow, joinedRowIter: Iterator[JoinedRow]) =>
              // For right side of left semi join, generate matched left rows only.
              joinedRowIter.map(_.getLeft)
          case _ => (_: InternalRow, joinedRowIter: Iterator[JoinedRow]) => joinedRowIter
        }

      nonLateRows.flatMap { row =>
        val thisRow = row.asInstanceOf[UnsafeRow]
        // If this row fails the pre join filter, that means it can never satisfy the full join
        // condition no matter what other side row it's matched with. This allows us to avoid
        // adding it to the state, and generate an outer join row immediately (or do nothing in
        // the case of inner join).
        if (preJoinFilter(thisRow)) {
          val key = keyGenerator(thisRow)
          val joinedRowIter: Iterator[JoinedRow] = otherSideJoiner.joinStateManager.getJoinedRows(
            key,
            thatRow => generateJoinedRow(thisRow, thatRow),
            postJoinFilter,
            excludeRowsAlreadyMatched)
          val outputIter = generateOutputIter(thisRow, joinedRowIter)
          new AddingProcessedRowToStateCompletionIterator(key, thisRow, outputIter)
        } else {
          generateFilteredJoinedRow(thisRow)
        }
      }
    }

    private class AddingProcessedRowToStateCompletionIterator(
        key: UnsafeRow,
        thisRow: UnsafeRow,
        subIter: Iterator[InternalRow])
      extends CompletionIterator[InternalRow, Iterator[InternalRow]](subIter) {

      private val iteratorNotEmpty: Boolean = super.hasNext

      override def completion(): Unit = {
        // The criteria of whether the input has to be added into state store or not:
        // - Left side: input can be skipped to be added to the state store if it's already matched
        //   and the join type is left semi.
        //   For other cases, the input should be added, including the case it's going to be evicted
        //   in this batch. It hasn't yet evaluated with inputs from right side for this batch.
        //   Refer to the classdoc of SteramingSymmetricHashJoinExec about how stream-stream join
        //   works.
        // - Right side: for this side, the evaluation with inputs from left side for this batch
        //   is done at this point. That said, input can be skipped to be added to the state store
        //   if input is going to be evicted in this batch. Though, input should be added to the
        //   state store if it's right outer join or full outer join, as unmatched output is
        //   handled during state eviction.
        val isLeftSemiWithMatch = joinType == LeftSemi && joinSide == LeftSide && iteratorNotEmpty
        val shouldAddToState = if (isLeftSemiWithMatch) {
          false
        } else if (joinSide == LeftSide) {
          true
        } else {
          // joinSide == RightSide

          // if the input is not evicted in this batch (hence need to be persisted)
          val isNotEvictingInThisBatch =
            !stateKeyWatermarkPredicateFunc(key) && !stateValueWatermarkPredicateFunc(thisRow)

          isNotEvictingInThisBatch ||
            // if the input is producing "unmatched row" in this batch
            (
              (joinType == RightOuter && !iteratorNotEmpty) ||
                (joinType == FullOuter && !iteratorNotEmpty)
            )
        }

        if (shouldAddToState) {
          joinStateManager.append(key, thisRow, matched = iteratorNotEmpty)
          updatedStateRowsCount += 1
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
    def removeOldState(): Iterator[KeyToValuePair] = {
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

    def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo = {
      joinStateManager.getLatestCheckpointInfo()
    }

    def numUpdatedStateRows: Long = updatedStateRowsCount
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): StreamingSymmetricHashJoinExec =
    copy(left = newLeft, right = newRight)

  private class LazilyInitializingJoinedRowIterator(
      initFn: () => Iterator[JoinedRow]) extends Iterator[JoinedRow] {
    private lazy val iter: Iterator[JoinedRow] = initFn()

    override def hasNext: Boolean = iter.hasNext
    override def next(): JoinedRow = iter.next()
  }

  // If `STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS` is enabled, counting the number
  // of skipped null values as custom metric of stream join operator.
  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] =
    if (storeConf.skipNullsForStreamStreamJoins) {
      Seq(StatefulOperatorCustomSumMetric("skippedNullValueCount",
        "number of skipped null values"))
    } else {
      Nil
    }

  // This operator will evict based on the state watermark on both side of inputs; we would like
  // to let users leverage both sides of event time column for output of join, so the watermark
  // must be lower bound of both sides of event time column. The lower bound of event time column
  // for each side is determined by state watermark, hence we take a minimum of (left state
  // watermark, right state watermark, input watermark) to decide the output watermark.
  override def produceOutputWatermark(inputWatermarkMs: Long): Option[Long] = {
    val (leftStateWatermark, rightStateWatermark) =
      StreamingSymmetricHashJoinHelper.getStateWatermark(
        left.output, right.output, leftKeys, rightKeys, condition.full, Some(inputWatermarkMs),
        !allowMultipleStatefulOperators)

    Some((leftStateWatermark ++ rightStateWatermark ++ Some(inputWatermarkMs)).min)
  }
}
