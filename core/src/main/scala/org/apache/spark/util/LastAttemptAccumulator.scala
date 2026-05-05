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
package org.apache.spark.util

import scala.math.Ordering.Implicits._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.{LogEntry, Logging, LogKey, LogKeys}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskInfo

/*
 *  Last Attempt Accumulators are Accumulators that track the value of a metric aggregated across
 *  the "last execution" that produced the values. "Last execution" can be defined as:
 *  - For RDDs: the last execution of a given RDD partition, in the latest Stage and Stage attempt
 *    that recomputed it.
 *  - Across RDDs: lastAttemptValueForRDDId, lastAttemptValueForRDDIds, lastAttemptValueForAllRDDs,
 *    lastAttemptValueForHighestRDDId let specify that only values from specific RDDs should be
 *    aggregated.
 *  - For Spark SQL Execution: In SQLLastAttemptAccumulator, lastAttemptValueForDataset,
 *    lastAttemptValueForQueryExecution let specify that only values from the last SQL execution of
 *    a specific Dataset (or QueryExecution) should be aggregated.
 *
 *  In specific situations the last attempt value cannot be computed. This is both because of known
 *  specific user actions (e.g. mixing driver updates with task updates), and because the
 *  accumulator performs (and logs) various internal sanity checks and bails out if it detects an
 *  unexpected situation. Therefore, all the lastAttempt methods return an Option[OUT], where None
 *  means that it has bailed out.
 *
 *  Updates to the accumulator from completed Tasks are merged in mergeLastAttempt, called from
 *  DAGScheduler.updateAccumulators, called from DAGScheduler.handleTaskCompletion in the single
 *  threaded DAGScheduler event loop. Therefore, we don't need to worry about concurrency control
 *  when updating the accumulator values. However, reading of the last attempt value can potentially
 *  be done concurrently, so we use synchronization. When there is normally no contention, JVM
 *  synchronization should be very low overhead.
 *
 *  In order to be able to provide last attempt value, we need to keep track of partial metric
 *  values, so that after a partial re-attempt the partial value can be updated, and then
 *  re-aggregated.
 *  There are various sources of re-attempts that we have to track:
 *
 *  1. Spark Core.
 *  ==============
 *    - Updates from failed tasks are filtered in Task.collectAccumulatorUpdates before they are
 *      even passed back to the driver. We don't need to worry about them here.
 *    - We should not get results from two successful attempts of a Task in the same Stage attempt.
 *      TaskSetManager.handleSuccessfulTask ensures that.
 *    - Therefore we only need to track Stage retries. The Last Attempt Metric will aggregate the
 *      metric value of a given RDD partition from the last attempt of the Stage with the highest
 *      stageId.
 *      Normally recomputation creates a new stageAttemptId in the same Stage, but there can also
 *      be multiple new Stages due to:
 *      - In AQE, a materialized QueryStage is submitted as a new Stage, which would normally get
 *        skipped, as it is already materialized. However, if results of that stage have been lost,
 *        the recomputation will happen in that Stage.
 *      - If the same Dataset with the same QueryExecution and same executedPlan is reused for
 *        another execution (e.g. again calling collect()). All map stages should be materialized,
 *        so like with AQE, they should be skipped, unless the results have been lost. Then,
 *        recomputation will happen in that Stage. The result stage computing the action will be
 *        fully re-executed.
 *    - Due to the async nature of cancellation, there can be tasks from previous attempts that
 *      arrive later than the last attempt. Therefore, we need to track and compare stageId and
 *      stageAttemptId of every computed RDD partition, in order to discard latecomers.
 *
 *  2. Spark SQL.
 *  =============
 *  LastAttemptAccumulator offers simple tracking of the last SQL execution, by assuming that
 *  the last execution will be in the scope of an RDD with the highest id, and using
 *  [[lastAttemptValueForHighestRDDId]]. See SQLLastAttemptAccumulator for more possibilities
 *  of tracking SQL execution.
 *
 *  Simple last SQL execution tracking
 *  ----------------------------------
 *  Whenever an AQE replan happens, or a repeated execution is submitted, there will be a new
 *  RDD created for that execution. If AQE creates a new plan, it always uses it and cancels
 *  the previous one. So, aggregating the metric updates from the RDD with the highest id
 *  should correspond to the last execution and the latest AQE plan.
 *  This has some limitations, e.g. doesn't work if the same metric is used in multiple places
 *  in the query plan, and we want all occurrences to be aggregated together.
 *  It also wouldn't work if a SparkPlan splits its execution into multiple RDDs. This for example
 *  happens in BroadcastNestedLoopJoinExec with matchedStreamRows and notMatchedBroadcastRows.
 *  One can use this simple last attempt tracking by using lastAttemptLastRDDValue.
 *
 *  3. Driver only updates.
 *  =======================
 *  Sometimes the metric is manipulated directly from the driver, not from within a Task.
 *  It can be either explicit by user code, or implicit by Catalyst Optimizer, for example
 *  ConvertToLocalRelation rule, folding a piece of the plan by evaluating it manually on the
 *  driver.
 *  When this happens, LastAttemptAccumulator has no information to reason about what was the
 *  last execution. If the only metric updates are coming from the driver, it assumes that these are
 *  the "last attempt". If there are both updates from executors and from the driver, it bails out.
 *
 *  Implementation
 *  ==============
 *  To track the last attempts, we track a map of metric values per RDD id:
 *  - Map[RddId, LastAttemptRDDVals[PARTIAL]]
 *
 *  In LastAttemptRDDVals we track an Array of per RDD partition partial merge values, together with
 *  the stageId and stageAttemptId and taskAttemptNumber to record task execution.
 *  We also track the RDD id, RDDScope id and last SQL execution id updating that RDD.
 *
 * Normally to merge partial values, two full Accumulators are used. However, accumulator classes
 * that support Last Attempt have to implement partialMerge which merges PARTIAL type.
 * This is used to have more compact representation, as PARTIAL can be e.g. a primitive type as
 * opposed to a full AccumulatorV2 object instance.
 */


private class LastAttemptRDDVals[@specialized T](
    val rddId: Int,
    val rddScopeId: Option[String],
    // Arrays of partial metric values, and the corresponding stage, stage attempt and task attempt,
    // with index representing RDD partition id.
    // Metric updates to a given RDD partition can come from different stageAttempts if a retry
    // happens while a Job with the Stage is running (a downstream Stage within a Job detects
    // missing blocks and triggers recompute), or from different Stages, if a retry happens later
    // (a new Job is submitted that depends on data from the RDD, if it finds it's missing it will
    // recompute it in a new Stage).
    // If a missing output is detected in a Stage while the stage is still running (e.g. executor
    // is lost or decommissioned while the stage is running, and loses the output of some already
    // finished tasks), a new Task with new taskAttemptNumber will be started for that Task.
    // There may be multiple Tasks with different taskAttemptNumbers running in parallel due to
    // speculation, but DAGScheduler guarantees that only one of them will reach metrics reporting,
    // so it doesn't have to be dealt with here.
    //
    // There may be partitions that are either not computed at all (for example, due to early stop
    // in take/limit), or AQE task coalescing may be visible as an update of the partition id of
    // the first partition of the coalesced range. AQE guarantees that if these are retried, they
    // will be coalesced in the same ranges, so update the same values.
    // Not computed partitions should have EMPTY_ID in all the Int Arrays.
    //
    // Arrays of primitive types are more memory efficient than an array of objects due to
    // references, object headers and paddings overheads.
    // The `@specialized` annotation should make scala specialize it to use primitive array instead
    // of boxed objects.
    val partitionPartialVals: Array[T],
    val stageIds: Array[Int],
    val stageAttemptIds: Array[Int],
    val taskAttemptNumbers: Array[Int])
  {

  // In a case of repeated execution of the same QueryExecution and reuse of the SparkPlan
  // (for example multiple `collect()` on the same Dataset), a new RDD may be executed in the same
  // RDDOperationScope for the new execution. Hence, we can have multiple RDDs with the same
  // RDDOperationScope, coming from different SQL executions and we should only count the last one.
  // However, it may also be an old RDD that is reused in the new execution, but needs to be
  // partially recomputed because part of it is missing. In that case, the last attempt value needs
  // to still be aggregated over the whole RDD, because the whole RDD is used in the new execution.
  // Note that this only applies per RDDOperationScope/SparkPlan, because other plans in the same
  // new execution may have reused their RDD in whole, and hence have the last SQL executionId
  // come from an earlier execution.
  // Note: This doesn't work in case a user concurrently executed multiple actions on the same
  // Dataset, resulting in multiple concurrent executions trying to compute the same RDD. This
  // however should not happen in practice and would likely produce other unexpected effects.
  var lastSqlExecutionId: Option[Long] = None

  def numPartitions: Int = stageIds.length

  def isEmptyAt(partitionId: Int): Boolean = {
    if (stageIds(partitionId) == LastAttemptRDDVals.EMPTY_ID) {
      assert(stageAttemptIds(partitionId) == LastAttemptRDDVals.EMPTY_ID)
      assert(taskAttemptNumbers(partitionId) == LastAttemptRDDVals.EMPTY_ID)
      true
    } else {
      false
    }
  }

  def update(partialValue: AccumulatorPartialVal[T]): Unit = {
    partitionPartialVals(partialValue.rddPartitionId) = partialValue.partialMergeVal
    stageIds(partialValue.rddPartitionId) = partialValue.stageId
    stageAttemptIds(partialValue.rddPartitionId) = partialValue.stageAttemptId
    taskAttemptNumbers(partialValue.rddPartitionId) = partialValue.taskAttemptNumber
    lastSqlExecutionId = partialValue.sqlExecutionId
  }

  def partialValueAt(partId: Int): AccumulatorPartialVal[T] = {
    AccumulatorPartialVal(
      partialMergeVal = partitionPartialVals(partId),
      rddId = rddId,
      rddPartitionId = partId,
      rddNumPartitions = stageIds.length,
      rddScopeId = rddScopeId,
      stageId = stageIds(partId),
      stageAttemptId = stageAttemptIds(partId),
      taskAttemptNumber = taskAttemptNumbers(partId),
      sqlExecutionId = lastSqlExecutionId)
  }

  override def toString: String = {
    s"""LastAttemptVal(
       |  rddId=$rddId,
       |  rddScopeId=$rddScopeId,
       |  lastSqlExecutionId=$lastSqlExecutionId,
       |  partitionPartialVals=${partitionPartialVals.mkString("[", ",", "]")},
       |  stageIds=${stageIds.mkString("[", ",", "]")},
       |  stageAttemptIds=${stageAttemptIds.mkString("[", ",", "]")},
       |  taskAttemptNumbers=${taskAttemptNumbers.mkString("[", ",", "]")}
       |)""".stripMargin
  }
}

private object LastAttemptRDDVals {
  // EMPTY_ID in stageId means that the partition was not computed.
  val EMPTY_ID: Int = -1

  def apply[@specialized T](
      rddId: Int,
      rddScopeId: Option[String],
      numPartitions: Int)(implicit ct: ClassTag[T]): LastAttemptRDDVals[T] = {
    new LastAttemptRDDVals[T](
      rddId,
      rddScopeId,
      new Array[T](numPartitions),
      Array.fill(numPartitions)(LastAttemptRDDVals.EMPTY_ID),
      Array.fill(numPartitions)(LastAttemptRDDVals.EMPTY_ID),
      Array.fill(numPartitions)(LastAttemptRDDVals.EMPTY_ID))
  }

  def createFromFirstUpdate[@specialized T](
      update: AccumulatorPartialVal[T])(implicit ct: ClassTag[T]): LastAttemptRDDVals[T] = {
    val newVal = LastAttemptRDDVals[T](
      rddId = update.rddId,
      rddScopeId = update.rddScopeId,
      update.rddNumPartitions)
    newVal.update(update)
    newVal
  }
}

private class LastAttemptMap[K, V] {
  // Map used to keep metric updates, keyed by RDD id or RDD scope id, backed by a List.
  // In the majority of cases (when there are no stage retries and no AQE replanning
  // cancelling already running stages), there will be only one key, so a list backed map
  // should have less overhead.
  //
  // Accumulators are modified only from DAGScheduler.updateAccumulators -> mergeLastAttempt,
  // which is running from a single thread (scheduling loop), so no concurrency control is needed
  // for updates. Read accesses to an immutable list should use a consistent state without extra
  // synchronization.

  @volatile private var map: List[(K, V)] = Nil

  def contains(key: K): Boolean = map.exists(_._1 == key)

  def get(key: K): Option[V] = map.collectFirst { case (k, v) if k == key => v }

  def put(key: K, value: V): Unit = synchronized {
    map = (key, value) :: map.filterNot(_._1 == key)
  }

  def keys: Iterable[K] = map.map(_._1)
  def values: Iterable[V] = map.map(_._2)
  def isEmpty: Boolean = map.isEmpty
  def nonEmpty: Boolean = map.nonEmpty
  def clear(): Unit = synchronized { map = Nil }

  override def toString: String = map
    .map(elem => s"${elem._1} -> ${elem._2}").mkString("LastAttemptMap {\n", ",\n", "\n}")
}

private case class AccumulatorPartialVal[PARTIAL](
    partialMergeVal: PARTIAL,
    rddId: Int,
    rddPartitionId: Int,
    rddNumPartitions: Int,
    rddScopeId: Option[String],
    stageId: Int,
    stageAttemptId: Int,
    taskAttemptNumber: Int,
    sqlExecutionId: Option[Long]
) {
  override def toString: String = {
    s"""AccumulatorPartialVal(
       |  partialMergeVal=$partialMergeVal,
       |  rddId=$rddId,
       |  rddPartitionId=$rddPartitionId,
       |  rddNumPartitions=$rddNumPartitions,
       |  rddScopeId=$rddScopeId,
       |  stageId=$stageId,
       |  stageAttemptId=$stageAttemptId,
       |  taskAttemptNumber=$taskAttemptNumber,
       |  sqlExecutionId=$sqlExecutionId
       |)""".stripMargin
  }

  /** Tuple of stage id, stage attempt id and taskAttemptNumber, defining the order of attempts. */
  val attempt: (Int, Int, Int) = (stageId, stageAttemptId, taskAttemptNumber)
}

/**
 * A trait that can be mixed into a subclass of [[AccumulatorV2]] to track the "logical"
 * value of the "last attempt" of the execution using the accumulator - aggregated from the last
 * attempts of any Task that calculated some RDD partitions and used this accumulator, and
 * discarding any values coming from earlier attempts that have been recomputed.
 * If the accumulator is used by multiple RDDs, the last attempt value is tracked separately for
 * each, and can be retrieved for each or all of them separately, see lastAttemptValueForX methods.
 * If the accumulator is used directly on the Spark Driver using [[AccumulatorV2#add]],
 * that value is considered the last attempt value.
 * If the accumulator was both used in Tasks and updated directly on the driver, it can't determine
 * what should be considered the last attempt, and lastAttemptValueForX methods will return None.
 *
 * Contract for driver-only updates:
 * A driver-side value (set via [[AccumulatorV2#add]] on the driver, outside any Task) is only
 * returned by methods that do not narrow by RDD, namely [[lastAttemptValueForAllRDDs]] and
 * [[lastAttemptValueForHighestRDDId]]. Methods that narrow to specific RDDs or RDD scopes
 * ([[lastAttemptValueForRDDId]], [[lastAttemptValueForRDDIds]], [[lastAttemptValueForRDDScopes]])
 * return the zero value when a driver-only value is present, because a driver-side update cannot
 * be attributed to any particular RDD or scope.
 *
 * [[LastAttemptAccumulator]] is not reset by the [[AccumulatorV2#reset]] method implementation,
 * and its state is not copied by the [[AccumulatorV2#copy]] method implementation, and it should
 * not be serialized to the Executors. The internal state should only be initialized by the
 * [[initializeLastAttemptAccumulator]] method on the "main" instance of the accumulator, that was
 * created and registered with [[AccumulatorContext]] with [[AccumulatorV2#register]]. All the
 * interfaces of [[LastAttemptAccumulator]]: [[mergeLastAttempt]] (used only by DAGScheduler) and
 * lastAttemptValueForX, [[logAccumulatorState]] (used by the using code) should only be invoked on
 * that instance, on the Spark Driver.
 *
 * The [[LastAttemptAccumulator]] is not thread-safe. [[mergeLastAttempt]] should only be used by
 * DAGScheduler, by the scheduler thread. Retrieving the value using lastAttemptValueForXXX while
 * it is concurrently updated (execution is running) can produce some inconsistencies, but should
 * not crash.
 * If an RDD using the [[LastAttemptAccumulator]] is used concurrently by multiple actions that
 * all try to recompute it, it may produce unexpected results and the semantics of what is "last
 * attempt" becomes ambiguous. This should not be done in practice, and will likely result in more
 * unexpected behaviours in Spark.
 *
 * Implementations must implement [[partialMergeVal]] and [[partialMerge]] methods operating on
 * PARTIAL type. In regular [[AccumulatorV2]] implementations, the [[AccumulatorV2]] object
 * itself holds the intermediate value of the accumulator, and [[AccumulatorV2#merge]] method is
 * used to merge these objects together. [[LastAttemptAccumulator]] needs to keep track of partial
 * values of every partition of every RDD that used the accumulator, and holding a full
 * [[AccumulatorV2]] object for each would have a high overhead. Therefore, an implementation should
 * be able to return PARTIAL value from [[partialMergeVal]] that represents an intermediate
 * mergeable value, and a [[partialMerge]] method that can merge that value into the accumulator.
 * Implementations must also implement an [[isMergeable]] method that checks if the other
 * [[AccumulatorV2]] is of a compatible type to be merged with this using [[partialMergeVal]]. In
 * regular [[AccumulatorV2]] implementations, this check is normally done inside the
 * [[AccumulatorV2#merge]] method, which is not used here.
 *
 * If an implementation is used to keep user data in the accumulator, it should override
 * [[accumulatorStoresUserData]] to return true, to ensure correct structured logging annotation.
 * Otherwise it should override it to false.
 */
trait LastAttemptAccumulator[IN, OUT, PARTIAL] extends Logging {
  this: AccumulatorV2[IN, OUT] =>

  // For every RDD that participated in the computation of this accumulator, keep the partial
  // value of the accumulator for the latest stage and stage attempt that computed it.
  // Keyed by rdd.id.
  // Only kept and accessed on the driver, in the instance of the LastAttemptAccumulator that was
  // created and registered with AccumulatorContext with AccumulatorV2.register().
  // Should not be copied / reset by the implementation of copy() / reset() functions.
  // Transient: only needed on the driver and doesn't need to be serialized.
  @transient
  private var lastAttemptRddsMap: LastAttemptMap[Int, LastAttemptRDDVals[PARTIAL]] = _

  // ClassTag for PARTIAL, captured at initialization time.
  @transient private var partialClassTag: ClassTag[PARTIAL] = _

  // Metric value set directly on the driver, not from within a task.
  // Only kept and accessed on the driver, in the instance of the LastAttemptAccumulator that was
  // created and registered with AccumulatorContext with AccumulatorV2.register().
  // Should not be copied / reset by the implementation of copy() / reset() functions.
  // Transient: only needed on the driver and doesn't need to be serialized.
  @transient
  private var lastAttemptDirectDriverValue: Option[OUT] = _

  // Flipped to true if unexpected metrics updates are received and we can no longer reason
  // about the last attempt.
  // Should not be copied / reset by the implementation of copy() / reset() functions.
  // Transient: only needed on the driver and doesn't need to be serialized.
  @transient
  protected var lastAttemptAccumulatorInvalid: Boolean = false

  // Indicates that the LastAttemptAccumulator has been initialized.
  // It is initialized in assertValid().
  // Should not be copied / reset by the implementation of copy() / reset() functions.
  // Transient: only needed on the driver and doesn't need to be serialized.
  @transient
  protected var lastAttemptAccumulatorInitialized: Boolean = false

  /** Reset the state of the last attempt accumulator, discarding all the past attempts, and
   *  making it valid again if it was invalidated. */
  def resetLastAttemptAccumulator(): Unit = try {
    lastAttemptRddsMap.clear()
    lastAttemptDirectDriverValue = None
    lastAttemptAccumulatorInvalid = false
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in resetLastAttemptAccumulator",
        exception = Some(e))
  }

  def initializeLastAttemptAccumulator()(implicit ct: ClassTag[PARTIAL]): Unit = try {
    assert(isAtDriverSide)
    assert(!lastAttemptAccumulatorInitialized)
    assert(!lastAttemptAccumulatorInvalid)
    assert(lastAttemptRddsMap == null)
    assert(lastAttemptDirectDriverValue == null)
    partialClassTag = ct
    lastAttemptRddsMap = new LastAttemptMap[Int, LastAttemptRDDVals[PARTIAL]]
    lastAttemptDirectDriverValue = None
    lastAttemptAccumulatorInitialized = true
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in initializeLastAttemptAccumulator",
        exception = Some(e))
  }

  private def accumulatorId: Long = {
    // This can throw if this is a copy/serialized accumulator,
    // not the instance registered with AccumulatorContext.
    // Catch it so we can safely use it for logging in unexpected situations.
    try {
      this.id
    } catch {
      case NonFatal(e) =>
        logWarning(log"Unexpected exception in getting accumulator id", e)
        -1L // needs to be a long for LogKeys.ACCUMULATOR_ID
    }
  }

  /** Log entry to log debug information about the internal state of the accumulator. */
  def logAccumulatorState: LogEntry = try {
    log"""LastAttemptAccumulator id=${MDC(LogKeys.ACCUMULATOR_ID, accumulatorId)}:
    |Invalidated: ${MDC(LogKeys.LAST_ATTEMPT_ACC_INVALIDATE, lastAttemptAccumulatorInvalid)}.
    |Direct driver value: ${MDC(logKeyAccumulatorState, lastAttemptDirectDriverValue)}.
    |Value: ${MDC(logKeyAccumulatorState, value)}.
    |lastAttemptRddsMap:
    |${MDC(logKeyAccumulatorState, lastAttemptRddsMap)}."""
      .stripMargin
  } catch {
    case NonFatal(e) =>
      logWarning(log"Unexpected exception in logAccumulatorState", e)
      log"<Unexpected exception in logAccumulatorState>"
  }

  private def logAccumulatorUpdate(
      newAccumPartialValue: Option[AccumulatorPartialVal[PARTIAL]] = None,
      oldAccumPartialValue: Option[AccumulatorPartialVal[PARTIAL]] = None): LogEntry = try {
    log"""Old partial RDD value: ${MDC(logKeyAccumulatorState, oldAccumPartialValue)}.
    |New partial RDD value: ${MDC(logKeyAccumulatorState, newAccumPartialValue)}."""
      .stripMargin
  } catch {
    case NonFatal(e) =>
      logWarning(log"Unexpected exception in logAccumulatorUpdate", e)
      log"<Unexpected exception in logAccumulatorUpdate>"
  }

  private def unexpectedLastAttemptMetricUpdate(
      invalidate: Boolean,
      reason: String,
      exception: Option[Throwable] = None,
      newAccumPartialValue: Option[AccumulatorPartialVal[PARTIAL]] = None,
      oldAccumPartialValue: Option[AccumulatorPartialVal[PARTIAL]] = None): Unit = {
    val logEntry =
      log"""Unexpected last attempt tracking for accumulator ${
        MDC(LogKeys.ACCUMULATOR_ID, accumulatorId)}.
      |Invalidate: ${MDC(LogKeys.LAST_ATTEMPT_ACC_INVALIDATE, invalidate)}.
      |Reason: ${MDC(LogKeys.LAST_ATTEMPT_ACC_UNEXPECTED_REASON, reason)}.
      |""".stripMargin +
      log"State:\n" + logAccumulatorState +
      log"Update:\n" + logAccumulatorUpdate(newAccumPartialValue, oldAccumPartialValue)
    exception match {
      case Some(e) => logWarning(logEntry, e)
      case None => logWarning(logEntry)
    }
    if (invalidate) {
      lastAttemptAccumulatorInvalid = true
    }
    if (Utils.isTesting && lastAttemptAccumulatorInitialized && exception.isDefined) {
      // If this is a test, rethrow the exception.
      // (Rethrow only if lastAttemptAccumulatorInitialized. In some tests, we check for proper
      // graceful handling of unexpected exceptions in accumulators that are not properly
      // initialized, so we don't want to throw there.)
      throw exception.get
    }
  }

  protected def unexpectedLastAttemptMetricOperation(
      invalidate: Boolean,
      reason: String,
      exception: Option[Throwable] = None): Unit = {
    // subclasses don't have visibility of private class AccumulatorPartialVal.
    unexpectedLastAttemptMetricUpdate(
      invalidate = invalidate,
      reason = reason,
      exception = exception,
      newAccumPartialValue = None,
      oldAccumPartialValue = None)
  }

  /** Set of assertions that should always hold for a valid [[LastAttemptAccumulator]]. */
  protected def assertValid(): Unit = {
    assert(lastAttemptAccumulatorInitialized)
    assert(!lastAttemptAccumulatorInvalid)
    assert(isAtDriverSide)
    assert(metadata != null)
    assert(!metadata.countFailedValues)
    assert(lastAttemptDirectDriverValue.isEmpty || lastAttemptRddsMap.isEmpty)
  }

  /**
   * Accumulator subclasses where metric values can contain user data (for example, maximum of
   * processed values, observable metrics) as opposed to system measurements (for example, count
   * of processed rows) should return true to ensure correct structured logging annotation.
   */
  protected def accumulatorStoresUserData: Boolean

  protected def logKeyAccumulatorState: LogKey = {
    if (accumulatorStoresUserData) {
      LogKeys.LAST_ATTEMPT_ACC_USER_METRIC
    } else {
      LogKeys.LAST_ATTEMPT_ACC_SYSTEM_METRIC
    }
  }

  /** Return intermediate value of PARTIAL type that can be merged together by partialMerge. */
  protected def partialMergeVal: PARTIAL

  /** Merge together partial values of PARTIAL type returned by partialMergeVal. */
  protected def partialMerge(otherVal: PARTIAL): Unit

  /** Check if the other accumulator is mergeable with this one. */
  protected def isMergeable(other: AccumulatorV2[_, _]): Boolean

  /**
   * Check if the value is set on the driver side, not from within a task.
   * This must be called from `add` and `set` methods of any AccumulatorV2 subclass supporting
   * last attempt metrics to set what the `value` of the metric is after the operation.
   */
  protected def setValueIfOnDriverSide(value: OUT): Unit = try {
    if (isAtDriverSide && lastAttemptAccumulatorInitialized && !lastAttemptAccumulatorInvalid) {
      // Direct update on the driver, not from within a task.
      // This gives little information about the source of the update, so we can't reason about
      // "last attempt" if it's mixed with non-driver updates.
      lastAttemptDirectDriverValue = Some(value)
      if (lastAttemptRddsMap.nonEmpty) {
        unexpectedLastAttemptMetricUpdate(
          invalidate = true,
          reason = "Incoming direct driver value while task updates exist")
      }
    }
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in setValueIfOnDriverSide",
        exception = Some(e))
  }

  /**
   * It needs Task and Stage information to reason about the last attempt.
   *
   * Called from a single thread in DAGScheduler, no synchronization needed.
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   */
  private[spark] def mergeLastAttempt(
      other: AccumulatorV2[_, _],
      rdd: RDD[_],
      taskInfo: TaskInfo,
      stageId: Int,
      stageAttemptId: Int,
      localProperties: java.util.Properties): Unit = try {
    implicit val ct: ClassTag[PARTIAL] = partialClassTag
    if (lastAttemptAccumulatorInvalid) return
    // Skip zero-value updates. They contribute nothing to the aggregate and can come
    // from stages where the accumulator was present in the task closure but never incremented.
    if (other.isZero) return
    assertValid()

    if (!isMergeable(other)) {
      // This should never happen.
      unexpectedLastAttemptMetricUpdate(
        invalidate = true,
        "Merging accumulators of different types")
      return
    }

    if (!other.isInstanceOf[LastAttemptAccumulator[_, _, _]]) {
      // This should never happen.
      unexpectedLastAttemptMetricUpdate(
        invalidate = true,
        "Merging with accumulator which is not SLAM")
      return
    }
    val lastAttemptOther = other
      .asInstanceOf[LastAttemptAccumulator[IN, OUT, PARTIAL]]

    val update = AccumulatorPartialVal(
      partialMergeVal = lastAttemptOther.partialMergeVal,
      rddId = rdd.id,
      rddPartitionId = taskInfo.partitionId,
      rddNumPartitions = rdd.getNumPartitions,
      rddScopeId = rdd.scope.map(_.id),
      stageId = stageId,
      stageAttemptId = stageAttemptId,
      taskAttemptNumber = taskInfo.attemptNumber,
      sqlExecutionId =
        Option(localProperties.getProperty(SparkContext.SQL_EXECUTION_ID_KEY)).map(_.toLong))

    if (lastAttemptDirectDriverValue.nonEmpty) {
      unexpectedLastAttemptMetricUpdate(invalidate = true,
        "Incoming task updates while direct driver value exists",
        newAccumPartialValue = Some(update))
      return
    }

    lastAttemptRddsMap.get(update.rddId) match {
      case Some(oldRDDValue) => // This RDD was already seen.
        val oldValue = oldRDDValue.partialValueAt(update.rddPartitionId)

        logTrace(log"mergeLastAttempt existing RDD update:\n" +
          log"${MDC(logKeyAccumulatorState, oldRDDValue)}\n" +
          logAccumulatorUpdate(
          newAccumPartialValue = Some(update), oldAccumPartialValue = Some(oldValue)))

        // Check basic consistency
        if (oldValue.rddNumPartitions != update.rddNumPartitions) {
          unexpectedLastAttemptMetricUpdate(
            invalidate = true,
            reason = "RDD with changing number of partitions",
            newAccumPartialValue = Some(update),
            oldAccumPartialValue = Some(oldValue))
          return
        }
        if (oldValue.rddScopeId != update.rddScopeId) {
          unexpectedLastAttemptMetricUpdate(
            invalidate = true,
            reason = "RDD with changing RDDOperationScope",
            newAccumPartialValue = Some(update),
            oldAccumPartialValue = Some(oldValue))
          return
        }

        if (oldRDDValue.isEmptyAt(update.rddPartitionId)) {
          // No previous attempt for this RDD partition.
          oldRDDValue.update(update)
        } else {
          if (update.attempt > oldValue.attempt) {
            // New last attempt for this RDD partition.
            oldRDDValue.update(update)
          } else if (update.attempt == oldValue.attempt) {
            // Same attempt, should not happen.
            unexpectedLastAttemptMetricUpdate(
              invalidate = true,
              reason = "Same stage, stageAttemptId and taskAttemptNumber reported multiple times",
              newAccumPartialValue = Some(update),
              oldAccumPartialValue = Some(oldValue))
          }
          // else: Older attempt reported after newer attempt. Not fatal, discard it.
        }

      case None => // First time we see this RDD.
        logTrace(log"mergeLastAttempt new RDD update:\n" + logAccumulatorUpdate(
          newAccumPartialValue = Some(update), oldAccumPartialValue = None))
        val newVal = LastAttemptRDDVals.createFromFirstUpdate(update)
        lastAttemptRddsMap.put(update.rddId, newVal)
    }
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricUpdate(
        invalidate = true,
        reason = "Unexpected exception in mergeLastAttempt",
        exception = Some(e))
  }

  /** Accumulates last attempt values from given RDD into an acc. */
  private def lastAttemptValueAggregateInternal(rddId: Int, acc: this.type) = {
    // Note: even if the given RDD is not present, we can't tell if it executed but just never
    // updated this accumulator, so we still report the zero value back.
    for {
      lastAttemptVal <- lastAttemptRddsMap.get(rddId)
      partitionId <- lastAttemptVal.partitionPartialVals.indices
    } {
      // Some partitions may not be computed.
      // May be because of operations like take.
      // May be because of AQE coalescing executing tasks covering multiple partitions.
      if (!lastAttemptVal.isEmptyAt(partitionId)) {
        acc.partialMerge(lastAttemptVal.partitionPartialVals(partitionId))
      }
    }
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from a set of RDDs.
   *
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForRDDIds(rddIds: Seq[Int]): Option[OUT] = try {
    if (lastAttemptAccumulatorInvalid) return None
    assertValid()
    if (lastAttemptDirectDriverValue.isDefined) {
      // return zero value if there is no RDD execution recorded.
      return Some(copyAndReset().asInstanceOf[this.type].value)
    }

    val acc = copyAndReset().asInstanceOf[this.type]
    rddIds.distinct.foreach(lastAttemptValueAggregateInternal(_, acc))
    Some(acc.value)
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in lastAttemptValueForRDDs",
        exception = Some(e))
      None
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from a specific RDD.
   *
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForRDDId(rddId: Int): Option[OUT] = try {
    lastAttemptValueForRDDIds(Seq(rddId))
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in lastAttemptValueForRDD",
        exception = Some(e))
      None
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from all RDDs that ever
   * returned any values for it.
   *
   * If the metric was used directly on the driver, and was not used in any RDD execution,
   * the driver value will be used instead.
   *
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForAllRDDs(): Option[OUT] = try {
    if (lastAttemptAccumulatorInvalid) return None
    assertValid()
    if (lastAttemptDirectDriverValue.isDefined) return lastAttemptDirectDriverValue
    lastAttemptValueForRDDIds(lastAttemptRddsMap.keys.toSeq)
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in lastAttemptValueForAllRDDs",
        exception = Some(e))
      None
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from the RDD with the highest
   * id that ever returned any values for it.
   *
   * If the metric was used directly on the driver, and was not used in any RDD execution,
   * the driver value will be used instead.
   *
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForHighestRDDId(): Option[OUT] = try {
    if (lastAttemptAccumulatorInvalid) return None
    assertValid()
    if (lastAttemptDirectDriverValue.isDefined) return lastAttemptDirectDriverValue

    if (lastAttemptRddsMap.nonEmpty) {
      lastAttemptValueForRDDId(lastAttemptRddsMap.keys.max)
    } else {
      // return zero value if there is no RDD execution recorded.
      Some(copyAndReset().asInstanceOf[this.type].value)
    }
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in lastAttemptValueForHighestRDDId",
        exception = Some(e))
      None
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from RDDs with given scope ids.
   *
   * Should be used only on the Spark Driver, on the instance of [[LastAttemptAccumulator]] that
   * was created and registered in [[AccumulatorContext]] by [[AccumulatorV2#register]].
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForRDDScopes(rddScopeIds: Seq[String]): Option[OUT] = try {
    if (lastAttemptAccumulatorInvalid) return None
    assertValid()
    if (lastAttemptDirectDriverValue.isDefined) {
      // Return zero value if there is no RDD execution recorded.
      return Some(copyAndReset().asInstanceOf[this.type].value)
    }
    val scopesLookup = rddScopeIds.toSet
    val matchingRDDs = lastAttemptRddsMap.values.filter { rddVal =>
      rddVal.rddScopeId.exists(scopesLookup.contains)
    }.toSeq
    // When multiple RDDs share the same scope (e.g. repeated Dataset.collect() calls create
    // new wrapper RDDs in the same scope, or BroadcastNestedLoopJoin executing the probe side
    // twice), only aggregate the latest one per scope, identified by the highest RDD id.
    // RDD ids are globally monotonic, so the highest id is the latest.
    val rddIds = matchingRDDs.groupBy(_.rddScopeId).values.map(_.maxBy(_.rddId).rddId).toSeq
    lastAttemptValueForRDDIds(rddIds)
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in lastAttemptValueForRDDScopes",
        exception = Some(e))
      None
  }

  /** Visible for testing. */
  def getDirectDriverValue: Option[OUT] = {
    lastAttemptDirectDriverValue
  }

  /** Visible for testing */
  def getHighestRDDId: Option[Int] = {
    if (lastAttemptRddsMap.nonEmpty) Some(lastAttemptRddsMap.keys.max) else None
  }

  /** Visible for testing */
  def getNumRDDs: Int = {
    lastAttemptRddsMap.keys.size
  }

  /** Visible for testing */
  def getValid: Boolean = {
    !lastAttemptAccumulatorInvalid
  }
}
