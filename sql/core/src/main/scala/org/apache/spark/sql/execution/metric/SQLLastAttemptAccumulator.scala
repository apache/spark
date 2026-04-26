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
package org.apache.spark.sql.execution.metric

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.{LogEntry, Logging}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.{BaseSubqueryExec, QueryExecution, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec, SubqueryExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.util.{AccumulatorV2, LastAttemptAccumulator}

/*
 * SQLLastAttemptAccumulator is a LastAttemptAccumulator that allows tracking the last attempt
 * updates that happened in the scope of execution of a plan created by a specific Dataset's
 * QueryExecution.
 *
 * Tracking RDDs belonging to a Dataset execution.
 * -----------------------------------------------
 * Dataset executes executedPlan from its QueryExecution. Each SparkPlan node in the
 * executedPlan saves the RDD with its execution (executeRDD or executeColumnarRDD). However,
 * the root RDD of a Spark Stage that actually gets submitted and executed is not necessarily
 * that RDD. It may be an ephemeral RDD created on the fly when submitting the job, e.g.:
 * - for result stages, there may be additional transformations to format the results, like
 *   apply an Encoder (e.g. turn InternalRows into Rows)
 *   or transformations for Arrow
 * - for map stages, there may be some additional transformations to prepare the shuffle
 *   data in correct format.
 * - operations like dataframe caching may wrap the plan results to format and write to cache.
 * We therefore cannot track the metrics updates just by RDD id. However, each SparkPlan also
 * creates an RDDOperationScope, and wraps the execution it submits by that scope.
 * The completed Tasks should have RDDOperationScope of the SparkPlan that submitted the
 * Stage. We need to extract the RDDOperationScopes from Dataset.queryExecution.executedPlan
 * to track last attempt metric updates coming from that execution.
 *
 * Additionally, it is possible that the same queryExecution.executedPlan is reused. For
 * example, when collect() is called multiple times on the same Dataset.
 * - Part of the execution (e.g. the shuffles) should then be reused. Accumulator should still
 *   keep their partial values associated to its RDDOperationScope, and return it for this
 *   new attempt.
 * - Some of the execution (e.g. the result stage) may be recomputed. Since the SparkPlan will
 *   be the same, RDDOperationScope will be the same, and this should become a newer execution
 *   of the same RDD, which should replace the previous one.
 *
 * AQE plan changes
 * ----------------
 * AQE re-optimizes LogicalPlan and creates new SparkPlan. If the new plan doesn't contain
 * some of the QueryStages from the previous plan, they can be cancelled while they already
 * started running and accumulated some metric results.
 * If the metric is part of SparkPlan.metrics, then the newly created plan will have new
 * metrics and the old metrics would have been discarded; so nothing needs to be tracked here.
 * But if the metric is coming from outside, it can be reused by the new SparkPlan.
 * A new plan will have a new RDD and a new RDDOperationScope, so by tracking these for the
 * final AQE plan, only values from the final plan and execution should be aggregated.
 *
 * It can also happen that the new AQE plan reuses SparkPlan instances from the old plan,
 * see CancelShuffleStageInBroadcastJoin. However, in that case, the old plan will be put
 * under some new plan in newly submitted Stages. Since we only truly track the plans that
 * submit Stages, these should be different and enough to disambiguate.
 *
 * Driver only updates
 * -------------------
 * The metric can be updated directly on the driver side, during the execution of catalyst
 * optimizer. One example is [[ConvertToLocalRelation]] optimization rule, which constant folds
 * pieces of the plan.
 * Execution in this scope is tagged with [[QueryExecution.id]] using
 * [[SparkContext.DATASET_QUERY_EXECUTION_ID_KEY]] property, and this metric is tracking
 * the metric value separately for each QueryExecution.
 * Like with LastAttemptAccumulator, the metric will bail out if it's updated both from the driver
 * and from executor Tasks.
 *
 * Cached / Checkpointed plans
 * ---------------------------
 * If the metric was used inside a cached (df.cache, df.persist) or checkpointed (df.checkpoint,
 * df.localCheckpoint) plan, which is then turned into an RDDScanExec or InMemoryTableScanExec
 * in the Dataset's executedPlan, [[lastAttemptValueForDataset]] and
 * [[lastAttemptValueForQueryExecution]] are declared undefined behavior. In this case,
 * [[lastAttemptValueForHighestRDDId()]] should be used instead, which returns the value from
 * the execution in which the plan was cached/checkpointed.
 *
 * The main issue is if the metric is in the top stage of the cached plan. When that plan is
 * executed in some Dataset (as lazy execution), the metric will be executed in the scope of the
 * stage that contains the InMemoryTableScanExec / RDDScanExec, which will be some parent of that
 * plan, and not plan of the cached plan. So if the cached plan is then used in another Dataset,
 * that Dataset will not have information about that parent.
 * There could be some hacks done to fix it by recording in the InMemoryRelation the scopes in
 * which it was materialized. There are also other issues, like that checkpoint throws away the
 * plan, so it would also have to record the RDD scopes used during checkpointing. This gets
 * further complicated if recomputations are involved, and are done in yet another scope.
 * It was declared undefined behavior instead of pursuing this.
 */

/**
 * A trait that can be mixed into a subclass of [[AccumulatorV2]] to track the "logical"
 * value of the "last attempt" of the execution using the accumulator.
 * In addition to what [[LastAttemptAccumulator]] does, it allows tracking the last attempt
 * executed in the scope of a Dataset's QueryExecution, via
 * [[lastAttemptValueForDataset]] and [[lastAttemptValueForQueryExecution]] methods.
 */
trait SQLLastAttemptAccumulator[IN, OUT, PARTIAL, DRIVER_ACC]
    extends LastAttemptAccumulator[IN, OUT, PARTIAL] {
  this: AccumulatorV2[IN, OUT] =>

  /** Create a fresh accumulator to hold driver-side values for one QueryExecution. */
  protected def newDriverQueryExecutionAcc(): DRIVER_ACC
  /** Add a value to a driver-side per-QueryExecution accumulator. */
  protected def addToDriverAcc(acc: DRIVER_ACC, value: IN): Unit
  /** Set the value of a driver-side per-QueryExecution accumulator. */
  protected def setDriverAcc(acc: DRIVER_ACC, value: OUT): Unit
  /** Read the value of a driver-side per-QueryExecution accumulator. */
  protected def driverAccValue(acc: DRIVER_ACC): OUT

  @transient
  private var lastAttemptDirectDriverQueryExecutionValues: mutable.Map[String, DRIVER_ACC] = _

  override def initializeLastAttemptAccumulator()(implicit ct: ClassTag[PARTIAL]): Unit = try {
    super.initializeLastAttemptAccumulator()(ct)
    lastAttemptDirectDriverQueryExecutionValues = new mutable.HashMap[String, DRIVER_ACC]()
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in initializeLastAttemptAccumulator",
        exception = Some(e))
  }

  override def resetLastAttemptAccumulator(): Unit = try {
    super.resetLastAttemptAccumulator()
    lastAttemptDirectDriverQueryExecutionValues = new mutable.HashMap[String, DRIVER_ACC]()
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in resetLastAttemptAccumulator",
        exception = Some(e))
  }

  override protected def assertValid() = {
    super.assertValid()
    assert(lastAttemptDirectDriverQueryExecutionValues != null)
  }

  protected def getOrCreateDirectDriverQueryExecutionValue(queryExecutionId: String): DRIVER_ACC = {
    lastAttemptDirectDriverQueryExecutionValues.synchronized {
      if (!lastAttemptDirectDriverQueryExecutionValues.contains(queryExecutionId)) {
        lastAttemptDirectDriverQueryExecutionValues.put(
          queryExecutionId, newDriverQueryExecutionAcc())
      }
      lastAttemptDirectDriverQueryExecutionValues(queryExecutionId)
    }
  }

  protected def getActiveDatasetQueryExecutionId: Option[String] = {
    SparkContext
      .getActive
      .flatMap(sc => Option(sc.getLocalProperty(SparkContext.DATASET_QUERY_EXECUTION_ID_KEY)))
  }

  /**
   * Check if the value is added on the driver side, not from within a task.
   * If it is set in the scope of a Dataset's QueryExecution, associate it with that scope.
   * This must be called from `add` methods of any AccumulatorV2 subclass supporting
   * SQL last attempt metrics to set what the `value` of the metric is after the operation.
   * This should be called there after [[setValueIfOnDriverSide]].
   */
  protected def addQueryExecutionValueIfOnDriverSide(value: IN): Unit = try {
    // Note: setValueIfOnDriverSide will already make it invalid if there are also RDD updates.
    if (isAtDriverSide && lastAttemptAccumulatorInitialized && !lastAttemptAccumulatorInvalid) {
      // Direct update on the driver, not from within a task.
      getActiveDatasetQueryExecutionId match {
        case Some(qeId) =>
          addToDriverAcc(getOrCreateDirectDriverQueryExecutionValue(qeId), value)
        case None => // pass
      }
    }
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in addQueryExecutionValueIfOnDriverSide",
        exception = Some(e))
  }

  /**
   * Like [[addQueryExecutionValueIfOnDriverSide]], but for set operations.
   */
  protected def setQueryExecutionValueIfOnDriverSide(value: OUT): Unit = try {
    if (isAtDriverSide && lastAttemptAccumulatorInitialized && !lastAttemptAccumulatorInvalid) {
      getActiveDatasetQueryExecutionId match {
        case Some(qeId) =>
          setDriverAcc(getOrCreateDirectDriverQueryExecutionValue(qeId), value)
        case None => // pass
      }
    }
  } catch {
    case NonFatal(e) =>
      unexpectedLastAttemptMetricOperation(
        invalidate = true,
        reason = "Unexpected exception in setQueryExecutionValueIfOnDriverSide",
        exception = Some(e))
  }

  override def logAccumulatorState: LogEntry = try {
    val driverQEVals = Option(lastAttemptDirectDriverQueryExecutionValues)
      .map(_.map { case (key, acc) => s"$key -> ${driverAccValue(acc)}" }.mkString("\n"))
      .getOrElse("<not initialized>")
    super.logAccumulatorState +
      log"""
         |Direct driver QE values:
         |${MDC(logKeyAccumulatorState, driverQEVals)}
         """.stripMargin
  } catch {
    case NonFatal(e) =>
      logWarning(log"Unexpected exception in logAccumulatorState", e)
      log"<Unexpected exception in logAccumulatorState>"
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from the last execution of this
   * QueryExecution.
   *
   * @note The output of this method is undefined if this metric was used inside a part of the plan
   *       which was either checkpointed (e.g. df.localCheckpoint(), df.checkpoint()) or cached
   *       (e.g. df.cache(), df.persist()).
   *       [[lastAttemptValueForHighestRDDId()]] should be used instead, which returns the
   *       value from the execution in which the plan was cached/checkpointed.
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForQueryExecution(qe: QueryExecution): Option[OUT] = {
    if (lastAttemptAccumulatorInvalid) return None
    assertValid()
    // If there was a driver set value defined in the scope of this QueryExecution, return that.
    lastAttemptDirectDriverQueryExecutionValues.get(qe.id.toString) match {
      case Some(acc) => return Some(driverAccValue(acc))
      case None => // pass
    }
    // Otherwise, gather the RDD scopes from the plan and find metric updates from these scopes.
    val scopes = SQLLastAttemptAccumulator.extractStageRDDScopes(qe.executedPlan)
    scopes match {
      case Left(bailOutReason) =>
        unexpectedLastAttemptMetricOperation(
          invalidate = false,
          reason = s"Unable to extract RDD scopes from query execution plan: $bailOutReason")
        None
      case Right(scopes) =>
        lastAttemptValueForRDDScopes(scopes)
    }
  }

  /**
   * Returns the last attempt value of this accumulator, aggregated from the last execution of this
   * Dataset.
   *
   * @note The output of this method is undefined if this metric was used inside a part of the plan
   *       which was either checkpointed (e.g. df.localCheckpoint(), df.checkpoint()) or cached
   *       (e.g. df.cache(), df.persist()).
   *       [[lastAttemptValueForHighestRDDId()]] should be used instead, which returns the
   *       value from the execution in which the plan was cached/checkpointed.
   *
   * @return None if the last attempt value cannot be established, Some(value) otherwise.
   */
  def lastAttemptValueForDataset(ds: Dataset[_]): Option[OUT] = {
    lastAttemptValueForQueryExecution(ds.queryExecution)
  }

  /** Visible for testing. */
  def getDirectDriverQueryExecutionValue(qeId: String): Option[OUT] = {
    lastAttemptDirectDriverQueryExecutionValues.get(qeId).map(driverAccValue)
  }
}

object SQLLastAttemptAccumulator extends Logging {

  private[metric] def extractStageRDDScopes(sparkPlan: SparkPlan): Either[String, Seq[String]] = {
    var bailOutReason: Option[String] = None

    // recurse, setting the bailOutReason on failure, or returning the list of scopes on success.
    def recurse(sparkPlan: SparkPlan): Seq[String] = {
      if (bailOutReason.isDefined) {
        Nil
      } else {
        extractStageRDDScopes(sparkPlan) match {
          case Left(reason) =>
            bailOutReason = Some(reason)
            Nil
          case Right(scopes) => scopes
        }
      }
    }

    def scopeIds(sparkPlan: SparkPlan): Seq[String] = {
      AdaptiveSparkPlanHelper.stripAQEPlan(sparkPlan) match {
        case w: WholeStageCodegenExec =>
          // WholeStageCodegenExec can fallback and execute the child plan without codegen instead,
          // we don't know when this happens, so we need to account for both cases.
          // It will never be both at the same time as this is a compilation time decision, so
          // returning both won't result in duplicates.
          Seq(w.rddScopeId, w.child.rddScopeId)
        case p => Seq(p.rddScopeId)
      }
    }

    // The root of the plan is submitted as a result stage.
    val resultStageScopes = scopeIds(sparkPlan)

    val stagesScopes = AdaptiveSparkPlanHelper.flatMap(sparkPlan) {
      case _ if bailOutReason.isDefined => Nil

      // broadcast exchange stage submitting nodes
      case bl: BroadcastExchangeLike => bl match {
        case b: BroadcastExchangeExec =>
          // The job is submitted in scope of child of the broadcast exchange.
          // ```
          // val rs = child.executeCollectResult()
          // ```
          // <- executeCollectResult() is called on the child, and child executes it in its scope.
          scopeIds(b.child)
        case p =>
          // Bail out if future unknown implementation is encountered.
          bailOutReason = Some(s"Unsupported BroadcastExchangeLike: ${p.getClass.getName}")
          Nil
      }

      // shuffle exchange stage submitting nodes
      case sl: ShuffleExchangeLike => sl match {
        // All shuffle exchange implementations create the ShuffledRowRDD / ShuffledBlockRDD
        // with its own scope, and it will be executed in that scope.
        case s: ShuffleExchangeExec => scopeIds(s)
        case p =>
          // Bail out if future unknown implementation is encountered.
          bailOutReason = Some(s"Unsupported ShuffleExchangeLike: ${p.getClass.getName}")
          Nil
      }

      // reused exchange
      case r: ReusedExchangeExec =>
        // Reused exchange is going to reuse stuff executed in the scope of its child,
        // i.e. the exchange it reuses.
        recurse(r.child)

      case sl: BaseSubqueryExec => sl match {
        case s: SubqueryExec =>
          // ```
          // val rows: Array[InternalRow] = if (maxNumRows.isDefined) {
          //  child.executeTake(maxNumRows.get)
          // } else {
          //   child.executeCollect()
          // }
          // ```
          // will launch stages in scope of child.
          scopeIds(s.child)
        case _: SubqueryBroadcastExec =>
          // Used by DPP filter only, not part of main flow of query execution.
          Nil
        case _: SubqueryAdaptiveBroadcastExec =>
          // Used by DPP filter only.
          Nil
        case p =>
          // Bail out if future unknown implementation is encountered.
          bailOutReason = Some(s"Unsupported BaseSubqueryExec: ${p.getClass.getName}")
          Nil
      }

      /* Useful comments for posterity.
      // cached table node
      case _: InMemoryTableScanLike =>
        // Do nothing for cached tables. There are many border cases where it wouldn't work.
        // Some notes for posterity:
        // For [[InMemoryTableScanExec]], we could recursed into the cachedPlan, but:
        // - if the metric is in the top stage of that plan, then it would be executed in the scope
        //   of the stage of whatever execution that InMemoryTableScanExec is part of when the
        //   plan is cached. [[InMemoryTableScanExec]] is not a stage submitting node by itself, and
        //   by itself it doesn't have visibility into the parent that submits the stage that
        //   materializes the cache. If the current executedPlan is not the one that materializes,
        //   then the metric would return 0 instead of the value from the cached execution. If the
        //   current executedPlan is the one that materializes the cache, then it would be the
        //   correct value.
        // - if the metric is in a map stage of the cachedPlan, then it would be correctly
        //   annotated with the scope of that stage, and it would work correctly.
        //
        // Since it's hard to achieve a consistent behavior here, we just do not support it.
        Nil

      // RDD node
      case _: RDDScanExec =>
        // Similar as with cached tables, do nothing with RDDs.
        // This could be a plan coming from an execution of df.checkpoint().
        // Since checkpointing cuts the references to the original plan, there is no way to descend
        // into it to check attribution.
        // We could try to make checkpoint collect and store the scopes of the original execution,
        // but even then it would face similar inconsistencies as described above for cached plans.
        // - if the metric is in the top stage of that plan, then if it was executed in the scope
        //   of this execution, it would be attributed to the scope of the parent stage that is
        //   consuming the checkpointed RDD, not to any scope of the original plan.
        // - if the metric is in a map stage of the plan that was checkpointed, it requires that
        //   checkpoint would track these stages and scopes.
        //
        // Since it's hard to achieve a consistent behavior here, we just do not support it.
        Nil
      */

      case _ => Nil // only extract from nodes that submit stages
    }

    // also collect the plan scopes of all subqueries, which are executed "on the side".
    val subqueriesScopes = AdaptiveSparkPlanHelper.flatMap(sparkPlan) { p =>
      p.subqueries.flatMap(recurse)
    }

    if (bailOutReason.isDefined) {
      Left(bailOutReason.get)
    } else {
      Right(resultStageScopes ++ stagesScopes ++ subqueriesScopes)
    }
  }
}
