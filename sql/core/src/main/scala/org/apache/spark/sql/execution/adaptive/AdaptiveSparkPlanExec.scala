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

package org.apache.spark.sql.execution.adaptive

import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec._
import org.apache.spark.sql.execution.adaptive.OrphanBSCollect.OrphanBSCollect
import org.apache.spark.sql.execution.bucketing.DisableUnnecessaryBucketedScan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastHashJoinUtil, BroadcastVarPushDownData, HashedRelation, JoiningKeyData, ProxyBroadcastVarAndStageIdentifier, SELF_PUSH}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLAdaptiveSQLMetricUpdates, SQLPlanMetric}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkFatalException, ThreadUtils}


/**
 * A root node to execute the query plan adaptively. It splits the query plan into independent
 * stages and executes them in order according to their dependencies. The query stage
 * materializes its output at the end. When one stage completes, the data statistics of the
 * materialized output will be used to optimize the remainder of the query.
 *
 * To create query stages, we traverse the query tree bottom up. When we hit an exchange node,
 * and if all the child query stages of this exchange node are materialized, we create a new
 * query stage for this exchange node. The new stage is then materialized asynchronously once it
 * is created.
 *
 * When one query stage finishes materialization, the rest query is re-optimized and planned based
 * on the latest statistics provided by all materialized stages. Then we traverse the query plan
 * again and create more stages if possible. After all stages have been materialized, we execute
 * the rest of the plan.
 */
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan,
    @transient context: AdaptiveExecutionContext,
    @transient preprocessingRules: Seq[Rule[SparkPlan]],
    @transient isSubquery: Boolean,
    @transient override val supportsColumnar: Boolean = false)
  extends LeafExecNode {

  @transient private val lock = new Object()

  @transient private val logOnLevel: ( => String) => Unit = conf.adaptiveExecutionLogLevel match {
    case "TRACE" => logTrace(_)
    case "DEBUG" => logDebug(_)
    case "INFO" => logInfo(_)
    case "WARN" => logWarning(_)
    case "ERROR" => logError(_)
    case _ => logDebug(_)
  }

  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // The logical plan optimizer for re-optimizing the current logical plan.
  @transient private val optimizer = new AQEOptimizer(conf,
    session.sessionState.adaptiveRulesHolder.runtimeOptimizerRules)

  // `EnsureRequirements` may remove user-specified repartition and assume the query plan won't
  // change its output partitioning. This assumption is not true in AQE. Here we check the
  // `inputPlan` which has not been processed by `EnsureRequirements` yet, to find out the
  // effective user-specified repartition. Later on, the AQE framework will make sure the final
  // output partitioning is not changed w.r.t the effective user-specified repartition.
  @transient private val requiredDistribution: Option[Distribution] = if (isSubquery) {
    // Subquery output does not need a specific output partitioning.
    Some(UnspecifiedDistribution)
  } else {
    AQEUtils.getRequiredDistribution(inputPlan)
  }

  @transient private val costEvaluator =
    conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS) match {
      case Some(className) => CostEvaluator.instantiate(className, session.sparkContext.getConf)
      case _ => SimpleCostEvaluator(conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))
    }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
    // For cases like `df.repartition(a, b).select(c)`, there is no distribution requirement for
    // the final plan, but we do need to respect the user-specified repartition. Here we ask
    // `EnsureRequirements` to not optimize out the user-specified repartition-by-col to work
    // around this case.
    val ensureRequirements =
      EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)
    Seq(
      RemoveRedundantProjects,
      ensureRequirements,
      AdjustShuffleExchangePosition,
      ValidateSparkPlan,
      ReplaceHashWithSortAgg,
      RemoveRedundantSorts,
      DisableUnnecessaryBucketedScan,
      OptimizeSkewedJoin(ensureRequirements)
    ) ++ context.session.sessionState.adaptiveRulesHolder.queryStagePrepRules
  }

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    PlanAdaptiveDynamicPruningFilters(this),
    ReuseAdaptiveSubquery(context.subqueryCache),
    OptimizeSkewInRebalancePartitions,
    CoalesceShufflePartitions(context.session),
    // `OptimizeShuffleWithLocalRead` needs to make use of 'AQEShuffleReadExec.partitionSpecs'
    // added by `CoalesceShufflePartitions`, and must be executed after it.
    OptimizeShuffleWithLocalRead
  )

  // This rule is stateful as it maintains the codegen stage ID. We can't create a fresh one every
  // time and need to keep it in a variable.
  @transient private val collapseCodegenStagesRule: Rule[SparkPlan] =
    CollapseCodegenStages()

  // A list of physical optimizer rules to be applied right after a new stage is created. The input
  // plan to these rules has exchange as its root node.
  private def postStageCreationRules(outputsColumnar: Boolean) = Seq(
    ApplyColumnarRulesAndInsertTransitions(
      context.session.sessionState.columnarRules, outputsColumnar),
    collapseCodegenStagesRule
  )

  private def optimizeQueryStage(plan: SparkPlan, isFinalStage: Boolean): SparkPlan = {
    val optimized = queryStageOptimizerRules.foldLeft(plan) { case (latestPlan, rule) =>
      val applied = rule.apply(latestPlan)
      val result = rule match {
        case _: AQEShuffleReadRule if !applied.fastEquals(latestPlan) =>
          val distribution = if (isFinalStage) {
            // If `requiredDistribution` is None, it means `EnsureRequirements` will not optimize
            // out the user-specified repartition, thus we don't have a distribution requirement
            // for the final plan.
            requiredDistribution.getOrElse(UnspecifiedDistribution)
          } else {
            UnspecifiedDistribution
          }
          if (ValidateRequirements.validate(applied, distribution)) {
            applied
          } else {
            logDebug(s"Rule ${rule.ruleName} is not applied as it breaks the " +
              "distribution requirement of the query plan.")
            latestPlan
          }
        case _ => applied
      }
      planChangeLogger.logRule(rule.ruleName, latestPlan, result)
      result
    }
    planChangeLogger.logBatch("AQE Query Stage Optimization", plan, optimized)
    optimized
  }

  @transient val initialPlan = context.session.withActive {
    applyPhysicalRules(
      inputPlan, queryStagePreparationRules, Some((planChangeLogger, "AQE Preparations")))
  }

  @volatile private var currentPhysicalPlan = initialPlan
  private var isFinalPlan = false
  private var currentStageId = 0

  /**
   * Return type for `createQueryStages`
   * @param newPlan the new plan with created query stages.
   * @param allChildStagesMaterialized whether all child stages have been materialized.
   * @param newStages the newly created query stages, including new reused query stages.
   */
  private case class CreateStageResult(
    newPlan: SparkPlan,
    allChildStagesMaterialized: Boolean,
    newStages: Seq[QueryStageExec],
    orphanBatchScansWithProxyVar: Seq[BatchScanExec]
  )

  def executedPlan: SparkPlan = currentPhysicalPlan

  override def conf: SQLConf = context.session.sessionState.conf

  override def output: Seq[Attribute] = inputPlan.output

  override def doCanonicalize(): SparkPlan = inputPlan.canonicalized

  override def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    executedPlan.resetMetrics()
  }

  private def getExecutionId: Option[Long] = {
    // If the `QueryExecution` does not match the current execution ID, it means the execution ID
    // belongs to another (parent) query, and we should not call update UI in this query.
    Option(context.session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(
      _.toLong).filter(SQLExecution.getQueryExecution(_) eq context.qe)
  }

  def finalPhysicalPlan: SparkPlan = withFinalPlanUpdate(identity)

  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    if (isFinalPlan) return currentPhysicalPlan
    // TODO: Asif: try to find a clean solution to this:
    /**
     * right now  a case like:
     *        bhj
     *   |          |
     * BrdQS       bhj
     *              |
     *       BRQS      batchscan
     *
     * is a situation because batchscan is not part of any stage. as a result it can get into
     * re optimization , with partial broadcast var added.
     * till we come to a proper criteria in terms of say we do not re optimize till all the
     * pushdown bhjs are satisfied, or some thing else. we will go with this criteria.
     *
     *
     */
    // In case of this adaptive plan being executed out of `withActive` scoped functions, e.g.,
    // `plan.queryExecution.rdd`, we need to set active session here as new plan nodes can be
    // created in the middle of the execution.
    context.session.withActive {
      val executionId = getExecutionId
      // Use inputPlan logicalLink here in case some top level physical nodes may be removed
      // during `initialPlan`
      var currentLogicalPlan = inputPlan.logicalLink.get
      val levelWaitForDelayedStageMaterialization = conf.levelWaitForDelayedStageMaterialization
      val doBroadcastVarPush = conf.pushBroadcastedJoinKeysASFilterToScan
      var allStages = Map[Int, QueryStageExec]()
      val cachedBatchScansForStage = mutable.Map[Int, Seq[BatchScanExec]]()
      val stageIdToBuildsideJoinKeys = mutable.Map[Int, (LogicalPlan,
        Seq[ProxyBroadcastVarAndStageIdentifier], Seq[Expression])]()
      var allStageIdsProcessed = Set[Int]()
      val events = new LinkedBlockingQueue[StageMaterializationEvent]()
      val errors = new mutable.ArrayBuffer[Throwable]()
      var stagesToReplace = Seq.empty[QueryStageExec]
      var delayedStages = Set[Int]()
      var orphanBatchScans = Set[BatchScanExec]()
      var result = createQueryStages(currentPhysicalPlan, stageIdToBuildsideJoinKeys,
        OrphanBSCollect.orphan)
      orphanBatchScans ++= result.orphanBatchScansWithProxyVar
      var collectOrphans = OrphanBSCollect.no_collect
      while (!result.allChildStagesMaterialized) {
        allStages ++= result.newStages.map(stg => stg.id -> stg)
        orphanBatchScans ++= result.orphanBatchScansWithProxyVar
        // first identify of the new statges added , which can be processed immediately
        // and which are to be delayed
        val (newReadyStages, newUnreadyStages) = result.newStages.partition(
          BroadcastHashJoinUtil.isStageReadyForMaterialization(_, cachedBatchScansForStage))
        // check from the existing delayed batch, if any stages are ready for materialization
        // and not already in process
        val prevStgsNowReady = getDelayedStagesNowReady(allStages, cachedBatchScansForStage,
          delayedStages)
        val stgsReadyForMat = newReadyStages ++ prevStgsNowReady
        delayedStages ++= newUnreadyStages.map(_.id)
        val stagesToMaterialize = {
          // from the stages which are ready for materialization, check from delayed
          // stage if any stage re-using the exchange id for stage which is ready for mat
          // and put those also in materialization. if reuse id does not exist in allStages that
          // means it is in other plans, and hence materialized.
          val reuseStageNowReady = filterReusingStagesNowReady(allStages, delayedStages,
            stgsReadyForMat)
          val totalStagesReady = stgsReadyForMat ++ reuseStageNowReady.map(allStages)
          // if no new stage is going to be materialized and all scheduled stages are materialized
          // then do a check if delayed stages are satisfied by any of the
          // globally materialized stages.. else
          // throw exception instead of going to wait queue

          if (totalStagesReady.isEmpty && (allStageIdsProcessed.size + delayedStages.size) ==
            allStages.size && delayedStages.nonEmpty) {
            pushBroadcastVarToDelayedStages(allStages, stageIdToBuildsideJoinKeys.toMap,
              allStageIdsProcessed, delayedStages, cachedBatchScansForStage)
            // see if new stages are available for materialization now
            val anyReadyStages = getDelayedStagesNowReady(allStages, cachedBatchScansForStage,
              delayedStages)
            if (anyReadyStages.isEmpty) {
              throw new IllegalStateException("Unsatisfied stages or orphan batch scan remain " +
              s" all problematic stages = ${delayedStages.mkString(",")}")
            } else {
              anyReadyStages
            }
          } else {
            totalStagesReady
          }
        }
        currentPhysicalPlan = result.newPlan

        if (stagesToMaterialize.nonEmpty || result.newStages.nonEmpty) {
          if (result.newStages.nonEmpty) {
            stagesToReplace = result.newStages ++ stagesToReplace
            executionId.foreach(onUpdatePlan(_, result.newStages.map(_.plan)))
          }

          // SPARK-33933: we should submit tasks of broadcast stages first, to avoid waiting
          // for tasks to be scheduled and leading to broadcast timeout.
          // This partial fix only guarantees the start of materialization for BroadcastQueryStage
          // is prior to others, but because the submission of collect job for broadcasting is
          // running in another thread, the issue is not completely resolved.
          scheduleStagesForMaterialization(stagesToMaterialize, events)
          delayedStages --= stagesToMaterialize.map(_.id)
        }
        val (stagedIdsBatchProcessed, errorsList) = waitForNextStageToMaterialize(events)
        errors ++= errorsList
        allStageIdsProcessed ++= stagedIdsBatchProcessed
        pushBroadcastVarToDelayedStages(allStages, stageIdToBuildsideJoinKeys.toMap,
            allStageIdsProcessed, delayedStages, cachedBatchScansForStage, orphanBatchScans)
        // In case of errors, we cancel all running stages and throw exception.
        if (errors.nonEmpty) {
          cleanUpAndThrowException(errors, None)
        }
        // remove satisfied orphans
        orphanBatchScans = orphanBatchScans.filterNot(BroadcastHashJoinUtil.isBatchScanReady)
        val readyStages = getDelayedStagesNowReady(allStages, cachedBatchScansForStage,
          delayedStages)
        val reuseStageNowReady = filterReusingStagesNowReady(allStages, delayedStages,
          readyStages)
        val totalStagesReady = readyStages ++ reuseStageNowReady.map(allStages)
        if(totalStagesReady.nonEmpty) {
          scheduleStagesForMaterialization(totalStagesReady, events)
          delayedStages --= totalStagesReady.map(_.id)
        }
        // Try re-optimizing and re-planning. Adopt the new plan if its cost is equal to or less
        // than that of the current plan; otherwise keep the current physical plan together with
        // the current logical plan since the physical plan's logical links point to the logical
        // plan it has originated from.
        // Meanwhile, we keep a list of the query stages that have been created since last plan
        // update, which stands for the "semantic gap" between the current logical and physical
        // plans. And each time before re-planning, we replace the corresponding nodes in the
        // current logical plan with logical query stages to make it semantically in sync with
        // the current physical plan. Once a new plan is adopted and both logical and physical
        // plans are updated, we can clear the query stage list because at this point the two plans
        // are semantically and physically in sync again.

        val reoptimizeConditionsMet = !doBroadcastVarPush ||
          (orphanBatchScans.isEmpty && (levelWaitForDelayedStageMaterialization match {
            case 0 => true
            case 1 => delayedStages.isEmpty
            case 2 => delayedStages.isEmpty && allStageIdsProcessed.size == allStages.size
          }))
        if (reoptimizeConditionsMet) {
          val logicalPlan = replaceWithQueryStagesInLogicalPlan(currentLogicalPlan, stagesToReplace)
          val afterReOptimize = reOptimize(logicalPlan)
          if (afterReOptimize.isDefined) {
            val (newPhysicalPlan, newLogicalPlan) = afterReOptimize.get
            val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
            val newCost = costEvaluator.evaluateCost(newPhysicalPlan)
            if (newCost < origCost ||
              (newCost == origCost && currentPhysicalPlan != newPhysicalPlan)) {
              logOnLevel(s"Plan changed from $currentPhysicalPlan to $newPhysicalPlan")
              cleanUpTempTags(newPhysicalPlan)
              currentPhysicalPlan = newPhysicalPlan
              currentLogicalPlan = newLogicalPlan
              stagesToReplace = Seq.empty[QueryStageExec]
              // remove all the remaining orphans as there batchscan instances may have been re
              // created and that too without proxyBCVar.
              orphanBatchScans = Set.empty[BatchScanExec]
              // also at this point we would want to recollect the orphans if any
              // once the BroadcastFilterPushdown rule is added for re optimized plan.
              collectOrphans = OrphanBSCollect.no_collect
            }
          }
        }
        // Now that some stages have finished, we can try creating new stages.
        result = createQueryStages(currentPhysicalPlan, stageIdToBuildsideJoinKeys, collectOrphans)
        collectOrphans = OrphanBSCollect.no_collect
      }
      // Run the final plan when there's no more unfinished stages.
      currentPhysicalPlan = applyPhysicalRules(
        optimizeQueryStage(result.newPlan, isFinalStage = true),
        postStageCreationRules(supportsColumnar),
        Some((planChangeLogger, "AQE Post Stage Creation")))
      isFinalPlan = true
      executionId.foreach(onUpdatePlan(_, Seq(currentPhysicalPlan)))
      currentPhysicalPlan
    }
  }

  private def filterReusingStagesNowReady(
      allStages: Map[Int, QueryStageExec],
      delayedStages: Set[Int],
      stgsReadyForMat: Seq[QueryStageExec]): Set[Int] = delayedStages.filter(id => {
      val stg = allStages(id)
      stg.reuseSource.fold(false)(reuseId => !allStages.contains(reuseId) ||
        stgsReadyForMat.exists(_.id == reuseId))
    })

  private def scheduleStagesForMaterialization(stagesToMaterialize: Seq[QueryStageExec],
      events: LinkedBlockingQueue[StageMaterializationEvent]): Unit = {
    val reorderedNewStages = stagesToMaterialize
      .sortWith {
        case (_: BroadcastQueryStageExec, _: BroadcastQueryStageExec) => false
        case (_: BroadcastQueryStageExec, _) => true
        case _ => false
      }
    // Start materialization of all new stages and fail fast if any stages failed eagerly
    reorderedNewStages.foreach { stage =>
      try {
        stage.materialize().onComplete { res =>
          if (res.isSuccess) {
            events.offer(StageSuccess(stage, res.get))
          } else {
            events.offer(StageFailure(stage, res.failed.get))
          }
        }(AdaptiveSparkPlanExec.executionContext)
      } catch {
        case e: Throwable =>
          cleanUpAndThrowException(Seq(e), Some(stage.id))
      }
    }
  }

  private def pushBroadcastVarToDelayedStages(
      allStages: Map[Int, QueryStageExec],
      stageIdToBuildsideJoinKeys: Map[Int, (LogicalPlan,
        Seq[ProxyBroadcastVarAndStageIdentifier], Seq[Expression])],
      allStageIdsProcessed: Set[Int],
      delayedStages: Set[Int],
      cachedBatchScansForStage: mutable.Map[Int, Seq[BatchScanExec]],
      anyOtherUnsatisfiedBatchScans: Set[BatchScanExec] = Set.empty): Unit = {
    val delayedStagesBatchscans = delayedStages.flatMap(id =>
      BroadcastHashJoinUtil.partitionBatchScansToReadyAndUnready(allStages(id),
        cachedBatchScansForStage)._2).toSeq ++ anyOtherUnsatisfiedBatchScans
    if (delayedStagesBatchscans.nonEmpty) {
      pushBroadcastVarToUnreadyBatchScans(delayedStagesBatchscans, stageIdToBuildsideJoinKeys,
        allStages, allStageIdsProcessed)
    }
  }

  private def getDelayedStagesNowReady(
      allStages: Map[Int, QueryStageExec],
      cachedBatchScansForStage: mutable.Map[Int, Seq[BatchScanExec]],
      delayedStages: Set[Int]): Seq[QueryStageExec] =
    delayedStages.flatMap(id => {
      val stage = allStages(id)
      if (BroadcastHashJoinUtil.isStageReadyForMaterialization(stage, cachedBatchScansForStage)
      && stage.reuseSource.isEmpty) {
        Seq(stage)
      } else {
        Seq.empty
      }
    }).toSeq

  private def waitForNextStageToMaterialize(events: LinkedBlockingQueue[StageMaterializationEvent]):
  (Set[Int], Seq[Throwable]) = {
    // Wait on the next completed stage, which indicates new stats are available and probably
    // new stages can be created. There might be other stages that finish at around the same
    // time, so we process those stages too in order to reduce re-planning.
    val errors = mutable.Buffer[Throwable]()
    val nextMsg = events.take()
    val rem = new util.ArrayList[StageMaterializationEvent]()
    events.drainTo(rem)
    val stagedIdsBatchProcessed = (for (message <- (Seq(nextMsg) ++ rem.asScala)) yield {
      val stage = message match {
        case StageSuccess(stage, res) =>
          stage.resultOption.set(Some(res))
          stage
        case StageFailure(stage, ex) =>
          errors.append(ex)
          stage
      }
      stage.id
    }).toSet
    stagedIdsBatchProcessed -> errors
  }

  private def pushBroadcastVarToUnreadyBatchScans(
      allUnreadyBatchScans: Seq[BatchScanExec],
      stageIdToBuildsideJoinKeys: Map[Int, (LogicalPlan,
        Seq[ProxyBroadcastVarAndStageIdentifier], Seq[Expression])],
      allStages: Map[Int, QueryStageExec],
      allProcessedStageIds: Set[Int]): Unit = {
    val identityHashMap = new util.IdentityHashMap[BatchScanExec, java.util.Set[java.lang.Long]]()
    allUnreadyBatchScans.map(bs =>
      bs -> bs.scan.asInstanceOf[SupportsRuntimeFiltering].getPushedBroadcastVarIds).foreach {
      case (bs, set) => identityHashMap.put(bs, set)
    }
    val stagesAndBatchScanForPush =
      allProcessedStageIds.filter(stageIdToBuildsideJoinKeys.contains).map(id => {
        val joinLegAndKeysOpt = stageIdToBuildsideJoinKeys.get(id)
        val batchScansToPush = allUnreadyBatchScans.flatMap(bs => {
          if (!identityHashMap.get(bs).contains(java.lang.Long.valueOf(id)))  {
            val proxyVars = bs.proxyForPushedBroadcastVar.get
            proxyVars.flatMap(proxy => {
              val buildLegPlan = proxy.buildLegPlan
                if (joinLegAndKeysOpt.fold(false) {
                  case(buildLp, buildProxies, _) =>
                    (buildLp.eq(buildLegPlan) || buildLp.canonicalized == buildLegPlan
                     .canonicalized) &&
                    (proxy.buildLegProxyBroadcastVarAndStageIdentifiers.isEmpty ||
                      (buildProxies.size ==
                        proxy.buildLegProxyBroadcastVarAndStageIdentifiers.size &&
                        buildProxies.forall(
                          proxy.buildLegProxyBroadcastVarAndStageIdentifiers.contains)
                      )
                    )
                }
              ) {
                  proxy.joiningKeysData.flatMap {jkd =>
                    joinLegAndKeysOpt.fold(Seq.empty[(BatchScanExec, JoiningKeyData)]) {
                      case (_, _, exprs) =>
                        if (exprs.exists(_.canonicalized ==
                            jkd.buildSideJoinKeyAtJoin.canonicalized)) {
                          Seq(bs -> jkd)
                        } else {
                          Seq.empty
                        }
                    }
                  }
              } else {
                Seq.empty
              }
            })
          } else {
            Seq.empty
          }
        })
        id -> batchScansToPush
      })

    stagesAndBatchScanForPush.foreach {
      case (stageId, data) =>
        if (data.nonEmpty) {
          val hashedRelation = allStages.getOrElse(stageId,
            context.stageCache.find(_._2.id == stageId).getOrElse(
              throw new IllegalStateException(s"Stage with id = $stageId not found"))).
                asInstanceOf[BroadcastQueryStageExec].
              broadcast.relationFuture.get().asInstanceOf[Broadcast[HashedRelation]]
              val pushData = data.map {
                case (bs, jkd) => BroadcastHashJoinUtil.convertJoinKeyDataToPushDownData(bs, jkd)
              }
            BroadcastHashJoinUtil.pushBroadcastVar(hashedRelation,
              stageIdToBuildsideJoinKeys(stageId)._3, pushData)
        }
    }
  }

  // Use a lazy val to avoid this being called more than once.
  @transient private lazy val finalPlanUpdate: Unit = {
    // Subqueries that don't belong to any query stage of the main query will execute after the
    // last UI update in `getFinalPhysicalPlan`, so we need to update UI here again to make sure
    // the newly generated nodes of those subqueries are updated.
    if (!isSubquery && currentPhysicalPlan.exists(_.subqueries.nonEmpty)) {
      getExecutionId.foreach(onUpdatePlan(_, Seq.empty))
    }
    logOnLevel(s"Final plan: $currentPhysicalPlan")
  }

  override def executeCollect(): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeCollect())
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTake(n))
  }

  override def executeTail(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTail(n))
  }

  override def doExecute(): RDD[InternalRow] = {
    withFinalPlanUpdate(_.execute())
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    withFinalPlanUpdate(_.executeColumnar())
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    withFinalPlanUpdate { finalPlan =>
      assert(finalPlan.isInstanceOf[BroadcastQueryStageExec])
      finalPlan.doExecuteBroadcast()
    }
  }

  private def withFinalPlanUpdate[T](fun: SparkPlan => T): T = {
    val plan = getFinalPhysicalPlan()
    val result = fun(plan)
    finalPlanUpdate
    result
  }

  protected override def stringArgs: Iterator[Any] = Iterator(s"isFinalPlan=$isFinalPlan")

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    super.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    if (currentPhysicalPlan.fastEquals(initialPlan)) {
      currentPhysicalPlan.generateTreeString(
        depth + 1,
        lastChildren :+ true,
        append,
        verbose,
        prefix = "",
        addSuffix = false,
        maxFields,
        printNodeId,
        indent)
    } else {
      generateTreeStringWithHeader(
        if (isFinalPlan) "Final Plan" else "Current Plan",
        currentPhysicalPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
      generateTreeStringWithHeader(
        "Initial Plan",
        initialPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
    }
  }

  private def generateTreeStringWithHeader(
      header: String,
      plan: SparkPlan,
      depth: Int,
      append: String => Unit,
      verbose: Boolean,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    append("   " * depth)
    append(s"+- == $header ==\n")
    plan.generateTreeString(
      0,
      Nil,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      indent = depth + 1)
  }

  override def hashCode(): Int = inputPlan.hashCode()

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[AdaptiveSparkPlanExec]) {
      return false
    }
    this.inputPlan == obj.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
  }

  /**
   * This method is called recursively to traverse the plan tree bottom-up and create a new query
   * stage or try reusing an existing stage if the current node is an [[Exchange]] node and all of
   * its child stages have been materialized.
   *
   * With each call, it returns:
   * 1) The new plan replaced with [[QueryStageExec]] nodes where new stages are created.
   * 2) Whether the child query stages (if any) of the current node have all been materialized.
   * 3) A list of the new query stages that have been created.
   */
  private def createQueryStages(plan: SparkPlan,
      stageIdToBuildsideJoinKeys: mutable.Map[Int, (LogicalPlan,
        Seq[ProxyBroadcastVarAndStageIdentifier], Seq[Expression])],
      orphanBSCollect: OrphanBSCollect)
  : CreateStageResult = plan match {
    case e: Exchange =>
      // First have a quick check in the `stageCache` without having to traverse down the node.
      context.stageCache.get(e.canonicalized) match {
        case Some(existingStage) if conf.exchangeReuseEnabled =>
          val stage = reuseQueryStage(existingStage, e)
          val isMaterialized = stage.isMaterialized
          CreateStageResult(
            newPlan = stage,
            allChildStagesMaterialized = isMaterialized,
            newStages = if (isMaterialized) Seq.empty else Seq(stage),
            orphanBatchScansWithProxyVar = Seq.empty
            )

        case _ =>
          val result = createQueryStages(e.child, stageIdToBuildsideJoinKeys,
            orphanBSCollect match {
              case OrphanBSCollect.no_collect => orphanBSCollect
              case _ => OrphanBSCollect.under_exchange_node
          })
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          if (result.allChildStagesMaterialized) {
            var newStage = newQueryStage(newPlan)
            if (conf.exchangeReuseEnabled) {
              // Check the `stageCache` again for reuse. If a match is found, ditch the new stage
              // and reuse the existing stage found in the `stageCache`, otherwise update the
              // `stageCache` with the new stage.
              val queryStage = context.stageCache.getOrElseUpdate(
                newStage.plan.canonicalized, newStage)
              if (queryStage.ne(newStage)) {
                newStage = reuseQueryStage(queryStage, e)
              }
            }
            val isMaterialized = newStage.isMaterialized
            CreateStageResult(
              newPlan = newStage,
              allChildStagesMaterialized = isMaterialized,
              newStages = if (isMaterialized) Seq.empty else Seq(newStage),
              orphanBatchScansWithProxyVar = result.orphanBatchScansWithProxyVar
            )
          } else {
            CreateStageResult(newPlan = newPlan, allChildStagesMaterialized = false,
              newStages = result.newStages,
              result.orphanBatchScansWithProxyVar)
          }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = q.isMaterialized, newStages = Seq.empty,
        orphanBatchScansWithProxyVar = Seq.empty)

    case bhj: BroadcastHashJoinExec if bhj.bcVarPushNode == SELF_PUSH =>
      val (buildPlan, streamPlan, buildKeys, _) =
        bhj.buildSide match {
          case BuildRight => (bhj.right, bhj.left, bhj.rightKeys, bhj.leftKeys)
          case BuildLeft => (bhj.left, bhj.right, bhj.leftKeys, bhj.rightKeys)
        }
      val buildSideStageResult = createQueryStages(buildPlan, stageIdToBuildsideJoinKeys,
        orphanBSCollect)
      val streamsideStageResult = createQueryStages(streamPlan, stageIdToBuildsideJoinKeys,
        orphanBSCollect match {
          case OrphanBSCollect.no_collect => orphanBSCollect
          case _ => OrphanBSCollect.orphan
        })
      val buildLPOpt = buildSideStageResult.newPlan match {
        case qse: QueryStageExec if !stageIdToBuildsideJoinKeys.contains(qse.id) =>
          val buildLegProxies = BroadcastHashJoinUtil.getAllBatchScansForSparkPlan(buildPlan).
            flatMap(_.proxyForPushedBroadcastVar.getOrElse(Seq.empty))
          Option(qse.id -> (BroadcastHashJoinUtil.getLogicalPlanFor(buildPlan), buildLegProxies))
        case _ => None
      }
      buildLPOpt.foreach {
        case (stgId, (buildLp, buildProxies)) => stageIdToBuildsideJoinKeys += stgId -> (buildLp,
         buildProxies, buildKeys.map(_.canonicalized))
      }
      if (buildSideStageResult.allChildStagesMaterialized) {
        // we have to handle the case like of reuse of exchange, where build stage
        // obtained is already  materialized. so there will not be any callback as
        // the build stage is already materialized. so we have to push variable here itself
        val pushDownData = buildLPOpt.fold(Seq.empty[BroadcastVarPushDownData]) {
          case (_, buildLp) => BroadcastHashJoinUtil.
            getPushdownDataForBatchScansUsingJoinKeys(buildKeys, streamPlan, buildLp)
        }
        if (pushDownData.nonEmpty) {
          val hashedRelation = buildSideStageResult.newPlan.
            asInstanceOf[BroadcastQueryStageExec].
            broadcast.relationFuture.get().asInstanceOf[Broadcast[HashedRelation]]
          BroadcastHashJoinUtil.pushBroadcastVar(hashedRelation, buildKeys, pushDownData)
        }
      }
      val results = Seq(buildSideStageResult, streamsideStageResult)
      val (leftPlan, rightPlan) = getLeftAndRightPlan(streamsideStageResult, buildSideStageResult,
        bhj)
      val newBhj = bhj.copy(left = leftPlan, right = rightPlan)
      newBhj.copyTagsFrom(bhj)
      CreateStageResult(
        newPlan = newBhj,
        allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
        newStages = results.flatMap(_.newStages),
        orphanBatchScansWithProxyVar = results.flatMap(_.orphanBatchScansWithProxyVar)
      )

    case bs: BatchScanExec if bs.proxyForPushedBroadcastVar.isDefined && orphanBSCollect ==
      OrphanBSCollect.orphan =>
      CreateStageResult(newPlan = plan, allChildStagesMaterialized = true,
        newStages = Seq.empty, orphanBatchScansWithProxyVar = Seq(bs))
      val results = Seq(buildSideStageResult, streamsideStageResult)
      val (leftPlan, rightPlan) = getLeftAndRightPlan(streamsideStageResult,
        buildSideStageResult, bhj)
      val newBhj = bhj.copy(left = leftPlan, right = rightPlan)
      newBhj.copyTagsFrom(bhj)

      CreateStageResult(
        newPlan = newBhj,
        allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
        newStages = results.flatMap(_.newStages),
        orphanBatchScansWithProxyVar = results.flatMap(_.orphanBatchScansWithProxyVar)
      )
    case bs: BatchScanExec if bs.proxyForPushedBroadcastVar.isDefined && orphanBSCollect ==
      OrphanBSCollect.orphan =>
      CreateStageResult(newPlan = plan, allChildStagesMaterialized = true,
        newStages = Seq.empty, orphanBatchScansWithProxyVar = Seq(bs))
    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesMaterialized = true,
          newStages = Seq.empty, orphanBatchScansWithProxyVar = Seq.empty)
      } else {
        val results = plan.children.map(sp => createQueryStages(sp,
          stageIdToBuildsideJoinKeys, orphanBSCollect))
        val newPlan = plan.withNewChildren(results.map(_.newPlan))
        CreateStageResult(newPlan = newPlan,
          allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
          newStages = results.flatMap(_.newStages),
          orphanBatchScansWithProxyVar = results.flatMap(_.orphanBatchScansWithProxyVar)
        )
      }
  }

  def getLeftAndRightPlan(streamStageResult: CreateStageResult,
      buildStageResult: CreateStageResult, bhj: BroadcastHashJoinExec): (SparkPlan, SparkPlan) =
    bhj.buildSide match {
      case BuildRight => (streamStageResult.newPlan, buildStageResult.newPlan)
      case BuildLeft => (buildStageResult.newPlan, streamStageResult.newPlan)
    }

  private def newQueryStage(e: Exchange): QueryStageExec = {
    val optimizedPlan = optimizeQueryStage(e.child, isFinalStage = false)
    val queryStage = e match {
      case s: ShuffleExchangeLike =>
        val newShuffle = applyPhysicalRules(
          s.withNewChildren(Seq(optimizedPlan)),
          postStageCreationRules(outputsColumnar = s.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation")))
        if (!newShuffle.isInstanceOf[ShuffleExchangeLike]) {
          throw new IllegalStateException(
            "Custom columnar rules cannot transform shuffle node to something else.")
        }
        ShuffleQueryStageExec(currentStageId, newShuffle, s.canonicalized)
      case b: BroadcastExchangeLike =>
        val newBroadcast = applyPhysicalRules(
          b.withNewChildren(Seq(optimizedPlan)),
          postStageCreationRules(outputsColumnar = b.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation")))
        if (!newBroadcast.isInstanceOf[BroadcastExchangeLike]) {
          throw new IllegalStateException(
            "Custom columnar rules cannot transform broadcast node to something else.")
        }
        BroadcastQueryStageExec(currentStageId, newBroadcast, b.canonicalized)
    }
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, e)
    queryStage
  }

  private def reuseQueryStage(existing: QueryStageExec, exchange: Exchange): QueryStageExec = {
    val queryStage = existing.newReuseInstance(currentStageId, exchange.output)
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, exchange)
    queryStage
  }

  /**
   * Set the logical node link of the `stage` as the corresponding logical node of the `plan` it
   * encloses. If an `plan` has been transformed from a `Repartition`, it should have `logicalLink`
   * available by itself; otherwise traverse down to find the first node that is not generated by
   * `EnsureRequirements`.
   */
  private def setLogicalLinkForNewQueryStage(stage: QueryStageExec, plan: SparkPlan): Unit = {
    val link = plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(
      plan.logicalLink.orElse(plan.collectFirst {
        case p if p.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
          p.getTagValue(TEMP_LOGICAL_PLAN_TAG).get
        case p if p.logicalLink.isDefined => p.logicalLink.get
      }))
    assert(link.isDefined)
    stage.setLogicalLink(link.get)
  }

  /**
   * For each query stage in `stagesToReplace`, find their corresponding logical nodes in the
   * `logicalPlan` and replace them with new [[LogicalQueryStage]] nodes.
   * 1. If the query stage can be mapped to an integral logical sub-tree, replace the corresponding
   *    logical sub-tree with a leaf node [[LogicalQueryStage]] referencing this query stage. For
   *    example:
   *        Join                   SMJ                      SMJ
   *      /     \                /    \                   /    \
   *    r1      r2    =>    Xchg1     Xchg2    =>    Stage1     Stage2
   *                          |        |
   *                          r1       r2
   *    The updated plan node will be:
   *                               Join
   *                             /     \
   *    LogicalQueryStage1(Stage1)     LogicalQueryStage2(Stage2)
   *
   * 2. Otherwise (which means the query stage can only be mapped to part of a logical sub-tree),
   *    replace the corresponding logical sub-tree with a leaf node [[LogicalQueryStage]]
   *    referencing to the top physical node into which this logical node is transformed during
   *    physical planning. For example:
   *     Agg           HashAgg          HashAgg
   *      |               |                |
   *    child    =>     Xchg      =>     Stage1
   *                      |
   *                   HashAgg
   *                      |
   *                    child
   *    The updated plan node will be:
   *    LogicalQueryStage(HashAgg - Stage1)
   */
  private def replaceWithQueryStagesInLogicalPlan(
      plan: LogicalPlan,
      stagesToReplace: Seq[QueryStageExec]): LogicalPlan = {
    var logicalPlan = plan
    stagesToReplace.foreach {
      case stage if currentPhysicalPlan.exists(_.eq(stage)) =>
        val logicalNodeOpt = stage.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(stage.logicalLink)
        assert(logicalNodeOpt.isDefined)
        val logicalNode = logicalNodeOpt.get
        val physicalNode = currentPhysicalPlan.collectFirst {
          case p if p.eq(stage) ||
            p.getTagValue(TEMP_LOGICAL_PLAN_TAG).exists(logicalNode.eq) ||
            p.logicalLink.exists(logicalNode.eq) => p
        }
        assert(physicalNode.isDefined)
        // Set the temp link for those nodes that are wrapped inside a `LogicalQueryStage` node for
        // they will be shared and reused by different physical plans and their usual logical links
        // can be overwritten through re-planning processes.
        setTempTagRecursive(physicalNode.get, logicalNode)
        // Replace the corresponding logical node with LogicalQueryStage
        val newLogicalNode = LogicalQueryStage(logicalNode, physicalNode.get)
        val newLogicalPlan = logicalPlan.transformDown {
          case p if p.eq(logicalNode) => newLogicalNode
        }
        logicalPlan = newLogicalPlan

      case _ => // Ignore those earlier stages that have been wrapped in later stages.
    }
    logicalPlan
  }

  /**
   * Re-optimize and run physical planning on the current logical plan based on the latest stats.
   */

  private def reOptimize(logicalPlan: LogicalPlan): Option[(SparkPlan, LogicalPlan)] = {
    try {
      logicalPlan.invalidateStatsCache()
      val optimized = optimizer.execute(logicalPlan)
      val sparkPlan = context.session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
      val newPlan = applyPhysicalRules(
        sparkPlan,
        preprocessingRules ++ queryStagePreparationRules,
        Some((planChangeLogger, "AQE Replanning")))
      // When both enabling AQE and DPP, `PlanAdaptiveDynamicPruningFilters` rule will
      // add the `BroadcastExchangeExec` node manually in the DPP subquery,
      // not through `EnsureRequirements` rule. Therefore, when the DPP subquery is complicated
      // and need to be re-optimized, AQE also need to manually insert the `BroadcastExchangeExec`
      // node to prevent the loss of the `BroadcastExchangeExec` node in DPP subquery.
      // Here, we also need to avoid to insert the `BroadcastExchangeExec` node when the newPlan is
      // already the `BroadcastExchangeExec` plan after apply the `LogicalQueryStageStrategy` rule.
      val finalPlan = inputPlan match {
        case b: BroadcastExchangeLike
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) => b.withNewChildren(Seq(newPlan))
        case _ => newPlan
      }
      Some((finalPlan, optimized))
    } catch {
      case e: InvalidAQEPlanException[_] =>
        logOnLevel(s"Re-optimize - ${e.getMessage()}:\n${e.plan}")
        None
    }

  }

  /**
   * Recursively set `TEMP_LOGICAL_PLAN_TAG` for the current `plan` node.
   */
  private def setTempTagRecursive(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    plan.setTagValue(TEMP_LOGICAL_PLAN_TAG, logicalPlan)
    plan.children.foreach(c => setTempTagRecursive(c, logicalPlan))
  }

  /**
   * Unset all `TEMP_LOGICAL_PLAN_TAG` tags.
   */
  private def cleanUpTempTags(plan: SparkPlan): Unit = {
    plan.foreach {
      case plan: SparkPlan if plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
        plan.unsetTagValue(TEMP_LOGICAL_PLAN_TAG)
      case _ =>
    }
  }

  /**
   * Notify the listeners of the physical plan change.
   */
  private def onUpdatePlan(executionId: Long, newSubPlans: Seq[SparkPlan]): Unit = {
    if (isSubquery) {
      // When executing subqueries, we can't update the query plan in the UI as the
      // UI doesn't support partial update yet. However, the subquery may have been
      // optimized into a different plan and we must let the UI know the SQL metrics
      // of the new plan nodes, so that it can track the valid accumulator updates later
      // and display SQL metrics correctly.
      val newMetrics = newSubPlans.flatMap { p =>
        p.flatMap(_.metrics.values.map(m => SQLPlanMetric(m.name.get, m.id, m.metricType)))
      }
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveSQLMetricUpdates(
        executionId, newMetrics))
    } else {
      val planDescriptionMode = ExplainMode.fromString(conf.uiExplainMode)
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        executionId,
        context.qe.explainString(planDescriptionMode),
        SparkPlanInfo.fromSparkPlan(context.qe.executedPlan)))
    }
  }

  /**
   * Cancel all running stages with best effort and throw an Exception containing all stage
   * materialization errors and stage cancellation errors.
   */
  private def cleanUpAndThrowException(
       errors: Seq[Throwable],
       earlyFailedStage: Option[Int]): Unit = {
    currentPhysicalPlan.foreach {
      // earlyFailedStage is the stage which failed before calling doMaterialize,
      // so we should avoid calling cancel on it to re-trigger the failure again.
      case s: QueryStageExec if !earlyFailedStage.contains(s.id) =>
        try {
          s.cancel()
        } catch {
          case NonFatal(t) =>
            logError(s"Exception in cancelling query stage: ${s.treeString}", t)
        }
      case _ =>
    }
    // Respect SparkFatalException which can be thrown by BroadcastExchangeExec
    val originalErrors = errors.map {
      case fatal: SparkFatalException => fatal.throwable
      case other => other
    }
    val e = if (originalErrors.size == 1) {
      originalErrors.head
    } else {
      val se = QueryExecutionErrors.multiFailuresInStageMaterializationError(originalErrors.head)
      originalErrors.tail.foreach(se.addSuppressed)
      se
    }
    throw e
  }
}

object OrphanBSCollect extends Enumeration {
  type OrphanBSCollect = Value

  val no_collect, under_exchange_node, orphan = Value
}

object AdaptiveSparkPlanExec {
  private[adaptive] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))

  /**
   * The temporary [[LogicalPlan]] link for query stages.
   *
   * Physical nodes wrapped in a [[LogicalQueryStage]] can be shared among different physical plans
   * and thus their usual logical links can be overwritten during query planning, leading to
   * situations where those nodes point to a new logical plan and the rest point to the current
   * logical plan. In this case we use temp logical links to make sure we can always trace back to
   * the original logical links until a new physical plan is adopted, by which time we can clear up
   * the temp logical links.
   */
  val TEMP_LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("temp_logical_plan")

  /**
   * Apply a list of physical operator rules on a [[SparkPlan]].
   */
  def applyPhysicalRules(
      plan: SparkPlan,
      rules: Seq[Rule[SparkPlan]],
      loggerAndBatchName: Option[(PlanChangeLogger[SparkPlan], String)] = None): SparkPlan = {
    if (loggerAndBatchName.isEmpty) {
      rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    } else {
      val (logger, batchName) = loggerAndBatchName.get
      val newPlan = rules.foldLeft(plan) { case (sp, rule) =>
        val result = rule.apply(sp)
        logger.logRule(rule.ruleName, sp, result)
        result
      }
      logger.logBatch(batchName, plan, newPlan)
      newPlan
    }
  }
}

/**
 * The execution context shared between the main query and all sub-queries.
 */
case class AdaptiveExecutionContext(session: SparkSession, qe: QueryExecution) {

  /**
   * The subquery-reuse map shared across the entire query.
   */
  val subqueryCache: TrieMap[SparkPlan, BaseSubqueryExec] =
    new TrieMap[SparkPlan, BaseSubqueryExec]()

  /**
   * The exchange-reuse map shared across the entire query, including sub-queries.
   */
  val stageCache: TrieMap[SparkPlan, QueryStageExec] =
    new TrieMap[SparkPlan, QueryStageExec]()
}


/**
 * The event type for stage materialization.
 */
sealed trait StageMaterializationEvent

/**
 * The materialization of a query stage completed with success.
 */
case class StageSuccess(stage: QueryStageExec, result: Any) extends StageMaterializationEvent

/**
 * The materialization of a query stage hit an error and failed.
 */
case class StageFailure(stage: QueryStageExec, error: Throwable) extends StageMaterializationEvent
