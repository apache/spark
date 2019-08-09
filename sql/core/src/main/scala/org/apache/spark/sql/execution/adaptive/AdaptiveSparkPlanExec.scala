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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec._
import org.apache.spark.sql.execution.adaptive.rule.ReduceNumShufflePartitions
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

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
    initialPlan: SparkPlan,
    @transient session: SparkSession,
    @transient subqueryMap: Map[Long, ExecSubqueryExpression],
    @transient stageCache: TrieMap[SparkPlan, QueryStageExec],
    @transient queryExecution: QueryExecution)
  extends LeafExecNode {

  @transient private val lock = new Object()

  // The logical plan optimizer for re-optimizing the current logical plan.
  @transient private val optimizer = new RuleExecutor[LogicalPlan] {
    // TODO add more optimization rules
    override protected def batches: Seq[Batch] = Seq()
  }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = Seq(
    PlanAdaptiveSubqueries(subqueryMap),
    EnsureRequirements(conf)
  )

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    ReduceNumShufflePartitions(conf),
    ApplyColumnarRulesAndInsertTransitions(session.sessionState.conf,
      session.sessionState.columnarRules),
    CollapseCodegenStages(conf)
  )

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
    newStages: Seq[(Exchange, QueryStageExec)])

  def executedPlan: SparkPlan = currentPhysicalPlan

  override def conf: SQLConf = session.sessionState.conf

  override def output: Seq[Attribute] = initialPlan.output

  override def doCanonicalize(): SparkPlan = initialPlan.canonicalized

  override def doExecute(): RDD[InternalRow] = lock.synchronized {
    if (isFinalPlan) {
      currentPhysicalPlan.execute()
    } else {
      // Make sure we only update Spark UI if this plan's `QueryExecution` object matches the one
      // retrieved by the `sparkContext`'s current execution ID. Note that sub-queries do not have
      // their own execution IDs and therefore rely on the main query to update UI.
      val executionId = Option(
        session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).flatMap { idStr =>
        val id = idStr.toLong
        val qe = SQLExecution.getQueryExecution(id)
        if (qe.eq(queryExecution)) Some(id) else None
      }
      var currentLogicalPlan = currentPhysicalPlan.logicalLink.get
      var result = createQueryStages(currentPhysicalPlan)
      val events = new LinkedBlockingQueue[StageMaterializationEvent]()
      val errors = new mutable.ArrayBuffer[SparkException]()
      while (!result.allChildStagesMaterialized) {
        currentPhysicalPlan = result.newPlan
        currentLogicalPlan = updateLogicalPlan(currentLogicalPlan, result.newStages)
        currentPhysicalPlan.setTagValue(SparkPlan.LOGICAL_PLAN_TAG, currentLogicalPlan)
        executionId.foreach(onUpdatePlan)

        // Start materialization of all new stages.
        result.newStages.map(_._2).foreach { stage =>
          stage.materialize().onComplete { res =>
            if (res.isSuccess) {
              events.offer(StageSuccess(stage, res.get))
            } else {
              events.offer(StageFailure(stage, res.failed.get))
            }
          }(AdaptiveSparkPlanExec.executionContext)
        }

        // Wait on the next completed stage, which indicates new stats are available and probably
        // new stages can be created. There might be other stages that finish at around the same
        // time, so we process those stages too in order to reduce re-planning.
        val nextMsg = events.take()
        val rem = new util.ArrayList[StageMaterializationEvent]()
        events.drainTo(rem)
        (Seq(nextMsg) ++ rem.asScala).foreach {
          case StageSuccess(stage, res) =>
            stage.resultOption = Some(res)
          case StageFailure(stage, ex) =>
            errors.append(
              new SparkException(s"Failed to materialize query stage: ${stage.treeString}", ex))
        }

        // In case of errors, we cancel all running stages and throw exception.
        if (errors.nonEmpty) {
          cleanUpAndThrowException(errors)
        }

        // Do re-planning and try creating new stages on the new physical plan.
        val (newPhysicalPlan, newLogicalPlan) = reOptimize(currentLogicalPlan)
        currentPhysicalPlan = newPhysicalPlan
        currentLogicalPlan = newLogicalPlan
        result = createQueryStages(currentPhysicalPlan)
      }

      // Run the final plan when there's no more unfinished stages.
      currentPhysicalPlan = applyPhysicalRules(result.newPlan, queryStageOptimizerRules)
      currentPhysicalPlan.setTagValue(SparkPlan.LOGICAL_PLAN_TAG, currentLogicalPlan)
      isFinalPlan = true

      val ret = currentPhysicalPlan.execute()
      logDebug(s"Final plan: $currentPhysicalPlan")
      executionId.foreach(onUpdatePlan)
      ret
    }
  }

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String =
    s"AdaptiveSparkPlan(isFinalPlan=$isFinalPlan)"

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    super.generateTreeString(depth, lastChildren, append, verbose, prefix, addSuffix, maxFields)
    currentPhysicalPlan.generateTreeString(
      depth + 1, lastChildren :+ true, append, verbose, "", addSuffix = false, maxFields)
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
  private def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      // First have a quick check in the `stageCache` without having to traverse down the node.
      stageCache.get(e.canonicalized) match {
        case Some(existingStage) if conf.exchangeReuseEnabled =>
          val reusedStage = reuseQueryStage(existingStage, e.output)
          // When reusing a stage, we treat it a new stage regardless of whether the existing stage
          // has been materialized or not. Thus we won't skip re-optimization for a reused stage.
          CreateStageResult(newPlan = reusedStage,
            allChildStagesMaterialized = false, newStages = Seq((e, reusedStage)))

        case _ =>
          val result = createQueryStages(e.child)
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          if (result.allChildStagesMaterialized) {
            var newStage = newQueryStage(newPlan)
            if (conf.exchangeReuseEnabled) {
              // Check the `stageCache` again for reuse. If a match is found, ditch the new stage
              // and reuse the existing stage found in the `stageCache`, otherwise update the
              // `stageCache` with the new stage.
              val queryStage = stageCache.getOrElseUpdate(e.canonicalized, newStage)
              if (queryStage.ne(newStage)) {
                newStage = reuseQueryStage(queryStage, e.output)
              }
            }

            // We've created a new stage, which is obviously not ready yet.
            CreateStageResult(newPlan = newStage,
              allChildStagesMaterialized = false, newStages = Seq((e, newStage)))
          } else {
            CreateStageResult(newPlan = newPlan,
              allChildStagesMaterialized = false, newStages = result.newStages)
          }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = q.resultOption.isDefined, newStages = Seq.empty)

    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesMaterialized = true, newStages = Seq.empty)
      } else {
        val results = plan.children.map(createQueryStages)
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
          newStages = results.flatMap(_.newStages))
      }
  }

  private def newQueryStage(e: Exchange): QueryStageExec = {
    val optimizedPlan = applyPhysicalRules(e.child, queryStageOptimizerRules)
    val queryStage = e match {
      case s: ShuffleExchangeExec =>
        ShuffleQueryStageExec(currentStageId, s.copy(child = optimizedPlan))
      case b: BroadcastExchangeExec =>
        BroadcastQueryStageExec(currentStageId, b.copy(child = optimizedPlan))
    }
    currentStageId += 1
    queryStage
  }

  private def reuseQueryStage(s: QueryStageExec, output: Seq[Attribute]): QueryStageExec = {
    val queryStage = ReusedQueryStageExec(currentStageId, s, output)
    currentStageId += 1
    queryStage
  }

  /**
   * Returns the updated logical plan after new query stages have been created and the physical
   * plan has been updated with the newly created stages.
   * 1. If the new query stage can be mapped to an integral logical sub-tree, replace the
   *    corresponding logical sub-tree with a leaf node [[LogicalQueryStage]] referencing the new
   *    query stage. For example:
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
   * 2. Otherwise (which means the new query stage can only be mapped to part of a logical
   *    sub-tree), replace the corresponding logical sub-tree with a leaf node
   *    [[LogicalQueryStage]] referencing to the top physical node into which this logical node is
   *    transformed during physical planning. For example:
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
  private def updateLogicalPlan(
      logicalPlan: LogicalPlan,
      newStages: Seq[(Exchange, QueryStageExec)]): LogicalPlan = {
    var currentLogicalPlan = logicalPlan
    newStages.foreach {
      case (exchange, stage) =>
        // Get the corresponding logical node for `exchange`. If `exchange` has been transformed
        // from a `Repartition`, it should have `logicalLink` available by itself; otherwise
        // traverse down to find the first node that is not generated by `EnsureRequirements`.
        val logicalNodeOpt = exchange.logicalLink.orElse(exchange.collectFirst {
          case p if p.logicalLink.isDefined => p.logicalLink.get
        })
        assert(logicalNodeOpt.isDefined)
        val logicalNode = logicalNodeOpt.get
        val physicalNode = currentPhysicalPlan.collectFirst {
          case p if p.eq(stage) || p.logicalLink.exists(logicalNode.eq) => p
        }
        assert(physicalNode.isDefined)
        // Replace the corresponding logical node with LogicalQueryStage
        val newLogicalNode = LogicalQueryStage(logicalNode, physicalNode.get)
        val newLogicalPlan = currentLogicalPlan.transformDown {
          case p if p.eq(logicalNode) => newLogicalNode
        }
        assert(newLogicalPlan != currentLogicalPlan,
          s"logicalNode: $logicalNode; " +
            s"logicalPlan: $currentLogicalPlan " +
            s"physicalPlan: $currentPhysicalPlan" +
            s"stage: $stage")
        currentLogicalPlan = newLogicalPlan
    }
    currentLogicalPlan
  }

  /**
   * Re-optimize and run physical planning on the current logical plan based on the latest stats.
   */
  private def reOptimize(logicalPlan: LogicalPlan): (SparkPlan, LogicalPlan) = {
    logicalPlan.invalidateStatsCache()
    val optimized = optimizer.execute(logicalPlan)
    SparkSession.setActiveSession(session)
    val sparkPlan = session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
    val newPlan = applyPhysicalRules(sparkPlan, queryStagePreparationRules)
    (newPlan, optimized)
  }

  /**
   * Notify the listeners of the physical plan change.
   */
  private def onUpdatePlan(executionId: Long): Unit = {
    session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
      executionId,
      SQLExecution.getQueryExecution(executionId).toString,
      SparkPlanInfo.fromSparkPlan(this)))
  }

  /**
   * Cancel all running stages with best effort and throw an Exception containing all stage
   * materialization errors and stage cancellation errors.
   */
  private def cleanUpAndThrowException(errors: Seq[SparkException]): Unit = {
    val runningStages = currentPhysicalPlan.collect {
      case s: QueryStageExec => s
    }
    val cancelErrors = new mutable.ArrayBuffer[SparkException]()
    try {
      runningStages.foreach { s =>
        try {
          s.cancel()
        } catch {
          case NonFatal(t) =>
            cancelErrors.append(
              new SparkException(s"Failed to cancel query stage: ${s.treeString}", t))
        }
      }
    } finally {
      val ex = new SparkException(
        "Adaptive execution failed due to stage materialization failures.", errors.head)
      errors.tail.foreach(ex.addSuppressed)
      cancelErrors.foreach(ex.addSuppressed)
      throw ex
    }
  }
}

object AdaptiveSparkPlanExec {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))

  /**
   * Creates the list of physical plan rules to be applied before creation of query stages.
   */
  def createQueryStagePreparationRules(
      conf: SQLConf,
      subqueryMap: Map[Long, ExecSubqueryExpression]): Seq[Rule[SparkPlan]] = {
    Seq(
      PlanAdaptiveSubqueries(subqueryMap),
      EnsureRequirements(conf))
  }

  /**
   * Apply a list of physical operator rules on a [[SparkPlan]].
   */
  def applyPhysicalRules(plan: SparkPlan, rules: Seq[Rule[SparkPlan]]): SparkPlan = {
    rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }
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
