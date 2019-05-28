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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
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
    session: SparkSession,
    subqueryMap: Map[Long, ExecSubqueryExpression],
    stageCache: TrieMap[StructType, mutable.Buffer[(Exchange, QueryStageExec)]])
  extends LeafExecNode {

  def executedPlan: SparkPlan = currentPhysicalPlan

  override def output: Seq[Attribute] = initialPlan.output

  override def doCanonicalize(): SparkPlan = initialPlan.canonicalized

  override def doExecute(): RDD[InternalRow] = {
    val events = new LinkedBlockingQueue[StageMaterializationEvent]()
    var result = createQueryStages(currentPhysicalPlan)
    while(!result.allChildStagesMaterialized) {
      currentPhysicalPlan = result.newPlan
      updateLogicalPlan(result.newStages)
      onUpdatePlan()
      result.newStages.map(_._2).foreach { stage =>
        stage.materialize().onComplete { res =>
          if (res.isSuccess) {
            stage.resultOption = Some(res.get)
            events.offer(StageSuccess(stage))
          } else {
            events.offer(StageFailure(stage, res.failed.get))
          }
        }
      }
      // Wait on the next completed stage, which indicates new stats are available and probably
      // new stages can be created. There might be other stages that finish at around the same
      // time, so we process those stages too in order to reduce re-planning.
      val nextMsg = events.take()
      val rem = mutable.ArrayBuffer.empty[StageMaterializationEvent]
      (Seq(nextMsg) ++ rem).foreach{ e => e match
        {
          case StageSuccess(stage) =>
            completedStages += stage.id
          case StageFailure(stage, ex) =>
            throw new SparkException(
              s"""
                 |Fail to materialize query stage ${stage.id}:
                 |${stage.plan.treeString}
               """.stripMargin, ex)
        }
      }
      reOptimize()
      result = createQueryStages(currentPhysicalPlan)
    }

    // Run the final plan when there's no more unfinished stages.
    currentPhysicalPlan =
      AdaptiveSparkPlanExec.applyPhysicalRules(result.newPlan, queryStageOptimizerRules)
    logDebug(s"Final plan: $currentPhysicalPlan")
    onUpdatePlan()
    currentPhysicalPlan.execute()
  }

  override def generateTreeString(
    depth: Int,
    lastChildren: Seq[Boolean],
    append: String => Unit,
    verbose: Boolean,
    prefix: String = "",
    addSuffix: Boolean = false,
    maxFields: Int): Unit = {
    currentPhysicalPlan.generateTreeString(
      depth, lastChildren, append, verbose, "", false, maxFields)
  }

  @transient private val executionId = Option(
    session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(_.toLong)

  @transient private val completedStages = mutable.HashSet.empty[Int]

  @volatile private var currentStageId = 0

  @volatile private var currentPhysicalPlan = initialPlan

  @volatile private var currentLogicalPlan = initialPlan.logicalLink.get

  // The logical plan optimizer for re-optimizing the current logical plan.
  private object Optimizer extends RuleExecutor[LogicalPlan] {
    // TODO add more optimization rules
    override protected def batches: Seq[Batch] = Seq()
  }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] =
    AdaptiveSparkPlanExec.createQueryStagePreparationRules(conf, subqueryMap)

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    AssertChildStagesMaterialized,
    CollapseCodegenStages(conf)
  )

  // The execution context for stage completion callbacks.
  private implicit def executionContext: ExecutionContextExecutorService =
    AdaptiveSparkPlanExec.executionContext

  /**
   * Return type for `createQueryStages`
   * @param newPlan   the new plan with created query stages.
   * @param allChildStagesMaterialized    whether all child stages have been materialized.
   * @param newStages the newly created query stages, including new reused query stages.
   */
  private case class CreateStageResult(
    newPlan: SparkPlan,
    allChildStagesMaterialized: Boolean,
    newStages: Seq[(Exchange, QueryStageExec)])

  /**
   * This method is called recursively to traverse the plan tree bottom-up and checks whether or
   * not the child query stages (if any) of the current node have all been materialized. If the
   * current node is:
   * 1. An Exchange:
   *    1) If the Exchange is equivalent to another Exchange of an existing query stage, create
   *       a reused query stage and replace the current node with the reused stage.
   *    2) If all its child query stages have been materialized, create a new query stage and
   *       replace the current node with the new stage.
   *    3) Otherwise, do nothing.
   *    In any of the above cases, the method will return `false` for `allChildStagesMaterialized`.
   * 2. A query stage:
   *    return `true` for `allChildStagesMaterialized` if this stage is completed, otherwise
   *    `false`.
   * 3. Recursively visit all child nodes, and propagate the return value.
   */
  private def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      val similarStages = stageCache.getOrElseUpdate(e.schema, mutable.Buffer.empty)
      similarStages.synchronized {
        similarStages.find(_._1.sameResult(e)) match {
          case Some((_, existingStage)) if conf.exchangeReuseEnabled =>
            // This is not a new stage, but rather a new leaf node.
            val reusedStage = reuseQueryStage(existingStage, e.output)
            CreateStageResult(newPlan = reusedStage,
              allChildStagesMaterialized = false, newStages = Seq((e, reusedStage)))

          case _ =>
            val result = createQueryStages(e.child)
            val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
            // Create a query stage only when all the child query stages are ready.
            if (result.allChildStagesMaterialized) {
              val queryStage = newQueryStage(newPlan)
              similarStages.append(e -> queryStage)

              // We've created a new stage, which is obviously not ready yet.
              CreateStageResult(newPlan = queryStage,
                allChildStagesMaterialized = false, newStages = Seq((e, queryStage)))
            } else {
              CreateStageResult(newPlan = newPlan,
                allChildStagesMaterialized = false, newStages = result.newStages)
            }
        }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = completedStages.contains(q.id), newStages = Seq.empty)

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
    val optimizedPlan = AdaptiveSparkPlanExec.applyPhysicalRules(e.child, queryStageOptimizerRules)
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
   * Update the logical plan after new query stages have been created and the physical plan has
   * been updated with the newly created stages.
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
  private def updateLogicalPlan(newStages: Seq[(Exchange, QueryStageExec)]): Unit = {
    newStages.foreach { n =>
      val (oldNode: SparkPlan, newNode: SparkPlan) = n
      val logicalNodeOpt = oldNode.logicalLink.orElse(oldNode.collectFirst {
        case p if p.logicalLink.isDefined => p.logicalLink.get
      })
      assert(logicalNodeOpt.isDefined)
      val logicalNode = logicalNodeOpt.get
      val physicalNode = currentPhysicalPlan.collectFirst {
        case p if p.eq(newNode) || p.logicalLink.exists(logicalNode.eq) => p
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
          s"stage: $newNode")
      // force update logical link
      physicalNode.get.setTagValue(SparkPlan.LOGICAL_PLAN_TAG, newLogicalNode)
      currentLogicalPlan = newLogicalPlan
    }
  }

  /**
   * Re-optimize and run physical planning on the current logical plan based on the latest stats.
   */
  private def reOptimize(): Unit = {
    currentLogicalPlan.invalidateStatsCache()
    val optimized = Optimizer.execute(currentLogicalPlan)
    SparkSession.setActiveSession(session)
    val sparkPlan = session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
    val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(sparkPlan, queryStagePreparationRules)
    currentPhysicalPlan = newPlan
  }

  private def onUpdatePlan(): Unit = {
    executionId.foreach { id =>
      session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        id,
        SQLExecution.getQueryExecution(id).toString,
        SparkPlanInfo.fromSparkPlan(currentPhysicalPlan)))
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
      conf: SQLConf, subqueryMap:
      Map[Long, ExecSubqueryExpression]): Seq[Rule[SparkPlan]] = {
    Seq(
      PlanAdaptiveSubqueries(subqueryMap),
      EnsureRequirements(conf))
  }

  /**
   * Apply a list of physical operator rules on a [[SparkPlan]].
   */
  def applyPhysicalRules(plan: SparkPlan, preparationRules: Seq[Rule[SparkPlan]]): SparkPlan = {
    preparationRules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }
}

/**
 * The event type for stage materialization.
 */
sealed trait StageMaterializationEvent

/**
 * The materialization of a query stage completed with success.
 */
case class StageSuccess(stage: QueryStageExec) extends StageMaterializationEvent

/**
 * The materialization of a query stage hit an error and failed.
 */
case class StageFailure(stage: QueryStageExec, error: Throwable) extends StageMaterializationEvent
