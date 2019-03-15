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

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, SparkPlan}
import org.apache.spark.sql.execution.adaptive.rule._
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{EventLoop, ThreadUtils}

/**
 * This class inserts [[QueryStageExec]] into the query plan in a bottom-up fashion, and
 * materializes the query stages asynchronously as soon as they are created.
 *
 * When one query stage finishes materialization, a list of adaptive optimizer rules will be
 * executed, trying to optimize the query plan with the data statistics collected from the the
 * materialized data. Then we travers the query plan again and try to insert more query stages.
 *
 * To create query stages, we traverse the query tree bottom up. When we hit an exchange node,
 * and all the child query stages of this exchange node are materialized, we create a new
 * query stage for this exchange node.
 *
 * Right before the stage creation, a list of query stage optimizer rules will be executed. These
 * optimizer rules are different from the adaptive optimizer rules. Query stage optimizer rules only
 * focus on a plan sub-tree of a specific query stage, and they will be executed only after all the
 * child stages are materialized.
 */
class QueryStageManager(
    initialPlan: SparkPlan,
    session: SparkSession,
    callback: QueryStageManagerCallback)
  extends EventLoop[QueryStageManagerEvent]("QueryFragmentCreator") {

  private def conf = session.sessionState.conf

  private val readyStages = mutable.HashSet.empty[Int]

  private var currentStageId = 0

  private val stageCache =
    mutable.HashMap.empty[StructType, mutable.Buffer[(Exchange, QueryStageExec)]]

  private var currentPlan = initialPlan

  private val localProperties = session.sparkContext.getLocalProperties

  private implicit def executionContext: ExecutionContextExecutorService = {
    QueryStageManager.executionContext
  }

  // A list of optimizer rules that will be applied when a query stage finishes materialization.
  // These rules need to travers the entire query plan, and find chances to optimize the query plan
  // with the data statistics collected from materialized query stage's output.
  private val adaptiveOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    RemoveRedundantShuffles)

  // A list of optimizer rules that will be applied right before a query stage is created.
  // These rules need to traverse the plan sub-tree of the query stage to be created, and find
  // chances to optimize this query stage given the all its child query stages.
  private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    AssertChildStagesMaterialized,
    ReduceNumShufflePartitions(conf),
    CollapseCodegenStages(conf))

  private def optimizeEntirePlan(plan: SparkPlan): SparkPlan = {
    adaptiveOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  private def optimizeQueryStage(plan: SparkPlan): SparkPlan = {
    queryStageOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  override protected def onReceive(event: QueryStageManagerEvent): Unit = event match {
    case Start =>
      // set active session and local properties for the event loop thread.
      SparkSession.setActiveSession(session)
      session.sparkContext.setLocalProperties(localProperties)
      currentPlan = createQueryStages(initialPlan)

    case MaterializeStage(stage) =>
      stage.materialize().onComplete { res =>
        if (res.isSuccess) {
          post(StageReady(stage))
        } else {
          callback.onStageMaterializationFailed(stage, res.failed.get)
          stop()
        }
      }

    case StageReady(stage) =>
      readyStages += stage.id
      currentPlan = optimizeEntirePlan(currentPlan)
      currentPlan = createQueryStages(currentPlan)
  }

  override protected def onStart(): Unit = {
    post(Start)
  }

  /**
   * Traverse the query plan bottom-up, and creates query stages as many as possible.
   */
  private def createQueryStages(plan: SparkPlan): SparkPlan = {
    val result = createQueryStages0(plan)
    if (result.allChildStagesReady) {
      val finalPlan = optimizeQueryStage(result.newPlan)
      callback.onFinalPlan(finalPlan)
      finalPlan
    } else {
      callback.onPlanUpdate(result.newPlan)
      result.newPlan
    }
  }

  /**
   * This method is called recursively to traverse the plan tree bottom-up. This method returns two
   * information: 1) the new plan after we insert query stages. 2) whether or not the child query
   * stages of the new plan are all ready.
   *
   * if the current plan is an exchange node, and all its child query stages are ready, we create
   * a new query stage.
   */
  private def createQueryStages0(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      val similarStages = stageCache.getOrElseUpdate(e.schema, mutable.Buffer.empty)
      similarStages.find(_._1.sameResult(e)) match {
        case Some((_, existingStage)) if conf.exchangeReuseEnabled =>
          CreateStageResult(
            newPlan = ReusedQueryStageExec(existingStage, e.output),
            allChildStagesReady = readyStages.contains(existingStage.id))

        case _ =>
          val result = createQueryStages0(e.child)
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          if (result.allChildStagesReady) {
            val queryStage = createQueryStage(newPlan)
            similarStages.append(e -> queryStage)
            // We've created a new stage, which is obviously not ready yet.
            CreateStageResult(newPlan = queryStage, allChildStagesReady = false)
          } else {
            CreateStageResult(newPlan = newPlan, allChildStagesReady = false)
          }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q, allChildStagesReady = readyStages.contains(q.id))

    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesReady = true)
      } else {
        val results = plan.children.map(createQueryStages0)
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesReady = results.forall(_.allChildStagesReady))
      }
  }

  private def createQueryStage(e: Exchange): QueryStageExec = {
    val optimizedPlan = optimizeQueryStage(e.child)
    val queryStage = e match {
      case s: ShuffleExchangeExec =>
        ShuffleQueryStageExec(currentStageId, s.copy(child = optimizedPlan))
      case b: BroadcastExchangeExec =>
        BroadcastQueryStageExec(currentStageId, b.copy(child = optimizedPlan))
    }
    currentStageId += 1
    post(MaterializeStage(queryStage))
    queryStage
  }

  override protected def onError(e: Throwable): Unit = callback.onError(e)
}

case class CreateStageResult(newPlan: SparkPlan, allChildStagesReady: Boolean)

object QueryStageManager {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryFragmentCreator", 16))
}

trait QueryStageManagerCallback {
  def onPlanUpdate(updatedPlan: SparkPlan): Unit
  def onFinalPlan(finalPlan: SparkPlan): Unit
  def onStageMaterializationFailed(stage: QueryStageExec, e: Throwable): Unit
  def onError(e: Throwable): Unit
}

sealed trait QueryStageManagerEvent

object Start extends QueryStageManagerEvent

case class MaterializeStage(stage: QueryStageExec) extends QueryStageManagerEvent

case class StageReady(stage: QueryStageExec) extends QueryStageManagerEvent
