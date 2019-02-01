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
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollapseCodegenStages, SparkPlan}
import org.apache.spark.sql.execution.adaptive.rule.{AssertChildStagesMaterialized, ReduceNumShufflePartitions}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{EventLoop, ThreadUtils}

/**
 * This class dynamically creates [[QueryStage]] bottom-up, optimize the query plan of query stages
 * and materialize them. It creates as many query stages as possible at the same time, and
 * materialize a query stage when all its child stages are materialized.
 *
 * To create query stages, we traverse the query tree bottom up. When we hit an exchange node, and
 * all the child query stages of this exchange node are materialized, we try to create a new query
 * stage for this exchange node.
 *
 * To create a new query stage, we first optimize the sub-tree of the exchange. After optimization,
 * we check the output partitioning of the optimized sub-tree, and see if the exchange node is still
 * necessary.
 *
 * If the exchange node becomes unnecessary, remove it and give up this query stage creation, and
 * continue to traverse the query plan tree until we hit the next exchange node.
 *
 * If the exchange node is still needed, create the query stage and optimize its sub-tree again.
 * It's necessary to have both the pre-creation optimization and post-creation optimization, because
 * these 2 optimization have different assumptions. For pre-creation optimization, the shuffle node
 * may be removed later on and the current sub-tree may be only a part of a query stage, so we don't
 * have the big picture of the query stage yet. For post-creation optimization, the query stage is
 * created and we have the big picture of the query stage.
 *
 * After the query stage is optimized, we materialize it asynchronously, and continue to traverse
 * the query plan tree to create more query stages.
 *
 * When a query stage completes materialization, we trigger the process of query stages creation and
 * traverse the query plan tree again.
 */
class QueryStageCreator(
    initialPlan: SparkPlan,
    session: SparkSession,
    callback: QueryStageCreatorCallback)
  extends EventLoop[QueryStageCreatorEvent]("QueryStageCreator") {

  private def conf = session.sessionState.conf

  private val readyStages = mutable.HashSet.empty[Int]

  private var currentStageId = 0

  private val stageCache = mutable.HashMap.empty[StructType, mutable.Buffer[(Exchange, QueryStage)]]

  // The optimizer rules that will be applied to a sub-tree of the query plan before the stage is
  // created. Note that we may end up not creating the query stage, so the rules here should not
  // assume the given sub-plan-tree is the entire query plan of the query stage. For example, if a
  // rule want to collect all the child query stages, it should not be put here.
  private val preStageCreationOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    AssertChildStagesMaterialized
  )

  // The optimizer rules that will be applied to a sub-tree of the query plan after the stage is
  // created. Note that once the stage is created, we will not remove it anymore. If a rule changes
  // the output partitioning of the sub-plan-tree, which may help to remove the exchange node, it's
  // better to put it in `preStageCreationOptimizerRules`, so that we may create less query stages.
  private val postStageCreationOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    ReduceNumShufflePartitions(conf),
    CollapseCodegenStages(conf))

  private var currentPlan = initialPlan

  private val localProperties = session.sparkContext.getLocalProperties

  private implicit def executionContext: ExecutionContextExecutorService = {
    QueryStageCreator.executionContext
  }

  override protected def onReceive(event: QueryStageCreatorEvent): Unit = event match {
    case StartCreation =>
      // set active session and local properties for the event loop thread.
      SparkSession.setActiveSession(session)
      session.sparkContext.setLocalProperties(localProperties)
      currentPlan = createQueryStages(initialPlan)

    case MaterializeStage(stage) =>
      stage.materialize().onComplete { res =>
        if (res.isSuccess) {
          post(StageReady(stage))
        } else {
          callback.onStageMaterializingFailed(stage, res.failed.get)
          stop()
        }
      }

    case StageReady(stage) =>
      if (stage.isInstanceOf[ResultQueryStage]) {
        callback.onPlanUpdate(stage)
        stop()
      } else {
        readyStages += stage.id
        currentPlan = createQueryStages(currentPlan)
      }
  }

  override protected def onStart(): Unit = {
    post(StartCreation)
  }

  private def preStageCreationOptimize(plan: SparkPlan): SparkPlan = {
    preStageCreationOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  private def postStageCreationOptimize(plan: SparkPlan): SparkPlan = {
    postStageCreationOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  /**
   * Traverse the query plan bottom-up, and creates query stages as many as possible.
   */
  private def createQueryStages(plan: SparkPlan): SparkPlan = {
    val result = createQueryStages0(plan)
    if (result.allChildStagesReady) {
      val finalPlan = postStageCreationOptimize(preStageCreationOptimize(result.newPlan))
      post(StageReady(ResultQueryStage(currentStageId, finalPlan)))
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
   * if the current plan is an exchange node, and all its child query stages are ready, we try to
   * create a new query stage.
   */
  private def createQueryStages0(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      val similarStages = stageCache.getOrElseUpdate(e.schema, mutable.Buffer.empty)
      similarStages.find(_._1.sameResult(e)) match {
        case Some((_, existingStage)) if conf.exchangeReuseEnabled =>
          CreateStageResult(
            newPlan = ReusedQueryStage(existingStage, e.output),
            allChildStagesReady = readyStages.contains(existingStage.id))

        case _ =>
          val result = createQueryStages0(e.child)
          // Try to create a query stage only when all the child query stages are ready.
          if (result.allChildStagesReady) {
            val optimizedPlan = preStageCreationOptimize(result.newPlan)
            e match {
              case s: ShuffleExchangeExec =>
                (s.desiredPartitioning, optimizedPlan.outputPartitioning) match {
                  case (desired: HashPartitioning, actual: HashPartitioning)
                      if desired.semanticEquals(actual) =>
                    // This shuffle exchange is unnecessary now, remove it. The reason maybe:
                    //   1. the child plan has changed its output partitioning after optimization,
                    //      and makes this exchange node unnecessary.
                    //   2. this exchange node is user specified, which turns out to be unnecessary.
                    CreateStageResult(newPlan = optimizedPlan, allChildStagesReady = true)
                  case _ =>
                    val queryStage = createQueryStage(s.copy(child = optimizedPlan))
                    similarStages.append(e -> queryStage)
                    // We've created a new stage, which is obviously not ready yet.
                    CreateStageResult(newPlan = queryStage, allChildStagesReady = false)
                }

              case b: BroadcastExchangeExec =>
                val queryStage = createQueryStage(b.copy(child = optimizedPlan))
                similarStages.append(e -> queryStage)
                // We've created a new stage, which is obviously not ready yet.
                CreateStageResult(newPlan = queryStage, allChildStagesReady = false)
            }
          } else {
            CreateStageResult(
              newPlan = e.withNewChildren(Seq(result.newPlan)),
              allChildStagesReady = false)
          }
      }

    case q: QueryStage =>
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

  private def createQueryStage(e: Exchange): QueryStage = {
    val optimizedPlan = postStageCreationOptimize(e.child)
    val queryStage = e match {
      case s: ShuffleExchangeExec =>
        ShuffleQueryStage(currentStageId, s.copy(child = optimizedPlan))
      case b: BroadcastExchangeExec =>
        BroadcastQueryStage(currentStageId, b.copy(child = optimizedPlan))
    }
    currentStageId += 1
    post(MaterializeStage(queryStage))
    queryStage
  }

  override protected def onError(e: Throwable): Unit = callback.onError(e)
}

case class CreateStageResult(newPlan: SparkPlan, allChildStagesReady: Boolean)

object QueryStageCreator {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))
}

trait QueryStageCreatorCallback {
  def onPlanUpdate(updatedPlan: SparkPlan): Unit
  def onStageMaterializingFailed(stage: QueryStage, e: Throwable): Unit
  def onError(e: Throwable): Unit
}

sealed trait QueryStageCreatorEvent

object StartCreation extends QueryStageCreatorEvent

case class MaterializeStage(stage: QueryStage) extends QueryStageCreatorEvent

case class StageReady(stage: QueryStage) extends QueryStageCreatorEvent
