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

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.{EventLoop, ThreadUtils}

/**
 * This class triggers [[QueryStage]] bottom-up, apply planner rules for query stages and
 * materialize them. It triggers as many query stages as possible at the same time, and triggers
 * the parent query stage when all its child stages are materialized.
 */
class QueryStageTrigger(session: SparkSession, callback: QueryStageTriggerCallback)
  extends EventLoop[QueryStageTriggerEvent]("QueryStageTrigger") {

  private val stageToParentStages = HashMap.empty[Int, ListBuffer[QueryStage]]

  private val idToUpdatedStage = HashMap.empty[Int, QueryStage]

  private val stageToNumPendingChildStages = HashMap.empty[Int, Int]

  private val submittedStages = HashSet.empty[Int]

  private val readyStages = HashSet.empty[Int]

  private val planner = new QueryStagePlanner(session.sessionState.conf)

  def trigger(stage: QueryStage): Unit = {
    post(SubmitStage(stage))
  }

  private implicit def executionContext: ExecutionContextExecutorService = {
    QueryStageTrigger.executionContext
  }

  override protected def onReceive(event: QueryStageTriggerEvent): Unit = event match {
    case SubmitStage(stage) =>
      // We may submit a query stage multiple times, because of stage reuse. Here we avoid
      // re-submitting a query stage.
      if (!submittedStages.contains(stage.id)) {
        submittedStages += stage.id
        val pendingChildStages = stage.plan.collect {
          // The stage being submitted may have child stages that are already ready, if the child
          // stage is a reused stage.
          case stage: QueryStage if !readyStages.contains(stage.id) => stage
        }
        if (pendingChildStages.isEmpty) {
          // This is a leaf stage, or all its child stages are ready, we can plan it now.
          post(PlanStage(stage))
        } else {
          // This stage has some pending child stages, we store the connection of this stage and
          // its child stages, and submit all the child stages, so that we can plan this stage
          // later when all its child stages are ready.
          stageToNumPendingChildStages(stage.id) = pendingChildStages.length
          pendingChildStages.foreach { child =>
            // a child may have multiple parents, because of query stage reuse.
            val parentStages = stageToParentStages.getOrElseUpdate(child.id, new ListBuffer)
            parentStages += stage
            post(SubmitStage(child))
          }
        }
      }

    case PlanStage(stage) =>
      Future {
        // planning needs active SparkSession in current thread.
        SparkSession.setActiveSession(session)
        planner.execute(stage.plan)
      }.onComplete { res =>
        if (res.isSuccess) {
          post(StagePlanned(stage, res.get))
        } else {
          callback.onStagePlanningFailed(stage, res.failed.get)
          stop()
        }
      }
      submittedStages += stage.id

    case StagePlanned(stage, optimizedPlan) =>
      val newStage = stage.withNewPlan(optimizedPlan)
      // We store the new stage with the new query plan after planning, so that later on we can
      // update the query plan of its parent stage.
      idToUpdatedStage(newStage.id) = newStage
      // This stage has optimized its plan, notify the callback about this change.
      callback.onStageUpdated(newStage)

      newStage.materialize().onComplete { res =>
        if (res.isSuccess) {
          post(StageReady(stage))
        } else {
          callback.onStageMaterializingFailed(newStage, res.failed.get)
          stop()
        }
      }

    case StageReady(stage) =>
      readyStages += stage.id
      stageToParentStages.remove(stage.id).foreach { parentStages =>
        parentStages.foreach { parent =>
          val numPendingChildStages = stageToNumPendingChildStages(parent.id)
          if (numPendingChildStages == 1) {
            stageToNumPendingChildStages.remove(parent.id)
            // All its child stages are ready, here we update the query plan via replacing the old
            // child stages with new ones that are planned.
            val newPlan = parent.plan.transform {
              case q: QueryStage => idToUpdatedStage(q.id)
            }
            // We can plan this stage now.
            post(PlanStage(parent.withNewPlan(newPlan)))
          } else {
            assert(numPendingChildStages > 1)
            stageToNumPendingChildStages(parent.id) = numPendingChildStages - 1
          }
        }
      }
  }

  override protected def onError(e: Throwable): Unit = callback.onError(e)
}

object QueryStageTrigger {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageTrigger", 16))
}

trait QueryStageTriggerCallback {
  def onStageUpdated(stage: QueryStage): Unit
  def onStagePlanningFailed(stage: QueryStage, e: Throwable): Unit
  def onStageMaterializingFailed(stage: QueryStage, e: Throwable): Unit
  def onError(e: Throwable): Unit
}

sealed trait QueryStageTriggerEvent

case class SubmitStage(stage: QueryStage) extends QueryStageTriggerEvent

case class PlanStage(stage: QueryStage) extends QueryStageTriggerEvent

case class StagePlanned(stage: QueryStage, optimizedPlan: SparkPlan) extends QueryStageTriggerEvent

case class StageReady(stage: QueryStage) extends QueryStageTriggerEvent
