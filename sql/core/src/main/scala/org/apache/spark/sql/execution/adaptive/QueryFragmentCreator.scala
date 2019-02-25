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
import org.apache.spark.sql.execution.adaptive.rule.{AssertChildFragmentsMaterialized, ReduceNumShufflePartitions}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{EventLoop, ThreadUtils}

/**
 * This class dynamically creates [[QueryFragmentExec]] bottom-up, optimize the query plan of query
 * fragments and materialize them. It creates as many query fragments as possible at the same time,
 * and materialize a query fragment when all its child fragments are materialized.
 *
 * To create query fragments, we traverse the query tree bottom up. When we hit an exchange node,
 * and all the child query fragments of this exchange node are materialized, we try to create a new
 * query fragment for this exchange node.
 *
 * To create a new query fragment, we first optimize the sub-tree of the exchange. After
 * optimization, we check the output partitioning of the optimized sub-tree, and see if the
 * exchange node is still necessary.
 *
 * If the exchange node becomes unnecessary, remove it and give up this query fragment creation,
 * and continue to traverse the query plan tree until we hit the next exchange node.
 *
 * If the exchange node is still needed, create the query fragment and optimize its sub-tree again.
 * It's necessary to have both the pre-creation optimization and post-creation optimization, because
 * these 2 optimization have different assumptions. For pre-creation optimization, the shuffle node
 * may be removed later on and the current sub-tree may be only a part of a query fragment, so we
 * don't have the big picture of the query fragment yet. For post-creation optimization, the query
 * fragment is created and we have the big picture of the query fragment.
 *
 * After the query fragment is optimized, we materialize it asynchronously, and continue to traverse
 * the query plan tree to create more query fragments.
 *
 * When a query fragment completes materialization, we trigger the process of query fragments
 * creation and traverse the query plan tree again.
 */
class QueryFragmentCreator(
    initialPlan: SparkPlan,
    session: SparkSession,
    callback: QueryFragmentCreatorCallback)
  extends EventLoop[QueryFragmentCreatorEvent]("QueryFragmentCreator") {

  private def conf = session.sessionState.conf

  private val readyFragments = mutable.HashSet.empty[Int]

  private var currentFragmentId = 0

  private val fragmentCache =
    mutable.HashMap.empty[StructType, mutable.Buffer[(Exchange, QueryFragmentExec)]]

  // The optimizer rules that will be applied to a sub-tree of the query plan before the fragment is
  // created. Note that we may end up not creating the query fragment, so the rules here should not
  // assume the given sub-plan-tree is the entire query plan of the query fragment. For example, if
  // a rule want to collect all the child query fragments, it should not be put here.
  private val preFragmentCreationOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    AssertChildFragmentsMaterialized
  )

  // The optimizer rules that will be applied to a sub-tree of the query plan after the fragment is
  // created. Note that once the fragment is created, we will not remove it anymore. If a rule
  // changes the output partitioning of the sub-plan-tree, which may help to remove the exchange
  // node, it's better to put it in `preFragmentCreationOptimizerRules`, so that we may create less
  // query fragments.
  private val postFragmentCreationOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    ReduceNumShufflePartitions(conf),
    CollapseCodegenStages(conf))

  private var currentPlan = initialPlan

  private val localProperties = session.sparkContext.getLocalProperties

  private implicit def executionContext: ExecutionContextExecutorService = {
    QueryFragmentCreator.executionContext
  }

  override protected def onReceive(event: QueryFragmentCreatorEvent): Unit = event match {
    case StartCreation =>
      // set active session and local properties for the event loop thread.
      SparkSession.setActiveSession(session)
      session.sparkContext.setLocalProperties(localProperties)
      currentPlan = createQueryFragments(initialPlan)

    case MaterializeFragment(fragment) =>
      fragment.materialize().onComplete { res =>
        if (res.isSuccess) {
          post(FragmentReady(fragment))
        } else {
          callback.onFragmentMaterializingFailed(fragment, res.failed.get)
          stop()
        }
      }

    case FragmentReady(fragment) =>
      if (fragment.isInstanceOf[ResultQueryFragmentExec]) {
        callback.onPlanUpdate(fragment)
        stop()
      } else {
        readyFragments += fragment.id
        currentPlan = createQueryFragments(currentPlan)
      }
  }

  override protected def onStart(): Unit = {
    post(StartCreation)
  }

  private def preFragmentCreationOptimize(plan: SparkPlan): SparkPlan = {
    preFragmentCreationOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  private def postFragmentCreationOptimize(plan: SparkPlan): SparkPlan = {
    postFragmentCreationOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  /**
   * Traverse the query plan bottom-up, and creates query fragments as many as possible.
   */
  private def createQueryFragments(plan: SparkPlan): SparkPlan = {
    val result = createQueryFragments0(plan)
    if (result.allChildFragmentsReady) {
      val finalPlan = postFragmentCreationOptimize(preFragmentCreationOptimize(result.newPlan))
      post(FragmentReady(ResultQueryFragmentExec(currentFragmentId, finalPlan)))
      finalPlan
    } else {
      callback.onPlanUpdate(result.newPlan)
      result.newPlan
    }
  }

  /**
   * This method is called recursively to traverse the plan tree bottom-up. This method returns two
   * information: 1) the new plan after we insert query fragments. 2) whether or not the child query
   * fragments of the new plan are all ready.
   *
   * if the current plan is an exchange node, and all its child query fragments are ready, we try to
   * create a new query fragment.
   */
  private def createQueryFragments0(plan: SparkPlan): CreateFragmentResult = plan match {
    case e: Exchange =>
      val similarFragments = fragmentCache.getOrElseUpdate(e.schema, mutable.Buffer.empty)
      similarFragments.find(_._1.sameResult(e)) match {
        case Some((_, existingFragment)) if conf.exchangeReuseEnabled =>
          CreateFragmentResult(
            newPlan = ReusedQueryFragmentExec(existingFragment, e.output),
            allChildFragmentsReady = readyFragments.contains(existingFragment.id))

        case _ =>
          val result = createQueryFragments0(e.child)
          // Try to create a query fragment only when all the child query fragments are ready.
          if (result.allChildFragmentsReady) {
            val optimizedPlan = preFragmentCreationOptimize(result.newPlan)
            e match {
              case s: ShuffleExchangeExec =>
                (s.desiredPartitioning, optimizedPlan.outputPartitioning) match {
                  case (desired: HashPartitioning, actual: HashPartitioning)
                      if desired.semanticEquals(actual) =>
                    // This shuffle exchange is unnecessary now, remove it. The reason maybe:
                    //   1. the child plan has changed its output partitioning after optimization,
                    //      and makes this exchange node unnecessary.
                    //   2. this exchange node is user specified, which turns out to be unnecessary.
                    CreateFragmentResult(newPlan = optimizedPlan, allChildFragmentsReady = true)
                  case _ =>
                    val queryFragment = createQueryFragment(s.copy(child = optimizedPlan))
                    similarFragments.append(e -> queryFragment)
                    // We've created a new fragment, which is obviously not ready yet.
                    CreateFragmentResult(newPlan = queryFragment, allChildFragmentsReady = false)
                }

              case b: BroadcastExchangeExec =>
                val queryFragment = createQueryFragment(b.copy(child = optimizedPlan))
                similarFragments.append(e -> queryFragment)
                // We've created a new fragment, which is obviously not ready yet.
                CreateFragmentResult(newPlan = queryFragment, allChildFragmentsReady = false)
            }
          } else {
            CreateFragmentResult(
              newPlan = e.withNewChildren(Seq(result.newPlan)),
              allChildFragmentsReady = false)
          }
      }

    case q: QueryFragmentExec =>
      CreateFragmentResult(newPlan = q, allChildFragmentsReady = readyFragments.contains(q.id))

    case _ =>
      if (plan.children.isEmpty) {
        CreateFragmentResult(newPlan = plan, allChildFragmentsReady = true)
      } else {
        val results = plan.children.map(createQueryFragments0)
        CreateFragmentResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildFragmentsReady = results.forall(_.allChildFragmentsReady))
      }
  }

  private def createQueryFragment(e: Exchange): QueryFragmentExec = {
    val optimizedPlan = postFragmentCreationOptimize(e.child)
    val queryFragment = e match {
      case s: ShuffleExchangeExec =>
        ShuffleQueryFragmentExec(currentFragmentId, s.copy(child = optimizedPlan))
      case b: BroadcastExchangeExec =>
        BroadcastQueryFragmentExec(currentFragmentId, b.copy(child = optimizedPlan))
    }
    currentFragmentId += 1
    post(MaterializeFragment(queryFragment))
    queryFragment
  }

  override protected def onError(e: Throwable): Unit = callback.onError(e)
}

case class CreateFragmentResult(newPlan: SparkPlan, allChildFragmentsReady: Boolean)

object QueryFragmentCreator {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryFragmentCreator", 16))
}

trait QueryFragmentCreatorCallback {
  def onPlanUpdate(updatedPlan: SparkPlan): Unit
  def onFragmentMaterializingFailed(fragment: QueryFragmentExec, e: Throwable): Unit
  def onError(e: Throwable): Unit
}

sealed trait QueryFragmentCreatorEvent

object StartCreation extends QueryFragmentCreatorEvent

case class MaterializeFragment(fragment: QueryFragmentExec) extends QueryFragmentCreatorEvent

case class FragmentReady(fragment: QueryFragmentExec) extends QueryFragmentCreatorEvent
