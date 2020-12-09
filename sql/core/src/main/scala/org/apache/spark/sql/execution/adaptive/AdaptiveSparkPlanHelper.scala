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

import org.apache.spark.sql.execution.SparkPlan

/**
 * This class provides utility methods related to tree traversal of an [[AdaptiveSparkPlanExec]]
 * plan. Unlike their counterparts in [[org.apache.spark.sql.catalyst.trees.TreeNode]] or
 * [[org.apache.spark.sql.catalyst.plans.QueryPlan]], these methods traverse down leaf nodes of
 * adaptive plans, i.e., [[AdaptiveSparkPlanExec]] and [[QueryStageExec]].
 */
trait AdaptiveSparkPlanHelper {

  /**
   * Find the first [[SparkPlan]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def find(p: SparkPlan)(f: SparkPlan => Boolean): Option[SparkPlan] = if (f(p)) {
    Some(p)
  } else {
    allChildren(p).foldLeft(Option.empty[SparkPlan]) { (l, r) => l.orElse(find(r)(f)) }
  }

  /**
   * Runs the given function on this node and then recursively on children.
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(p: SparkPlan)(f: SparkPlan => Unit): Unit = {
    f(p)
    allChildren(p).foreach(foreach(_)(f))
  }

  /**
   * Runs the given function recursively on children then on this node.
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(p: SparkPlan)(f: SparkPlan => Unit): Unit = {
    allChildren(p).foreach(foreachUp(_)(f))
    f(p)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   */
  def mapPlans[A](p: SparkPlan)(f: SparkPlan => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(p)(ret += f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
  def flatMap[A](p: SparkPlan)(f: SparkPlan => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(p)(ret ++= f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
  def collect[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(p)(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the leaves in this tree.
   */
  def collectLeaves(p: SparkPlan): Seq[SparkPlan] = {
    collect(p) { case plan if allChildren(plan).isEmpty => plan }
  }

  /**
   * Finds and returns the first [[SparkPlan]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
  def collectFirst[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Option[B] = {
    val lifted = pf.lift
    lifted(p).orElse {
      allChildren(p).foldLeft(Option.empty[B]) { (l, r) => l.orElse(collectFirst(r)(pf)) }
    }
  }

  /**
   * Returns a sequence containing the result of applying a partial function to all elements in this
   * plan, also considering all the plans in its (nested) subqueries
   */
  def collectWithSubqueries[B](p: SparkPlan)(f: PartialFunction[SparkPlan, B]): Seq[B] = {
    (p +: subqueriesAll(p)).flatMap(collect(_)(f))
  }

  /**
   * Returns a sequence containing the subqueries in this plan, also including the (nested)
   * subqueries in its children
   */
  def subqueriesAll(p: SparkPlan): Seq[SparkPlan] = {
    val subqueries = flatMap(p)(_.subqueries)
    subqueries ++ subqueries.flatMap(subqueriesAll)
  }

  private def allChildren(p: SparkPlan): Seq[SparkPlan] = p match {
    case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
    case s: QueryStageExec => Seq(s.plan)
    case _ => p.children
  }

  /**
   * Strip the executePlan of AdaptiveSparkPlanExec leaf node.
   */
  def stripAQEPlan(p: SparkPlan): SparkPlan = p match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other => other
  }
}
