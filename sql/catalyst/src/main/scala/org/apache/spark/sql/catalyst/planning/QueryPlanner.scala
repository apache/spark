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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Abstract class for transforming [[plans.logical.LogicalPlan LogicalPlan]]s into physical plans.
 * Child classes are responsible for specifying a list of [[Strategy]] objects that each of which
 * can return a list of possible physical plan options.  If a given strategy is unable to plan all
 * of the remaining operators in the tree, it can call [[planLater]], which returns a placeholder
 * object that will be filled in using other available strategies.
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[Strategy]

  /**
   * Given a [[plans.logical.LogicalPlan LogicalPlan]], returns a list of `PhysicalPlan`s that can
   * be used for execution. If this strategy does not apply to the give logical operation then an
   * empty list should be returned.
   */
  abstract protected class Strategy extends Logging {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan]
  }

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan) = apply(plan).next()

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}
