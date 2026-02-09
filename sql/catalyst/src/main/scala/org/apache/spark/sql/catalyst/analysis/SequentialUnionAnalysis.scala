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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SequentialStreamingUnion}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Flattens nested SequentialStreamingUnion nodes into a single level.
 * This allows chaining: df1.followedBy(df2).followedBy(df3)
 */
object FlattenSequentialStreamingUnion extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(SEQUENTIAL_STREAMING_UNION)) {
    case SequentialStreamingUnion(children, byName, allowMissingCol) =>
      val flattened = SequentialStreamingUnion.flatten(children)
      SequentialStreamingUnion(flattened, byName, allowMissingCol)
  }
}

/**
 * Validates SequentialStreamingUnion constraints during analysis:
 * - All children must be streaming relations
 * - No stateful operations in any child subtrees
 *
 * Note: Minimum 2 children is enforced by the resolved property, not explicit validation.
 * Note: Nesting validation happens after optimization (see
 *       ValidateSequentialStreamingUnionNesting).
 */
object ValidateSequentialStreamingUnion extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreach {
      case su: SequentialStreamingUnion =>
        validateAllStreaming(su)
        validateNoStatefulDescendants(su)
      case _ =>
    }
    plan
  }

  private def validateAllStreaming(su: SequentialStreamingUnion): Unit = {
    val nonStreamingChildren = su.children.filterNot(_.isStreaming)
    if (nonStreamingChildren.nonEmpty) {
      throw QueryCompilationErrors.notStreamingDatasetError("SequentialStreamingUnion")
    }
  }

  private def validateNoStatefulDescendants(su: SequentialStreamingUnion): Unit = {
    su.children.foreach { child =>
      if (child.exists(UnsupportedOperationChecker.isStatefulOperation)) {
        throw QueryCompilationErrors.statefulChildrenNotSupportedInSequentialStreamingUnionError()
      }
    }
  }
}

/**
 * Validates that SequentialStreamingUnion nodes have no nesting after optimization.
 * This runs as a post-optimization check to ensure the optimizer has properly flattened
 * all nested SequentialStreamingUnions (including those wrapped in stateless operations).
 *
 * Runs after CombineUnions has flattened nested unions.
 */
object ValidateSequentialStreamingUnionNesting extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreach {
      case su: SequentialStreamingUnion =>
        su.children.foreach { child =>
          if (child.containsPattern(SEQUENTIAL_STREAMING_UNION)) {
            throw QueryCompilationErrors.nestedSequentialStreamingUnionError()
          }
        }
      case _ =>
    }
    plan
  }
}
