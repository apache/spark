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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BloomFilterMightContain, EqualTo, Expression, PredicateHelper, TransformExpression, XxHash64}
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.{isGlobalCheck, perPartitionBloomFilter, perPartitionSemiJoin}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LEFT_SEMI_OR_ANTI_JOIN

/**
 * Moves the semi join of a per-partition runtime bloom filter down to where predicate pushdown put
 * the global bloom filter of the same key. [[InjectRuntimeFilter]] adds both on top of the
 * application side, and pushdown moves only the filter, for example below a join on that side.
 * Both check each row on its key alone, so the semi join is correct wherever the filter is. Down
 * there, the operators it moved past no longer get the rows it rules out.
 *
 * The semi join stays where it is when:
 *  - it has no global bloom filter to follow;
 *  - pushdown put that filter in more than one place, as below a union;
 *  - only projections and filters separate the two.
 */
object PushDownPerPartitionBloomFilters extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.runtimeBloomFilterPerPartitionEnabled) {
      plan
    } else {
      plan.transformDownWithPruning(_.containsPattern(LEFT_SEMI_OR_ANTI_JOIN)) {
        case top @ Project(_, Join(Project(leftList, child), creation, LeftSemi, Some(c), _)) =>
          semiJoinParts(c, leftList).flatMap { case (global, transform, creationKey, bloom) =>
            pushedGlobalCheck(child, global).map { case (filter, key) =>
              val semiJoin =
                perPartitionSemiJoin(filter, key, transform, creation, creationKey, bloom)
              top.copy(child = child.transformUp { case node if node eq filter => semiJoin })
            }
          }.getOrElse(top)
      }
    }
  }

  // For a semi join that `InjectRuntimeFilter.injectPerPartitionBloomFilter` builds next to a
  // global bloom filter: the exprId of that filter's subquery, the partition transform the
  // application side joins on, the creation side's partition key and the bloom filter column.
  private def semiJoinParts(
      condition: Expression,
      leftList: Seq[Expression]): Option[(Long, TransformExpression, Attribute, Attribute)] = {
    val keys = splitConjunctivePredicates(condition).collectFirst {
      case EqualTo(application: Attribute, creation: Attribute) =>
        leftList.collectFirst {
          case a @ Alias(t: TransformExpression, _) if a.exprId == application.exprId =>
            (t, creation)
        }
    }.flatten
    for {
      (bloom, global) <- perPartitionBloomFilter(condition)
      if global >= 0
      (transform, creationKey) <- keys
    } yield (global, transform, creationKey, bloom)
  }

  // The only filter below `child` that checks the bloom filter of the subquery with the exprId
  // `global`, and the key it checks, when pushdown moved it below something other than projections
  // and filters.
  private def pushedGlobalCheck(child: LogicalPlan, global: Long): Option[(Filter, Expression)] = {
    def checkedKey(condition: Expression): Option[Expression] =
      splitConjunctivePredicates(condition).collectFirst {
        case b @ BloomFilterMightContain(_, XxHash64(Seq(key), _))
            if isGlobalCheck(global)(b) => key
      }
    // What is below the projections and filters on top of `child`, unless one of those filters
    // holds the check.
    @tailrec
    def belowProjectsAndFilters(node: LogicalPlan): Option[LogicalPlan] = node match {
      case Filter(condition, _) if checkedKey(condition).isDefined => None
      case Project(_, grandChild) => belowProjectsAndFilters(grandChild)
      case Filter(_, grandChild) => belowProjectsAndFilters(grandChild)
      case _ => Some(node)
    }
    belowProjectsAndFilters(child).flatMap { below =>
      below.collect {
        case filter @ Filter(condition, _) => checkedKey(condition).map((filter, _))
      }.flatten match {
        case Seq(pushed) => Some(pushed)
        case _ => None
      }
    }
  }
}
