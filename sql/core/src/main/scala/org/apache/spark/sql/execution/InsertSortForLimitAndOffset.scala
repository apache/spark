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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{Project, Sort}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.internal.SQLConf

/**
 * When LIMIT/OFFSET is the root node, Spark plans it as CollectLimitExec which preserves the data
 * ordering. However, when OFFSET/LIMIT is not the root node, Spark uses GlobalLimitExec which
 * shuffles all the data into one partition and then gets a slice of it. Unfortunately, the shuffle
 * reader fetches shuffle blocks in a random order and can not preserve the data ordering, which
 * violates the requirement of LIMIT/OFFSET.
 *
 * This rule inserts an extra local sort before LIMIT/OFFSET to preserve the data ordering.
 * TODO: add a order preserving mode in the shuffle reader.
 */
object InsertSortForLimitAndOffset extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ORDERING_AWARE_LIMIT_OFFSET)) return plan

    plan transform {
      case l @ GlobalLimitExec(
          _,
          // Should not match AQE shuffle stage because we only target un-submitted stages which
          // we can still rewrite the query plan.
          s @ ShuffleExchangeExec(SinglePartition, child, _, _),
          _) if child.logicalLink.isDefined =>
        extractOrderingAndPropagateOrderingColumns(child) match {
          case Some((ordering, newChild)) =>
            val newShuffle = s.withNewChildren(Seq(newChild))
            val sorted = SortExec(ordering, global = false, child = newShuffle)
            // We must set the logical plan link to avoid losing the added SortExec and ProjectExec
            // during AQE re-optimization, where we turn physical plan back to logical plan.
            val logicalSort = Sort(ordering, global = false, child = s.child.logicalLink.get)
            sorted.setLogicalLink(logicalSort)
            val projected = if (sorted.output == s.output) {
              sorted
            } else {
              val p = ProjectExec(s.output, sorted)
              p.setLogicalLink(Project(s.output, logicalSort))
              p
            }
            l.withNewChildren(Seq(projected))
          case _ => l
        }
    }
  }

  // Note: this is not implementing a generalized notion of "global order preservation", but just
  // a best effort to catch the common query patterns that the data ordering should be preserved.
  private def extractOrderingAndPropagateOrderingColumns(
      plan: SparkPlan): Option[(Seq[SortOrder], SparkPlan)] = plan match {
    case p: SortExec if p.global => Some(p.sortOrder, p)
    case p: UnaryExecNode if
        p.isInstanceOf[LocalLimitExec] ||
          p.isInstanceOf[WholeStageCodegenExec] ||
          p.isInstanceOf[FilterExec] ||
          p.isInstanceOf[EvalPythonExec] =>
      extractOrderingAndPropagateOrderingColumns(p.child) match {
        case Some((ordering, newChild)) => Some((ordering, p.withNewChildren(Seq(newChild))))
        case _ => None
      }
    case p: ProjectExec =>
      extractOrderingAndPropagateOrderingColumns(p.child) match {
        case Some((ordering, newChild)) =>
          val orderingCols = ordering.flatMap(_.references)
          if (orderingCols.forall(p.outputSet.contains)) {
            Some((ordering, p.withNewChildren(Seq(newChild))))
          } else {
            // In order to do the sort after shuffle, we must propagate the ordering columns in the
            // pre-shuffle ProjectExec.
            val missingCols = orderingCols.filterNot(p.outputSet.contains)
            val newProj = p.copy(projectList = p.projectList ++ missingCols, child = newChild)
            newProj.copyTagsFrom(p)
            Some((ordering, newProj))
          }
        case _ => None
      }
    case _ => None
  }
}
