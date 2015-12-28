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

package org.apache.spark.sql.hive

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.{CombineFilters, ProjectCollapsing}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

case class NamedRelation(databaseName: String, tableName: String, output: Seq[Attribute])
  extends LeafNode

class QueryNormalizer(sqlContext: SQLContext) extends RuleExecutor[LogicalPlan] {
  override protected val batches: Seq[Batch] = Seq(
    Batch("Reorder Operators", FixedPoint(100),
      ReorderPredicate,
      ReorderLimit,
      CombineFilters,
      ProjectCollapsing
    ),

    Batch("Fill Missing Operators", Once,
      ProjectStar
    ),

    Batch("Recover Scope Information", FixedPoint(100),
      ReplaceWithNamedRelation
    )
  )

  object ReorderPredicate extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case project @ Project(_, filter @ Filter(_, child)) =>
        filter.copy(child = project.copy(child = child))

      case agg @ Aggregate(_, _, filter @ Filter(_, child)) =>
        filter.copy(child = agg.copy(child = child))

      case filter @ Filter(_, sort @ Sort(_, _, child)) =>
        sort.copy(child = filter.copy(child = child))
    }
  }

  object ReorderLimit extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case agg @ Aggregate(_, _, limit @ Limit(_, child)) =>
        limit.copy(child = agg.copy(child = child))

      case project @ Project(_, limit @ Limit(_, child)) =>
        limit.copy(child = project.copy(child = child))

      case filter @ Filter(_, limit @ Limit(_, child)) =>
        limit.copy(child = filter.copy(child = child))

      case sort @ Sort(_, _, limit @ Limit(_, child)) =>
        limit.copy(child = sort.copy(child = child))
    }
  }

  object ProjectStar extends Rule[LogicalPlan] {
    def projectStar(plan: LogicalPlan): LogicalPlan = {
      sqlContext.analyzer.execute(Project(UnresolvedStar(None) :: Nil, plan))
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case filter @ Filter(_, _: Aggregate | _: Project) =>
        filter

      case filter @ Filter(_, child) =>
        filter.copy(child = projectStar(child))

      case limit @ Limit(_, child) =>
        limit.copy(child = projectStar(child))
    }
  }

  object ReplaceWithNamedRelation extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case r @ MetastoreRelation(_, _, Some(alias)) =>
        Subquery(alias, NamedRelation(r.databaseName, r.tableName, r.output))

      case r @ MetastoreRelation(_, _, None) =>
        NamedRelation(r.databaseName, r.tableName, r.output)
    }
  }
}
