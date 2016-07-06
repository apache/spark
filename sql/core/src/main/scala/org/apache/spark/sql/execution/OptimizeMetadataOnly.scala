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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, InternalRow}
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * When scanning only partition columns, get results based on partition data without scanning files.
 * It's used for operators that only need distinct values. Currently only [[Aggregate]] operator
 * which satisfy the following conditions are supported:
 * 1. aggregate expression is partition columns.
 *  e.g. SELECT col FROM tbl GROUP BY col.
 * 2. aggregate function on partition columns with DISTINCT.
 *  e.g. SELECT count(DISTINCT col1) FROM tbl GROUP BY col2.
 * 3. aggregate function on partition columns which have same result w or w/o DISTINCT keyword.
 *  e.g. SELECT Max(col2) FROM tbl GROUP BY col1.
 */
case class OptimizeMetadataOnly(
    catalog: SessionCatalog,
    conf: CatalystConf) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.optimizerMetadataOnly) {
      return plan
    }

    plan.transform {
      case a @ Aggregate(_, aggExprs, child @ PartitionedRelation(partAttrs, relation)) =>
        if (a.references.subsetOf(partAttrs)) {
          val aggFunctions = aggExprs.flatMap(_.collect {
            case agg: AggregateExpression => agg
          })
          val isPartitionDataOnly = aggFunctions.isEmpty || aggFunctions.forall { agg =>
            agg.isDistinct || (agg.aggregateFunction match {
              case _: Max => true
              case _: Min => true
              case _ => false
            })
          }
          if (isPartitionDataOnly) {
            a.withNewChildren(Seq(usePartitionData(child, relation)))
          } else {
            a
          }
        } else {
          a
        }
    }
  }

  private def usePartitionData(child: LogicalPlan, relation: LogicalPlan): LogicalPlan = {
    child transform {
      case plan if plan eq relation =>
        relation match {
          case l @ LogicalRelation(fsRelation: HadoopFsRelation, _, _) =>
            val partColumns = fsRelation.partitionSchema.map(_.name.toLowerCase).toSet
            val partAttrs = l.output.filter(a => partColumns.contains(a.name.toLowerCase))
            val partitionData = fsRelation.location.listFiles(Nil)
            LocalRelation(partAttrs, partitionData.map(_.values))

          case relation: CatalogRelation =>
            val partColumns = relation.catalogTable.partitionColumnNames.map(_.toLowerCase).toSet
            val partAttrs = relation.output.filter(a => partColumns.contains(a.name.toLowerCase))
            val partitionData = catalog.listPartitions(relation.catalogTable.identifier).map { p =>
              InternalRow.fromSeq(partAttrs.map { attr =>
                Cast(Literal(p.spec(attr.name)), attr.dataType).eval()
              })
            }
            LocalRelation(partAttrs, partitionData)

          case _ => throw new IllegalStateException()
        }
    }
  }

  object PartitionedRelation {
    def unapply(plan: LogicalPlan): Option[(AttributeSet, LogicalPlan)] = plan match {
      case l @ LogicalRelation(fsRelation: HadoopFsRelation, _, _)
        if fsRelation.partitionSchema.nonEmpty =>
        val partColumns = fsRelation.partitionSchema.map(_.name.toLowerCase).toSet
        val partAttrs = l.output.filter(a => partColumns.contains(a.name.toLowerCase))
        Some(AttributeSet(partAttrs), l)

      case relation: CatalogRelation if relation.catalogTable.partitionColumnNames.nonEmpty =>
        val partColumns = relation.catalogTable.partitionColumnNames.map(_.toLowerCase).toSet
        val partAttrs = relation.output.filter(a => partColumns.contains(a.name.toLowerCase))
        Some(AttributeSet(partAttrs), relation)

      case p @ Project(projectList, child) if projectList.forall(_.deterministic) =>
        unapply(child).flatMap {
          case (partAttrs, relation) =>
            if (p.references.subsetOf(partAttrs)) {
              Some(p.outputSet, relation)
            } else {
              None
            }
        }

      case f @ Filter(condition, child) if condition.deterministic =>
        unapply(child).flatMap {
          case (partAttrs, relation) =>
            if (f.references.subsetOf(partAttrs)) {
              Some(f.outputSet, relation)
            } else {
              None
            }
        }
      case _ => None
    }
  }
}
