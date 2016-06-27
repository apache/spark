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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * When scanning only partition columns, get results based on metadata without scanning files.
 * It is used for distinct, distinct aggregations or distinct-like aggregations(example: Max/Min).
 * First of all, scanning only partition columns are required, then the rule does the following
 * things here:
 * 1. aggregate expression is partition columns,
 *  e.g. SELECT col FROM tbl GROUP BY col or SELECT col FROM tbl GROUP BY cube(col).
 * 2. aggregate function on partition columns with DISTINCT,
 *  e.g. SELECT count(DISTINCT col) FROM tbl GROUP BY col.
 * 3. aggregate function on partition columns which have same result with DISTINCT keyword.
 *  e.g. SELECT Max(col2) FROM tbl GROUP BY col1.
 */
case class MetadataOnlyOptimizer(
    sparkSession: SparkSession,
    catalog: SessionCatalog) extends Rule[LogicalPlan] {

  private def canSupportMetadataOnly(a: Aggregate): Boolean = {
    val aggregateExpressions = a.aggregateExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression => agg
      }
    }.distinct
    if (aggregateExpressions.isEmpty) {
      // Support for aggregate that has no aggregateFunction when expressions are partition columns
      // example: select partitionCol from table group by partitionCol.
      // Moreover, multiple-distinct has been rewritted into it by RewriteDistinctAggregates.
      true
    } else {
      aggregateExpressions.forall { agg =>
        if (agg.isDistinct) {
          true
        } else {
          // If function can be evaluated on just the distinct values of a column, it can be used
          // by metadata-only optimizer.
          agg.aggregateFunction match {
            case max: Max => true
            case min: Min => true
            case hyperLog: HyperLogLogPlusPlus => true
            case _ => false
          }
        }
      }
    }
  }

  private def collectAliases(fields: Seq[Expression]): Map[ExprId, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute.exprId -> child
  }.toMap

  private def substitute(aliases: Map[ExprId, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref.exprId)
          .map(Alias(_, name)(a.exprId, a.qualifier, isGenerated = a.isGenerated))
          .getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a.exprId)
          .map(Alias(_, a.name)(a.exprId, a.qualifier, isGenerated = a.isGenerated)).getOrElse(a)
    }
  }

  private def findRelation(plan: LogicalPlan)
      : (Option[LogicalPlan], Seq[NamedExpression], Seq[Expression], Map[ExprId, Expression]) = {
    plan match {
      case relation @ LogicalRelation(files: HadoopFsRelation, _, table)
        if files.partitionSchema.nonEmpty =>
        (Some(relation), Seq.empty[NamedExpression], Seq.empty[Expression], Map.empty)

      case relation: CatalogRelation if relation.catalogTable.partitionColumnNames.nonEmpty =>
        (Some(relation), Seq.empty[NamedExpression], Seq.empty[Expression], Map.empty)

      case p @ Project(fields, child) if fields.forall(_.deterministic) =>
        val (plan, _, filters, aliases) = findRelation(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (plan, substitutedFields, filters, collectAliases(substitutedFields))

      case f @ Filter(condition, child) if condition.deterministic =>
        val (plan, fields, filters, aliases) = findRelation(child)
        val substitutedCondition = substitute(aliases)(condition)
        (plan, fields, filters ++ Seq(substitutedCondition), aliases)

      case e @ Expand(_, _, child) =>
        findRelation(child)

      case _ => (None, Seq.empty[NamedExpression], Seq.empty[Expression], Map.empty)
    }
  }

  private def convertToMetadataOnlyPlan(
      parent: LogicalPlan,
      projectList: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: LogicalPlan): LogicalPlan = relation match {
    case l @ LogicalRelation(files: HadoopFsRelation, _, _) =>
      val attributeMap = l.output.map(attr => (attr.name, attr)).toMap
      val partitionColumns = files.partitionSchema.map { field =>
        attributeMap.getOrElse(field.name, throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${l.output.map(_.name).mkString(", ")}]"))
      }
      val filterColumns = filters.flatMap(_.references)
      val projectSet = parent.references ++ AttributeSet(filterColumns)
      if (projectSet.subsetOf(AttributeSet(partitionColumns))) {
        val selectedPartitions = files.location.listFiles(filters)
        val valuesRdd = sparkSession.sparkContext.parallelize(selectedPartitions.map(_.values), 1)
        val valuesPlan = LogicalRDD(partitionColumns, valuesRdd)(sparkSession)
        parent.transform {
          case l @ LogicalRelation(files: HadoopFsRelation, _, _) =>
            valuesPlan
        }
      } else {
        parent
      }

    case relation: CatalogRelation =>
      val attributeMap = relation.output.map(attr => (attr.name, attr)).toMap
      val partitionColumns = relation.catalogTable.partitionColumnNames.map { column =>
        attributeMap.getOrElse(column, throw new AnalysisException(
          s"Unable to resolve ${column} given [${relation.output.map(_.name).mkString(", ")}]"))
      }
      val filterColumns = filters.flatMap(_.references)
      val projectSet = parent.references ++ AttributeSet(filterColumns)
      if (projectSet.subsetOf(AttributeSet(partitionColumns))) {
        val partitionColumnDataTypes = partitionColumns.map(_.dataType)
        val partitionValues = catalog.listPartitions(relation.catalogTable.identifier)
          .map { p =>
            InternalRow.fromSeq(
              partitionColumns.map(a => p.spec(a.name)).zip(partitionColumnDataTypes).map {
                case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
              })
          }
        val valuesRdd = sparkSession.sparkContext.parallelize(partitionValues, 1)
        val valuesPlan = LogicalRDD(partitionColumns, valuesRdd)(sparkSession)
        parent.transform {
          case relation: CatalogRelation =>
            valuesPlan
        }
      } else {
        parent
      }

    case _ =>
      parent
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sparkSession.sessionState.conf.optimizerMetadataOnly) {
      return plan
    }
    plan.transform {
      case a @ Aggregate(_, _, child) if canSupportMetadataOnly(a) =>
        val (plan, projectList, filters, _) = findRelation(child)
        if (plan.isDefined) {
          convertToMetadataOnlyPlan(a, projectList, filters, plan.get)
        } else {
          a
        }
    }
  }
}
