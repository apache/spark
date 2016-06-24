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
 * Example: select Max(partition) from table.
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
      // Cannot support for aggregate that has no aggregateFunction.
      // example: select col1 from table group by col1.
      false
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

  private def findRelation(plan: LogicalPlan): (Option[LogicalPlan], Seq[Expression]) = {
    plan match {
      case relation @ LogicalRelation(files: HadoopFsRelation, _, table)
        if files.partitionSchema.nonEmpty =>
        (Some(relation), Seq.empty[Expression])

      case relation: CatalogRelation if relation.catalogTable.partitionColumnNames.nonEmpty =>
        (Some(relation), Seq.empty[Expression])

      case p @ Project(_, child) =>
        findRelation(child)

      case f @ Filter(filterCondition, child) =>
        val (plan, conditions) = findRelation(child)
        (plan, conditions ++ Seq(filterCondition))

      case _ => (None, Seq.empty[Expression])
    }
  }

  private def convertToMetadataOnlyPlan(
      parent: LogicalPlan,
      project: Option[LogicalPlan],
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
        val partitionValues = selectedPartitions.map(_.values)
        val valuesRdd = sparkSession.sparkContext.parallelize(partitionValues, 1)
        val valuesPlan = LogicalRDD(partitionColumns, valuesRdd)(sparkSession)
        val scanPlan = project.map(_.withNewChildren(valuesPlan :: Nil)).getOrElse(valuesPlan)
        parent.withNewChildren(scanPlan :: Nil)
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
        val partitionValues = catalog.getPartitionsByFilter(relation.catalogTable, filters)
          .map { p =>
            InternalRow.fromSeq(
              partitionColumns.map(a => p.spec(a.name)).zip(partitionColumnDataTypes).map {
                case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
              })
          }
        val valuesRdd = sparkSession.sparkContext.parallelize(partitionValues, 1)
        val valuesPlan = LogicalRDD(partitionColumns, valuesRdd)(sparkSession)
        val filterPlan =
          filters.reduceLeftOption(And).map(Filter(_, valuesPlan)).getOrElse(valuesPlan)
        val scanPlan = project.map(_.withNewChildren(filterPlan :: Nil)).getOrElse(filterPlan)
        parent.withNewChildren(scanPlan :: Nil)
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
        val (plan, filters) = findRelation(child)
        if (plan.isDefined) {
          convertToMetadataOnlyPlan(a, None, filters, plan.get)
        } else {
          a
        }

      case d @ Distinct(p @ Project(_, _)) =>
        val (plan, filters) = findRelation(p)
        if (plan.isDefined) {
          convertToMetadataOnlyPlan(d, Some(p), filters, plan.get)
        } else {
          d
        }
    }
  }
}
