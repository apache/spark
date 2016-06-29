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
 * It's used for operators that only need distinct values. Currently only [[Aggregate]] operator
 * which satisfy the following conditions are supported:
 * 1. aggregate expression is partition columns,
 *  e.g. SELECT col FROM tbl GROUP BY col, SELECT col FROM tbl GROUP BY cube(col).
 * 2. aggregate function on partition columns with DISTINCT,
 *  e.g. SELECT count(DISTINCT col) FROM tbl GROUP BY col.
 * 3. aggregate function on partition columns which have same result with DISTINCT keyword.
 *  e.g. SELECT Max(col2) FROM tbl GROUP BY col1.
 */
case class MetadataOnlyOptimizer(
    sparkSession: SparkSession,
    catalog: SessionCatalog) extends Rule[LogicalPlan] {

  private def canSupportMetadataOnly(a: Aggregate): Boolean = {
    if (!a.references.forall(_.isPartitionColumn)) {
      // Support for scanning only partition columns
      false
    } else {
      val aggregateExpressions = a.aggregateExpressions.flatMap { expr =>
        expr.collect {
          case agg: AggregateExpression => agg
        }
      }.distinct
      if (aggregateExpressions.isEmpty) {
        // Support for aggregate that has no aggregateFunction when expressions are partition
        // columns. example: select partitionCol from table group by partitionCol.
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
  }

  private def convertLogicalToMetadataOnly(
      filter: Option[Expression],
      logical: LogicalRelation,
      files: HadoopFsRelation): LogicalPlan = {
    val attributeMap = logical.output.map(attr => (attr.name, attr)).toMap
    val partitionColumns = files.partitionSchema.map { field =>
      attributeMap.getOrElse(field.name, throw new AnalysisException(
        s"Unable to resolve ${field.name} given [${logical.output.map(_.name).mkString(", ")}]"))
    }
    val selectedPartitions = files.location.listFiles(filter.map(Seq(_)).getOrElse(Seq.empty))
    val valuesRdd = sparkSession.sparkContext.parallelize(selectedPartitions.map(_.values), 1)
    val valuesPlan = LogicalRDD(partitionColumns, valuesRdd)(sparkSession)
    valuesPlan
  }

  private def convertCatalogToMetadataOnly(relation: CatalogRelation): LogicalPlan = {
    val attributeMap = relation.output.map(attr => (attr.name, attr)).toMap
    val partitionColumns = relation.catalogTable.partitionColumnNames.map { column =>
      attributeMap.getOrElse(column, throw new AnalysisException(
        s"Unable to resolve ${column} given [${relation.output.map(_.name).mkString(", ")}]"))
    }
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
    valuesPlan
  }

  /**
   * When scanning only partition columns, convert LogicalRelation or CatalogRelation to LogicalRDD.
   * Now support logical plan:
   *  Aggregate [Expand] Project [Filter] (LogicalRelation | CatalogRelation)
   */
  private def convertToMetadataOnly(plan: LogicalPlan): LogicalPlan = plan match {
    case p @ Project(fields, child) if p.references.forall(_.isPartitionColumn) =>
      child match {
        case f @ Filter(condition, l @ LogicalRelation(files: HadoopFsRelation, _, _))
          if files.partitionSchema.nonEmpty && f.references.forall(_.isPartitionColumn) =>
          val plan = convertLogicalToMetadataOnly(Some(condition), l, files)
          p.withNewChildren(f.withNewChildren(plan :: Nil) :: Nil)

        case l @ LogicalRelation(files: HadoopFsRelation, _, _)
          if files.partitionSchema.nonEmpty =>
          val plan = convertLogicalToMetadataOnly(None, l, files)
          p.withNewChildren(plan :: Nil)

        case f @ Filter(condition, relation: CatalogRelation)
          if relation.catalogTable.partitionColumnNames.nonEmpty &&
            f.references.forall(_.isPartitionColumn) =>
          val plan = convertCatalogToMetadataOnly(relation)
          p.withNewChildren(f.withNewChildren(plan :: Nil) :: Nil)

        case relation: CatalogRelation
          if relation.catalogTable.partitionColumnNames.nonEmpty =>
          val plan = convertCatalogToMetadataOnly(relation)
          p.withNewChildren(plan :: Nil)

        case other =>
          other
      }

    case e: Expand =>
      e.withNewChildren(e.children.map(convertToMetadataOnly(_)))

    case other =>
      other
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sparkSession.sessionState.conf.optimizerMetadataOnly) {
      return plan
    }
    plan.transform {
      case a @ Aggregate(_, _, child) if canSupportMetadataOnly(a) =>
        a.withNewChildren(convertToMetadataOnly(child) :: Nil)
    }
  }
}
