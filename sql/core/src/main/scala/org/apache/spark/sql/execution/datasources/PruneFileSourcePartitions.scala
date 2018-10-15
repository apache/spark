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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.BooleanType

private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op @ PhysicalOperation(projects, filters,
        logicalRelation @
          LogicalRelation(fsRelation @
            HadoopFsRelation(
              catalogFileIndex: CatalogFileIndex,
              partitionSchema,
              _,
              _,
              _,
              _),
            _,
            _,
            _))
        if filters.nonEmpty && fsRelation.partitionSchemaOption.isDefined =>

      val sparkSession = fsRelation.sparkSession
      val partitionColumns =
        logicalRelation.resolve(
          partitionSchema, sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we donot need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transformUp {
          case a: AttributeReference =>
            a.withName(logicalRelation.output.find(_.semanticEquals(a)).get.name)
          // Replace the nonPartitionOps field with true in the And(partitionOps, nonPartitionOps)
          // to make the partition can be pruned
          case and @ And(left, right) =>
            val leftPartition = left.references.filter(partitionSet.contains(_))
            val rightPartition = right.references.filter(partitionSet.contains(_))
            if (leftPartition.size == left.references.size && rightPartition.isEmpty) {
              and.withNewChildren(Seq(left, Literal(true, BooleanType)))
            } else if (leftPartition.isEmpty && rightPartition.size == right.references.size) {
              and.withNewChildren(Seq(Literal(true, BooleanType), right))
            } else and
        }
      }
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters
          .filterNot(SubqueryExpression.hasSubquery(_))
          .filter(_.references.subsetOf(partitionSet)))

      if (partitionKeyFilters.nonEmpty) {
        val prunedFileIndex = catalogFileIndex.filterPartitions(partitionKeyFilters.toSeq)
        val prunedFsRelation =
          fsRelation.copy(location = prunedFileIndex)(sparkSession)
        // Change table stats based on the sizeInBytes of pruned files
        val withStats = logicalRelation.catalogTable.map(_.copy(
          stats = Some(CatalogStatistics(sizeInBytes = BigInt(prunedFileIndex.sizeInBytes)))))
        val prunedLogicalRelation = logicalRelation.copy(
          relation = prunedFsRelation, catalogTable = withStats)
        // Keep partition-pruning predicates so that they are visible in physical planning
        val filterExpression = filters.reduceLeft(And)
        val filter = Filter(filterExpression, prunedLogicalRelation)
        Project(projects, filter)
      } else {
        op
      }
  }
}
