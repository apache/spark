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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, In, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AS_OF_JOIN, EXCEPT, INNER_LIKE_JOIN, INTERSECT, JOIN, LATERAL_JOIN, LEFT_SEMI_OR_ANTI_JOIN, NATURAL_LIKE_JOIN, OUTER_JOIN, UNION}

object FillStaticPartitions extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(!_.containsAnyPattern(OUTER_JOIN, JOIN, LATERAL_JOIN, AS_OF_JOIN,
      INNER_LIKE_JOIN, LEFT_SEMI_OR_ANTI_JOIN, NATURAL_LIKE_JOIN, INTERSECT, EXCEPT, UNION)) {
      case i @ InsertIntoHadoopFsRelationCommand(
      _, _, _, partitionColumns, _, _, _, query, _, _, _, _, _)
        if i.catalogTable.nonEmpty && i.staticPartitions.isEmpty &&
          i.fillStaticPartitions.isEmpty =>
        val fillStaticPartitions = mutable.Map[String, String]()

        query foreach {
          // exclude the case that the project contains partition column to be calculated
          case _ @ Project(projectList, _) =>
            val partitionColumnNotContainsEval = projectList.filter(x => partitionColumns
                .map(_.name).contains(x.name)).map { project =>
              val leaves = project.collectLeaves()
              leaves.size == 1 && leaves.head.isInstanceOf[AttributeReference]
              }
            if (partitionColumnNotContainsEval.nonEmpty &&
              !partitionColumnNotContainsEval.reduceLeft(_ && _)) {
              return i
            }
          case _ @ PhysicalOperation(_, filters,
          logicalRelation @
            LogicalRelation(fsRelation @
              HadoopFsRelation(
              _,
              partitionSchema,
              _,
              _,
              _,
              _),
            _,
            _,
            _))
            if filters.nonEmpty && fsRelation.partitionSchema.nonEmpty =>
            val normalizedFilters = DataSourceStrategy.normalizeExprs(
              filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f)),
              logicalRelation.output)
            val (partitionKeyFilters, _) = DataSourceUtils
              .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)

            partitionKeyFilters.map {
              case EqualTo(AttributeReference(name, _, _, _), Literal(value, _)) =>
                fillStaticPartitions += (name -> value.toString)
              case In(AttributeReference(name, _, _, _), list @ Seq(Literal(value, _)))
                if list.size == 1 => fillStaticPartitions += (name -> value.toString)
              case _ => // do nothing
            }
          case _ => // do nothing
        }

        i.copy(outputPath = i.outputPath,
          staticPartitions = i.staticPartitions,
          ifPartitionNotExists = i.ifPartitionNotExists,
          partitionColumns = i.partitionColumns,
          bucketSpec = i.bucketSpec,
          fileFormat = i.fileFormat,
          options = i.options,
          query = query,
          mode = i.mode,
          catalogTable = i.catalogTable,
          fileIndex = i.fileIndex,
          outputColumnNames = i.outputColumnNames,
          fillStaticPartitions.toMap)
    }
}
