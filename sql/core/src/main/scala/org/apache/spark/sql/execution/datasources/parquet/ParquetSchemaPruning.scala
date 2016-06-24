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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ProjectionOverSchema, SelectedField}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Prunes unnecessary Parquet columns given a [[PhysicalOperation]] over a
 * [[ParquetRelation]]. By "Parquet column", we mean a column as defined in the
 * Parquet format. In Spark SQL, a root-level Parquet column corresponds to a
 * SQL column, and a nested Parquet column corresponds to a [[StructField]].
 */
private[sql] object ParquetSchemaPruning extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
          l @ LogicalRelation(hadoopFsRelation @ HadoopFsRelation(_, partitionSchema,
            dataSchema, _, parquetFormat: ParquetFileFormat, _), _, _, isStreaming)) =>
        val projectionFields = projects.flatMap(getFields)
        val filterFields = filters.flatMap(getFields)
        val requestedFields = (projectionFields ++ filterFields).distinct

        // If [[requestedFields]] includes a proper field, continue. Otherwise,
        // return [[op]]
        if (requestedFields.exists { case (_, optAtt) => optAtt.isEmpty }) {
          val prunedSchema = requestedFields
            .map { case (field, _) => field }
            .map(field => StructType(Array(field)))
            .reduceLeft(_ merge _)
          val parquetDataColumnNames = dataSchema.fieldNames
          val prunedDataSchema =
            StructType(prunedSchema.filter(f => parquetDataColumnNames.contains(f.name)))
          val parquetDataFields = dataSchema.fields.toSet
          val prunedDataFields = prunedDataSchema.fields.toSet

          // If the original Parquet relation data fields are different from the
          // pruned data fields, continue. Otherwise, return [[op]]
          if (parquetDataFields != prunedDataFields) {
            val dataSchemaFieldNames = hadoopFsRelation.dataSchema.fieldNames
            val newDataSchema =
              StructType(prunedSchema.filter(f => dataSchemaFieldNames.contains(f.name)))
            val prunedParquetRelation =
              hadoopFsRelation.copy(dataSchema = newDataSchema)(hadoopFsRelation.sparkSession)

            // We need to replace the expression ids of the pruned relation output attributes
            // with the expression ids of the original relation output attributes so that
            // references to the original relation's output are not broken
            val outputIdMap = l.output.map(att => (att.name, att.exprId)).toMap
            val prunedRelationOutput =
              prunedParquetRelation
                .schema
                .toAttributes
                .map {
                  case att if outputIdMap.contains(att.name) =>
                    att.withExprId(outputIdMap(att.name))
                  case att => att
                }
            val prunedRelation =
              LogicalRelation(prunedParquetRelation, prunedRelationOutput, None, isStreaming)

            val projectionOverSchema = ProjectionOverSchema(prunedDataSchema)

            // Construct a new target for our projection by rewriting and
            // including the original filters where available
            val projectionChild =
              if (filters.nonEmpty) {
                val projectedFilters = filters.map(_.transformDown {
                  case projectionOverSchema(expr) => expr
                })
                val newFilterCondition = projectedFilters.reduce(And)
                Filter(newFilterCondition, prunedRelation)
              } else {
                prunedRelation
              }

            val nonDataPartitionColumnNames =
              partitionSchema.filterNot(parquetDataFields.contains).map(_.name)

            // Construct the new projections of our [[Project]] by
            // rewriting the original projections
            val newProjects = projects.map {
              case project if (nonDataPartitionColumnNames.contains(project.name)) => project
              case project =>
                (project transformDown {
                  case projectionOverSchema(expr) => expr
                }).asInstanceOf[NamedExpression]
            }

            logDebug("New projects:\n" + newProjects.map(_.treeString).mkString("\n"))

            require(prunedDataSchema == prunedParquetRelation.dataSchema,
              s"""Pruned data schema != pruned parquet relation schema
                 |Pruned data schema:
                 |${prunedDataSchema.treeString}
                 |
                 |Pruned parquet relation schema:
                 |${prunedParquetRelation.schema.treeString}""".stripMargin)

            logDebug(s"Pruned data schema:\n${prunedDataSchema.treeString}")

            Project(newProjects, projectionChild)
          } else {
            op
          }
        } else {
          op
        }
    }

  /**
   * Gets the top-level (no-parent) [[StructField]]s for the given [[Expression]].
   * When [[expr]] is an [[Attribute]], construct a field around it and return the
   * attribute as the second component of the returned tuple.
   */
  private def getFields(expr: Expression): Seq[(StructField, Option[Attribute])] =
    expr match {
      case SelectedField(field) => (field, None) :: Nil
      case att: Attribute =>
        (StructField(att.name, att.dataType, att.nullable), Some(att)) :: Nil
      case _ =>
        expr.children.flatMap(getFields)
    }
}
