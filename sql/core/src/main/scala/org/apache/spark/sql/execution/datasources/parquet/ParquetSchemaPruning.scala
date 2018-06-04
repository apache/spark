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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Prunes unnecessary Parquet columns given a [[PhysicalOperation]] over a
 * [[ParquetRelation]]. By "Parquet column", we mean a column as defined in the
 * Parquet format. In Spark SQL, a root-level Parquet column corresponds to a
 * SQL column, and a nested Parquet column corresponds to a [[StructField]].
 */
private[sql] object ParquetSchemaPruning extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.nestedSchemaPruningEnabled) {
      apply0(plan)
    } else {
      plan
    }

  private def apply0(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
          l @ LogicalRelation(hadoopFsRelation @ HadoopFsRelation(_, partitionSchema,
            dataSchema, _, parquetFormat: ParquetFileFormat, _), _, _, _)) =>
        val projectionRootFields = projects.flatMap(getRootFields)
        val filterRootFields = filters.flatMap(getRootFields)
        val requestedRootFields = (projectionRootFields ++ filterRootFields).distinct

        // If [[requestedRootFields]] includes a nested field, continue. Otherwise,
        // return [[op]]
        if (requestedRootFields.exists { case RootField(_, derivedFromAtt) => !derivedFromAtt }) {
          val prunedSchema = requestedRootFields
            .map { case RootField(field, _) => StructType(Array(field)) }
            .reduceLeft(_ merge _)
          val dataSchemaFieldNames = dataSchema.fieldNames.toSet
          val prunedDataSchema =
            StructType(prunedSchema.filter(f => dataSchemaFieldNames.contains(f.name)))

          // If the data schema is different from the pruned data schema, continue. Otherwise,
          // return [[op]]. We effect this comparison by counting the number of "leaf" fields in
          // each schemata, assuming the fields in [[prunedDataSchema]] are a subset of the fields
          // in [[dataSchema]].
          if (countLeaves(dataSchema) > countLeaves(prunedDataSchema)) {
            val prunedParquetRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)

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
              l.copy(relation = prunedParquetRelation, output = prunedRelationOutput)

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
              partitionSchema.map(_.name).filterNot(dataSchemaFieldNames.contains).toSet

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
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When [[expr]] is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable), true) :: Nil
      case SelectedField(field) => RootField(field, false) :: Nil
      case _ =>
        expr.children.flatMap(getRootFields)
    }
  }

  /**
   * Counts the "leaf" fields of the given [[dataType]]. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[dataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }

  /**
   * A "root" schema field (aka top-level, no-parent) and whether it was derived from
   * an attribute or had a proper child.
   */
  private case class RootField(field: StructField, derivedFromAtt: Boolean)
}
