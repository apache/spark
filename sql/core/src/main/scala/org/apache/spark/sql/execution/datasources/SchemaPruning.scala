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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Prunes unnecessary physical columns given a [[PhysicalOperation]] over a data source relation.
 * By "physical column", we mean a column as defined in the data source format like Parquet format
 * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
 * column, and a nested Parquet column corresponds to a [[StructField]].
 */
object SchemaPruning extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.nestedSchemaPruningEnabled) {
      apply0(plan)
    } else {
      plan
    }

  private def apply0(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
          l @ LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, _, _))
        if canPruneRelation(hadoopFsRelation) =>
        val (normalizedProjects, normalizedFilters) =
          normalizeAttributeRefNames(l, projects, filters)
        val requestedRootFields = identifyRootFields(normalizedProjects, normalizedFilters)

        // If requestedRootFields includes a nested field, continue. Otherwise,
        // return op
        if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {
          val dataSchema = hadoopFsRelation.dataSchema
          val prunedDataSchema = pruneDataSchema(dataSchema, requestedRootFields)

          // If the data schema is different from the pruned data schema, continue. Otherwise,
          // return op. We effect this comparison by counting the number of "leaf" fields in
          // each schemata, assuming the fields in prunedDataSchema are a subset of the fields
          // in dataSchema.
          if (countLeaves(dataSchema) > countLeaves(prunedDataSchema)) {
            val prunedHadoopRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)

            val prunedRelation = buildPrunedRelation(l, prunedHadoopRelation)
            val projectionOverSchema = ProjectionOverSchema(prunedDataSchema)

            buildNewProjection(normalizedProjects, normalizedFilters, prunedRelation,
              projectionOverSchema)
          } else {
            op
          }
        } else {
          op
        }
    }

  /**
   * Checks to see if the given relation can be pruned. Currently we support Parquet and ORC v1.
   */
  private def canPruneRelation(fsRelation: HadoopFsRelation) =
    fsRelation.fileFormat.isInstanceOf[ParquetFileFormat] ||
      fsRelation.fileFormat.isInstanceOf[OrcFileFormat]

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      logicalRelation: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = logicalRelation.output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = projects.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    }).map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }

  /**
   * Builds the new output [[Project]] Spark SQL operator that has the pruned output relation.
   */
  private def buildNewProjection(
      projects: Seq[NamedExpression], filters: Seq[Expression], prunedRelation: LogicalRelation,
      projectionOverSchema: ProjectionOverSchema) = {
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

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects = projects.map(_.transformDown {
      case projectionOverSchema(expr) => expr
    }).map { case expr: NamedExpression => expr }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }

    Project(newProjects, projectionChild)
  }

  /**
   * Builds a pruned logical relation from the output of the output relation and the schema of the
   * pruned base relation.
   */
  private def buildPrunedRelation(
      outputRelation: LogicalRelation,
      prunedBaseRelation: HadoopFsRelation) = {
    // We need to replace the expression ids of the pruned relation output attributes
    // with the expression ids of the original relation output attributes so that
    // references to the original relation's output are not broken
    val outputIdMap = outputRelation.output.map(att => (att.name, att.exprId)).toMap
    val prunedRelationOutput =
      prunedBaseRelation
        .schema
        .toAttributes
        .map {
          case att if outputIdMap.contains(att.name) =>
            att.withExprId(outputIdMap(att.name))
          case att => att
        }
    outputRelation.copy(relation = prunedBaseRelation, output = prunedRelationOutput)
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
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


}
