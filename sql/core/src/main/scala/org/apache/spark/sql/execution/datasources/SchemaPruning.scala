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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.SchemaUtils._

/**
 * Prunes unnecessary physical columns given a [[PhysicalOperation]] over a data source relation.
 * By "physical column", we mean a column as defined in the data source format like Parquet format
 * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
 * column, and a nested Parquet column corresponds to a [[StructField]].
 *
 * Also prunes the unnecessary metadata columns if any for all file formats.
 */
object SchemaPruning extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
      l @ LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, _, _)) =>
        prunePhysicalColumns(l, projects, filters, hadoopFsRelation,
          (prunedDataSchema, prunedMetadataSchema) => {
            val prunedHadoopRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)
            buildPrunedRelation(l, prunedHadoopRelation, prunedMetadataSchema)
          }).getOrElse(op)
    }

  /**
   * This method returns optional logical plan. `None` is returned if no nested field is required or
   * all nested fields are required.
   *
   * This method will prune both the data schema and the metadata schema
   */
  private def prunePhysicalColumns(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      hadoopFsRelation: HadoopFsRelation,
      leafNodeBuilder: (StructType, StructType) => LeafNode): Option[LogicalPlan] = {

    val (normalizedProjects, normalizedFilters) =
      normalizeAttributeRefNames(relation.output, projects, filters)
    val requestedRootFields = identifyRootFields(normalizedProjects, normalizedFilters)

    // If requestedRootFields includes a nested field, continue. Otherwise,
    // return op
    if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {

      val prunedDataSchema = if (canPruneDataSchema(hadoopFsRelation)) {
        pruneSchema(hadoopFsRelation.dataSchema, requestedRootFields)
      } else {
        hadoopFsRelation.dataSchema
      }

      val metadataSchema =
        relation.output.collect { case FileSourceMetadataAttribute(attr) => attr }.toStructType
      val prunedMetadataSchema = if (metadataSchema.nonEmpty) {
        pruneSchema(metadataSchema, requestedRootFields)
      } else {
        metadataSchema
      }

      // If the data schema is different from the pruned data schema
      // OR
      // the metadata schema is different from the pruned metadata schema, continue.
      // Otherwise, return None.
      if (countLeaves(hadoopFsRelation.dataSchema) > countLeaves(prunedDataSchema) ||
        countLeaves(metadataSchema) > countLeaves(prunedMetadataSchema)) {
        val prunedRelation = leafNodeBuilder(prunedDataSchema, prunedMetadataSchema)
        val projectionOverSchema =
          ProjectionOverSchema(prunedDataSchema.merge(prunedMetadataSchema))
        Some(buildNewProjection(projects, normalizedProjects, normalizedFilters,
          prunedRelation, projectionOverSchema))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Checks to see if the given relation can be pruned. Currently we support Parquet and ORC v1.
   */
  private def canPruneDataSchema(fsRelation: HadoopFsRelation): Boolean =
    conf.nestedSchemaPruningEnabled && (
      fsRelation.fileFormat.isInstanceOf[ParquetFileFormat] ||
        fsRelation.fileFormat.isInstanceOf[OrcFileFormat])

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      output: Seq[AttributeReference],
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = output.map(att => (att.exprId, att.name)).toMap
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
   * Builds the new output [[Project]] Spark SQL operator that has the `leafNode`.
   */
  private def buildNewProjection(
      projects: Seq[NamedExpression],
      normalizedProjects: Seq[NamedExpression],
      filters: Seq[Expression],
      leafNode: LeafNode,
      projectionOverSchema: ProjectionOverSchema): Project = {
    // Construct a new target for our projection by rewriting and
    // including the original filters where available
    val projectionChild =
      if (filters.nonEmpty) {
        val projectedFilters = filters.map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        val newFilterCondition = projectedFilters.reduce(And)
        Filter(newFilterCondition, leafNode)
      } else {
        leafNode
      }

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects = normalizedProjects.map(_.transformDown {
      case projectionOverSchema(expr) => expr
    }).map { case expr: NamedExpression => expr }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }

    Project(restoreOriginalOutputNames(newProjects, projects.map(_.name)), projectionChild)
  }

  /**
   * Builds a pruned logical relation from the output of the output relation and the schema of the
   * pruned base relation.
   */
  private def buildPrunedRelation(
      outputRelation: LogicalRelation,
      prunedBaseRelation: HadoopFsRelation,
      prunedMetadataSchema: StructType) = {
    val finalSchema = prunedBaseRelation.schema.merge(prunedMetadataSchema)
    val prunedOutput = getPrunedOutput(outputRelation.output, finalSchema)
    outputRelation.copy(relation = prunedBaseRelation, output = prunedOutput)
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(
      output: Seq[AttributeReference],
      requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to update the data type of the output attributes to use the pruned ones.
    // so that references to the original relation's output are not broken
    val nameAttributeMap = output.map(att => (att.name, att)).toMap
    requiredSchema
      .toAttributes
      .map {
        case att if nameAttributeMap.contains(att.name) =>
          nameAttributeMap(att.name).withDataType(att.dataType)
        case att => att
      }
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
