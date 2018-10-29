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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectionOverSchema, SelectedField}
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
            val prunedParquetRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)

            val prunedRelation = buildPrunedRelation(l, prunedParquetRelation)
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
   * Checks to see if the given relation is Parquet and can be pruned.
   */
  private def canPruneRelation(fsRelation: HadoopFsRelation) =
    fsRelation.fileFormat.isInstanceOf[ParquetFileFormat]

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
   * Returns the set of fields from the Parquet file that the query plan needs.
   */
  private def identifyRootFields(projects: Seq[NamedExpression], filters: Seq[Expression]) = {
    val projectionRootFields = projects.flatMap(getRootFields)
    val filterRootFields = filters.flatMap(getRootFields)

    // Kind of expressions don't need to access any fields of a root fields, e.g., `IsNotNull`.
    // For them, if there are any nested fields accessed in the query, we don't need to add root
    // field access of above expressions.
    // For example, for a query `SELECT name.first FROM contacts WHERE name IS NOT NULL`,
    // we don't need to read nested fields of `name` struct other than `first` field.
    val (rootFields, optRootFields) = (projectionRootFields ++ filterRootFields)
      .distinct.partition(_.contentAccessed)

    optRootFields.filter { opt =>
      !rootFields.exists(_.field.name == opt.field.name)
    } ++ rootFields
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
   * Filters the schema from the given file by the requested fields.
   * Schema field ordering from the file is preserved.
   */
  private def pruneDataSchema(
      fileDataSchema: StructType,
      requestedRootFields: Seq[RootField]) = {
    // Merge the requested root fields into a single schema. Note the ordering of the fields
    // in the resulting schema may differ from their ordering in the logical relation's
    // original schema
    val mergedSchema = requestedRootFields
      .map { case root: RootField => StructType(Array(root.field)) }
      .reduceLeft(_ merge _)
    val dataSchemaFieldNames = fileDataSchema.fieldNames.toSet
    val mergedDataSchema =
      StructType(mergedSchema.filter(f => dataSchemaFieldNames.contains(f.name)))
    // Sort the fields of mergedDataSchema according to their order in dataSchema,
    // recursively. This makes mergedDataSchema a pruned schema of dataSchema
    sortLeftFieldsByRight(mergedDataSchema, fileDataSchema).asInstanceOf[StructType]
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
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When expr is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable), derivedFromAtt = true) :: Nil
      case SelectedField(field) => RootField(field, derivedFromAtt = false) :: Nil
      // Root field accesses by `IsNotNull` and `IsNull` are special cases as the expressions
      // don't actually use any nested fields. These root field accesses might be excluded later
      // if there are any nested fields accesses in the query plan.
      case IsNotNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, contentAccessed = false) :: Nil
      case IsNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, contentAccessed = false) :: Nil
      case IsNotNull(_: Attribute) | IsNull(_: Attribute) =>
        expr.children.flatMap(getRootFields).map(_.copy(contentAccessed = false))
      case _ =>
        expr.children.flatMap(getRootFields)
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

  /**
  * Sorts the fields and descendant fields of structs in left according to their order in
  * right. This function assumes that the fields of left are a subset of the fields of
  * right, recursively. That is, left is a "subschema" of right, ignoring order of
  * fields.
  */
  private def sortLeftFieldsByRight(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, containsNull), ArrayType(rightElementType, _)) =>
        ArrayType(
          sortLeftFieldsByRight(leftElementType, rightElementType),
          containsNull)
      case (MapType(leftKeyType, leftValueType, containsNull),
          MapType(rightKeyType, rightValueType, _)) =>
        MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType),
          containsNull)
      case (leftStruct: StructType, rightStruct: StructType) =>
        val filteredRightFieldNames = rightStruct.fieldNames.filter(leftStruct.fieldNames.contains)
        val sortedLeftFields = filteredRightFieldNames.map { fieldName =>
          val leftFieldType = leftStruct(fieldName).dataType
          val rightFieldType = rightStruct(fieldName).dataType
          val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
          StructField(fieldName, sortedLeftFieldType)
        }
        StructType(sortedLeftFields)
      case _ => left
    }

  /**
   * This represents a "root" schema field (aka top-level, no-parent). `field` is the
   * `StructField` for field name and datatype. `derivedFromAtt` indicates whether it
   * was derived from an attribute or had a proper child. `contentAccessed` means whether
   * it was accessed with its content by the expressions refer it.
   */
  private case class RootField(field: StructField, derivedFromAtt: Boolean,
    contentAccessed: Boolean = true)
}
