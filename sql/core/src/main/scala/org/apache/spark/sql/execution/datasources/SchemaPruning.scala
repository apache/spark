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
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils._

/**
 * Prunes unnecessary physical columns given a [[ScanOperation]] over a data source relation.
 * By "physical column", we mean a column as defined in the data source format like Parquet format
 * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
 * column, and a nested Parquet column corresponds to a [[StructField]].
 *
 * Also prunes the unnecessary metadata columns if any for all file formats.
 *
 * SPARK-47230: Enhanced to support pruning nested columns accessed through chained LATERAL VIEW.
 */
object SchemaPruning extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // SPARK-47230: Collect GetArrayStructFields AND GetStructField expressions,
    // plus Generate mappings
    val allArrayStructFields = scala.collection.mutable.ArrayBuffer[GetArrayStructFields]()
    val allStructFields = scala.collection.mutable.ArrayBuffer[GetStructField]()
    val generateMappings = scala.collection.mutable.Map[ExprId, Expression]()

    plan.foreach { node =>
      // Collect Generate nodes to map exploded aliases back to original arrays
      node match {
        case g: Generate =>
          // Generate creates new attributes for exploded array elements
          // Map the output attribute IDs to the generator expression
          g.generatorOutput.foreach { attr =>
            generateMappings(attr.exprId) = g.generator
          }
        case _ =>
      }

      // Collect GetArrayStructFields and GetStructField
      node.expressions.foreach { expr =>
        expr.foreach {
          case gasf: GetArrayStructFields => allArrayStructFields += gasf
          case gsf: GetStructField => allStructFields += gsf
          case _ =>
        }
      }
    }

    plan transformDown {
      // SPARK-47230: Handle cases with Generate nodes where ScanOperation doesn't match
      case p @ Project(_, l @ LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, _, _))
          if generateMappings.nonEmpty &&
             canPruneDataSchema(hadoopFsRelation) &&
             (allArrayStructFields.nonEmpty || allStructFields.nonEmpty) =>
        // SPARK-47230: Collect all top-level columns referenced in the Project
        val relationExprIds = l.output.map(_.exprId).toSet
        val requiredColumns = p.projectList.flatMap { expr =>
          expr.collect {
            case a: AttributeReference if relationExprIds.contains(a.exprId) => a.name
          }
        }.toSet

        // Use the expressions collected at the beginning
        tryEnhancedNestedArrayPruning(
          l, allArrayStructFields.toSeq, allStructFields.toSeq,
          generateMappings.toMap, hadoopFsRelation, requiredColumns) match {
          case Some(prunedRelation) => p.copy(child = prunedRelation)
          case None => p
        }

      case op @ ScanOperation(projects, filtersStayUp, filtersPushDown,
        l @ LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, _, _)) =>


        val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp

        // SPARK-47230: Try enhanced pruning only if there are Generate nodes (LATERAL VIEW)
        val enhancedPruning = if (canPruneDataSchema(hadoopFsRelation) &&
            generateMappings.nonEmpty &&
            (allArrayStructFields.nonEmpty || allStructFields.nonEmpty)) {
          // Collect all top-level columns referenced in projects and filters
          val relationExprIds = l.output.map(_.exprId).toSet
          val requiredColumns = (projects ++ allFilters).flatMap { expr =>
            expr.collect {
              case a: AttributeReference if relationExprIds.contains(a.exprId) => a.name
            }
          }.toSet

          tryEnhancedNestedArrayPruning(
            l, allArrayStructFields.toSeq, allStructFields.toSeq,
            generateMappings.toMap, hadoopFsRelation, requiredColumns)
        } else {
          None
        }

        // Fall back to standard pruning if enhanced pruning doesn't apply
        enhancedPruning.getOrElse {
          prunePhysicalColumns(l, projects, allFilters, hadoopFsRelation,
          (prunedDataSchema, prunedMetadataSchema) => {
            val prunedHadoopRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)
            buildPrunedRelation(l, prunedHadoopRelation, prunedMetadataSchema)
          }).getOrElse(op)
        }
    }
  }

  /**
   * SPARK-47230: Enhanced pruning for nested arrays accessed via GetArrayStructFields.
   *
   * This handles cases where arrays of structs are accessed through chained explodes.
   * Standard Spark pruning doesn't handle GetArrayStructFields properly.
   *
   * @param requiredColumns Top-level columns that must be preserved even if they have no
   *                        nested field accesses (e.g., columns used in GROUP BY)
   */
  private def tryEnhancedNestedArrayPruning(
      relation: LogicalRelation,
      arrayStructFields: Seq[GetArrayStructFields],
      structFields: Seq[GetStructField],
      generateMappings: Map[ExprId, Expression],
      hadoopFsRelation: HadoopFsRelation,
      requiredColumns: Set[String]): Option[LogicalPlan] = {

    if (arrayStructFields.isEmpty && structFields.isEmpty) {
      return None
    }


    // Build a map: root column name -> set of field paths accessed
    val nestedFieldAccesses = scala.collection.mutable.Map[String, Set[Seq[String]]]()
    // Track paths from GetArrayStructFields separately - these use ordinals
    // and need special handling to preserve field order
    val arrayStructFieldPaths =
      scala.collection.mutable.Map[String, Set[Seq[String]]]()
    var tracedThroughGenerate = false

    // Get relation column names and IDs
    val relationColumnNames = relation.output.map(_.name).toSet
    val relationExprIds = relation.output.map(_.exprId).toSet

    // Process GetArrayStructFields
    arrayStructFields.foreach { gasf =>
      val (rootAndPath, usedGenerate) = traceToRootColumnThroughGenerates(
        gasf, generateMappings, relationExprIds)
      if (usedGenerate) tracedThroughGenerate = true
      rootAndPath.foreach { case (rootCol, path) =>
        logInfo(s"SCHEMA_PRUNING_DEBUG: GetArrayStructFields path=$path")
        val existingPaths = nestedFieldAccesses.getOrElse(rootCol, Set.empty)
        nestedFieldAccesses(rootCol) = existingPaths + path
        // Track this as an array struct field path
        val existingArrayPaths = arrayStructFieldPaths.getOrElse(rootCol, Set.empty)
        arrayStructFieldPaths(rootCol) = existingArrayPaths + path
      }
    }

    // Process GetStructField expressions (like request.available)
    // BUT skip paths that are prefixes of GetArrayStructFields paths
    // (e.g., if we have request.servedItems.clicked via GetArrayStructFields,
    // don't also add request.servedItems from GetStructField)
    structFields.foreach { gsf =>
      val (rootAndPath, usedGenerate) = traceStructFieldThroughGenerates(
        gsf, generateMappings, relationExprIds)
      if (usedGenerate) tracedThroughGenerate = true
      rootAndPath.foreach { case (rootCol, path) =>
        // Check if this path is a prefix of any GetArrayStructFields path
        val arrayPathsForCol = arrayStructFieldPaths.getOrElse(rootCol, Set.empty)
        val isPrefix = arrayPathsForCol.exists(_.startsWith(path))

        logInfo(s"SCHEMA_PRUNING_DEBUG: GetStructField path=$path, " +
          s"arrayPaths=$arrayPathsForCol, isPrefix=$isPrefix")

        if (!isPrefix) {
          // Only add if it's not a prefix of a GetArrayStructFields path
          val existingPaths = nestedFieldAccesses.getOrElse(rootCol, Set.empty)
          nestedFieldAccesses(rootCol) = existingPaths + path
        }
      }
    }

    // SPARK-47230: Only apply enhanced pruning if we actually traced through Generate nodes
    // This enables pruning for explode/posexplode cases
    if (!tracedThroughGenerate) {
      return None
    }

    if (nestedFieldAccesses.isEmpty) {
      return None
    }

    // Don't prune when multiple fields AT THE SAME DEPTH are accessed from the same
    // root column after a generator
    // (SPARK-34638/SPARK-41961: "Currently we don't prune multiple field case")
    // This prevents pruning when accessing like friend.first AND friend.middle
    // But allows pruning for chained explodes: request.available (depth 1) +
    // request.servedItems.clicked (depth 2)
    nestedFieldAccesses.foreach { case (rootCol, paths) =>
      logInfo(s"SCHEMA_PRUNING_DEBUG: rootCol=$rootCol, allPaths=$paths")
      val groupedByDepth = paths.groupBy(_.length)
      groupedByDepth.foreach { case (depth, pathsAtDepth) =>
        logInfo(s"SCHEMA_PRUNING_DEBUG: depth=$depth, pathsAtDepth=$pathsAtDepth, " +
          s"count=${pathsAtDepth.size}")
      }
    }
    if (nestedFieldAccesses.values.exists { paths =>
      // Group paths by their depth (length), check if any depth level has multiple paths
      val hasMultipleAtSameDepth = paths.groupBy(_.length).values.exists(_.size > 1)
      if (hasMultipleAtSameDepth) {
        logInfo(s"SCHEMA_PRUNING_DEBUG: BLOCKING PRUNING - multiple fields at same depth: $paths")
      }
      hasMultipleAtSameDepth
    }) {
      return None
    }

    // Filter to only keep root columns that exist in the relation
    logInfo(s"SCHEMA_PRUNING_DEBUG: relationColumnNames=$relationColumnNames")
    val filteredAccesses = nestedFieldAccesses.filter { case (rootCol, _) =>
      relationColumnNames.contains(rootCol)
    }

    logInfo(s"SCHEMA_PRUNING_DEBUG: filteredAccesses=$filteredAccesses")
    if (filteredAccesses.isEmpty) {
      logInfo(s"SCHEMA_PRUNING_DEBUG: RETURNING None - filteredAccesses is empty!")
      return None
    }

    // Build pruned schema by keeping only accessed nested fields
    val originalSchema = hadoopFsRelation.dataSchema
    // Pass arrayStructFieldPaths to avoid pruning array structs with ordinal-based access
    val filteredArrayPaths = arrayStructFieldPaths.filter { case (rootCol, _) =>
      relationColumnNames.contains(rootCol)
    }
    val prunedSchema = pruneNestedArraySchema(
      originalSchema, filteredAccesses.toMap, requiredColumns, filteredArrayPaths.toMap)


    // Check if we actually pruned anything
    val originalFieldCount = countLeaves(originalSchema)
    val prunedFieldCount = countLeaves(prunedSchema)

    if (prunedFieldCount < originalFieldCount) {
      val prunedHadoopRelation =
        hadoopFsRelation.copy(dataSchema = prunedSchema)(hadoopFsRelation.sparkSession)
      Some(buildPrunedRelation(relation, prunedHadoopRelation, StructType(Nil)))
    } else {
      None
    }
  }

  /**
   * SPARK-47230: Trace a GetArrayStructFields back to its root column through Generate nodes.
   * Returns (Option[(rootColumnName, Seq[fieldNames])], usedGenerate)
   */
  private def traceToRootColumnThroughGenerates(
      expr: Expression,
      generateMappings: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case GetArrayStructFields(child, field, _, _, _) =>
        val (result, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, relationExprIds)
        (result.map { case (rootCol, path) => (rootCol, path :+ field.name) }, usedGen)
      case _ => (None, false)
    }
  }

  /**
   * SPARK-47230: Trace a GetStructField back to its root column through Generate nodes.
   * Returns (Option[(rootColumnName, Seq[fieldNames])], usedGenerate)
   */
  private def traceStructFieldThroughGenerates(
      expr: Expression,
      generateMappings: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case GetStructField(child, _, Some(fieldName)) =>
        val (result, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, relationExprIds)
        (result.map { case (rootCol, path) => (rootCol, path :+ fieldName) }, usedGen)
      case _ => (None, false)
    }
  }

  /**
   * Trace a GetArrayStructFields back to its root column and field path.
   * Returns (rootColumnName, Seq[fieldNames])
   */
  private def traceToRootColumn(expr: Expression): Option[(String, Seq[String])] = {
    expr match {
      case GetArrayStructFields(child, field, _, _, _) =>
        traceArrayAccess(child) match {
          case Some((rootCol, path)) =>
            Some((rootCol, path :+ field.name))
          case None => None
        }
      case _ => None
    }
  }

  /**
   * SPARK-47230: Trace back through Generate nodes to find the original relation column.
   * Returns (Option[(rootColumnName, Seq[fieldNamesLeadingToArray])], usedGenerate)
   */
  private def traceArrayAccessThroughGenerates(
      expr: Expression,
      generateMappings: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case attr: AttributeReference =>
        // Check if this attribute is from a Generate node
        generateMappings.get(attr.exprId) match {
          case Some(gen: UnaryExpression) =>
            // This attribute comes from a unary generator (explode, posexplode, etc.)
            // Extract the child and trace through it
            val (result, _) = traceArrayAccessThroughGenerates(
              gen.child, generateMappings, relationExprIds)
            (result, true)
          case Some(other) =>
            // Other generator types without a single child - mark as used Generate
            val (result, _) = traceArrayAccessThroughGenerates(
              other, generateMappings, relationExprIds)
            (result, true)
          case None =>
            // Not from a Generate node - this is a regular attribute
            if (relationExprIds.contains(attr.exprId)) {
              (Some((attr.name, Seq.empty)), false)
            } else {
              (None, false)
            }
        }

      case GetStructField(child, _, nameOpt) =>
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, relationExprIds)
        (childResult.flatMap { case (rootCol, path) =>
          nameOpt.map(fieldName => (rootCol, path :+ fieldName))
        }, usedGen)

      case GetArrayItem(child, _, _) =>
        traceArrayAccessThroughGenerates(child, generateMappings, relationExprIds)

      case GetArrayStructFields(child, field, _, _, _) =>
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, relationExprIds)
        (childResult.map { case (rootCol, path) =>
          (rootCol, path :+ field.name)
        }, usedGen)

      case _ => (None, false)
    }
  }

  /**
   * Trace back to the root column through nested field accesses.
   * Returns (rootColumnName, Seq[fieldNamesLeadingToArray])
   */
  private def traceArrayAccess(expr: Expression): Option[(String, Seq[String])] = {
    expr match {
      case attr: AttributeReference =>
        Some((attr.name, Seq.empty))

      case GetStructField(child, _, nameOpt) =>
        traceArrayAccess(child).flatMap { case (rootCol, path) =>
          nameOpt.map(fieldName => (rootCol, path :+ fieldName))
        }

      case GetArrayItem(child, _, _) =>
        traceArrayAccess(child)

      case GetArrayStructFields(child, field, _, _, _) =>
        // Nested GetArrayStructFields
        traceArrayAccess(child).map { case (rootCol, path) =>
          (rootCol, path :+ field.name)
        }

      case _ => None
    }
  }

  /**
   * Prune schema keeping only accessed nested fields.
   * Key fix: When a field path is accessed, we keep ONLY that path, not sibling fields.
   */
  private def pruneNestedArraySchema(
      schema: StructType,
      nestedFieldAccesses: Map[String, Set[Seq[String]]],
      requiredColumns: Set[String],
      arrayStructFieldPaths: Map[String, Set[Seq[String]]]): StructType = {

    logInfo(s"SCHEMA_PRUNING_DEBUG: pruneNestedArraySchema called")
    logInfo(s"SCHEMA_PRUNING_DEBUG: nestedFieldAccesses=$nestedFieldAccesses")
    logInfo(s"SCHEMA_PRUNING_DEBUG: arrayStructFieldPaths=$arrayStructFieldPaths")

    val prunedFields = schema.fields.flatMap { field =>
      nestedFieldAccesses.get(field.name) match {
        case Some(paths) if paths.nonEmpty =>
          // This root field has specific nested paths accessed
          // Get the GetArrayStructFields paths for this field to pass down
          val arrayPaths = arrayStructFieldPaths.getOrElse(field.name, Set.empty)
          logInfo(s"SCHEMA_PRUNING_DEBUG: Processing field ${field.name}, " +
            s"paths=$paths, arrayPaths=$arrayPaths")
          val prunedField = pruneFieldByPaths(field, paths.toSeq, arrayPaths)
          logInfo(s"SCHEMA_PRUNING_DEBUG: After pruning ${field.name}, " +
            s"fieldCount=${countLeaves(prunedField.dataType)}")
          Some(prunedField)
        case None =>
          // SPARK-47230: Preserve top-level columns that are directly referenced
          // (e.g., columns used in GROUP BY, WHERE without nested access)
          if (requiredColumns.contains(field.name)) {
            Some(field)
          } else {
            // This field is not accessed, don't include it
            None
          }
      }
    }

    StructType(prunedFields)
  }

  /**
   * Prune a StructType while preserving all field positions (for GetArrayStructFields ordinals).
   *
   * This function keeps ALL fields in the struct to maintain ordinal positions,
   * but recursively prunes nested levels for fields that have deeper paths (length > 1).
   *
   * For example, if paths = Seq(Seq("a"), Seq("b", "c")) and the struct has fields a, b, d:
   * - Keep field "a" entirely (path length 1)
   * - Keep field "b" but recursively prune it to only contain "c"
   * - Keep field "d" as-is (not accessed, but needed for ordinal preservation)
   */
  private def pruneStructPreservingFieldOrder(struct: StructType, paths: Seq[Seq[String]],
      arrayStructFieldPaths: Set[Seq[String]]): StructType = {
    logInfo(s"SCHEMA_PRUNING_DEBUG: pruneStructPreservingFieldOrder called with " +
      s"${struct.fields.length} fields, paths=$paths")

    // Group paths by their first component
    val pathsByFirstField = paths.groupBy(_.head)

    // Keep ALL fields to preserve ordinal positions, but prune nested levels
    val prunedFields = struct.fields.map { field =>
      pathsByFirstField.get(field.name) match {
        case Some(fieldPaths) =>
          // This field is accessed. Check if there are nested paths to prune.
          val remainingPaths = fieldPaths.map(_.tail).filter(_.nonEmpty)

          // Get array paths for this field (removing first component)
          val fieldArrayPaths = arrayStructFieldPaths
            .filter(_.headOption.contains(field.name))
            .map(_.tail)

          if (remainingPaths.isEmpty) {
            // Direct access to this field (path length was 1), keep it entirely
            logInfo(s"SCHEMA_PRUNING_DEBUG: Keeping field ${field.name} entirely (direct access)")
            field
          } else {
            // Has nested accesses - recursively prune nested levels
            logInfo(s"SCHEMA_PRUNING_DEBUG: Recursively pruning field ${field.name} " +
              s"with remainingPaths=$remainingPaths")
            pruneFieldByPaths(field, remainingPaths, fieldArrayPaths)
          }
        case None =>
          // Field not accessed, but keep it anyway to preserve ordinals
          logInfo(s"SCHEMA_PRUNING_DEBUG: Keeping field ${field.name} for ordinal preservation")
          field
      }
    }

    StructType(prunedFields)
  }

  /**
   * Prune a field to keep only the specified paths.
   *
   * Key insight: paths = Seq(Seq("a", "b"), Seq("a", "c")) means we need:
   * - The field containing "a", which contains both "b" and "c"
   * - NOT any sibling fields of "a"
   */
  private def pruneFieldByPaths(field: StructField, paths: Seq[Seq[String]],
      arrayStructFieldPaths: Set[Seq[String]] = Set.empty): StructField = {
    logInfo(s"SCHEMA_PRUNING_DEBUG: pruneFieldByPaths field=${field.name}, " +
      s"paths=$paths, arrayStructFieldPaths=$arrayStructFieldPaths")

    // If any path is empty, we need the entire field
    if (paths.exists(_.isEmpty)) {
      logInfo(s"SCHEMA_PRUNING_DEBUG: Empty path found, keeping entire field ${field.name}")
      return field
    }

    field.dataType match {
      case ArrayType(elementType: StructType, containsNull) =>
        // For array<struct<...>>, check if GetArrayStructFields directly accesses
        // fields at THIS struct level (paths of length 1).
        // GetArrayStructFields uses ORDINALS, so we can't prune if it accesses this level.
        val hasDirectArrayAccess = arrayStructFieldPaths.exists(_.length == 1)
        logInfo(s"SCHEMA_PRUNING_DEBUG: Field ${field.name} is array<struct>, " +
          s"hasDirectArrayAccess=$hasDirectArrayAccess")

        val prunedElementType = if (hasDirectArrayAccess) {
          // GetArrayStructFields accesses fields at this struct level using ordinals.
          // We MUST keep all fields to preserve ordinal positions.
          // BUT: we still recursively prune nested levels (paths with length > 1)
          logInfo(s"SCHEMA_PRUNING_DEBUG: Preserving field order for ${field.name}, " +
            s"but pruning nested levels...")
          pruneStructPreservingFieldOrder(elementType, paths, arrayStructFieldPaths)
        } else {
          // No GetArrayStructFields at this level - safe to prune normally
          logInfo(s"SCHEMA_PRUNING_DEBUG: Safe to prune ${field.name} normally")
          pruneStructByPaths(elementType, paths, arrayStructFieldPaths)
        }
        field.copy(dataType = ArrayType(prunedElementType, containsNull))

      case struct: StructType =>
        // For struct<...>, prune it
        // (GetArrayStructFields doesn't apply to non-array structs)
        val prunedStruct = pruneStructByPaths(struct, paths, arrayStructFieldPaths)
        field.copy(dataType = prunedStruct)

      case _ =>
        // Leaf type, return as-is
        field
    }
  }

  /**
   * Prune a StructType to keep only fields in the given paths.
   *
   * For paths like Seq(Seq("a"), Seq("b", "c")):
   * - Keep field "a" entirely
   * - Keep field "b", but recursively prune it to only contain "c"
   */
  private def pruneStructByPaths(struct: StructType, paths: Seq[Seq[String]],
      arrayStructFieldPaths: Set[Seq[String]] = Set.empty): StructType = {
    // Group paths by their first component
    val pathsByFirstField = paths.groupBy(_.head)

    // Keep only fields that are accessed, preserving original field order
    // to maintain field ordinals for GetArrayStructFields expressions
    val prunedFields = struct.fields.flatMap { field =>
      pathsByFirstField.get(field.name).map { fieldPaths =>
        // Get remaining path components after removing the first
        val remainingPaths = fieldPaths.map(_.tail).filter(_.nonEmpty)

        // Get array paths for this field (removing first component)
        val fieldArrayPaths = arrayStructFieldPaths
          .filter(_.headOption.contains(field.name))
          .map(_.tail)

        if (remainingPaths.isEmpty) {
          // This field itself is accessed, keep it entirely
          field
        } else {
          // This field has nested accesses, recurse
          pruneFieldByPaths(field, remainingPaths, fieldArrayPaths)
        }
      }
    }

    StructType(prunedFields)
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
    val attrNameMap = relation.output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = normalizeAttributeRefNames(attrNameMap, projects)
      .asInstanceOf[Seq[NamedExpression]]
    val normalizedFilters = normalizeAttributeRefNames(attrNameMap, filters)
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
        val projectionOverSchema = ProjectionOverSchema(
          prunedDataSchema.merge(prunedMetadataSchema), AttributeSet(relation.output))
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
   * Normalizes the names of the attribute references in the given expressions to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      attrNameMap: Map[ExprId, String],
      exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map(_.transform {
      case att: AttributeReference if attrNameMap.contains(att.exprId) =>
        att.withName(attrNameMap(att.exprId))
    })
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
        // bottom-most filters are put in the left of the list.
        projectedFilters.foldLeft[LogicalPlan](leafNode)((plan, cond) => Filter(cond, plan))
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
    val prunedRelation = outputRelation.copy(relation = prunedBaseRelation, output = prunedOutput)
    prunedRelation.copyTagsFrom(outputRelation)
    prunedRelation
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(
      output: Seq[AttributeReference],
      requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to update the data type of the output attributes to use the pruned ones.
    // so that references to the original relation's output are not broken
    val nameAttributeMap = output.map(att => (att.name, att)).toMap
    toAttributes(requiredSchema)
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
