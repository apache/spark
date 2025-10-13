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
    logInfo(s"SCHEMA_PRUNING_DEBUG: === SchemaPruning.apply called ===")
    logInfo(s"SCHEMA_PRUNING_DEBUG: Plan structure:")
    plan.foreach { node =>
      logInfo(s"SCHEMA_PRUNING_DEBUG:   - ${node.getClass.getSimpleName}: ${node.nodeName}")
    }

    // SPARK-47230: Collect GetArrayStructFields, GetStructField, Generate mappings,
    // and _extract_ alias mappings
    val allArrayStructFields = scala.collection.mutable.ArrayBuffer[GetArrayStructFields]()
    val allStructFields = scala.collection.mutable.ArrayBuffer[GetStructField]()
    val generateMappings = scala.collection.mutable.Map[ExprId, Expression]()
    val extractAliases = scala.collection.mutable.Map[ExprId, Expression]()

    plan.foreach { node =>
      // Collect Generate nodes
      node match {
        case g: Generate =>
          println(s"[PRUNING] Found Generate: ${g.generator.getClass.getSimpleName}")
          println(s"[PRUNING]   Generator child: ${g.generator.children.headOption.getOrElse("none")}")
          println(s"[PRUNING]   Generator output count: ${g.generatorOutput.size}")
          g.generatorOutput.foreach { attr =>
            println(s"[PRUNING]   Mapping output ${attr.name}#${attr.exprId} -> ${g.generator.getClass.getSimpleName}")
            generateMappings(attr.exprId) = g.generator
          }
        case p: Project =>
          // Collect _extract_ aliases created by NestedColumnAliasing
          p.output.zip(p.projectList).foreach { case (attr, namedExpr) =>
            namedExpr match {
              case Alias(child, name) if name.startsWith("_extract_") =>
                println(s"[PRUNING] Found _extract_ alias: ${attr.name}#${attr.exprId} -> $child")
                extractAliases(attr.exprId) = child
              case _ =>
            }
          }
        case _ =>
      }

      // Collect GetArrayStructFields and GetStructField
      node.expressions.foreach { expr =>
        expr.foreach {
          case gasf: GetArrayStructFields =>
            allArrayStructFields += gasf
            println(s"[PRUNING] Collected GetArrayStructFields: ${gasf.field.name}")
          case gsf: GetStructField =>
            allStructFields += gsf
            println(s"[PRUNING] Collected GetStructField: ${gsf.name.getOrElse("unnamed")}")
          case _ =>
        }
      }
    }

    println(s"[PRUNING] === SUMMARY ===")
    println(s"[PRUNING] Generator mappings: ${generateMappings.size}")
    println(s"[PRUNING] _extract_ aliases: ${extractAliases.size}")
    println(s"[PRUNING] GetArrayStructFields: ${allArrayStructFields.size}")
    println(s"[PRUNING] GetStructField: ${allStructFields.size}")

    val transformedPlan = plan transformDown {
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
          l, p.projectList, Seq.empty, allArrayStructFields.toSeq, allStructFields.toSeq,
          generateMappings.toMap, extractAliases.toMap, hadoopFsRelation, requiredColumns).getOrElse(p)

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
            l, projects, allFilters, allArrayStructFields.toSeq, allStructFields.toSeq,
            generateMappings.toMap, extractAliases.toMap, hadoopFsRelation, requiredColumns)
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

    // SPARK-47230: Return the plan after schema pruning.
    // Generator ordinal rewriting is now handled by the separate GeneratorOrdinalRewriting rule.
    transformedPlan
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
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      arrayStructFields: Seq[GetArrayStructFields],
      structFields: Seq[GetStructField],
      generateMappings: Map[ExprId, Expression],
      extractAliases: Map[ExprId, Expression],
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
    println(s"[PRUNING] === Processing ${arrayStructFields.size} GetArrayStructFields ===")
    arrayStructFields.foreach { gasf =>
      println(s"[PRUNING] Tracing GetArrayStructFields: ${gasf.field.name}")
      val (rootAndPath, usedGenerate) = traceToRootColumnThroughGenerates(
        gasf, generateMappings, extractAliases, relationExprIds)
      println(s"[PRUNING]   Result: rootAndPath=$rootAndPath, usedGenerate=$usedGenerate")
      if (usedGenerate) tracedThroughGenerate = true
      rootAndPath.foreach { case (rootCol, path) =>
        println(s"[PRUNING]   Adding path to $rootCol: $path")
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
        gsf, generateMappings, extractAliases, relationExprIds)
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

    // SPARK-47230: Removed the restriction that blocked pruning when multiple fields
    // at the same depth were accessed. This restriction was too conservative and prevented
    // legitimate use cases like accessing multiple fields from exploded arrays.
    // The ordinal rewriting logic (lines 148-187) correctly handles multiple fields.
    nestedFieldAccesses.foreach { case (rootCol, paths) =>
      logInfo(s"SCHEMA_PRUNING_DEBUG: rootCol=$rootCol, allPaths=$paths")
      val groupedByDepth = paths.groupBy(_.length)
      groupedByDepth.foreach { case (depth, pathsAtDepth) =>
        logInfo(s"SCHEMA_PRUNING_DEBUG: depth=$depth, pathsAtDepth=$pathsAtDepth, " +
          s"count=${pathsAtDepth.size}")
      }
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
      val prunedRelation = buildPrunedRelation(relation, prunedHadoopRelation, StructType(Nil))

      // Apply ProjectionOverSchema to rewrite expressions with correct ordinals
      val attrNameMap = relation.output.map(att => (att.exprId, att.name)).toMap
      val normalizedProjects = normalizeAttributeRefNames(attrNameMap, projects)
        .asInstanceOf[Seq[NamedExpression]]
      val normalizedFilters = normalizeAttributeRefNames(attrNameMap, filters)

      val projectionOverSchema = ProjectionOverSchema(prunedSchema, AttributeSet(relation.output))
      val prunedPlan = buildNewProjection(projects, normalizedProjects, normalizedFilters,
        prunedRelation, projectionOverSchema)

      // SPARK-47230: Generate nodes are NOT in this subtree (they're in the parent plan).
      // The generator rewriting will happen in the main apply() method's transformUp
      // at lines 138-181. We just return the pruned plan here.
      Some(prunedPlan)
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
      extractAliases: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case GetArrayStructFields(child, field, _, _, _) =>
        val (result, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
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
      extractAliases: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case GetStructField(child, _, Some(fieldName)) =>
        val (result, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
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
      extractAliases: Map[ExprId, Expression],
      relationExprIds: Set[ExprId]): (Option[(String, Seq[String])], Boolean) = {
    expr match {
      case attr: AttributeReference =>
        println(s"[TRACE] AttributeReference: ${attr.name}#${attr.exprId}")
        // Check if this attribute is from a Generate node
        generateMappings.get(attr.exprId) match {
          case Some(gen: UnaryExpression) =>
            // This attribute comes from a unary generator (explode, posexplode, etc.)
            // Extract the child and trace through it
            println(s"[TRACE]   Found in mappings as ${gen.getClass.getSimpleName}, child: ${gen.child}")
            val (result, _) = traceArrayAccessThroughGenerates(
              gen.child, generateMappings, extractAliases, relationExprIds)
            println(s"[TRACE]   Traced result: $result")
            (result, true)
          case Some(other) =>
            // Other generator types without a single child - mark as used Generate
            println(s"[TRACE]   Found in mappings as ${other.getClass.getSimpleName} (non-unary)")
            val (result, _) = traceArrayAccessThroughGenerates(
              other, generateMappings, extractAliases, relationExprIds)
            (result, true)
          case None =>
            // Not from a Generate node - check if it's an _extract_ alias
            extractAliases.get(attr.exprId) match {
              case Some(aliasChild) =>
                // SPARK-47230: This is an _extract_ attribute created by NestedColumnAliasing
                // Trace through the alias child expression
                println(s"[TRACE]   Found _extract_ alias, child: $aliasChild")
                val (result, usedGen) = traceArrayAccessThroughGenerates(
                  aliasChild, generateMappings, extractAliases, relationExprIds)
                println(s"[TRACE]   _extract_ traced result: $result")
                (result, usedGen)
              case None =>
                // Not from a Generate node or _extract_ alias - this is a regular attribute
                val isRelation = relationExprIds.contains(attr.exprId)
                println(s"[TRACE]   NOT in mappings or extract aliases, isRelation=$isRelation")
                if (isRelation) {
                  (Some((attr.name, Seq.empty)), false)
                } else {
                  (None, false)
                }
            }
        }

      case GetStructField(child, _, nameOpt) =>
        println(s"[TRACE] GetStructField: ${nameOpt.getOrElse("unnamed")}, child=$child")
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
        val result = childResult.flatMap { case (rootCol, path) =>
          nameOpt.map(fieldName => (rootCol, path :+ fieldName))
        }
        println(s"[TRACE]   GetStructField result: $result")
        (result, usedGen)

      case GetArrayItem(child, _, _) =>
        println(s"[TRACE] GetArrayItem, child=$child")
        traceArrayAccessThroughGenerates(child, generateMappings, extractAliases, relationExprIds)

      case GetArrayStructFields(child, field, _, _, _) =>
        println(s"[TRACE] GetArrayStructFields: ${field.name}, child=$child")
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
        val result = childResult.map { case (rootCol, path) =>
          (rootCol, path :+ field.name)
        }
        println(s"[TRACE]   GetArrayStructFields result: $result")
        (result, usedGen)

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
        // For array<struct<...>>, prune normally
        // ProjectionOverSchema will rewrite GetArrayStructFields ordinals after pruning
        logInfo(s"SCHEMA_PRUNING_DEBUG: Pruning array<struct> field ${field.name}")
        logInfo(s"SCHEMA_PRUNING_DEBUG: elementType has ${elementType.fields.length} fields")
        logInfo(s"SCHEMA_PRUNING_DEBUG: About to call pruneStructByPaths")
        val prunedElementType = pruneStructByPaths(elementType, paths, arrayStructFieldPaths)
        logInfo(s"SCHEMA_PRUNING_DEBUG: pruneStructByPaths returned, " +
          s"prunedElementType has ${prunedElementType.fields.length} fields")
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
    logInfo(s"SCHEMA_PRUNING_DEBUG: pruneStructByPaths called with ${struct.fields.length} fields")
    logInfo(s"SCHEMA_PRUNING_DEBUG: Input paths: $paths")

    // Group paths by their first component
    val pathsByFirstField = paths.groupBy(_.head)
    logInfo(s"SCHEMA_PRUNING_DEBUG: pathsByFirstField keys: " +
      s"${pathsByFirstField.keys.mkString(", ")}")

    // Keep only fields that are accessed, preserving original field order
    // to maintain field ordinals for GetArrayStructFields expressions
    val prunedFields = struct.fields.flatMap { field =>
      val result = pathsByFirstField.get(field.name)
      if (result.isDefined) {
        logInfo(s"SCHEMA_PRUNING_DEBUG: Field '${field.name}' KEPT (found in paths)")
      } else {
        logInfo(s"SCHEMA_PRUNING_DEBUG: Field '${field.name}' FILTERED OUT (not in paths)")
      }

      result.map { fieldPaths =>
        // Get remaining path components after removing the first
        val remainingPaths = fieldPaths.map(_.tail).filter(_.nonEmpty)

        // Get array paths for this field (removing first component)
        val fieldArrayPaths = arrayStructFieldPaths
          .filter(_.headOption.contains(field.name))
          .map(_.tail)

        if (remainingPaths.isEmpty) {
          // This field itself is accessed, keep it entirely
          logInfo(s"SCHEMA_PRUNING_DEBUG: Field '${field.name}' - keeping entire field")
          field
        } else {
          // This field has nested accesses, recurse
          logInfo(s"SCHEMA_PRUNING_DEBUG: Field '${field.name}' - " +
            s"recursing with paths: $remainingPaths")
          pruneFieldByPaths(field, remainingPaths, fieldArrayPaths)
        }
      }
    }

    logInfo(s"SCHEMA_PRUNING_DEBUG: pruneStructByPaths returning ${prunedFields.length} fields " +
      s"(started with ${struct.fields.length})")
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

/**
 * SPARK-47230: Rewrites ordinals in GetArrayStructFields and GetStructField expressions
 * within Generator nodes after schema pruning has been applied.
 *
 * This rule runs AFTER SchemaPruning and operates on the assumption that:
 * 1. Schema pruning has already been completed
 * 2. AttributeReferences have been updated to reflect pruned schemas
 * 3. We only need to fix ordinals to match the current (pruned) schemas
 *
 * This separate rule approach solves the problem of nested LATERAL VIEWs where
 * intermediate nodes (like Project) need to be reconstructed with updated schemas
 * before the next Generate node can correctly rewrite its ordinals.
 */
object GeneratorOrdinalRewriting extends Rule[LogicalPlan] {

  /**
   * Recursively resolve the actual dataType of an expression using the schemaMap.
   * This is critical for nested arrays where we need to traverse through GetStructField
   * expressions to get the pruned schema.
   */
  private def resolveActualDataType(
      expr: Expression,
      schemaMap: scala.collection.Map[ExprId, DataType]): DataType = {
    expr match {
      case attr: AttributeReference if schemaMap.contains(attr.exprId) =>
        schemaMap(attr.exprId)

      case GetStructField(child, ordinal, nameOpt) =>
        // Recursively resolve the child's type, then extract the field
        resolveActualDataType(child, schemaMap) match {
          case st: StructType =>
            nameOpt match {
              case Some(fieldName) if st.fieldNames.contains(fieldName) =>
                st(fieldName).dataType
              case _ if ordinal < st.fields.length =>
                st.fields(ordinal).dataType
              case _ =>
                // Fallback to expression's dataType if resolution fails
                expr.dataType
            }
          case ArrayType(st: StructType, _) =>
            // Child is an array of structs, extract field from element type
            nameOpt match {
              case Some(fieldName) if st.fieldNames.contains(fieldName) =>
                ArrayType(StructType(Seq(st(fieldName))), true)
              case _ if ordinal < st.fields.length =>
                ArrayType(StructType(Seq(st.fields(ordinal))), true)
              case _ =>
                expr.dataType
            }
          case _ =>
            expr.dataType
        }

      case _ =>
        expr.dataType
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(s"[SPARK-47230 DEBUG] === GeneratorOrdinalRewriting.apply called ===")

    // Check if there are any Generate nodes that need ordinal rewriting
    var hasGenerateNodes = false
    plan.foreach {
      case _: Generate => hasGenerateNodes = true
      case _ =>
    }

    if (!hasGenerateNodes) {
      println(s"[SPARK-47230 DEBUG] No Generate nodes found, skipping")
      return plan
    }

    println(s"[SPARK-47230 DEBUG] Found Generate nodes, proceeding with ordinal rewriting")

    // First pass: collect ALL updated schemas from the plan (from all attributes)
    // This includes schemas that have been pruned by SchemaPruning
    println(s"[SPARK-47230 DEBUG] Collecting schemas from all attributes...")
    val schemaMap = scala.collection.mutable.Map[ExprId, DataType]()
    plan.foreach { node =>
      node.output.foreach { attr =>
        schemaMap(attr.exprId) = attr.dataType
        println(s"[SPARK-47230 DEBUG] Collected schema for ${attr.name}#${attr.exprId}: " +
          s"${attr.dataType.simpleString.take(100)}")
      }
    }
    println(s"[SPARK-47230 DEBUG] Collected ${schemaMap.size} schemas")

    // Transform the plan to rewrite ordinals in all Generate nodes
    // Use transformUp so we process child Generates first, and parent nodes
    // are reconstructed with updated child schemas
    println(s"[SPARK-47230 DEBUG] Starting transformUp to rewrite ordinals in Generate nodes...")
    val result = plan transformUp {
      case g @ Generate(generator, unrequiredChildIndex, outer, qualifier,
          generatorOutput, child) =>

        println(s"[SPARK-47230 DEBUG] === Processing Generate node ===")
        println(s"[SPARK-47230 DEBUG] Generator type: ${generator.getClass.getSimpleName}")
        println(s"[SPARK-47230 DEBUG] Child output:")
        child.output.foreach { a =>
          println(s"[SPARK-47230 DEBUG]   ${a.name}#${a.exprId}: ${a.dataType.simpleString.take(100)}")
        }

        // Rewrite GetArrayStructFields and GetStructField expressions in the generator
        // to use ordinals that match the CURRENT (pruned) schema from schemaMap
        println(s"[SPARK-47230 DEBUG] Rewriting ordinals in generator expressions...")

        // First, check if the generator child is an AttributeReference that needs dataType update
        val generatorWithFixedChild = generator.transformUp {
          case attr: AttributeReference if schemaMap.contains(attr.exprId) =>
            val actualType = schemaMap(attr.exprId)
            if (actualType != attr.dataType) {
              println(s"[SPARK-47230 DEBUG] FIXING generator child attribute ${attr.name}#${attr.exprId}:")
              println(s"[SPARK-47230 DEBUG]   FROM: ${attr.dataType.simpleString.take(100)}")
              println(s"[SPARK-47230 DEBUG]   TO:   ${actualType.simpleString.take(100)}")
              attr.withDataType(actualType)
            } else {
              attr
            }
        }.asInstanceOf[Generator]

        // Then rewrite ordinals in nested expressions
        val rewrittenGenerator = generatorWithFixedChild.transformUp {
          case gasf @ GetArrayStructFields(childExpr, field, ordinal, numFields, containsNull) =>
            // Recursively resolve actual dataType from schemaMap (handles nested expressions)
            println(s"[SPARK-47230 DEBUG] Found GetArrayStructFields: field=${field.name}, ordinal=$ordinal, numFields=$numFields")
            val actualDataType = resolveActualDataType(childExpr, schemaMap)
            println(s"[SPARK-47230 DEBUG] Resolved actual dataType: ${actualDataType.simpleString.take(100)}")

            actualDataType match {
              case ArrayType(st: StructType, _) =>
                val fieldName = field.name
                println(s"[SPARK-47230 DEBUG] Array element is StructType with fields: ${st.fieldNames.mkString(", ")}")
                if (st.fieldNames.contains(fieldName)) {
                  val newOrdinal = st.fieldIndex(fieldName)
                  val newNumFields = st.fields.length
                  if (newOrdinal != ordinal || newNumFields != numFields) {
                    println(s"[SPARK-47230 DEBUG] REWRITING GetArrayStructFields($fieldName): " +
                      s"ordinal $ordinal → $newOrdinal, numFields $numFields → $newNumFields")
                    GetArrayStructFields(childExpr, st.fields(newOrdinal), newOrdinal,
                      newNumFields, containsNull)
                  } else {
                    println(s"[SPARK-47230 DEBUG] No rewrite needed for GetArrayStructFields($fieldName)")
                    gasf
                  }
                } else {
                  println(s"[SPARK-47230 DEBUG] Field $fieldName NOT FOUND in pruned schema, keeping as-is")
                  gasf
                }
              case _ =>
                println(s"[SPARK-47230 DEBUG] actualDataType is not ArrayType(StructType), skipping")
                gasf
            }

          case gsf @ GetStructField(childExpr, ordinal, nameOpt) =>
            // Recursively resolve actual dataType from schemaMap (handles nested expressions)
            println(s"[SPARK-47230 DEBUG] Found GetStructField: ordinal=$ordinal, nameOpt=$nameOpt")
            val actualDataType = resolveActualDataType(childExpr, schemaMap)
            println(s"[SPARK-47230 DEBUG] Resolved actual dataType: ${actualDataType.simpleString.take(100)}")

            actualDataType match {
              case st: StructType =>
                println(s"[SPARK-47230 DEBUG] StructType with fields: ${st.fieldNames.mkString(", ")}")
                val fieldNameOpt = nameOpt.orElse {
                  if (ordinal < st.fields.length) Some(st.fields(ordinal).name) else None
                }

                fieldNameOpt match {
                  case Some(fieldName) if st.fieldNames.contains(fieldName) =>
                    val newOrdinal = st.fieldIndex(fieldName)
                    if (newOrdinal != ordinal) {
                      println(s"[SPARK-47230 DEBUG] REWRITING GetStructField($fieldName): " +
                        s"ordinal $ordinal → $newOrdinal")
                      GetStructField(childExpr, newOrdinal, Some(fieldName))
                    } else {
                      println(s"[SPARK-47230 DEBUG] No rewrite needed for GetStructField($fieldName)")
                      gsf
                    }
                  case _ =>
                    println(s"[SPARK-47230 DEBUG] GetStructField field not found, keeping as-is")
                    gsf
                }
              case _ =>
                println(s"[SPARK-47230 DEBUG] actualDataType is not StructType, skipping")
                gsf
            }
        }.asInstanceOf[Generator]

        println(s"[SPARK-47230 DEBUG] Generator rewriting complete")

        // Re-derive generator output attributes from the rewritten generator's element schema
        // CRITICAL: Use toAttributes() to get fresh attributes, then preserve ExprIds
        // This is the pattern used in GeneratorNestedColumnAliasing (lines 433-443)
        println(s"[SPARK-47230 DEBUG] Re-deriving generator output attributes...")
        val newGeneratorOutput = generatorOutput
          .zip(toAttributes(rewrittenGenerator.elementSchema))
          .map { case (oldAttr, newAttr) =>
            val updated = newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
            if (updated.dataType != oldAttr.dataType) {
              println(s"[SPARK-47230 DEBUG] Generator output ${oldAttr.name}#${oldAttr.exprId} updated:")
              println(s"[SPARK-47230 DEBUG]   FROM: ${oldAttr.dataType.simpleString.take(100)}")
              println(s"[SPARK-47230 DEBUG]   TO:   ${updated.dataType.simpleString.take(100)}")
            }
            updated
          }

        // Update schemaMap immediately with new generator outputs
        // This is critical for nested Generates to see each other's updated schemas
        println(s"[SPARK-47230 DEBUG] Updating schemaMap with new generator outputs...")
        newGeneratorOutput.foreach { attr =>
          schemaMap(attr.exprId) = attr.dataType
          println(s"[SPARK-47230 DEBUG] Updated schemaMap for ${attr.name}#${attr.exprId}")
        }

        Generate(rewrittenGenerator, unrequiredChildIndex, outer, qualifier,
          newGeneratorOutput, child)
    }

    // Second pass: Comprehensive expression rewriting throughout the ENTIRE plan
    // This fixes ordinals in downstream nodes (Project, Filter, etc.)
    // that reference generator outputs
    println(s"[SPARK-47230 DEBUG] === Starting comprehensive expression rewriting pass ===")

    val finalResult = result transformUp {
      case node =>
        // Rewrite ALL expressions in this node using schemaMap
        node.transformExpressionsUp {
          case gasf @ GetArrayStructFields(childExpr, field, ordinal, numFields, containsNull) =>
            val actualDataType = resolveActualDataType(childExpr, schemaMap)

            actualDataType match {
              case ArrayType(st: StructType, _) =>
                val fieldName = field.name
                if (st.fieldNames.contains(fieldName)) {
                  val newOrdinal = st.fieldIndex(fieldName)
                  val newNumFields = st.fields.length
                  if (newOrdinal != ordinal || newNumFields != numFields) {
                    println(s"[SPARK-47230 DEBUG] [Comprehensive] REWRITING GetArrayStructFields($fieldName): " +
                      s"ordinal $ordinal → $newOrdinal, numFields $numFields → $newNumFields")
                    GetArrayStructFields(childExpr, st.fields(newOrdinal), newOrdinal,
                      newNumFields, containsNull)
                  } else {
                    gasf
                  }
                } else {
                  gasf
                }
              case _ => gasf
            }

          case gsf @ GetStructField(childExpr, ordinal, nameOpt) =>
            val actualDataType = resolveActualDataType(childExpr, schemaMap)

            actualDataType match {
              case st: StructType =>
                val fieldNameOpt = nameOpt.orElse {
                  if (ordinal < st.fields.length) Some(st.fields(ordinal).name) else None
                }

                fieldNameOpt match {
                  case Some(fieldName) if st.fieldNames.contains(fieldName) =>
                    val newOrdinal = st.fieldIndex(fieldName)
                    if (newOrdinal != ordinal) {
                      println(s"[SPARK-47230 DEBUG] [Comprehensive] REWRITING GetStructField($fieldName): " +
                        s"ordinal $ordinal → $newOrdinal")
                      GetStructField(childExpr, newOrdinal, Some(fieldName))
                    } else {
                      gsf
                    }
                  case _ => gsf
                }
              case _ => gsf
            }
        }
    }

    println(s"[SPARK-47230 DEBUG] === Comprehensive rewriting pass completed ===")

    // Third pass: Update _extract_ attribute dataTypes in schemaMap to match their rewritten expression dataTypes
    // This fixes the issue where NestedColumnAliasing creates Alias(_extract_) nodes,
    // ProjectionOverSchema rewrites the Alias child, but schemaMap still has the old dataType
    // IMPORTANT: Only update for GetArrayStructFields expressions, not simple field accesses
    println(s"[SPARK-47230 DEBUG] === Starting attribute dataType correction pass ===")

    finalResult.foreach {
      case p @ Project(projectList, _) =>
        println(s"[SPARK-47230 DEBUG] Checking Project node for _extract_ attributes...")

        // Update schemaMap for any _extract_ attributes whose Alias child dataType differs
        p.output.zip(projectList).foreach { case (attr, expr) =>
          expr match {
            case alias @ Alias(GetArrayStructFields(_, _, _, _, _), _)
                if attr.name.startsWith("_extract_") =>
              // Only update for Alias wrapping GetArrayStructFields
              // This ensures we're updating the actual _extract_ attribute, not downstream projections
              val currentType = schemaMap.getOrElse(attr.exprId, attr.dataType)
              if (alias.child.dataType != currentType) {
                println(s"[SPARK-47230 DEBUG] UPDATING schemaMap for ${attr.name}#${attr.exprId}:")
                println(s"[SPARK-47230 DEBUG]   FROM: ${currentType.simpleString.take(100)}")
                println(s"[SPARK-47230 DEBUG]   TO:   ${alias.child.dataType.simpleString.take(100)}")

                // Update schemaMap so downstream references use the correct schema
                schemaMap(attr.exprId) = alias.child.dataType
              }
            case _ =>
          }
        }

      case _ =>
    }

    println(s"[SPARK-47230 DEBUG] === Attribute dataType correction pass completed ===")

    // Fourth pass: Comprehensive AttributeReference update throughout the ENTIRE plan
    // This is CRITICAL to fix the Size function error and other issues where
    // AttributeReferences still have unpruned dataTypes after schema pruning
    println(s"[SPARK-47230 DEBUG] === Starting comprehensive AttributeReference update pass ===")

    val planWithUpdatedAttributes = finalResult.transformAllExpressions {
      case attr: AttributeReference if schemaMap.contains(attr.exprId) =>
        val actualType = schemaMap(attr.exprId)
        if (actualType != attr.dataType) {
          println(s"[SPARK-47230 DEBUG] [Comprehensive Update] Updating AttributeReference ${attr.name}#${attr.exprId}:")
          println(s"[SPARK-47230 DEBUG]   FROM: ${attr.dataType.simpleString.take(100)}")
          println(s"[SPARK-47230 DEBUG]   TO:   ${actualType.simpleString.take(100)}")
          attr.withDataType(actualType)
        } else {
          attr
        }
    }

    println(s"[SPARK-47230 DEBUG] === Comprehensive AttributeReference update completed ===")
    planWithUpdatedAttributes
  }
}
