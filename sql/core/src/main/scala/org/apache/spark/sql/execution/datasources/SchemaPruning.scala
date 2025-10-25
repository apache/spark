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
    // SPARK-47230: Collect GetArrayStructFields, GetStructField, Generate mappings,
    // and _extract_ alias mappings
    val allArrayStructFields = scala.collection.mutable.ArrayBuffer[GetArrayStructFields]()
    val allStructFields = scala.collection.mutable.ArrayBuffer[GetStructField]()
    val generateMappings = scala.collection.mutable.Map[ExprId, Expression]()
    val extractAliases = scala.collection.mutable.Map[ExprId, Expression]()
    // Track columns that need full struct preservation (when entire struct is selected)
    val columnsNeedingFullPreservation = scala.collection.mutable.Set[String]()

    plan.foreach { node =>
      // Collect Generate nodes
      node match {
        case g: Generate =>
          g.generatorOutput.foreach { attr =>
            generateMappings(attr.exprId) = g.generator
          }

        case p: Project =>
          // Collect _extract_ aliases created by NestedColumnAliasing
          p.output.zip(p.projectList).foreach { case (attr, namedExpr) =>
            namedExpr match {
              case Alias(child, name) if name.startsWith("_extract_") =>
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
          case gsf: GetStructField =>
            allStructFields += gsf
          case _ =>
        }
      }
    }


    val transformedPlan = plan transformDown {
      // SPARK-47230: Handle Project over Generate pattern to detect full struct selection
      case p @ Project(projectList, g @ Generate(generator, _, _, _, generatorOutput, _)) =>
        // scalastyle:off println
        println("DEBUG: Found Project over Generate pattern")
        println(s"DEBUG: Project list size: ${projectList.size}")
        println(s"DEBUG: Generator output size: ${generatorOutput.size}")
        println(s"DEBUG: Generator: ${generator.getClass.getSimpleName}")
        // scalastyle:on println

        // Check if any generator output attributes are selected directly (not via GetStructField)
        val generatorOutputIds = generatorOutput.map(_.exprId).toSet
        // scalastyle:off println
        println(s"DEBUG: Generator output IDs: ${generatorOutputIds.mkString(", ")}")
        // scalastyle:on println

        projectList.foreach {
          case attr: AttributeReference if generatorOutputIds.contains(attr.exprId) =>
            // SPARK-47230: Check if this struct is used in another generator's child
            // (chained generator pattern) - if so, don't mark for full preservation
            val usedInChildGenerator = plan.exists {
              case Generate(childGen, _, _, _, _, _) if childGen != generator =>
                childGen.children.exists { child =>
                  child.find {
                    case a: AttributeReference => a.exprId == attr.exprId
                    case _ => false
                  }.isDefined
                }
              case _ => false
            }

            // This is a direct selection of generator output (e.g., SELECT friend)
            // IMPORTANT: Only trigger full struct preservation if this is a STRUCT attribute
            // For PosExplode, the first output is position (IntegerType), second is the struct
            // We should only preserve full struct for the struct output, not the position
            attr.dataType match {
              case _: StructType if !usedInChildGenerator =>
                // Trace back to find the root column
                generator match {
                  case e: Explode =>
                    traceArrayAccess(e.child).foreach { case (rootCol, _) =>
                      columnsNeedingFullPreservation += rootCol
                    }
                  case pe: PosExplode =>
                    traceArrayAccess(pe.child).foreach { case (rootCol, _) =>
                      columnsNeedingFullPreservation += rootCol
                    }
                  case _ =>
                }
              case _ =>
            }

          case Alias(attr: AttributeReference, _) if generatorOutputIds.contains(attr.exprId) =>
            // SPARK-47230: Check if this struct is used in another generator's child
            // (chained generator pattern) - if so, don't mark for full preservation
            val usedInChildGenerator = plan.exists {
              case Generate(childGen, _, _, _, _, _) if childGen != generator =>
                childGen.children.exists { child =>
                  child.find {
                    case a: AttributeReference => a.exprId == attr.exprId
                    case _ => false
                  }.isDefined
                }
              case _ => false
            }

            // This is a direct selection of generator output wrapped in Alias
            // (e.g., SELECT friend AS friend in LATERAL EXPLODE syntax)
            // IMPORTANT: Only trigger full struct preservation if this is a STRUCT attribute
            // For PosExplode, the first output is position (IntegerType), second is the struct
            // We should only preserve full struct for the struct output, not the position
            attr.dataType match {
              case _: StructType if !usedInChildGenerator =>
                // Trace back to find the root column
                generator match {
                  case e: Explode =>
                    traceArrayAccess(e.child).foreach { case (rootCol, _) =>
                      columnsNeedingFullPreservation += rootCol
                    }
                  case pe: PosExplode =>
                    traceArrayAccess(pe.child).foreach { case (rootCol, _) =>
                      columnsNeedingFullPreservation += rootCol
                    }
                  case _ =>
                }
              case _ =>
            }

          case other =>
            // scalastyle:off println
            println(s"DEBUG: Project item not a direct AttributeReference: " +
              s"${other.getClass.getSimpleName}")
            // scalastyle:on println
        }

        // scalastyle:off println
        println(s"DEBUG: columnsNeedingFullPreservation: " +
          s"${columnsNeedingFullPreservation.mkString(", ")}")
        // scalastyle:on println

        // Continue with normal processing
        p

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
          generateMappings.toMap, extractAliases.toMap, hadoopFsRelation, requiredColumns,
          columnsNeedingFullPreservation.toSet
        ).getOrElse(p)

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
            generateMappings.toMap, extractAliases.toMap, hadoopFsRelation, requiredColumns,
            columnsNeedingFullPreservation.toSet)
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
   * @param columnsNeedingFullPreservation Columns where the entire struct is selected directly
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
      requiredColumns: Set[String],
      columnsNeedingFullPreservation: Set[String]): Option[LogicalPlan] = {

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
        gasf, generateMappings, extractAliases, relationExprIds)
      if (usedGenerate) tracedThroughGenerate = true
      rootAndPath.foreach { case (rootCol, path) =>
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

        if (!isPrefix) {
          // Only add if it's not a prefix of a GetArrayStructFields path
          val existingPaths = nestedFieldAccesses.getOrElse(rootCol, Set.empty)
          nestedFieldAccesses(rootCol) = existingPaths + path
        }
      }
    }

    // SPARK-47230: Add columns needing full preservation (where entire struct was selected)
    // to the nestedFieldAccesses map with an empty path to signal full preservation
    columnsNeedingFullPreservation.foreach { rootCol =>
      val existingPaths = nestedFieldAccesses.getOrElse(rootCol, Set.empty)
      nestedFieldAccesses(rootCol) = existingPaths + Seq.empty[String]
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

    // Filter to only keep root columns that exist in the relation
    val filteredAccesses = nestedFieldAccesses.filter { case (rootCol, _) =>
      relationColumnNames.contains(rootCol)
    }

    if (filteredAccesses.isEmpty) {
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
        // Check if this attribute is from a Generate node
        generateMappings.get(attr.exprId) match {
          case Some(gen: UnaryExpression) =>
            // This attribute comes from a unary generator (explode, posexplode, etc.)
            // Extract the child and trace through it
            val (result, _) = traceArrayAccessThroughGenerates(
              gen.child, generateMappings, extractAliases, relationExprIds)
            (result, true)
          case Some(other) =>
            // Other generator types without a single child - mark as used Generate
            val (result, _) = traceArrayAccessThroughGenerates(
              other, generateMappings, extractAliases, relationExprIds)
            (result, true)
          case None =>
            // Not from a Generate node - check if it's an _extract_ alias
            extractAliases.get(attr.exprId) match {
              case Some(aliasChild) =>
                // SPARK-47230: This is an _extract_ attribute created by NestedColumnAliasing
                // Trace through the alias child expression
                val (result, usedGen) = traceArrayAccessThroughGenerates(
                  aliasChild, generateMappings, extractAliases, relationExprIds)
                (result, usedGen)
              case None =>
                // Not from a Generate node or _extract_ alias - this is a regular attribute
                val isRelation = relationExprIds.contains(attr.exprId)
                if (isRelation) {
                  (Some((attr.name, Seq.empty)), false)
                } else {
                  (None, false)
                }
            }
        }

      case GetStructField(child, _, nameOpt) =>
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
        val result = childResult.flatMap { case (rootCol, path) =>
          nameOpt.map(fieldName => (rootCol, path :+ fieldName))
        }
        (result, usedGen)

      case GetArrayItem(child, _, _) =>
        traceArrayAccessThroughGenerates(child, generateMappings, extractAliases, relationExprIds)

      case GetArrayStructFields(child, field, _, _, _) =>
        val (childResult, usedGen) = traceArrayAccessThroughGenerates(
          child, generateMappings, extractAliases, relationExprIds)
        val result = childResult.map { case (rootCol, path) =>
          (rootCol, path :+ field.name)
        }
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

    val prunedFields = schema.fields.flatMap { field =>
      nestedFieldAccesses.get(field.name) match {
        case Some(paths) if paths.nonEmpty =>
          // This root field has specific nested paths accessed
          // Get the GetArrayStructFields paths for this field to pass down
          val arrayPaths = arrayStructFieldPaths.getOrElse(field.name, Set.empty)
          val prunedField = pruneFieldByPaths(field, paths.toSeq, arrayPaths)
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
    // If any path is empty, we need the entire field
    if (paths.exists(_.isEmpty)) {
      return field
    }

    field.dataType match {
      case ArrayType(elementType: StructType, containsNull) =>
        // For array<struct<...>>, prune normally
        // ProjectionOverSchema will rewrite GetArrayStructFields ordinals after pruning
        val prunedElementType = pruneStructByPaths(elementType, paths, arrayStructFieldPaths)
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
   *
   * SPARK-47230: Uses case-insensitive field matching when configured.
   */
  private def pruneStructByPaths(struct: StructType, paths: Seq[Seq[String]],
      arrayStructFieldPaths: Set[Seq[String]] = Set.empty): StructType = {
    // Group paths by their first component using case-insensitive matching
    val resolver = conf.resolver

    // Keep only fields that are accessed, preserving original field order
    // to maintain field ordinals for GetArrayStructFields expressions
    val prunedFields = struct.fields.flatMap { field =>
      // Find all paths that start with this field (case-insensitive)
      val matchingPaths = paths.filter(path =>
        path.nonEmpty && resolver(field.name, path.head))

      if (matchingPaths.nonEmpty) {
        // Get remaining path components after removing the first
        val remainingPaths = matchingPaths.map(_.tail).filter(_.nonEmpty)

        // Get array paths for this field (removing first component)
        // Use case-insensitive matching here too
        val fieldArrayPaths = arrayStructFieldPaths
          .filter(p => p.nonEmpty && resolver(field.name, p.head))
          .map(_.tail)

        if (remainingPaths.isEmpty) {
          // This field itself is accessed, keep it entirely
          Some(field)
        } else {
          // This field has nested accesses, recurse
          Some(pruneFieldByPaths(field, remainingPaths, fieldArrayPaths))
        }
      } else {
        // This field is not accessed
        None
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
   * Find field ordinal using case-insensitive or case-sensitive lookup
   * depending on the session configuration.
   */
  private def findFieldOrdinal(struct: StructType, fieldName: String): Option[Int] = {
    val resolver = conf.resolver
    struct.fields.indexWhere(f => resolver(f.name, fieldName)) match {
      case -1 => None
      case ordinal => Some(ordinal)
    }
  }

  /**
   * Recursively resolve the actual dataType of an expression using the schemaMap.
   * This is critical for nested arrays where we need to traverse through GetStructField
   * expressions to get the pruned schema.
   * Uses case-insensitive field resolution when configured.
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
              case Some(fieldName) =>
                findFieldOrdinal(st, fieldName) match {
                  case Some(idx) => st.fields(idx).dataType
                  case None if ordinal < st.fields.length => st.fields(ordinal).dataType
                  case _ => expr.dataType
                }
              case _ if ordinal < st.fields.length =>
                st.fields(ordinal).dataType
              case _ =>
                // Fallback to expression's dataType if resolution fails
                expr.dataType
            }
          case ArrayType(st: StructType, _) =>
            // Child is an array of structs, extract field from element type
            nameOpt match {
              case Some(fieldName) =>
                findFieldOrdinal(st, fieldName) match {
                  case Some(idx) => ArrayType(StructType(Seq(st.fields(idx))), true)
                  case None if ordinal < st.fields.length =>
                    ArrayType(StructType(Seq(st.fields(ordinal))), true)
                  case _ => expr.dataType
                }
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

    // Check if there are any Generate nodes that need ordinal rewriting
    var hasGenerateNodes = false
    plan.foreach {
      case _: Generate => hasGenerateNodes = true
      case _ =>
    }

    if (!hasGenerateNodes) {
      return plan
    }


    // First pass: collect ALL updated schemas from the plan (from all attributes)
    // This includes schemas that have been pruned by SchemaPruning
    val schemaMap = scala.collection.mutable.Map[ExprId, DataType]()
    plan.foreach { node =>
      node.output.foreach { attr =>
        schemaMap(attr.exprId) = attr.dataType
      }
    }

    // Transform the plan to rewrite ordinals in all Generate nodes
    // Use transformUp so we process child Generates first, and parent nodes
    // are reconstructed with updated child schemas
    val result = plan transformUp {
      case g @ Generate(generator, unrequiredChildIndex, outer, qualifier,
          generatorOutput, child) =>

        // Rewrite GetArrayStructFields and GetStructField expressions in the generator
        // to use ordinals that match the CURRENT (pruned) schema from schemaMap

        // First, check if the generator child is an AttributeReference that needs dataType update
        val generatorWithFixedChild = generator.transformUp {
          case attr: AttributeReference if schemaMap.contains(attr.exprId) =>
            val actualType = schemaMap(attr.exprId)
            if (actualType != attr.dataType) {
              attr.withDataType(actualType)
            } else {
              attr
            }
        }.asInstanceOf[Generator]

        // Then rewrite ordinals in nested expressions
        val rewrittenGenerator = generatorWithFixedChild.transformUp {
          case gasf @ GetArrayStructFields(childExpr, field, ordinal, numFields, containsNull) =>
            // Recursively resolve actual dataType from schemaMap (handles nested expressions)
            val actualDataType = resolveActualDataType(childExpr, schemaMap)

            actualDataType match {
              case ArrayType(st: StructType, _) =>
                val fieldName = field.name
                // Use case-insensitive field lookup
                findFieldOrdinal(st, fieldName) match {
                  case Some(newOrdinal) =>
                    val newNumFields = st.fields.length
                    if (newOrdinal != ordinal || newNumFields != numFields) {
                      GetArrayStructFields(childExpr, st.fields(newOrdinal), newOrdinal,
                        newNumFields, containsNull)
                    } else {
                      gasf
                    }
                  case None =>
                    gasf
                }
              case _ =>
                gasf
            }

          case gsf @ GetStructField(childExpr, ordinal, nameOpt) =>
            // Recursively resolve actual dataType from schemaMap (handles nested expressions)
            val actualDataType = resolveActualDataType(childExpr, schemaMap)

            actualDataType match {
              case st: StructType =>
                val fieldNameOpt = nameOpt.orElse {
                  if (ordinal < st.fields.length) Some(st.fields(ordinal).name) else None
                }

                fieldNameOpt match {
                  case Some(fieldName) =>
                    // Use case-insensitive field lookup
                    findFieldOrdinal(st, fieldName) match {
                      case Some(newOrdinal) =>
                        if (newOrdinal != ordinal) {
                          // Use the actual field name from the schema (case-preserved)
                          GetStructField(childExpr, newOrdinal, Some(st.fields(newOrdinal).name))
                        } else {
                          gsf
                        }
                      case None =>
                        gsf
                    }
                  case _ =>
                    gsf
                }
              case _ =>
                gsf
            }
        }.asInstanceOf[Generator]


        // Re-derive generator output attributes from the rewritten generator's element schema
        // CRITICAL: Use toAttributes() to get fresh attributes, then preserve ExprIds
        // This is the pattern used in GeneratorNestedColumnAliasing (lines 433-443)
        val newGeneratorOutput = generatorOutput
          .zip(toAttributes(rewrittenGenerator.elementSchema))
          .map { case (oldAttr, newAttr) =>
            val updated = newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
            if (updated.dataType != oldAttr.dataType) {
            }
            updated
          }

        // Update schemaMap immediately with new generator outputs
        // This is critical for nested Generates to see each other's updated schemas
        newGeneratorOutput.foreach { attr =>
          schemaMap(attr.exprId) = attr.dataType
        }

        Generate(rewrittenGenerator, unrequiredChildIndex, outer, qualifier,
          newGeneratorOutput, child)
    }

    // Second pass: Comprehensive expression rewriting throughout the ENTIRE plan
    // This fixes ordinals in downstream nodes (Project, Filter, etc.)
    // that reference generator outputs

    val finalResult = result transformUp {
      case node =>
        // Rewrite ALL expressions in this node using schemaMap
        node.transformExpressionsUp {
          case gasf @ GetArrayStructFields(childExpr, field, ordinal, numFields, containsNull) =>
            val actualDataType = resolveActualDataType(childExpr, schemaMap)

            actualDataType match {
              case ArrayType(st: StructType, _) =>
                val fieldName = field.name
                // Use case-insensitive field lookup
                findFieldOrdinal(st, fieldName) match {
                  case Some(newOrdinal) =>
                    val newNumFields = st.fields.length
                    if (newOrdinal != ordinal || newNumFields != numFields) {
                      GetArrayStructFields(childExpr, st.fields(newOrdinal), newOrdinal,
                        newNumFields, containsNull)
                    } else {
                      gasf
                    }
                  case None =>
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
                  case Some(fieldName) =>
                    // Use case-insensitive field lookup
                    findFieldOrdinal(st, fieldName) match {
                      case Some(newOrdinal) =>
                        if (newOrdinal != ordinal) {
                          // Use the actual field name from the schema (case-preserved)
                          GetStructField(childExpr, newOrdinal, Some(st.fields(newOrdinal).name))
                        } else {
                          gsf
                        }
                      case None =>
                        gsf
                    }
                  case _ => gsf
                }
              case _ => gsf
            }
        }
    }


    // Third pass: Update _extract_ attribute dataTypes in schemaMap to match rewritten dataTypes
    // This fixes the issue where NestedColumnAliasing creates Alias(_extract_) nodes,
    // ProjectionOverSchema rewrites Alias child, but schemaMap still has the old dataType
    // IMPORTANT: Only update for GetArrayStructFields expressions, not simple field accesses

    finalResult.foreach {
      case p @ Project(projectList, _) =>

        // Update schemaMap for any _extract_ attributes whose Alias child dataType differs
        p.output.zip(projectList).foreach { case (attr, expr) =>
          expr match {
            case alias @ Alias(GetArrayStructFields(_, _, _, _, _), _)
                if attr.name.startsWith("_extract_") =>
              // Only update for Alias wrapping GetArrayStructFields
              // Ensures we update the actual _extract_ attribute, not downstream projections
              val currentType = schemaMap.getOrElse(attr.exprId, attr.dataType)
              if (alias.child.dataType != currentType) {

                // Update schemaMap so downstream references use the correct schema
                schemaMap(attr.exprId) = alias.child.dataType
              }
            case _ =>
          }
        }

      case _ =>
    }


    // Fourth pass: Comprehensive AttributeReference update throughout the ENTIRE plan
    // This is CRITICAL to fix the Size function error and other issues where
    // AttributeReferences still have unpruned dataTypes after schema pruning

    val planWithUpdatedAttributes = finalResult.transformAllExpressions {
      case attr: AttributeReference if schemaMap.contains(attr.exprId) =>
        val actualType = schemaMap(attr.exprId)
        if (actualType != attr.dataType) {
          attr.withDataType(actualType)
        } else {
          attr
        }
    }

    planWithUpdatedAttributes
  }
}
