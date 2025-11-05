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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * SPARK-47230: Analyzes which columns are used as inputs to generators and how their
 * nested fields are accessed downstream.
 *
 * This analyzer works in Phase 16 (before NestedColumnAliasing creates _extract_ aliases)
 * by directly identifying generator input columns and tracing field usage through generator
 * outputs to downstream operations.
 *
 * Direction 4: Direct Generator Input Analysis approach - avoids timing issues by not
 * relying on aliases that don't exist yet.
 */
object GeneratorInputAnalyzer {

  /**
   * Information about a column that feeds into a generator
   */
  case class GeneratorInputInfo(
      columnName: String,
      inputAttr: AttributeReference,
      dataType: DataType,
      generator: Expression,
      outputAttributes: Seq[Attribute],
      nestedFieldsAccessed: Set[Seq[String]],
      pathPrefix: Seq[String]) // Paths like Seq("servedItems", "clicked")

  /**
   * Collect all columns that are inputs to generators in the plan
   */
  def collectGeneratorInputs(plan: LogicalPlan): Map[String, GeneratorInputInfo] = {
    val generatorNodes = plan.collect { case g: Generate => g }

    if (generatorNodes.isEmpty) {
      return Map.empty
    }

    // Collect alias mappings (_extract_ aliases that may appear in generator inputs)
    val aliasMap = collectAliasMapping(plan)

    // First pass: identify generator input columns and their outputs
    val inputToGenerator = mutable.Map[String, (Generate, AttributeReference, Seq[String])]()

    generatorNodes.foreach { gen =>
      extractGeneratorInputColumn(gen, aliasMap).foreach { case (inputAttr, pathPrefix) =>
        inputToGenerator(inputAttr.name) = (gen, inputAttr, pathPrefix)
      }
    }

    // Second pass: analyze downstream access patterns for each generator output
    val results = inputToGenerator.map { case (_, (gen, inputAttr, pathPrefix)) =>
      val downstreamAccess = analyzeDownstreamFieldAccess(plan, gen.generatorOutput, aliasMap)

      inputAttr.name -> GeneratorInputInfo(
        columnName = inputAttr.name,
        inputAttr = inputAttr,
        dataType = inputAttr.dataType,
        generator = gen.generator,
        outputAttributes = gen.generatorOutput,
        nestedFieldsAccessed = downstreamAccess,
        pathPrefix = pathPrefix
      )
    }
    results.toMap
  }

  /**
   * Collect alias mappings from Project nodes, specifically _extract_ aliases
   */
  private def collectAliasMapping(plan: LogicalPlan): Map[ExprId, Expression] = {
    val aliases = mutable.Map[ExprId, Expression]()

    plan.foreach {
      case p: Project =>
        p.projectList.foreach {
          case alias @ Alias(child, _) =>
            aliases(alias.exprId) = child
          case _ =>
        }
      case _ =>
    }

    aliases.toMap
  }

  /**
   * Extract the input column from a generator expression with alias resolution.
   * Returns (columnName, originalAttribute, accessedFields) if found.
   * accessedFields contains the field names from GetArrayStructFields if resolved through alias.
   */
  private def extractGeneratorInputColumn(
      gen: Generate,
      aliasMap: Map[ExprId, Expression]): Option[(AttributeReference, Seq[String])] = {

    gen.generator match {
      case Explode(attr: AttributeReference) =>
        resolveAlias(attr, aliasMap)

      case PosExplode(attr: AttributeReference) =>
        resolveAlias(attr, aliasMap)

      case Inline(attr: AttributeReference) =>
        resolveAlias(attr, aliasMap)

      // Handle GetStructField wrapping (e.g., EXPLODE(struct_col.array_field))
      case Explode(GetStructField(attr: AttributeReference, _, Some(fieldName))) =>
        resolveAlias(attr, aliasMap).map { case (resolvedAttr, prefix) =>
          (resolvedAttr, prefix :+ fieldName)
        }

      case PosExplode(GetStructField(attr: AttributeReference, _, Some(fieldName))) =>
        resolveAlias(attr, aliasMap).map { case (resolvedAttr, prefix) =>
          (resolvedAttr, prefix :+ fieldName)
        }

      // Handle GetArrayItem (less common but possible)
      case Explode(GetArrayItem(attr: AttributeReference, _, _)) =>
        resolveAlias(attr, aliasMap)

      case PosExplode(GetArrayItem(attr: AttributeReference, _, _)) =>
        resolveAlias(attr, aliasMap)

      case _ =>
        // Complex generator expression we don't handle directly
        None
    }
  }

  /**
   * Resolve an attribute through the alias map.
   * If it's an _extract_ alias pointing to GetArrayStructFields, extract the base column and field.
   */
  private def resolveAlias(
      attr: AttributeReference,
      aliasMap: Map[ExprId, Expression]): Option[(AttributeReference, Seq[String])] = {

    aliasMap.get(attr.exprId) match {
      case Some(GetArrayStructFields(baseAttr: AttributeReference, field, _, _, _)) =>
        val resolved = resolveAlias(baseAttr, aliasMap).getOrElse((baseAttr, Seq.empty))
        Some((resolved._1, resolved._2 :+ field.name))

      case Some(GetStructField(childAttr: AttributeReference, _, Some(fieldName))) =>
        val resolved = resolveAlias(childAttr, aliasMap).getOrElse((childAttr, Seq.empty))
        Some((resolved._1, resolved._2 :+ fieldName))

      case Some(_) =>
        Some((attr, Seq.empty))

      case None =>
        Some((attr, Seq.empty))
    }
  }

  /**
   * Analyze how generator outputs are used downstream to determine what nested fields
   * are accessed.
   */
  private def analyzeDownstreamFieldAccess(
      plan: LogicalPlan,
      generatorOutput: Seq[Attribute],
      aliasMap: Map[ExprId, Expression]): Set[Seq[String]] = {

    val outputIds = generatorOutput.map(_.exprId).toSet
    val fieldPaths = mutable.Set[Seq[String]]()

    // Traverse the plan to find expressions that reference generator outputs
    plan.foreach { node =>
      node.expressions.foreach { expr =>
        fieldPaths ++= extractFieldPaths(expr, outputIds, aliasMap, Set.empty)
      }

      node match {
        case project: Project =>
          project.projectList.foreach {
            case Alias(child, _) =>
              fieldPaths ++= extractFieldPaths(child, outputIds, aliasMap, Set.empty)
            case _ =>
          }
        case aggregate: Aggregate =>
          (aggregate.aggregateExpressions ++ aggregate.groupingExpressions).foreach {
            case Alias(child, _) =>
              fieldPaths ++= extractFieldPaths(child, outputIds, aliasMap, Set.empty)
            case _ =>
          }
        case _ =>
      }
    }

    fieldPaths.toSet
  }

  /**
   * Extract field access paths from an expression that references generator outputs
   */
  private def extractFieldPaths(
      expr: Expression,
      targetOutputIds: Set[ExprId],
      aliasMap: Map[ExprId, Expression],
      visited: Set[ExprId]): Seq[Seq[String]] = {

    val paths = mutable.ArrayBuffer[Seq[String]]()

    expr match {
      case alias: Alias =>
        paths ++= extractFieldPaths(alias.child, targetOutputIds, aliasMap, visited)
      case _ =>
    }

    expr.foreach {
      case GetStructField(
            attr: AttributeReference,
            _,
            Some(fieldName))
          if targetOutputIds.contains(attr.exprId) =>
        paths += Seq(fieldName)

      case GetStructField(
            attr: AttributeReference,
            _,
            Some(fieldName))
          if aliasMap.contains(attr.exprId) && !visited.contains(attr.exprId) =>
        val childExpr = aliasMap(attr.exprId)
        val childPaths = extractFieldPaths(
          childExpr,
          targetOutputIds,
          aliasMap,
          visited + attr.exprId)
        if (childPaths.isEmpty) {
          paths += Seq(fieldName)
        } else {
          childPaths.foreach { suffix =>
            paths += (Seq(fieldName) ++ suffix)
          }
        }

      case GetStructField(
            arrayItem: GetArrayItem,
            _,
            Some(fieldName)) =>
        val childPaths = extractFieldPaths(
          arrayItem,
          targetOutputIds,
          aliasMap,
          visited)
        if (childPaths.isEmpty) {
          paths += Seq(fieldName)
        } else {
          childPaths.foreach { suffix =>
            paths += (suffix :+ fieldName)
          }
        }

      case gsf: GetStructField =>
        extractNestedPath(gsf, targetOutputIds, aliasMap, visited).foreach(paths += _)

      case GetArrayStructFields(
            attr: AttributeReference,
            field,
            _,
            _,
            _)
          if targetOutputIds.contains(attr.exprId) =>
        paths += Seq(field.name)

      case gasf: GetArrayStructFields =>
        extractNestedArrayPath(gasf, targetOutputIds, aliasMap, visited).foreach(paths += _)
        val childPaths = extractFieldPaths(
          gasf.child,
          targetOutputIds,
          aliasMap,
          visited)
        if (childPaths.isEmpty) {
          paths += Seq(gasf.field.name)
        } else {
          childPaths.foreach { suffix =>
            paths += (suffix :+ gasf.field.name)
          }
        }

      case attr: AttributeReference
          if aliasMap.contains(attr.exprId) && !visited.contains(attr.exprId) =>
        paths ++= extractFieldPaths(
          aliasMap(attr.exprId),
          targetOutputIds,
          aliasMap,
          visited + attr.exprId)

      case _ =>
    }

    paths.toSeq
  }

  private def extractNestedPath(
      gsf: GetStructField,
      targetOutputIds: Set[ExprId],
      aliasMap: Map[ExprId, Expression],
      visited: Set[ExprId]): Option[Seq[String]] = {

    def buildPath(
        expr: Expression,
        accumulated: List[String]): Option[Seq[String]] = expr match {
      case GetStructField(child, _, Some(fieldName)) =>
        buildPath(child, fieldName :: accumulated)
      case GetArrayStructFields(child, field, _, _, _) =>
        buildPath(child, field.name :: accumulated)
      case GetArrayItem(child, _, _) =>
        buildPath(child, accumulated)
      case attr: AttributeReference
          if targetOutputIds.contains(attr.exprId) =>
        Some(accumulated.toSeq)
      case attr: AttributeReference
          if aliasMap.contains(attr.exprId) && !visited.contains(attr.exprId) =>
        buildPath(aliasMap(attr.exprId), accumulated)
      case _ =>
        None
    }

    buildPath(gsf, List.empty)
  }

  private def extractNestedArrayPath(
      gasf: GetArrayStructFields,
      targetOutputIds: Set[ExprId],
      aliasMap: Map[ExprId, Expression],
      visited: Set[ExprId]): Option[Seq[String]] = {

    gasf.child match {
      case attr: AttributeReference if targetOutputIds.contains(attr.exprId) =>
        Some(Seq(gasf.field.name))
      case attr: AttributeReference
          if aliasMap.contains(attr.exprId) && !visited.contains(attr.exprId) =>
        extractNestedArrayPath(
          GetArrayStructFields(
            aliasMap(attr.exprId),
            gasf.field,
            gasf.ordinal,
            gasf.numFields,
            gasf.containsNull),
          targetOutputIds,
          aliasMap,
          visited + attr.exprId)
      case GetStructField(child, _, Some(parentField)) =>
        extractNestedArrayPath(
          GetArrayStructFields(
            child,
            gasf.field,
            gasf.ordinal,
            gasf.numFields,
            gasf.containsNull),
          targetOutputIds,
          aliasMap,
          visited).map(parentField +: _)
      case GetArrayItem(child, _, _) =>
        extractNestedArrayPath(
          GetArrayStructFields(
            child,
            gasf.field,
            gasf.ordinal,
            gasf.numFields,
            gasf.containsNull),
          targetOutputIds,
          aliasMap,
          visited)
      case _ =>
        None
    }
  }

  private def isComplexType(dataType: DataType): Boolean = dataType match {
    case _: StructType | _: ArrayType | _: MapType => true
    case _ => false
  }

  /**
   * Check if generator inputs indicate chained generator pattern.
   * Returns true if any generator output feeds into another generator.
   */
  def hasChainedGenerators(plan: LogicalPlan): Boolean = {
    val generators = plan.collect { case g: Generate => g }

    if (generators.length < 2) {
      return false
    }

    // Check if any generator's output is used as input to another generator
    generators.exists { gen1 =>
      val outputIds = gen1.generatorOutput.map(_.exprId).toSet

      generators.exists { gen2 =>
        gen2 != gen1 && gen2.generator.references.exists(ref => outputIds.contains(ref.exprId))
      }
    }
  }
}
