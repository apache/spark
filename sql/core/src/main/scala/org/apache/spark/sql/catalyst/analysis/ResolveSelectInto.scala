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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression, Literal, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, Except, Intersect, LogicalPlan, Project, SelectIntoVariable, Union, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

/**
 * Analyzer rule that resolves SELECT INTO statements into [[SelectIntoVariable]] nodes.
 *
 * This rule performs two operations:
 * 1. Marks each [[UnresolvedSelectInto]] with context flags indicating its position
 *    in the query tree (top-level, in set operation, in pipe operator)
 * 2. Validates context requirements and resolves to [[SelectIntoVariable]]
 *
 * SELECT INTO is only valid within SQL scripts (BEGIN...END blocks) and must appear
 * at the top level of a statement, not within subqueries, set operations, or pipe operators.
 *
 * The parser ensures [[UnresolvedSelectInto]] correctly wraps query organization clauses
 * (ORDER BY, LIMIT, OFFSET), so the analyzer only needs to mark context and validate.
 */
object ResolveSelectInto extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!containsUnresolvedSelectInto(plan)) {
      plan
    } else {
      // Pass 1: Mark UnresolvedSelectInto with context flags
      val markedPlan = markContext(plan, isTopLevel = true, isInSetOperation = false)

      // Pass 2: Validate context requirements and resolve to SelectIntoVariable
      resolveSelectInto(markedPlan)
    }
  }

  /**
   * Marks [[UnresolvedSelectInto]] nodes with context flags.
   *
   * Context flags track the position in the query tree:
   * - `isTopLevel`: True if this SELECT is the outermost query of the statement
   * - `isInSetOperation`: True if this SELECT is part of UNION/INTERSECT/EXCEPT
   *
   * @param plan the logical plan to process
   * @param isTopLevel true if processing the top level of the enclosing statement
   * @param isInSetOperation true if processing within a set operation
   * @return the plan with UnresolvedSelectInto nodes marked
   */
  private def markContext(
      plan: LogicalPlan,
      isTopLevel: Boolean,
      isInSetOperation: Boolean): LogicalPlan = {
    plan match {
      case u @ UnresolvedSelectInto(query, targetVariables, _, _, _) =>
        // Recursively process the query child
        val processedQuery = markContext(query, isTopLevel = false, isInSetOperation)
        // Update this node with the processed query and context flags
        u.copy(
          query = processedQuery,
          isTopLevel = isTopLevel,
          isInSetOperation = isInSetOperation)

      case p if isSetOperation(p) =>
        // All children of set operations are marked as being in a set operation
        p.mapChildren(child =>
          markContext(child, isTopLevel = false, isInSetOperation = true))

      case w: WithCTE =>
        // CTE definitions are never top-level; only the main query inherits top-level status
        val processedCteDefs = w.cteDefs.map(cteDef =>
          markContext(cteDef, isTopLevel = false, isInSetOperation)
            .asInstanceOf[CTERelationDef])
        val processedPlan = markContext(w.plan, isTopLevel, isInSetOperation)
        w.copy(plan = processedPlan, cteDefs = processedCteDefs)

      case _ =>
        // For other nodes, recursively mark their children
        // Unary nodes pass through top-level status to their single child
        val childIsTopLevel = isTopLevel && plan.children.length == 1
        plan.mapChildren(child =>
          markContext(child, childIsTopLevel, isInSetOperation))
    }
  }

  /**
   * Returns true if the plan contains any [[UnresolvedSelectInto]] nodes.
   */
  private def containsUnresolvedSelectInto(plan: LogicalPlan): Boolean = {
    plan.find(_.isInstanceOf[UnresolvedSelectInto]).isDefined
  }

  /**
   * Returns true if the plan is a set operation (UNION, INTERSECT, EXCEPT).
   */
  private def isSetOperation(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Union | _: Intersect | _: Except => true
      case _ => false
    }
  }

  /**
   * Resolves [[UnresolvedSelectInto]] nodes to [[SelectIntoVariable]] after validation.
   */
  private def resolveSelectInto(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case node @ UnresolvedSelectInto(query, targetVariables, isTopLevel, isInSetOperation,
          isPipeOperator) =>
        // Validate that SELECT INTO is within a SQL script context
        if (!withinSqlScript) {
          throw QueryCompilationErrors.selectIntoNotInSqlScript()
        }

        // Validate that SELECT INTO appears at top level, not in set operations or pipe operators
        if (!isTopLevel || isInSetOperation || isPipeOperator) {
          throw QueryCompilationErrors.selectIntoOnlyAtTopLevel()
        }

        // Resolve to SelectIntoVariable once all expressions are resolved
        if (query.resolved && targetVariables.forall(_.resolved)) {
          val numCols = query.output.length
          val numVars = targetVariables.length
          val finalTargetVars = extractTargetVariables(targetVariables)

          // Apply struct unpacking or validate cardinality
          val finalQuery = applyStructUnpackingOrValidate(query, finalTargetVars, numVars, numCols)

          // Create SelectIntoVariable node (zero-row behavior: keep variables unchanged)
          SelectIntoVariable(finalTargetVars, finalQuery)
        } else {
          // Wait for analyzer to resolve expressions
          node
        }
    }
  }

  /**
   * Apply struct unpacking if needed, or validate cardinality.
   *
   * Struct unpacking allows assigning multiple query columns to fields of a single struct variable.
   * For example:
   *   DECLARE emp STRUCT<id INT, name STRING>;
   *   SELECT emp_id, emp_name INTO emp FROM employees WHERE id = 1;
   *
   * This wraps the query columns into a struct that matches the target variable's type.
   *
   * @param query The source query producing values
   * @param targetVars The target variables (already extracted and validated)
   * @param numVars Number of target variables
   * @param numCols Number of columns in query output
   * @return Modified query with struct wrapping if needed
   */
  private def applyStructUnpackingOrValidate(
      query: LogicalPlan,
      targetVars: Seq[VariableReference],
      numVars: Int,
      numCols: Int): LogicalPlan = {
    if (numVars == 1 && numCols > 1) {
      // Single target variable with multiple columns - check if struct unpacking applies
      val targetVar = targetVars.head
      targetVar.dataType match {
        case structType: StructType =>
          // Target variable is a struct - validate field count matches column count
          val numFields = structType.fields.length
          if (numFields != numCols) {
            throw QueryCompilationErrors.selectIntoStructFieldMismatch(
              numCols, numFields, structType)
          }

          // Wrap the query columns into a struct
          // Create a CreateNamedStruct expression that packages all columns
          val fieldExprs = structType.fields.zip(query.output).flatMap {
            case (field, attr) =>
              // CreateNamedStruct takes alternating name/value pairs
              Seq(Literal(field.name), attr)
          }
          val structExpr = Alias(
            CreateNamedStruct(fieldExprs.toSeq),
            targetVar.originalNameParts.last)()

          Project(Seq(structExpr), query)

        case _ =>
          // Not a struct - normal cardinality check
          if (numVars != numCols) {
            throw QueryCompilationErrors.selectIntoVariableCountMismatch(numVars, numCols)
          }
          query
      }
    } else {
      // Normal case: number of variables must match number of columns
      if (numVars != numCols) {
        throw QueryCompilationErrors.selectIntoVariableCountMismatch(numVars, numCols)
      }
      query
    }
  }

  /**
   * Extracts [[VariableReference]] nodes from the target expressions in the INTO clause.
   *
   * Handles expressions that may be wrapped in [[Alias]] nodes and sets `canFold = false`
   * to prevent constant folding of variable references during optimization.
   *
   * @param targetVariables the expressions from the INTO clause
   * @return sequence of VariableReference nodes
   * @throws QueryCompilationError if any expression is not a valid variable reference
   */
  private def extractTargetVariables(targetVariables: Seq[Expression]): Seq[VariableReference] = {
    targetVariables.map {
      case alias: Alias =>
        alias.child match {
          case varRef: VariableReference =>
            varRef.copy(canFold = false)
          case _ =>
            throw QueryCompilationErrors.unsupportedParameterExpression(alias.child)
        }
      case varRef: VariableReference =>
        varRef.copy(canFold = false)
      case other =>
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
  }

  /**
   * Returns true if currently executing within a SQL script (BEGIN...END block).
   *
   * SQL scripts have an active [[SqlScriptingContextManager]] with a variable manager.
   */
  private def withinSqlScript: Boolean =
    SqlScriptingContextManager.get().map(_.getVariableManager).isDefined
}
