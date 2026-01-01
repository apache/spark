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
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, Except, GlobalLimit, Intersect, LocalLimit, LogicalPlan, Project, SelectIntoVariable, Sort, Union, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

/**
 * Analyzer rule that resolves SELECT INTO statements into [[SelectIntoVariable]] nodes.
 *
 * This rule performs three key operations:
 * 1. Restructures the logical plan by hoisting [[UnresolvedSelectInto]] above query
 *    organization operators (Sort, Limit)
 * 2. Marks each [[UnresolvedSelectInto]] with context flags indicating its position
 *    in the query tree (top-level, in set operation, in pipe operator)
 * 3. Validates context requirements and resolves to [[SelectIntoVariable]]
 *
 * SELECT INTO is only valid within SQL scripts (BEGIN...END blocks) and must appear
 * at the top level of a statement, not within subqueries, set operations, or pipe operators.
 */
object ResolveSelectInto extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!containsUnresolvedSelectInto(plan)) {
      plan
    } else {
      // Pass 1: Hoist UnresolvedSelectInto above Sort/Limit and mark with context flags
      val hoistedAndMarkedPlan = hoistAndMark(plan, isTopLevel = true, isInSetOperation = false)

      // Pass 2: Validate context requirements and resolve to SelectIntoVariable
      resolveSelectInto(hoistedAndMarkedPlan)
    }
  }

  /**
   * Hoists [[UnresolvedSelectInto]] nodes above query organization operators and marks
   * them with context flags.
   *
   * Hoisting is necessary because the parser creates a structure where UnresolvedSelectInto
   * appears below Sort and Limit nodes, but semantically the variable assignment should wrap
   * the entire ordered and limited query.
   *
   * Transformation example for: {{{SELECT * INTO v1, v2 FROM tbl ORDER BY name LIMIT 5}}}
   *
   * Parser output:
   * {{{
   *   Sort(orderBy=[name])
   *    +- Limit(5)
   *        +- UnresolvedSelectInto(variables=[v1, v2], isTopLevel=false)
   *            +- Project(*)
   *                +- Relation(tbl)
   * }}}
   *
   * After hoisting and marking:
   * {{{
   *   UnresolvedSelectInto(variables=[v1, v2], isTopLevel=true)
   *    +- Sort(orderBy=[name])
   *        +- Limit(5)
   *            +- Project(*)
   *                +- Relation(tbl)
   * }}}
   *
   * Context flags track the position in the query tree:
   * - `isTopLevel`: True if this SELECT is the outermost query of the statement
   * - `isInSetOperation`: True if this SELECT is part of UNION/INTERSECT/EXCEPT
   *
   * @param plan the logical plan to process
   * @param isTopLevel true if processing the top level of the enclosing statement
   * @param isInSetOperation true if processing within a set operation
   * @return the plan with UnresolvedSelectInto nodes hoisted and marked
   */
  private def hoistAndMark(
      plan: LogicalPlan,
      isTopLevel: Boolean,
      isInSetOperation: Boolean): LogicalPlan = {
    // First, recursively process children (bottom-up for hoisting)
    val processedPlan = plan match {
      case u @ UnresolvedSelectInto(query, targetVariables, _, _, _) =>
        // Recursively process the query child
        val processedQuery = hoistAndMark(query, isTopLevel = false, isInSetOperation)
        // Update this node with the processed query and context flags
        u.copy(
          query = processedQuery,
          isTopLevel = isTopLevel,
          isInSetOperation = isInSetOperation)

      case p if isSetOperation(p) =>
        // All children of set operations are marked as being in a set operation
        p.mapChildren(child =>
          hoistAndMark(child, isTopLevel = false, isInSetOperation = true))

      case w: WithCTE =>
        // CTE definitions are never top-level; only the main query inherits top-level status
        val processedCteDefs = w.cteDefs.map(cteDef =>
          hoistAndMark(cteDef, isTopLevel = false, isInSetOperation)
            .asInstanceOf[CTERelationDef])
        val processedPlan = hoistAndMark(w.plan, isTopLevel, isInSetOperation)
        w.copy(plan = processedPlan, cteDefs = processedCteDefs)

      case _ =>
        // For other nodes, continue traversal
        // Unary nodes (Sort, Limit, Project) pass through top-level status
        val childIsTopLevel = isTopLevel && plan.children.length == 1
        plan.mapChildren(child =>
          hoistAndMark(child, childIsTopLevel, isInSetOperation))
    }

    // If this node is a query organization operator containing UnresolvedSelectInto,
    // hoist the UnresolvedSelectInto to wrap this operator
    processedPlan match {
      case op if isQueryOrganizationOp(op) && isSelectIntoDescendant(getChild(op)) =>
        hoistFromOrganizationOp(op)
      case _ =>
        processedPlan
    }
  }

  /**
   * Check if the plan contains any UnresolvedSelectInto nodes.
   */
  private def containsUnresolvedSelectInto(plan: LogicalPlan): Boolean = {
    plan.find(_.isInstanceOf[UnresolvedSelectInto]).isDefined
  }

  /**
   * Hoist UnresolvedSelectInto nodes to wrap their parent Sort/Limit nodes.
   * This fixes the parser structure where ORDER BY/LIMIT are added after the SELECT clause,
   * so they end up as parents of UnresolvedSelectInto instead of children.
   *
   * Transforms:
   *   Sort -> Limit -> UnresolvedSelectInto -> query
   * Into:
   *   UnresolvedSelectInto -> Sort -> Limit -> query
   *
   * Example:
   *   Before: Sort(LocalLimit(UnresolvedSelectInto(Project(...))))
   *   After:  UnresolvedSelectInto(Sort(LocalLimit(Project(...))))
   */
  private def hoistSelectInto(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case op if isQueryOrganizationOp(op) && isSelectIntoDescendant(getChild(op)) =>
        hoistFromOrganizationOp(op)
    }
  }

  /**
   * Check if a plan is a query organization operator (Sort, GlobalLimit, or LocalLimit).
   * These are the operators that can appear between SELECT and UnresolvedSelectInto.
   */
  private def isQueryOrganizationOp(plan: LogicalPlan): Boolean = plan match {
    case _: Sort | _: GlobalLimit | _: LocalLimit => true
    case _ => false
  }

  /**
   * Get the child of a query organization operator.
   */
  private def getChild(op: LogicalPlan): LogicalPlan = op match {
    case s: Sort => s.child
    case l: GlobalLimit => l.child
    case l: LocalLimit => l.child
    case _ =>
      throw new IllegalArgumentException(
        s"Expected query organization op, got ${op.getClass}")
  }

  /**
   * Recreate a query organization operator with a new child.
   */
  private def withNewChild(op: LogicalPlan, newChild: LogicalPlan): LogicalPlan = op match {
    case s: Sort => s.copy(child = newChild)
    case l: GlobalLimit => l.copy(child = newChild)
    case l: LocalLimit => l.copy(child = newChild)
    case _ =>
      throw new IllegalArgumentException(
        s"Expected query organization op, got ${op.getClass}")
  }

  /**
   * Check if the child contains UnresolvedSelectInto either directly or within nested
   * query organization operators.
   *
   * Returns true if:
   * - child is UnresolvedSelectInto, OR
   * - child is a query organization op that contains UnresolvedSelectInto deeper down
   */
  private def isSelectIntoDescendant(child: LogicalPlan): Boolean = child match {
    case _: UnresolvedSelectInto => true
    case op if isQueryOrganizationOp(op) => isSelectIntoDescendant(getChild(op))
    case _ => false
  }

  /**
   * Hoist UnresolvedSelectInto from within a query organization operator.
   *
   * Recursively processes nested query organization operators until it finds
   * UnresolvedSelectInto, then hoists it to wrap all the operators above it.
   *
   * @param parent The query organization operator to hoist from
   * @return Either the hoisted UnresolvedSelectInto wrapping the parent chain,
   *         or the original parent if no hoisting occurred
   */
  private def hoistFromOrganizationOp(parent: LogicalPlan): LogicalPlan = {
    val child = getChild(parent)
    child match {
      case selectInto: UnresolvedSelectInto =>
        // Direct child is UnresolvedSelectInto - hoist it above parent
        // Transform: parent(selectInto(query)) -> selectInto(parent(query))
        selectInto.copy(query = withNewChild(parent, selectInto.query))

      case op if isQueryOrganizationOp(op) =>
        // Child is another query org op - recurse to find UnresolvedSelectInto deeper
        val hoisted = hoistFromOrganizationOp(op)
        hoisted match {
          case selectInto: UnresolvedSelectInto =>
            // Successfully hoisted from deeper level, now wrap parent too
            // Transform: parent(selectInto(nested)) -> selectInto(parent(nested))
            selectInto.copy(query = withNewChild(parent, selectInto.query))
          case _ =>
            // No hoisting happened deeper, return parent with updated child
            withNewChild(parent, hoisted)
        }

      case _ =>
        // Child is not a query organization op, no hoisting possible
        parent
    }
  }

  /**
   * Check if a plan is a set operation (UNION, INTERSECT, EXCEPT).
   */
  private def isSetOperation(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Union | _: Intersect | _: Except => true
      case _ => false
    }
  }

  /**
   * Resolve UnresolvedSelectInto nodes to SetVariable after validation.
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
