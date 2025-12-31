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
 * Analysis rule that resolves SELECT INTO statements.
 * This rule validates the context (SQL script only, top-level only, not in set operations)
 * and converts UnresolvedSelectInto to SelectIntoVariable.
 */
object ResolveSelectInto extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Early exit: if plan doesn't contain UnresolvedSelectInto, skip all processing
    if (!containsUnresolvedSelectInto(plan)) {
      plan
    } else {
      // First pass: mark top-level SELECT INTO nodes and detect set operations
      val markedPlan = markTopLevelAndSetOperations(
        plan, isTopLevel = true, isInSetOperation = false)

      // Second pass: hoist UnresolvedSelectInto above Sort/Limit
      // This fixes the parser structure where ORDER BY/LIMIT are added after SELECT INTO
      val hoistedPlan = hoistSelectInto(markedPlan)

      // Third pass: resolve and validate
      resolveSelectInto(hoistedPlan)
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
   */
  private def hoistSelectInto(plan: LogicalPlan): LogicalPlan = {
    // Transform to hoist UnresolvedSelectInto above query organization operators
    // Use resolveOperatorsUp since we're in an analysis rule
    plan.resolveOperatorsUp {
      case s: Sort if isSelectIntoDescendant(s.child) =>
        hoistFromOrganizationOp(s, s.child, c => s.copy(child = c))

      case l: GlobalLimit if isSelectIntoDescendant(l.child) =>
        hoistFromOrganizationOp(l, l.child, c => l.copy(child = c))

      case l: LocalLimit if isSelectIntoDescendant(l.child) =>
        hoistFromOrganizationOp(l, l.child, c => l.copy(child = c))
    }
  }

  /**
   * Check if the immediate child is either UnresolvedSelectInto or another query
   * organization operator that contains one.
   */
  private def isSelectIntoDescendant(child: LogicalPlan): Boolean = {
    child match {
      case _: UnresolvedSelectInto => true
      case s: Sort => isSelectIntoDescendant(s.child)
      case l: GlobalLimit => isSelectIntoDescendant(l.child)
      case l: LocalLimit => isSelectIntoDescendant(l.child)
      case _ => false
    }
  }

  /**
   * Hoist UnresolvedSelectInto from within a query organization operator.
   * Returns the hoisted plan.
   */
  private def hoistFromOrganizationOp(
      parent: LogicalPlan,
      child: LogicalPlan,
      recreateParent: LogicalPlan => LogicalPlan): LogicalPlan = {
    child match {
      case selectInto: UnresolvedSelectInto =>
        // Direct child is UnresolvedSelectInto - hoist it above parent
        selectInto.copy(query = recreateParent(selectInto.query))

      case s: Sort =>
        // Child is Sort, recurse to find UnresolvedSelectInto deeper
        val hoisted = hoistFromOrganizationOp(s, s.child, c => s.copy(child = c))
        hoisted match {
          case selectInto: UnresolvedSelectInto =>
            // Successfully hoisted from deeper level, now wrap parent too
            selectInto.copy(query = recreateParent(selectInto.query))
          case _ =>
            // No hoisting happened, return parent with updated child
            recreateParent(hoisted)
        }

      case l: GlobalLimit =>
        val hoisted = hoistFromOrganizationOp(l, l.child, c => l.copy(child = c))
        hoisted match {
          case selectInto: UnresolvedSelectInto =>
            selectInto.copy(query = recreateParent(selectInto.query))
          case _ =>
            recreateParent(hoisted)
        }

      case l: LocalLimit =>
        val hoisted = hoistFromOrganizationOp(l, l.child, c => l.copy(child = c))
        hoisted match {
          case selectInto: UnresolvedSelectInto =>
            selectInto.copy(query = recreateParent(selectInto.query))
          case _ =>
            recreateParent(hoisted)
        }

      case _ =>
        // Child is not a query organization op, no hoisting needed
        parent
    }
  }

  /**
   * Mark UnresolvedSelectInto nodes with context information about whether they are
   * at top level and whether they are in set operations.
   */
  private def markTopLevelAndSetOperations(
      plan: LogicalPlan,
      isTopLevel: Boolean,
      isInSetOperation: Boolean): LogicalPlan = {
    plan match {
      case u @ UnresolvedSelectInto(query, targetVariables, _, _) =>
        // Mark this node with the context information
        val markedQuery = markTopLevelAndSetOperations(query, isTopLevel = false, isInSetOperation)
        u.copy(query = markedQuery, isTopLevel = isTopLevel, isInSetOperation = isInSetOperation)

      case p if isSetOperation(p) =>
        // We're in a set operation - mark all children as being in set operation
        p.mapChildren(child =>
          markTopLevelAndSetOperations(child, isTopLevel = false, isInSetOperation = true))

      case w: WithCTE =>
        // Special case for WithCTE: it has multiple children (CTE definitions + main plan),
        // but only the main plan should be considered for top-level context.
        // Mark the CTE definitions as not top-level, but pass isTopLevel to the main plan.
        val markedCteDefs = w.cteDefs.map(cteDef =>
          markTopLevelAndSetOperations(cteDef, isTopLevel = false, isInSetOperation)
            .asInstanceOf[CTERelationDef])
        val markedPlan = markTopLevelAndSetOperations(w.plan, isTopLevel, isInSetOperation)
        w.copy(plan = markedPlan, cteDefs = markedCteDefs)

      case _ =>
        // For other nodes, continue traversal.
        // If we're at top level and this is a unary node (like Sort, Limit, Project),
        // the child can still be considered top-level.
        // Otherwise (binary nodes, multi-child nodes), children are not top-level.
        val childIsTopLevel = isTopLevel && plan.children.length == 1
        plan.mapChildren(child =>
          markTopLevelAndSetOperations(child, childIsTopLevel, isInSetOperation))
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
      case node @ UnresolvedSelectInto(query, targetVariables, isTopLevel, isInSetOperation) =>
        // Validation 1: Check if we're within a SQL script
        if (!withinSqlScript) {
          throw QueryCompilationErrors.selectIntoNotInSqlScript()
        }

        // Validation 2 & 3: Check if at top level and not in set operation
        if (!isTopLevel || isInSetOperation) {
          throw QueryCompilationErrors.selectIntoOnlyAtTopLevel()
        }

        // All validations passed - convert to SetVariable
        if (query.resolved && targetVariables.forall(_.resolved)) {
          val numCols = query.output.length
          val numVars = targetVariables.length
          val finalTargetVars = extractTargetVariables(targetVariables)

          // Handle struct unpacking:
          // If exactly one target variable AND it's a struct type AND query has > 1 column,
          // then wrap the columns into a struct
          val finalQuery = if (numVars == 1 && numCols > 1) {
            val targetVar = finalTargetVars.head
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

          // Use SelectIntoVariable which has different zero-row behavior than SetVariable.
          // When zero rows are returned, SelectIntoVariable keeps variables unchanged,
          // whereas SetVariable (used by EXECUTE IMMEDIATE INTO) sets them to null.
          SelectIntoVariable(finalTargetVars, finalQuery)
        } else {
          // Not all resolved yet - wait for next iteration
          node
        }
    }
  }

  /**
   * Extract target variables from the INTO clause.
   */
  private def extractTargetVariables(targetVariables: Seq[Expression]): Seq[VariableReference] = {
    targetVariables.map {
      case alias: Alias =>
        // Extract the VariableReference from the alias
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
   * Check if we're within a SQL script context.
   */
  private def withinSqlScript: Boolean =
    SqlScriptingContextManager.get().map(_.getVariableManager).isDefined
}
