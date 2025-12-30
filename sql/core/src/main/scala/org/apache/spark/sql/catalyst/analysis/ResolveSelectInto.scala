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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

/**
 * Analysis rule that resolves SELECT INTO statements.
 * This rule validates the context (SQL script only, top-level only, not in set operations)
 * and converts UnresolvedSelectInto to SetVariable.
 */
object ResolveSelectInto extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // First pass: mark top-level SELECT INTO nodes and detect set operations
    val markedPlan = markTopLevelAndSetOperations(plan, isTopLevel = true, isInSetOperation = false)

    // Second pass: resolve and validate
    resolveSelectInto(markedPlan)
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

      case _ =>
        // For other nodes, continue traversal but not at top level anymore
        plan.mapChildren(child =>
          markTopLevelAndSetOperations(child, isTopLevel = false, isInSetOperation))
    }
  }

  /**
   * Check if a plan is a set operation (UNION, INTERSECT, EXCEPT, SETMINUS).
   */
  private def isSetOperation(plan: LogicalPlan): Boolean = {
    plan.nodeName match {
      case "Union" | "Intersect" | "Except" | "Distinct" => true
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

        // Validation 2: Check if at top level
        if (!isTopLevel) {
          throw QueryCompilationErrors.selectIntoInNestedQuery()
        }

        // Validation 3: Check if in set operation
        if (isInSetOperation) {
          throw QueryCompilationErrors.selectIntoInSetOperation()
        }

        // All validations passed - convert to SetVariable
        if (query.resolved && targetVariables.forall(_.resolved)) {
          val numCols = query.output.length
          val numVars = targetVariables.length
          val finalTargetVars = extractTargetVariables(targetVariables)

          // Handle struct unpacking (Condition 4):
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

          // For SELECT INTO, when zero rows are returned, variables should remain unchanged.
          // This is different from EXECUTE IMMEDIATE INTO which sets variables to null.
          // We use skipNoRows=true to indicate this behavior.
          SetVariable(finalTargetVars, finalQuery, skipNoRows = true)
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
