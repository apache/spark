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

import scala.util.{Either, Left, Right}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, VariableReference}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, ExecutableDuringAnalysis, LocalRelation, LogicalPlan, SetVariable, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXECUTE_IMMEDIATE, TreePattern}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Logical plan representing execute immediate query.
 *
 * @param queryParam the query expression (first child)
 * @param args parameters from USING clause (subsequent children)
 * @param targetVariables variables to store the result of the query
 */
case class ExecuteImmediateQuery(
    queryParam: Expression,
    args: Seq[Expression],
    targetVariables: Seq[Expression])
  extends UnresolvedLeafNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * Logical plan representing a resolved execute immediate command that will recursively
 * invoke SQL execution.
 *
 * @param queryParam the resolved query expression
 * @param args parameters from USING clause
 */
case class ExecuteImmediateCommand(
    queryParam: Expression,
    args: Seq[Expression])
  extends UnaryNode with ExecutableDuringAnalysis {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)

  override def child: LogicalPlan = LocalRelation(Nil, Nil)

  override def output: Seq[Attribute] = child.output

  override lazy val resolved: Boolean = {
    // ExecuteImmediateCommand should not be considered resolved until it has been
    // executed and replaced by ExecuteExecutableDuringAnalysis rule.
    // This ensures that SetVariable waits for execution to complete.
    false
  }

  override def stageForExplain(): LogicalPlan = {
    // For EXPLAIN, just show the command without executing it
    copy()
  }

  override protected def withNewChildInternal(
      newChild: LogicalPlan): ExecuteImmediateCommand = {
    copy()
  }
}

/**
 * This rule substitutes execute immediate query node with a command node
 * that will handle recursive SQL execution.
 */
class SubstituteExecuteImmediate(
    val catalogManager: CatalogManager,
    resolveChild: LogicalPlan => LogicalPlan,
    checkAnalysis: LogicalPlan => Unit)
  extends Rule[LogicalPlan] {
  private val variableResolution = new VariableResolution(catalogManager.tempVariableManager)

  def resolveVariable(e: Expression): Expression = {

    /**
     * We know that the expression is either UnresolvedAttribute, Alias or Parameter, as passed from
     * the parser. If it is an UnresolvedAttribute, we look it up in the catalog and return it. If
     * it is an Alias, we resolve the child and return an Alias with the same name. If it is
     * a Parameter, we leave it as is because the parameter belongs to another parameterized
     * query and should be resolved later.
     */
    e match {
      case u: UnresolvedAttribute =>
        getVariableReference(u, u.nameParts)
      case a: Alias =>
        Alias(resolveVariable(a.child), a.name)()
      case p: Parameter => p
      case varRef: VariableReference => varRef // VariableReference is already resolved
      case other =>
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
  }

  def resolveArguments(expressions: Seq[Expression]): Seq[Expression] = {
    expressions.map { exp =>
      if (exp.resolved) {
        exp
      } else {
        resolveVariable(exp)
      }
    }
  }

  def extractQueryString(either: Either[String, UnresolvedAttribute]): String = {
    either match {
      case Left(v) => v
      case Right(u) =>
        val varReference = getVariableReference(u, u.nameParts)

        if (!varReference.dataType.sameType(StringType)) {
          throw QueryCompilationErrors.invalidExecuteImmediateExpressionType(varReference.dataType)
        }

        // Call eval with null value passed instead of a row.
        // This is ok as this is variable and invoking eval should
        // be independent of row value.
        val varReferenceValue = varReference.eval(null)

        if (varReferenceValue == null) {
          throw QueryCompilationErrors.nullSQLStringExecuteImmediate(u.name)
        }

        varReferenceValue.toString
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case e @ ExecuteImmediateQuery(queryParam, args, targetVariables) =>
        // Check if all expressions are resolved (they should be resolved by ResolveReferences now)
        val queryParamResolved = queryParam.resolved
        val allArgsResolved = args.forall(_.resolved)
        val targetVariablesResolved = targetVariables.forall {
          case _: UnresolvedAttribute => false // Unresolved attributes are not resolved
          case alias: Alias => alias.child.resolved // For aliases, check if child is resolved
          case _: VariableReference => true // VariableReference is already resolved
          case expr => expr.resolved // For other expressions, use standard resolved check
        }

        // Validate that USING clause expressions don't contain unsupported constructs
        validateUsingClauseExpressions(args)

        if (queryParamResolved && allArgsResolved && targetVariablesResolved) {
          // All resolved - transform based on whether we have target variables
          if (targetVariables.nonEmpty) {
            // EXECUTE IMMEDIATE ... INTO should generate SetVariable plan
            // SetVariable expects UnresolvedAttribute objects that ResolveSetVariable will resolve
            val finalTargetVars = targetVariables.map {
              case attr: UnresolvedAttribute =>
                // Keep as UnresolvedAttribute for ResolveSetVariable to handle
                attr
              case alias: Alias =>
                // Extract the UnresolvedAttribute from the alias
                alias.child match {
                  case attr: UnresolvedAttribute =>
                    attr
                  case varRef: VariableReference =>
                    // Convert back to UnresolvedAttribute for ResolveSetVariable
                    UnresolvedAttribute(varRef.originalNameParts)
                  case _ =>
                    throw QueryCompilationErrors.unsupportedParameterExpression(alias.child)
                }
              case varRef: VariableReference =>
                // Convert back to UnresolvedAttribute for ResolveSetVariable
                UnresolvedAttribute(varRef.originalNameParts)
              case other =>
                throw QueryCompilationErrors.unsupportedParameterExpression(other)
            }

            // Validate that the query is suitable for INTO clause
            validateQueryForInto(queryParam)
            // Create SetVariable plan with the execute immediate query as source
            val sourceQuery = ExecuteImmediateCommand(queryParam, args)
            SetVariable(finalTargetVars, sourceQuery)
          } else {
            // Regular EXECUTE IMMEDIATE without INTO
            ExecuteImmediateCommand(queryParam, args)
          }
        } else {
          // Not all resolved yet - wait for next iteration
          e
        }
    }

  private def parseStatement(
      queryString: String,
      targetVariables: Seq[Expression]): LogicalPlan = {
    // If targetVariables is defined, statement needs to be a query.
    // Otherwise, it can be anything.
    val plan = if (targetVariables.nonEmpty) {
      try {
        catalogManager.v1SessionCatalog.parser.parseQuery(queryString)
      } catch {
        case e: ParseException =>
          // Since we do not have a way of telling that parseQuery failed because of
          // actual parsing error or because statement was passed where query was expected,
          // we need to make sure that parsePlan wouldn't throw
          catalogManager.v1SessionCatalog.parser.parsePlan(queryString)

          // Plan was successfully parsed, but query wasn't - throw.
          throw QueryCompilationErrors.invalidStatementForExecuteInto(queryString)
      }
    } else {
      catalogManager.v1SessionCatalog.parser.parsePlan(queryString)
    }

    if (plan.isInstanceOf[CompoundBody]) {
      throw QueryCompilationErrors.sqlScriptInExecuteImmediate(queryString)
    }

    // do not allow nested execute immediate
    if (plan.containsPattern(EXECUTE_IMMEDIATE)) {
      throw QueryCompilationErrors.nestedExecuteImmediate(queryString)
    }

    plan
  }



  private def isQueryResolved(query: Either[String, UnresolvedAttribute]): Boolean = {
    query match {
      case Left(_) => true // String literals are always resolved
      case Right(attr) => attr.resolved // Check if the attribute is resolved
    }
  }

  private def getVariableReference(expr: Expression, nameParts: Seq[String]): VariableReference = {
    variableResolution.lookupVariable(
      nameParts = nameParts,
      resolvingExecuteImmediate = AnalysisContext.get.isExecuteImmediate
    ) match {
      case Some(variable) => variable
      case _ =>
        throw QueryCompilationErrors
          .unresolvedVariableError(
            nameParts,
            Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE),
            expr.origin)
    }
  }

  private def validateQueryForInto(queryParam: Expression): Unit = {
    // Extract the query string to validate
    val queryString = queryParam.eval(null) match {
      case null => return // Will be caught later by other validation
      case value => value.toString
    }

    // If targetVariables is defined, statement needs to be a query.
    // Try to parse as query first, then as general plan
    try {
      catalogManager.v1SessionCatalog.parser.parseQuery(queryString)
      // Success - it's a valid query, proceed
    } catch {
      case e: ParseException =>
        // parseQuery failed, try parsePlan to see if it's valid SQL but not a query
        try {
          catalogManager.v1SessionCatalog.parser.parsePlan(queryString)
          // Plan was successfully parsed, but query wasn't - throw error
          throw QueryCompilationErrors.invalidStatementForExecuteInto(queryString)
        } catch {
          case _: ParseException =>
            // Both failed - let the original parse error propagate
            throw e
        }
    }
  }

  private def validateUsingClauseExpressions(args: Seq[Expression]): Unit = {
    import org.apache.spark.sql.catalyst.expressions.{ScalarSubquery, Exists, ListQuery, InSubquery}
    args.foreach { expr =>
      // Check the expression and its children for unsupported constructs
      expr.foreach {
        case subquery: ScalarSubquery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(subquery)
        case exists: Exists =>
          throw QueryCompilationErrors.unsupportedParameterExpression(exists)
        case listQuery: ListQuery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(listQuery)
        case inSubquery: InSubquery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(inSubquery)
        case _ => // Other expressions are fine
      }
    }
  }
}
