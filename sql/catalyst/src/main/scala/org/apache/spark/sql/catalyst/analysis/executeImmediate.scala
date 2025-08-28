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

import org.apache.spark.sql.catalyst.analysis.{NamedParameter, PosParameter}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, VariableReference}
import org.apache.spark.sql.catalyst.parser.{ParameterHandler, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXECUTE_IMMEDIATE, TreePattern}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Logical plan representing execute immediate query.
 *
 * @param args parameters of query
 * @param query query string or variable
 * @param targetVariables variables to store the result of the query
 */
case class ExecuteImmediateQuery(
    args: Seq[Expression],
    query: Either[String, UnresolvedAttribute],
    targetVariables: Seq[UnresolvedAttribute])
  extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * This rule substitutes execute immediate query node with fully analyzed
 * plan that is passed as string literal or session parameter.
 */
class SubstituteExecuteImmediate(
    val catalogManager: CatalogManager,
    resolveChild: LogicalPlan => LogicalPlan,
    checkAnalysis: LogicalPlan => Unit)
  extends Rule[LogicalPlan] with ColumnResolutionHelper {

  private val parameterHandler = new ParameterHandler()

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
          throw QueryCompilationErrors.invalidExecuteImmediateVariableType(varReference.dataType)
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

  /**
   * Performs constant folding on expressions to convert foldable expressions to literals.
   * Uses the same approach as the ConstantFolding optimizer rule and tryEvalExpr pattern.
   */
  private def foldToLiteral(expr: Expression): Expression = {
    expr match {
      case alias: Alias =>
        if (alias.child.foldable) {
          Literal.create(alias.child.eval(), alias.child.dataType)
        } else {
          alias.child
        }
      case e if e.foldable =>
          Literal.create(e.eval(), e.dataType)
      case other => other
    }
  }



  /**
   * Convert an expression to its SQL string representation.
   */
  private def expressionToSqlValue(expr: Expression): String = expr match {
    case lit: Literal => lit.sql
    case _ =>
      try {
        expr.sql
      } catch {
        case _: Exception =>
          // Fall back to constant folding if SQL generation fails and expression is foldable
          if (expr.foldable) {
            try {
              val literal = Literal.create(expr.eval(), expr.dataType)
              literal.sql
            } catch {
              case _: Exception => expr.toString
            }
          } else {
            expr.toString
          }
      }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case e @ ExecuteImmediateQuery(expressions, _, _) if expressions.exists(!_.resolved) =>
        e.copy(args = resolveArguments(expressions))

      case ExecuteImmediateQuery(expressions, query, targetVariables)
        if expressions.forall(_.resolved) =>

        val queryString = extractQueryString(query)
        // Apply parameter substitution to the query string before parsing
        val substitutedQueryString = applyParameterSubstitution(queryString, expressions)

        // Set the origin to use the inner query string for proper error context
        val innerOrigin = org.apache.spark.sql.catalyst.trees.Origin(
          sqlText = Some(substitutedQueryString),
          startIndex = Some(0),
          stopIndex = Some(substitutedQueryString.length - 1)
        )
        val plan = org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin(innerOrigin) {
          parseStatement(substitutedQueryString, targetVariables)
        }
        // Update the origins of any Parameter nodes to use the inner query context
        val planWithUpdatedOrigins = plan.transformAllExpressions {
          case np: NamedParameter =>
            org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin(innerOrigin) {
              NamedParameter(np.name)
            }
          case pp: PosParameter =>
            org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin(innerOrigin) {
              PosParameter(pp.pos)
            }
          case other => other
        }

        val posNodes = planWithUpdatedOrigins.collect { case p: LogicalPlan =>
          p.expressions.flatMap(_.collect { case n: PosParameter => n })
        }.flatten
        val namedNodes = planWithUpdatedOrigins.collect { case p: LogicalPlan =>
          p.expressions.flatMap(_.collect { case n: NamedParameter => n })
        }.flatten

        val queryPlan = if (expressions.isEmpty || (posNodes.isEmpty && namedNodes.isEmpty)) {
          planWithUpdatedOrigins
        } else if (posNodes.nonEmpty && namedNodes.nonEmpty) {
          throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
        } else {
          if (posNodes.nonEmpty) {
            // Apply constant folding to positional parameters
            val foldedArgs = expressions.map(foldToLiteral)
            PosParameterizedQuery(planWithUpdatedOrigins, foldedArgs)
          } else {
            val aliases = expressions.collect {
              case e: Alias => e
              case u: VariableReference => Alias(u, u.identifier.name())()
            }

            if (aliases.size != expressions.size) {
              val nonAliases = expressions.filter(attr =>
                !attr.isInstanceOf[Alias] && !attr.isInstanceOf[VariableReference])

              throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(nonAliases)
            }

            // Extract names before folding
            val names = aliases.map(_.name)
            // Apply constant folding to named parameters
            val values = aliases.map(foldToLiteral)

            NameParameterizedQuery(
              planWithUpdatedOrigins,
              names,
              // We need to resolve arguments before Resolution batch to make sure
              // that some rule does not accidentally resolve our parameters.
              // We do not want this as they can resolve some unsupported parameters.
              values)
          }
        }

        // Fully analyze the generated plan. AnalysisContext.withExecuteImmediateContext makes sure
        // that SQL scripting local variables will not be accessed from the plan.
        // Use the inner query origin for proper error context during analysis
        val finalPlan = org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin(innerOrigin) {
          AnalysisContext.withExecuteImmediateContext {
            resolveChild(queryPlan)
          }
        }
        org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin(innerOrigin) {
          checkAnalysis(finalPlan)
        }

        if (targetVariables.nonEmpty) {
          SetVariable(targetVariables, finalPlan)
        } else { finalPlan }
    }

  /**
   * Apply parameter substitution to a query string using the provided expressions.
   */
  private def applyParameterSubstitution(
      queryString: String,
      expressions: Seq[Expression]): String = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!queryString.contains("?") && !queryString.contains(":")) {
      return queryString  // No parameter markers possible
    }

    // Detect parameter types and validate consistency
    val (hasPositional, hasNamed) = parameterHandler.detectParameters(queryString)
    if (!hasPositional && !hasNamed) {
      return queryString  // No parameters to substitute
    }

    // Check for mixed parameters before substitution
    if (hasPositional && hasNamed) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }

    // Always resolve expressions first
    val resolvedExprs = resolveArguments(expressions)

    // Convert expressions to parameter values based on what the SQL query needs
    if (hasNamed) {
      // For named parameters, extract names from resolved expressions
      val namedParamsBuilder = scala.collection.mutable.Map[String, Expression]()
      val unnamedExprs = scala.collection.mutable.ListBuffer[Expression]()

      resolvedExprs.foreach {
        case Alias(child, name) =>
          // Explicit alias provides the name
          namedParamsBuilder(name) = foldToLiteral(child)
        case vr: VariableReference =>
          // Variable reference provides name from variable identifier
          namedParamsBuilder(vr.identifier.name()) = foldToLiteral(vr)
        case other =>
          // Expression that can't provide a name for named parameters
          unnamedExprs += other
      }

      // If we have unnamed expressions for named parameters, error
      if (unnamedExprs.nonEmpty) {
        throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(unnamedExprs.toSeq)
      }

      parameterHandler.substituteNamedParameters(queryString, namedParamsBuilder.toMap)
    } else {
      // For positional parameters, process all resolved expressions positionally
      val positionalParams = resolvedExprs.map(foldToLiteral)
      parameterHandler.substitutePositionalParameters(queryString, positionalParams)
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

  private def getVariableReference(expr: Expression, nameParts: Seq[String]): VariableReference = {
    lookupVariable(nameParts) match {
      case Some(variable) => variable
      case _ =>
        throw QueryCompilationErrors
          .unresolvedVariableError(
            nameParts,
            Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE),
            expr.origin)
    }
  }
}
