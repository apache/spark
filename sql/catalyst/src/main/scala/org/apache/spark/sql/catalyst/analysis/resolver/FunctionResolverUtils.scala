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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.Locale

import com.databricks.sql.expressions.Search

import org.apache.spark.sql.catalyst.analysis.{
  ResolvedStar,
  Star,
  UnresolvedFunction,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Trait with utility methods shared between [[FunctionResolver]] and
 * [[HigherOrderFunctionResolver]].
 */
trait FunctionResolverUtils {
  protected def expressionResolver: ExpressionResolver
  protected def conf: SQLConf

  private val scopes = expressionResolver.getNameScopes

  /**
   * Expand all star expressions in arguments. Separately handles 2 cases with count function:
   *  - `count(*)` is replaced with `count(1)`. See [[normalizeCountExpression]]
   *
   *  - `count(table.*)` throws an exception if the flag
   *    [[SQLConf.ALLOW_STAR_WITH_SINGLE_TABLE_IDENTIFIER_IN_COUNT]] is false.
   *    (see [[assertSingleTableStarNotInCountFunction]])
   *    It is done to avoid confusion since `count(*)` and `count(table.*)` would produce
   *    different results:
   *    - `count(*)` returns the number of rows
   *    - `count(table.*)` returns the number of rows where all columns are not null. It's the same
   *    behavior as if explicitly listing all columns of the table in count.
   *
   // BEGIN-EDGE
   * This method also separately handles cases with search functions which have special semantics
   * for star expansion. See [[Search.expandStarExpression]] for more details.
   // END-EDGE
   * Returns [[UnresolvedFunction]] without any star expressions in arguments.
   */
  protected def handleStarInArguments(
      unresolvedFunction: UnresolvedFunction): UnresolvedFunction = {
    val functionContainsStarInArguments = unresolvedFunction.arguments.exists {
      case _: Star => true
      case _ => false
    }

    if (!functionContainsStarInArguments) {
      unresolvedFunction
    } else if (isNonDistinctCount(unresolvedFunction) &&
      hasSingleSimpleStarArgument(unresolvedFunction)) {
      normalizeCountExpression(unresolvedFunction)
      // BEGIN-EDGE
    } else if (Search.isSearchFunction(unresolvedFunction)) {
      Search.expandStarExpression(
        function = unresolvedFunction,
        expand = s => expressionResolver.expandStarExpressions(s :: Nil)
      )
      // END-EDGE
    } else {
      assertSingleTableStarNotInCountFunction(unresolvedFunction)
      unresolvedFunction.copy(
        arguments = expressionResolver.expandStarExpressions(unresolvedFunction.arguments)
      )
    }
  }

  /**
   * Check if the given unresolved function has one `*` as argument. Usually used to detect cases
   * where `count(*)` should be replaced with `count(1)`.
   *
   * Returns True for [[ResolvedStar]] and [[UnresolvedStar]] without specified target.
   *
   * Note that it's False even for other implementation of [[Star]] trait, for example
   * [[UnresolvedStarExceptOrReplace]] (`* except ...`) or [[UnresolvedStar]] with specified
   * target (`table.*`).
   */
  private def hasSingleSimpleStarArgument(unresolvedFunction: UnresolvedFunction): Boolean =
    unresolvedFunction.arguments match {
      case Seq(UnresolvedStar(None)) => true
      case Seq(_: ResolvedStar) => true
      case _ => false
    }

  /**
   * Method used to determine whether the given function is non-distinct `count` function,
   * with optional normalization.
   */
  private def isNonDistinctCount(
      unresolvedFunction: UnresolvedFunction,
      normalizeFunctionName: Boolean = true
  ): Boolean = {
    !unresolvedFunction.isDistinct && isCount(unresolvedFunction, normalizeFunctionName)
  }

  private def isCount(
      unresolvedFunction: UnresolvedFunction,
      normalizeFunctionName: Boolean = true
  ): Boolean = {
    val isCountName = if (normalizeFunctionName) {
      unresolvedFunction.nameParts.head.toLowerCase(Locale.ROOT) == "count"
    } else {
      unresolvedFunction.nameParts.head == "count"
    }

    unresolvedFunction.nameParts.length == 1 && isCountName
  }

  /**
   * Method used to replace the `count(*)` function with `count(1)` function. Resolution of the
   * `count(*)` is done in the following way:
   *  - SQL: It is done during the construction of the AST (in [[AstBuilder]]).
   *  - Dataframes: It is done during the analysis phase and that's why we need to do it here.
   */
  private def normalizeCountExpression(
      unresolvedFunction: UnresolvedFunction): UnresolvedFunction = {
    unresolvedFunction.copy(
      nameParts = Seq("count"),
      arguments = Seq(Literal(1)),
      filter = unresolvedFunction.filter
    )
  }

  /**
   * Throws an exception according to [[SQLConf.ALLOW_STAR_WITH_SINGLE_TABLE_IDENTIFIER_IN_COUNT]].
   *
   * Note that check for function name is case-sensitive. Even when flag is false we allow
   * `COUNT(tableName.*)` but block `count(tableName.*)`. This is the same behavior as in
   * fixed-point analyzer, and clearly it is a bug. If this is ever fixed it should be done in both
   * analyzers simultaneously.
   *
   * See [[handleStarInArguments]]
   */
  private def assertSingleTableStarNotInCountFunction(
      unresolvedFunction: UnresolvedFunction): Unit = {
    if (!conf.allowStarWithSingleTableIdentifierInCount && isCount(
        unresolvedFunction = unresolvedFunction,
        normalizeFunctionName = false
      ) && unresolvedFunction.arguments.length == 1) {
      unresolvedFunction.arguments.head match {
        case star: UnresolvedStar if scopes.current.isStarQualifiedByTable(star) =>
          throw QueryCompilationErrors
            .singleTableStarInCountNotAllowedError(star.target.get.mkString("."))
        case _ =>
      }
    }
  }
}
