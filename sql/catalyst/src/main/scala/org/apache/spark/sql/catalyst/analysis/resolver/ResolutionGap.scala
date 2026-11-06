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

/**
 * A known single-pass coverage gap reported by [[ExplicitlyUnsupportedResolverFeature]]. Its
 * `name` is logged unredacted as the single-pass resolution gap, so it must never contain customer
 * identifiers or data. The class is sealed so that every gap is defined in this file, with a `name`
 * built from constant strings or class names only.
 */
sealed abstract class ResolutionGap(val name: String)

object ResolutionGap {
  case object SQLFunctions extends ResolutionGap("SQL function expression")
  case object ViewBody extends ResolutionGap("View body")
  case object NonSQLFunction extends ResolutionGap("non-SQL function")
  case object DefaultReferencesInSetVariable
      extends ResolutionGap("DEFAULT references in SET VARIABLE")
  case object NonFoldableInlineTable
      extends ResolutionGap("VALUES lists with expressions that are evaluated when the query runs")
  case object ScalarSubqueryInInlineTable
      extends ResolutionGap("scalar subqueries in VALUES lists")
  case object AggregateFunctionsInOrderByWithGroupingAnalytics
      extends ResolutionGap("aggregate functions in ORDER BY over grouping analytics")
  case object MetadataColumnsReferencedByNestedOperators
      extends ResolutionGap("metadata columns referenced by both an operator and its ancestor")
  case object MissingExpressionsBelowPipeOperator
      extends ResolutionGap("hidden columns referenced across a pipe operator")
  case object UnresolvedColumnInHavingWithGroupingAnalytics
      extends ResolutionGap("unresolved columns in HAVING over grouping analytics")
  case object NaNPivotValues extends ResolutionGap("NaN pivot values")
  case object MissingAttributesInsideAndOutsideLambdaFunctions
      extends ResolutionGap("hidden columns referenced both inside and outside lambda functions")

  /** An operator that single-pass does not resolve, named by its class. */
  final case class UnsupportedOperator(operatorClass: Class[_])
      extends ResolutionGap(s"unsupported operator: ${operatorClass.getName}")

  /** A catalog function that is not a SQL function, named by its expression class. */
  final case class NonSQLCatalogFunction(expressionClass: Class[_])
      extends ResolutionGap(s"non-SQL catalog function: ${expressionClass.getSimpleName}")
}
