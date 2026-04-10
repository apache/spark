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

import java.util.HashSet

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{UnpivotTransformer, UnpivotTypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  ExprId,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan, Unpivot}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Resolver class that resolves [[Unpivot]] operators.
 */
class UnpivotResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Unpivot, LogicalPlan] {
  private val scopes: NameScopeStack = operatorResolver.getNameScopes
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner

  /**
   * Resolves an [[Unpivot]] operator. Do that using the following steps:
   *  1. Resolve the child operator.
   *  2. Resolve the ids and values expressions based on their presence (see respective scala docs
   *     for more info).
   *  3. Perform type coercion on the values expressions.
   *  4. Validate the lengths of the values expressions.
   *  5. Use the [[UnpivotTransformer]] to create the resolved operator (see its scala doc for more
   *     info).
   *  6. Map the output attributes to new ones with unique expression ids. This is needed since the
   *     [[UnpivotTransformer]] introduces new attributes (in the [[Expand.output]]).
   *  7. Overwrite the current scope's output with the resolved operator's output.
   *
   * For example in the given query:
   *
   * {{{
   *   SELECT * FROM VALUES (2024, 15000, 20000) AS sales(year, q1, q2)
   *   UNPIVOT (revenue FOR quarter IN (q1 AS `Q1`, q2 AS `Q2`));
   * }}}
   *
   * The unresolved plan would be:
   *
   * {{{
   *   'Project [*]
   *   +- 'Filter isnotnull(coalesce('revenue))
   *      +- 'Unpivot List(List('q1), List('q2)), List(Some(Q1), Some(Q2)), quarter, [revenue]
   *         +- SubqueryAlias sales
   *            +- LocalRelation [year#0, q1#1, q2#2]
   * }}}
   *
   * Whereas the resolved plan would be:
   *
   * {{{
   *   Project [year#0, quarter#4, revenue#5]
   *   +- Filter isnotnull(coalesce(revenue#5))
   *      +- Expand [[year#0, Q1, q1#1], [year#0, Q2, q2#2], [year#0, quarter#4, revenue#5]
   *         +- SubqueryAlias sales
   *            +- LocalRelation [year#0, q1#1, q2#2]
   * }}}
   */
  override def resolve(unpivot: Unpivot): LogicalPlan = {
    val resolvedChild = operatorResolver.resolve(unpivot.child)

    val (resolvedIds, resolvedValues) = (unpivot.ids, unpivot.values) match {
      case (Some(ids), Some(values)) => handleIdsDefinedValuesDefined(ids, values, unpivot)
      case (Some(ids), None) => handleIdsDefinedValuesUndefined(ids, unpivot)
      case (None, Some(values)) => handleIdsUndefinedValuesDefined(values, unpivot)
      case (None, None) => handleIdsUndefinedValuesUndefined()
    }

    val partiallyResolvedUnpivot =
      unpivot.copy(ids = Some(resolvedIds), values = Some(resolvedValues))

    val typeCoercedUnpivot = UnpivotTypeCoercion(partiallyResolvedUnpivot)

    validateValuesTypeCoercioned(typeCoercedUnpivot)

    validateValuesLength(typeCoercedUnpivot.values.get, typeCoercedUnpivot)

    val resolvedOperator: Expand = UnpivotTransformer(
      ids = typeCoercedUnpivot.ids.get,
      values = typeCoercedUnpivot.values.get,
      aliases = typeCoercedUnpivot.aliases,
      variableColumnName = typeCoercedUnpivot.variableColumnName,
      valueColumnNames = typeCoercedUnpivot.valueColumnNames,
      child = resolvedChild
    )

    val mappedOutput = resolvedOperator.output.map { attribute =>
      expressionIdAssigner.mapExpression(attribute, allowUpdatesForAttributeReferences = true)
    }

    val finalOperator = resolvedOperator.copy(output = mappedOutput)

    scopes.overwriteOutputAndExtendHiddenOutput(output = mappedOutput)

    finalOperator
  }

  /**
   * If both ids and values are defined, resolve them both. Below is an example in SQL:
   *
   * {{{
   *   SELECT * FROM VALUES (2024, 15000, 20000) AS sales(year, q1, q2)
   *   UNPIVOT (revenue FOR quarter IN (q1 AS `Q1`, q2 AS `Q2`));
   * }}}
   */
  private def handleIdsDefinedValuesDefined(
      ids: Seq[Expression],
      values: Seq[Seq[Expression]],
      unpivot: Unpivot): (Seq[NamedExpression], Seq[Seq[NamedExpression]]) = {
    val resolvedIds = resolveIds(ids, unpivot)
    val resolvedValues = resolveValues(values, unpivot)
    (resolvedIds, resolvedValues)
  }

  /**
   * If ids are defined but values are not, resolve the ids and validate them. Then, infer the
   * values by taking all output attributes from the current scope that are not part of the ids.
   * Finally, resolve the inferred values. This can happen in DFs:
   * {{{
   *   df.unpivot(
   *     ids = Array($"id"),
   *     values = Array.empty,
   *     variableColumnName = "var",
   *     valueColumnName = "val"
   *   )
   * }}}
   */
  private def handleIdsDefinedValuesUndefined(
      ids: Seq[Expression],
      unpivot: Unpivot): (Seq[NamedExpression], Seq[Seq[NamedExpression]]) = {
    val resolvedIds = resolveIds(ids, unpivot)

    validateIds(resolvedIds)

    val resolvedIdExprIds = new HashSet[ExprId](resolvedIds.size)
    resolvedIds.foreach { id =>
      resolvedIdExprIds.add(id.exprId)
    }
    val values = scopes.current.output.filterNot { attr =>
      resolvedIdExprIds.contains(attr.exprId)
    }

    val expandedValues = values.map(Seq(_))

    (resolvedIds, expandedValues)
  }

  /**
   * If values are defined but ids are not, resolve the values and validate them. Then, infer the
   * ids by taking all output attributes from the current scope that are not part of the values.
   * Finally, resolve the inferred ids. This can happen in DFs:
   * {{{
   *   df.unpivot(
   *     ids = Array.empty,
   *     values = Array($"str1", $"str2"),
   *     variableColumnName = "var",
   *     valueColumnName = "val"
   *   )
   * }}}
   */
  private def handleIdsUndefinedValuesDefined(
      values: Seq[Seq[Expression]],
      unpivot: Unpivot): (Seq[NamedExpression], Seq[Seq[NamedExpression]]) = {
    val resolvedValues = resolveValues(values, unpivot)

    validateValues(resolvedValues)

    val flattenedValues = resolvedValues.flatten
    val resolvedValueExprIds = new HashSet[ExprId](flattenedValues.size)
    flattenedValues.foreach { value =>
      resolvedValueExprIds.add(value.exprId)
    }
    val ids = scopes.current.output.filterNot { attr =>
      resolvedValueExprIds.contains(attr.exprId)
    }

    (ids, resolvedValues)
  }

  private def handleIdsUndefinedValuesUndefined(): Nothing = {
    throw SparkException.internalError("Both UNPIVOT ids and values cannot be None")
  }

  private def resolveIds(ids: Seq[Expression], unpivot: Unpivot): Seq[NamedExpression] = {
    expressionResolver.resolveUnpivotArguments(ids, unpivot)
  }

  /**
   * Resolves each value group in `values` using `expressionResolver.resolveUnpivotArguments`.
   * For each value group:
   *  - If a single expression expanded to multiple (star expansion), each resolved element
   *    becomes its own value group. Example:
   *
   *    {{{
   *      df.select($"str1", $"str2").unpivot(
   *         Array.empty,
   *         Array($"*"),
   *         variableColumnName = "var",
   *         valueColumnName = "val"
   *      )
   *    }}}
   *
   *    Here we would expand the star into two resolved expressions (`str1` and `str2`) and then
   *    separate them into their own value groups: `Seq(Seq(str1), Seq(str2))`.
   *  - Otherwise, all resolved elements stay together in one value group.
   * The results are flattened into a single sequence of value groups at the end.
   */
  private def resolveValues(
      values: Seq[Seq[Expression]],
      unpivot: Unpivot): Seq[Seq[NamedExpression]] = {
    values.flatMap { valueGroup =>
      val resolved = expressionResolver.resolveUnpivotArguments(valueGroup, unpivot)
      if (valueGroup.length == 1 && resolved.length > 1) {
        resolved.map(Seq(_))
      } else {
        Seq(resolved)
      }
    }
  }

  private def validateIds(ids: Seq[NamedExpression]): Unit = {
    val hasNonAttributeExpression = ids.exists {
      case _: AttributeReference => false
      case _ => true
    }
    if (hasNonAttributeExpression) {
      throw QueryCompilationErrors.unpivotRequiresAttributes("id", "value", ids)
    }
  }

  private def validateValues(values: Seq[Seq[NamedExpression]]): Unit = {
    val hasNonAttributeExpression = values.exists { valueGroup =>
      valueGroup.exists {
        case _: AttributeReference => false
        case _ => true
      }
    }
    if (hasNonAttributeExpression) {
      throw QueryCompilationErrors.unpivotRequiresAttributes("value", "id", values.flatten)
    }
  }

  private def validateValuesTypeCoercioned(typeCoercedUnpivot: Unpivot): Unit = {
    if (!typeCoercedUnpivot.valuesTypeCoercioned) {
      throw QueryCompilationErrors.unpivotValueDataTypeMismatchError(
        typeCoercedUnpivot.values.get
      )
    }
  }

  private def validateValuesLength(values: Seq[Seq[NamedExpression]], unpivot: Unpivot): Unit = {
    if (values.exists(_.length != unpivot.valueColumnNames.length)) {
      throw QueryCompilationErrors.unpivotValueSizeMismatchError(unpivot.valueColumnNames.length)
    }
  }
}
