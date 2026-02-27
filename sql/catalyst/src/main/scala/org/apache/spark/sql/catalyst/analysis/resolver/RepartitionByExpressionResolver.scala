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


import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.types.{DataType, VariantType}

/**
 * Resolver class that resolves [[RepartitionByExpression]] operators.
 */
class RepartitionByExpressionResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[RepartitionByExpression, LogicalPlan]
    with ResolvesNameByHiddenOutput {

  private val scopes = resolver.getNameScopes
  private val operatorResolutionContextStack = resolver.getOperatorResolutionContextStack

  /**
   * Resolves a [[RepartitionByExpression]] operator. This involves:
   *  1. Resolving the child operator.
   *  2. Resolving the partition expressions using the
   *     [[ExpressionResolve.resolveExpressionTreeInOperator]].
   *  3. In case the initial expressions list is non-empty, collect all the attributes referenced
   *     from the hidden output. This can happen in the following query:
   *
   *     {{{
   *       SELECT col1 FROM values(1, 2) WHERE col2 = 1 DISTRIBUTE BY col2;
   *     }}}
   *
   *     Parsed plan looks like:
   *
   *     {{{
   *       'RepartitionByExpression ['col2]
   *       +- 'Project ['col1]
   *          +- 'Filter ('col2 = 1)
   *             +- LocalRelation [col1#0, col2#1]
   *     }}}
   *
   *     Here `col2` from the `DISTRIBUTE BY` would be resolved using the hidden output and thus
   *     the analyzed plan looks like:
   *
   *     {{{
   *       Project [col1#0]
   *       +- RepartitionByExpression [col2#1]
   *          +- Project [col1#0, col2#1]
   *             +- Filter (col2#1 = 1)
   *                +- LocalRelation [col1#0, col2#1]
   *     }}}
   *  4. Inserting any missing attributes from the hidden output into the child operator.
   *  5. Retaining the original output of the operator (see [[ResolvesNameByHiddenOutput]] scala
   *     doc for more info).
   */
  override def resolve(repartitionByExpression: RepartitionByExpression): LogicalPlan = {
    val resolvedChild = resolver.resolve(repartitionByExpression.child)
    val resolvedExpressions = repartitionByExpression.expressions.map { expression =>
      expressionResolver.resolveExpressionTreeInOperator(
        unresolvedExpression = expression,
        parentOperator = repartitionByExpression
      )
    }

    val missingAttributes: Seq[Attribute] =
      if (repartitionByExpression.partitionExpressions.isEmpty) {
        Seq.empty
      } else {
        scopes.current.resolveMissingAttributesByHiddenOutput(
          expressionResolver.getLastReferencedAttributes
        )
      }

    val resolvedChildWithMissingAttributes =
      insertMissingExpressions(resolvedChild, missingAttributes)

    val resolvedRepartitionByExpression = repartitionByExpression.copy(
      partitionExpressions = resolvedExpressions,
      child = resolvedChildWithMissingAttributes
    )

    validateRepartitionByVariant(resolvedRepartitionByExpression)

    if (!resolvedChildWithMissingAttributes.eq(resolvedChild)) {
      retainOriginalOutput(
        operator = resolvedRepartitionByExpression,
        missingExpressions = missingAttributes,
        scopes = scopes,
        operatorResolutionContextStack = operatorResolutionContextStack
      )
    } else {
      resolvedRepartitionByExpression
    }
  }

  private def validateRepartitionByVariant(
      repartitionByExpression: RepartitionByExpression): Unit = {
    if (false) {
      val variantExpressionInPartitionExpression =
        repartitionByExpression.partitionExpressions.find(e => hasVariantType(e.dataType))

      if (variantExpressionInPartitionExpression.isDefined) {
        val variantExpr = variantExpressionInPartitionExpression.get
        repartitionByExpression.failAnalysis(
          errorClass = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
          messageParameters =
            Map("expr" -> toSQLExpr(variantExpr), "dataType" -> toSQLType(variantExpr.dataType))
        )
      }
    }
  }

  private def hasVariantType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[VariantType])
  }
}
