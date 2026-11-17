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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.immutable.VectorMap
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, Attribute, CreateNamedStruct, Expression, GetStructField, If, IsNull, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable, SortOrder, TransformValues}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.variant.CanonicalizeVariant
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, REPARTITION_OPERATION}
import org.apache.spark.sql.types.{ArrayType, MapType, StructType, VariantType}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

/**
 * Adds [[CanonicalizeVariant]] to grouping expressions and distinct aggregate arguments that
 * contain Variant values, ensuring logically equal Variants have identical bytes before
 * aggregation:
 *
 * SELECT variant_column, COUNT(*) FROM TABLE GROUP BY variant_column =>
 * SELECT _groupingvariantcanonicalization AS variant_column, COUNT(*) FROM (
 *   SELECT canonicalize_variant(variant_column) AS _groupingvariantcanonicalization FROM TABLE
 * ) GROUP BY _groupingvariantcanonicalization
 *
 * SELECT COUNT(DISTINCT variant_column) FROM TABLE =>
 * SELECT COUNT(DISTINCT _distinctvariantcanonicalization) FROM (
 *   SELECT canonicalize_variant(variant_column) AS _distinctvariantcanonicalization FROM TABLE
 * )
 *
 * This rule runs after [[PullOutGroupingExpressions]], so grouping keys are attributes or
 * literals. Distinct arguments are normalized here instead of in [[RewriteDistinctAggregates]]
 * because a single distinct group without a filter bypasses that rule and is handled by the
 * physical planner.
 */
object InsertVariantCanonicalizationInAggregate extends Rule[LogicalPlan] {
  import InsertVariantCanonicalization._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(AGGREGATE)) {
      return plan
    }
    val shouldRewrite = plan.exists {
      case agg: Aggregate =>
        agg.groupingExpressions.exists(variantTypeExistsRecursively) ||
          distinctAggregateChildren(agg.aggregateExpressions).exists(variantTypeExistsRecursively)
      case _ => false
    }
    if (!shouldRewrite) {
      return plan
    }

    plan transformUpWithNewOutput {
      case agg @ Aggregate(groupingExprs, aggregateExpressions, child, hint) =>
        val distinctExpressions = distinctAggregateChildren(aggregateExpressions)
        if (!(groupingExprs ++ distinctExpressions).exists(variantTypeExistsRecursively)) {
          agg -> Nil
        } else {
          val groupingAliases = canonicalizeExpressions(
            groupingExprs, "_groupingvariantcanonicalization")
          val distinctInputAliases =
            createDistinctInputAliases(distinctExpressions, groupingAliases)
          val distinctAliases = canonicalizeExpressions(
            distinctExpressions,
            "_distinctvariantcanonicalization",
            reusableAliases = groupingAliases,
            inputAliases = distinctInputAliases)
          if (groupingAliases.isEmpty && distinctAliases.isEmpty) {
            agg -> Nil
          } else {
            val newGroupingKeys = groupingExprs.map { expr =>
              groupingAliases.get(expr.canonicalized).map(_.toAttribute).getOrElse(expr)
            }
            val newAggregateExprs = aggregateExpressions.map {
              case named if groupingAliases.contains(named.canonicalized) =>
                groupingAliases(named.canonicalized).toAttribute.withName(named.name)
              case other =>
                // Replace whole distinct arguments before substituting grouping references.
                other.transformDown {
                  case ae: AggregateExpression if ae.isDistinct =>
                    ae.copy(aggregateFunction = ae.aggregateFunction.withNewChildren(
                      ae.aggregateFunction.children.map { argument =>
                        distinctAliases.get(argument.canonicalized)
                          .map(_.toAttribute).getOrElse(argument)
                      }).asInstanceOf[AggregateFunction])
                  case e =>
                    groupingAliases.get(e.canonicalized).map(_.toAttribute).getOrElse(e)
                }.asInstanceOf[NamedExpression]
            }
            val distinctInput = if (distinctInputAliases.nonEmpty) {
              // Struct reconstruction can reference a complex input more than once.
              Project(child.output ++ distinctInputAliases.values, child)
            } else {
              child
            }
            val newChild = Project(
              child.output ++ (groupingAliases ++ distinctAliases).values,
              distinctInput)
            val newAgg = Aggregate(newGroupingKeys, newAggregateExprs, newChild, hint)
            newAgg -> agg.output.zip(newAgg.output)
          }
        }
    }
  }

  private def distinctAggregateChildren(
      aggregateExpressions: Seq[NamedExpression]): Seq[Expression] = {
    aggregateExpressions.flatMap(_.collect {
      case ae: AggregateExpression if ae.isDistinct => ae.aggregateFunction.children
    }).flatten
  }

  private def createDistinctInputAliases(
      expressions: Seq[Expression],
      groupingAliases: Map[Expression, NamedExpression])
    : VectorMap[Expression, NamedExpression] = {
    val aliases = new mutable.LinkedHashMap[Expression, NamedExpression]
    expressions.foreach {
      case _: Attribute =>
      // Only the struct rebuild in `canonicalizeVariantsRecursively` references its input more
      // than once (null check plus one `GetStructField` per field); arrays and maps go through a
      // single lambda argument, so only complex struct inputs are projected first.
      case expr if expr.dataType.isInstanceOf[StructType] &&
          variantTypeExistsRecursively(expr) && !groupingAliases.contains(expr.canonicalized) =>
        aliases.getOrElseUpdate(
          expr.canonicalized, Alias(expr, "_distinctaggregateexpression")())
      case _ =>
    }
    VectorMap.from(aliases)
  }

  private def canonicalizeExpressions(
      expressions: Seq[Expression],
      aliasName: String,
      reusableAliases: Map[Expression, NamedExpression] = Map.empty,
      inputAliases: Map[Expression, NamedExpression] = Map.empty)
    : VectorMap[Expression, NamedExpression] = {
    val aliases = new mutable.LinkedHashMap[Expression, NamedExpression]
    expressions.foreach { expr =>
      val canonicalized = expr.canonicalized
      val input = inputAliases.get(canonicalized).map(_.toAttribute).getOrElse(expr)
      val inserted = canonicalizeVariantsRecursively(input)
      if (input.ne(inserted)) {
        aliases.getOrElseUpdate(
          canonicalized,
          reusableAliases.getOrElse(canonicalized, Alias(inserted, aliasName)()))
      }
    }
    VectorMap.from(aliases)
  }
}

/**
 * Adds [[CanonicalizeVariant]] to [[RepartitionByExpression]] expressions containing Variant
 * values, so logically equal Variants hash to the same partition:
 *
 * SELECT * FROM TABLE DISTRIBUTE BY variant_column =>
 * SELECT * FROM TABLE DISTRIBUTE BY canonicalize_variant(variant_column)
 */
object InsertVariantCanonicalizationInRepartitionExpressions extends Rule[LogicalPlan] {
  import InsertVariantCanonicalization._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsPattern(REPARTITION_OPERATION)) {
      case rep: RepartitionByExpression
          if rep.partitionExpressions.exists(variantTypeExistsRecursively) =>
        rep.copy(partitionExpressions = rep.partitionExpressions.map {
          // Range partitioning keys are wrapped in `SortOrder`, which only exposes its child's
          // data type and cannot be evaluated itself.
          case order: SortOrder => order.copy(child = canonicalizeVariantsRecursively(order.child))
          case expr => canonicalizeVariantsRecursively(expr)
        })
    }
  }
}

private[optimizer] object InsertVariantCanonicalization {

  def variantTypeExistsRecursively(expr: Expression): Boolean = {
    expr.dataType.existsRecursively(_ == VariantType)
  }

  /** Canonicalizes Variant results, including those nested in SQL containers. */
  def canonicalizeVariantsRecursively(expr: Expression): Expression = {
    expr.dataType match {
      case VariantType => CanonicalizeVariant(expr)

      case StructType(fields) if variantTypeExistsRecursively(expr) =>
        val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (field, i) =>
          Seq(Literal(field.name), canonicalizeVariantsRecursively(
            GetStructField(expr, i, Some(field.name))))
        }.toImmutableArraySeq)
        if (expr.nullable) {
          If(IsNull(expr), Literal(null, struct.dataType), struct)
        } else {
          struct
        }

      case ArrayType(elementType, containsNull) if variantTypeExistsRecursively(expr) =>
        val element = NamedLambdaVariable("x", elementType, containsNull)
        ArrayTransform(expr,
          LambdaFunction(canonicalizeVariantsRecursively(element), Seq(element)))

      case MapType(keyType, valueType, valueContainsNull)
          if valueType.existsRecursively(_ == VariantType) =>
        val key = NamedLambdaVariable("key", keyType, nullable = false)
        val value = NamedLambdaVariable("value", valueType, valueContainsNull)
        TransformValues(expr,
          LambdaFunction(canonicalizeVariantsRecursively(value), Seq(key, value)))

      case _ => expr
    }
  }
}
