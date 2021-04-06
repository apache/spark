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
package org.apache.spark.sql.execution.datasources.v2.pushdown.sql

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{DecimalType, DoubleType}


object PushDownAggUtils {
  /**
   * A single aggregate expression might appear multiple times in resultExpressions.
   * <p/>
   * In order to avoid evaluating an individual aggregate function multiple times, we'll
   * build a seq of the distinct aggregate expressions and build a function which can
   * be used to re-write expressions so that they reference the single copy of the
   * aggregate function which actually gets computed.
   */
  def getAggregationToPushedAliasMap(
    resultExpressions: Seq[NamedExpression],
    createAliasName: Option[() => String] = None)
  : Map[AggregateExpression, Seq[Alias]] = {
    val (nonDistinctAggregateExpressions, aggregateExpressionsToNamedExpressions) =
      resultExpressions.foldLeft(
        Seq.empty[AggregateExpression] -> Map.empty[AggregateExpression, Set[NamedExpression]]) {
        case ((aggExpressions, aggExpressionsToNamedExpressions), current) =>
          val aggregateExpressionsInCurrent =
            current.collect { case a: AggregateExpression => a }
          /**
           * We keep track of the outermost [[NamedExpression]] referencing the
           * [[AggregateExpression]] to always have distinct names for the named pushdown
           * expressions.
           */
          val updatedMapping =
            aggregateExpressionsInCurrent.foldLeft(aggExpressionsToNamedExpressions) {
              case (mapping, expression) =>
                val currentMapping = mapping.getOrElse(expression, Set.empty)
                mapping.updated(expression, currentMapping + current)
            }
          (aggExpressions ++ aggregateExpressionsInCurrent) -> updatedMapping
      }
    val aggregateExpressions = nonDistinctAggregateExpressions.distinct

    /**
     * We split and rewrite the given aggregate expressions to partial aggregate expressions
     * and keep track of the original aggregate expression for later referencing.
     */
    val aggregateExpressionsToAliases: Map[AggregateExpression, Seq[Alias]] =
      aggregateExpressions.map { agg =>
        agg -> rewriteAggregateExpressionsToPartial(agg,
          aggregateExpressionsToNamedExpressions(agg), createAliasName)
      }.toMap

    aggregateExpressionsToAliases
  }

  def getNamedGroupingExpressions(
      groupingExpressions: Seq[Expression],
      resultExpressions: Seq[NamedExpression]): Seq[(Expression, NamedExpression)] = {
    groupingExpressions.map {
      case ne: NamedExpression => ne -> ne
      /** If the expression is not a NamedExpressions, we add an alias.
       *  So, when we generate the result of the operator, the Aggregate Operator
       *  can directly get the Seq of attributes representing the grouping expressions.
       */
      case other =>
        val existingAlias = resultExpressions.find({
          case Alias(aliasChild, aliasName) => aliasChild == other
          case _ => false
        })

        // it could be that there is already an alias, so do not "double alias"
        val mappedExpression = existingAlias match {
          case Some(alias) => alias.toAttribute
          case None => Alias(other, other.toString)()
        }
        other -> mappedExpression
    }
  }

  /**
   * Since for pushdown only [[NamedExpression]]s are allowed, we do the following:
   *
   * - We extract [[Attribute]]s that can be pushed down straight.
   *
   * - For [[Expression]]s with [[AggregateExpression]]s in them, we can only push down the
   *    [[AggregateExpression]]s and leave the [[Expression]] for evaluation on spark side.
   *    If the [[Expression]] does not contain any [[AggregateExpression]], we can also directly
   *    push it down.
   */
  def getPushDownNameExpression(
      resultExpressions: Seq[NamedExpression],
      aggregateExpressionsToAliases: Map[AggregateExpression, Seq[Alias]])
  : Seq[NamedExpression] = {

    val pushdownExpressions = resultExpressions.flatMap {
      case attr: Attribute => Seq(attr)
      case Alias(attr: Attribute, _) => Seq(attr)
      case alias@Alias(expression, _) =>
        /**
         * If the collected sequence of [[AggregateExpression]]s is empty then there
         * is no dependency of a regular [[Expression]] to a distributed value computed by
         * an [[AggregateExpression]] and as such we can push it down. Otherwise, we just push
         * down the [[AggregateExpression]]s and apply the other [[Expression]]s via the
         * resultExpressions.
         */
        val aggs = expression.collect {
          case agg: AggregateExpression => agg
        }
        val nonEmptyExprs = if (aggs.isEmpty) {
          Seq(alias)
        } else {
          aggs
        }
        nonEmptyExprs
      case _ => Seq.empty
    }.distinct

    /**
     * With this step, we replace the pushdownExpressions with the corresponding
     * [[NamedExpression]]s that we can continue to work on. Regular [[NamedExpression]]s are
     * 'replaced' by themselves whereas the [[AggregateExpression]]s are replaced by their
     * partial versions hidden behind [[Alias]]es.
     */
    pushdownExpressions.flatMap {
      case agg: AggregateExpression => aggregateExpressionsToAliases(agg)
      case namedExpression: NamedExpression => Seq(namedExpression)
    }
  }

  /**
   * This method rewrites an [[AggregateExpression]] to the corresponding partial ones.
   * For instance an [[Average]] is rewritten to a [[Sum]] and a [[Count]].
   *
   * @param aggregateExpression [[AggregateExpression]] to rewrite.
   * @return A sequence of [[Alias]]es that represent the split up [[AggregateExpression]].
   */
  def rewriteAggregateExpressionsToPartial(
    aggregateExpression: AggregateExpression,
    outerNamedExpressions: Set[NamedExpression],
    createAliasName: Option[() => String] = None): Seq[Alias] = {
    val outerName = outerNamedExpressions.map(_.name).toSeq.sorted.mkString("", "_", "_")
    val inputBuffers = aggregateExpression.aggregateFunction.inputAggBufferAttributes
    aggregateExpression.aggregateFunction match {
      case avg: Average =>
        // two: sum and count
        val Seq(sumAlias, countAlias, _*) = inputBuffers
        val typedChild = avg.child.dataType match {
          case DoubleType | DecimalType.Fixed(_, _) => avg.child
          case _ => Cast(avg.child, DoubleType)
        }
        val sumExpression = // sum
          AggregateExpression(Sum(typedChild), mode = Partial, aggregateExpression.isDistinct)
        val countExpression = { // count
          AggregateExpression(Count(avg.child), mode = Partial, aggregateExpression.isDistinct)
        }
        val suffix = createAliasName.map(_.apply())
        val sumName = suffix.map("sum_" + _).getOrElse(outerName + sumAlias.name)
        val cntName = suffix.map("count_" + _ ).getOrElse(outerName + countAlias.name)
        Seq(
          referenceAs(sumName, sumAlias, sumExpression),
          referenceAs(cntName, countAlias, countExpression))

      case Count(_) | Sum(_) | Max(_) | Min(_) =>
        inputBuffers.map { ref =>
          val aliasName = createAliasName.map(_.apply()).getOrElse(outerName + ref.name)
          referenceAs(aliasName, ref, aggregateExpression.copy(mode = Partial))
        }

      case _ => throw new RuntimeException("Approached rewrite with unsupported expression")
    }
  }

  /**
   * References a given [[Expression]] as an [[Alias]] of the given [[Attribute]].
   *
   * @param attribute The [[Attribute]] to create the [[Alias]] reference of.
   * @param expression The [[Expression]] to reference as the given [[Attribute]].
   * @return An [[Alias]] of the [[Expression]] referenced as the [[Attribute]].
   */
  private def referenceAs(name: String, attribute: Attribute, expression: Expression): Alias = {
    Alias(expression, name)(attribute.exprId, attribute.qualifier, Some(attribute.metadata))
  }

  def rewriteResultExpressions(
      resultExpressions: Seq[NamedExpression],
      pushedAggregateMap: Map[AggregateExpression, Seq[Alias]],
      groupExpressionMap: Map[Expression, NamedExpression]): Seq[NamedExpression] =
    resultExpressions.map {
      rewriteResultExpression(_, pushedAggregateMap, groupExpressionMap)
    }

  private def rewriteResultExpression(
    resultExpression: NamedExpression,
    pushedAggregateMap: Map[AggregateExpression, Seq[Alias]],
    groupExpressionMap: Map[Expression, NamedExpression]): NamedExpression = {
    resultExpression.transformDown {
      case old@Alias(l@AggregateExpression(avg: Average, _, _, _, _), _) =>
        val as = pushedAggregateMap(l)
        val sum = as.head.toAttribute
        val count = as.last.toAttribute
        val average = avg.evaluateExpression.transformDown {
          case a: AttributeReference =>
            a.name match {
              case "sum" => Sum(sum).toAggregateExpression()
              case "count" => Sum(count).toAggregateExpression()
              case _ => a
            }
        }
        old.copy(child = average)(
          exprId = old.exprId,
          qualifier = old.qualifier,
          explicitMetadata = old.explicitMetadata,
          nonInheritableMetadataKeys = old.nonInheritableMetadataKeys)
      case a@AggregateExpression(agg, _, _, _, _) if pushedAggregateMap.contains(a) =>
        val x = pushedAggregateMap(a).head
        val newAgg = agg match {
          case _: Max => Max(x.toAttribute)
          case _: Min => Min(x.toAttribute)
          case _: Sum => Sum(x.toAttribute)
          case _: Count => Sum(x.toAttribute)
          case _ => throw new UnsupportedOperationException()
        }
        a.copy(aggregateFunction = newAgg)
      case expression =>

        /**
         * Since we're using `namedGroupingAttributes` to extract the grouping key
         * columns, we need to replace grouping key expressions with their corresponding
         * attributes. We do not rely on the equality check at here since attributes may
         * differ cosmetically. Instead, we use semanticEquals.
         */
        groupExpressionMap.collectFirst {
          case (grpExpr, ne) if grpExpr semanticEquals expression => ne.toAttribute
        }.getOrElse(expression)
    }.asInstanceOf[NamedExpression]
  }
}
