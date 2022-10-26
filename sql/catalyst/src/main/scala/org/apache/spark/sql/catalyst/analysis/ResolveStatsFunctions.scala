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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SUMMARY
import org.apache.spark.sql.types._

/**
 * Resolve StatsFunctions.
 */
object ResolveStatsFunctions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(SUMMARY), ruleId) {

    case s @ Summary(child, statistics) if s.childrenResolved =>
      val percentiles = statistics.filter(p => p.endsWith("%"))
        .map(p => p.stripSuffix("%").toDouble / 100.0)

      var mapExprs = Seq.empty[NamedExpression]
      child.output.foreach { attr =>
        if (attr.dataType.isInstanceOf[NumericType] || attr.dataType.isInstanceOf[StringType]) {
          val name = attr.name
          val casted: Expression = attr.dataType match {
            case StringType => Cast(attr, DoubleType, evalMode = EvalMode.TRY)
            case _ => attr
          }

          val approxPercentile = if (percentiles.nonEmpty) {
            Alias(
              new ApproximatePercentile(
                casted,
                Literal(percentiles.toArray),
                Literal(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
              ).toAggregateExpression(),
              s"__${name}_approx_percentile__")()
          } else null

          var aggExprs = Seq.empty[Expression]
          var percentileIndex = 0

          statistics.foreach { stat =>
            aggExprs :+= Literal(stat)

            stat match {
              case "count" =>
                aggExprs :+= Cast(Count(attr).toAggregateExpression(), StringType)
              case "count_distinct" =>
                aggExprs :+= Cast(Count(attr).toAggregateExpression(true), StringType)
              case "approx_count_distinct" =>
                aggExprs :+= Cast(HyperLogLogPlusPlus(attr).toAggregateExpression(), StringType)
              case "mean" =>
                aggExprs :+= Cast(Average(casted).toAggregateExpression(), StringType)
              case "stddev" =>
                aggExprs :+= Cast(StddevSamp(casted).toAggregateExpression(), StringType)
              case "min" =>
                aggExprs :+= Cast(Min(attr).toAggregateExpression(), StringType)
              case "max" =>
                aggExprs :+= Cast(Max(attr).toAggregateExpression(), StringType)
              case percentile if percentile.endsWith("%") =>
                aggExprs :+= Cast(new Get(approxPercentile, Literal(percentileIndex)), StringType)
                percentileIndex += 1
            }
          }

          mapExprs :+= Alias(CreateMap(aggExprs), name)()
        }
      }

      if (mapExprs.isEmpty) {
        LocalRelation.fromProduct(
          output = Seq(AttributeReference("summary", StringType, false)()),
          data = statistics.map(Tuple1.apply))

      } else {
        val aggregate = Aggregate(
          groupingExpressions = Nil,
          aggregateExpressions = mapExprs,
          child = child)

        val generate = Generate(
          generator = Explode(Literal(statistics.toArray)),
          unrequiredChildIndex = Nil,
          outer = false,
          qualifier = None,
          generatorOutput = Seq(AttributeReference("summary", StringType, false)()),
          child = aggregate)

        val summaryAttr = Alias(generate.output.last, "summary")()
        val mapAttrs = generate.output.take(generate.output.size - 1)
        val statAlias = mapAttrs.map(attr => Alias(ElementAt(attr, summaryAttr), attr.name)())

        Project(
          projectList = Seq(summaryAttr) ++ statAlias,
          child = generate)
      }
  }
}
