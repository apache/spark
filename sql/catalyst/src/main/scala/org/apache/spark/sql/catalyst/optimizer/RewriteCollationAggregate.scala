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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

/**
 * This rule rewrites Aggregate grouping expressions to ensure that non-binary collated strings
 * are converted to their binary-stable collation keys (via [[CollationKey]]).
 *
 * This allows hash-based aggregation (e.g., [[org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec]])
 * to work properly on data with non-binary collations, avoiding full sorting and spilling.
 *
 * Any original grouping expression referenced in the aggregate expressions (output) is preserved
 * by wrapping it in `First(expr, ignoreNulls = false)`, an arbitrary representative of each
 * collation-equal group.
 */
object RewriteCollationAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.collationHashAggregationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsPattern(AGGREGATE)) {
        case a @ Aggregate(groupingExpressions, aggregateExpressions, child, _)
            if a.resolved && groupingExpressions.exists(e => !UnsafeRowUtils.isBinaryStable(e.dataType)) =>
          val keyMapping = mutable.LinkedHashMap.empty[Expression, Expression]
          val newGroupingExpressions = groupingExpressions.map { ge =>
            val processed = processExpression(ge, ge.dataType)
            if (!processed.fastEquals(ge)) {
              keyMapping.put(ge.canonicalized, ge)
              processed
            } else {
              ge
            }
          }

          if (keyMapping.nonEmpty) {
            def replaceGroupingKeyReferences(e: Expression): Expression = {
              e match {
                case _: AggregateExpression => e
                case _ if e.foldable => e
                case _ if keyMapping.contains(e.canonicalized) =>
                  val origExpr = keyMapping(e.canonicalized)
                  First(origExpr, ignoreNulls = false).toAggregateExpression()
                case _ =>
                  e.mapChildren(replaceGroupingKeyReferences)
              }
            }

            val newAggregateExpressions = aggregateExpressions.map {
              case a @ Alias(child, name) =>
                val newChild = replaceGroupingKeyReferences(child)
                if (!newChild.fastEquals(child)) {
                  Alias(newChild, name)(exprId = a.exprId, explicitMetadata = a.explicitMetadata)
                } else {
                  a
                }
              case other =>
                replaceGroupingKeyReferences(other).asInstanceOf[NamedExpression]
            }

            a.copy(
              groupingExpressions = newGroupingExpressions,
              aggregateExpressions = newAggregateExpressions)
          } else {
            a
          }
      }
    }
  }

  /**
   * Recursively process the expression in order to replace non-binary collated strings with their
   * associated collation keys. This is necessary to ensure grouping is evaluated correctly for all
   * types containing non-binary collated strings, including structs and arrays.
   */
  private def processExpression(expr: Expression, dt: DataType): Expression = {
    dt match {
      // For binary stable expressions, no special handling is needed.
      case _ if UnsafeRowUtils.isBinaryStable(dt) =>
        expr

      // Inject CollationKey for non-binary collated strings.
      case _: StringType =>
        CollationKey(expr)

      // Recursively process struct fields for non-binary structs.
      case StructType(fields) =>
        processStruct(expr, fields)

      // Recursively process array elements for non-binary arrays.
      case ArrayType(et, containsNull) =>
        processArray(expr, et, containsNull)

      case _ =>
        expr
    }
  }

  private def processStruct(str: Expression, fields: Array[StructField]): Expression = {
    val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (f, i) =>
      Seq(Literal(f.name), processExpression(GetStructField(str, i, Some(f.name)), f.dataType))
    }.toImmutableArraySeq)
    if (str.nullable) {
      If(IsNull(str), Literal(null, struct.dataType), struct)
    } else {
      struct
    }
  }

  private def processArray(arr: Expression, et: DataType, containsNull: Boolean): Expression = {
    val param: NamedExpression = NamedLambdaVariable("a", et, containsNull)
    val funcBody: Expression = processExpression(param, et)
    if (!funcBody.fastEquals(param)) {
      ArrayTransform(arr, LambdaFunction(funcBody, Seq(param)))
    } else {
      arr
    }
  }
}
