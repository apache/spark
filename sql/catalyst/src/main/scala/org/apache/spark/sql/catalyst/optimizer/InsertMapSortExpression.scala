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

import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, CreateNamedStruct, Expression, GetStructField, If, IsNull, LambdaFunction, Literal, MapFromArrays, MapKeys, MapSort, MapValues, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, REPARTITION_OPERATION}
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

/**
 * Adds [[MapSort]] to [[Aggregate]] expressions containing map columns,
 * as the key/value pairs need to be in the correct order before grouping:
 *
 * SELECT map_column, COUNT(*) FROM TABLE GROUP BY map_column =>
 * SELECT _groupingmapsort as map_column, COUNT(*) FROM (
 *   SELECT map_sort(map_column) as _groupingmapsort FROM TABLE
 * ) GROUP BY _groupingmapsort
 */
object InsertMapSortInGroupingExpressions extends Rule[LogicalPlan] {
  import InsertMapSortExpression._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(AGGREGATE)) {
      return plan
    }
    val shouldRewrite = plan.exists {
      case agg: Aggregate if agg.groupingExpressions.exists(mapTypeExistsRecursively) => true
      case _ => false
    }
    if (!shouldRewrite) {
      return plan
    }

    plan transformUpWithNewOutput {
      case agg @ Aggregate(groupingExprs, aggregateExpressions, child, hint) =>
        val exprToMapSort = new mutable.HashMap[Expression, NamedExpression]
        val newGroupingKeys = groupingExprs.map { expr =>
          val inserted = insertMapSortRecursively(expr)
          if (expr.ne(inserted)) {
            exprToMapSort.getOrElseUpdate(
              expr.canonicalized,
              Alias(inserted, "_groupingmapsort")()
            ).toAttribute
          } else {
            expr
          }
        }
        val newAggregateExprs = aggregateExpressions.map {
          case named if exprToMapSort.contains(named.canonicalized) =>
            // If we replace the top-level named expr, then should add back the original name
            exprToMapSort(named.canonicalized).toAttribute.withName(named.name)
          case other =>
            other.transformUp {
              case e => exprToMapSort.get(e.canonicalized).map(_.toAttribute).getOrElse(e)
            }.asInstanceOf[NamedExpression]
        }
        val newChild = Project(child.output ++ exprToMapSort.values, child)
        val newAgg = Aggregate(newGroupingKeys, newAggregateExprs, newChild, hint)
        newAgg -> agg.output.zip(newAgg.output)
    }
  }
}

/**
 * Adds [[MapSort]] to [[RepartitionByExpression]] expressions containing map columns,
 * as the key/value pairs need to be in the correct order before repartitioning:
 *
 * SELECT * FROM TABLE DISTRIBUTE BY map_column =>
 * SELECT * FROM TABLE DISTRIBUTE BY map_sort(map_column)
 */
object InsertMapSortInRepartitionExpressions extends Rule[LogicalPlan] {
  import InsertMapSortExpression._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsPattern(REPARTITION_OPERATION)) {
      case rep: RepartitionByExpression
        if rep.partitionExpressions.exists(mapTypeExistsRecursively) =>
        val exprToMapSort = new mutable.HashMap[Expression, Expression]
        val newPartitionExprs = rep.partitionExpressions.map { expr =>
          val inserted = insertMapSortRecursively(expr)
          if (expr.ne(inserted)) {
            exprToMapSort.getOrElseUpdate(expr.canonicalized, inserted)
          } else {
            expr
          }
        }
        rep.copy(partitionExpressions = newPartitionExprs)
    }
  }
}

private[optimizer] object InsertMapSortExpression {

  /**
   * Returns true if the expression contains a [[MapType]] in DataType tree.
   */
  def mapTypeExistsRecursively(expr: Expression): Boolean = {
    expr.dataType.existsRecursively(_.isInstanceOf[MapType])
  }

  /**
   * Inserts [[MapSort]] recursively taking into account when it is nested inside a struct or array.
   */
  def insertMapSortRecursively(e: Expression): Expression = {
    e.dataType match {
      case m: MapType =>
        // Check if value type of MapType contains MapType (possibly nested)
        // and special handle this case.
        val mapSortExpr = if (m.valueType.existsRecursively(_.isInstanceOf[MapType])) {
          MapFromArrays(MapKeys(e), insertMapSortRecursively(MapValues(e)))
        } else {
          e
        }

        MapSort(mapSortExpr)

      case StructType(fields)
        if fields.exists(_.dataType.existsRecursively(_.isInstanceOf[MapType])) =>
        val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (f, i) =>
          Seq(Literal(f.name), insertMapSortRecursively(
            GetStructField(e, i, Some(f.name))))
        }.toImmutableArraySeq)
        if (struct.valExprs.forall(_.isInstanceOf[GetStructField])) {
          // No field needs MapSort processing, just return the original expression.
          e
        } else if (e.nullable) {
          If(IsNull(e), Literal(null, struct.dataType), struct)
        } else {
          struct
        }

      case ArrayType(et, containsNull) if et.existsRecursively(_.isInstanceOf[MapType]) =>
        val param = NamedLambdaVariable("x", et, containsNull)
        val funcBody = insertMapSortRecursively(param)

        ArrayTransform(e, LambdaFunction(funcBody, Seq(param)))

      case _ => e
    }
  }
}
