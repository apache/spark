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

import java.util.Locale

// import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExpectsInputTypes, Expression, StringTypeAnyCollation, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
// import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A rule that rewrites aggregation operations using plans that operate on collated strings.
 */
object RewriteGroupByCollation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case a: Aggregate =>
      val aliasMap = a.groupingExpressions.collect {
        case attr: AttributeReference if attr.dataType.isInstanceOf[StringType] =>
          attr -> CollationKey(attr) // Alias(CollationKey(attr), attr.name)()
      }.toMap

      val newGroupingExpressions = a.groupingExpressions.map {
        case attr: AttributeReference if aliasMap.contains(attr) =>
          aliasMap(attr)
        case other => other
      }

      val newAggregateExpressions = a.aggregateExpressions.map {
        case attr: AttributeReference if aliasMap.contains(attr) =>
          Alias(First(attr, ignoreNulls = false).toAggregateExpression(), attr.name)()
        case other => other
      }

      val newAggregate = a.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions
      )

      (newAggregate, a.output.zip(newAggregate.output))
  }
}

case class CollationKey(expr: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)
  override def dataType: DataType = expr.dataType

  final lazy val collationId: Int = dataType.asInstanceOf[StringType].collationId

  override def nullSafeEval(input: Any): Any = {
    val str: UTF8String = input.asInstanceOf[UTF8String]
    if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
      str
    } else if (collationId == CollationFactory.UTF8_BINARY_LCASE_COLLATION_ID) {
      UTF8String.fromString(str.toString.toLowerCase(Locale.ROOT))
    } else {
      val collator = CollationFactory.fetchCollation(collationId).collator
      val collationKey = collator.getCollationKey(str.toString)
      UTF8String.fromBytes(collationKey.toByteArray)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
      defineCodeGen(ctx, ev, c => s"$c")
    } else if (collationId == CollationFactory.UTF8_BINARY_LCASE_COLLATION_ID) {
      defineCodeGen(ctx, ev, c => s"$c.toLowerCase()")
    } else {
      defineCodeGen(ctx, ev, c => s"UTF8String.fromBytes(CollationFactory.fetchCollation" +
        s"($collationId).collator.getCollationKey($c.toString()).toByteArray())")
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(expr = newChild)
  }

  override def child: Expression = expr
}
