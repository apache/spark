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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExpectsInputTypes, Expression, StringTypeAnyCollation, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A rule that rewrites aggregation operations using plans that operate on collated strings.
 */
object RewriteGroupByCollation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case a: Aggregate =>
      val aliasMap = a.groupingExpressions.collect {
        case attr: AttributeReference if attr.dataType.isInstanceOf[StringType] =>
          attr -> CollationKey(attr)
        case attr: AttributeReference if attr.dataType.isInstanceOf[ArrayType]
        && attr.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StringType] =>
          attr -> CollationKey(attr)
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
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, ArrayType(StringType))
  override def dataType: DataType = expr.dataType

  final lazy val collationId: Int = dataType match {
    case _: StringType =>
      dataType.asInstanceOf[StringType].collationId
    case ArrayType(_: StringType, _) =>
      val arr = dataType.asInstanceOf[ArrayType]
      arr.elementType.asInstanceOf[StringType].collationId
  }

  override def nullSafeEval(input: Any): Any = dataType match {
    case _: StringType =>
      getCollationKey(input.asInstanceOf[UTF8String])
    case ArrayType(_: StringType, _) =>
      input match {
        case arr: Array[UTF8String] =>
          arr.map(getCollationKey)
        case arr: GenericArrayData =>
          val result = new Array[UTF8String](arr.numElements())
          for (i <- 0 until arr.numElements()) {
            result(i) = getCollationKey(arr.getUTF8String(i))
          }
          new GenericArrayData(result)
        case _ =>
          None
      }
  }

  def getCollationKey(str: UTF8String): UTF8String = {
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

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case _: StringType =>
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        defineCodeGen(ctx, ev, c => s"$c")
      } else if (collationId == CollationFactory.UTF8_BINARY_LCASE_COLLATION_ID) {
        defineCodeGen(ctx, ev, c => s"$c.toLowerCase()")
      } else {
        defineCodeGen(ctx, ev, c => s"UTF8String.fromBytes(CollationFactory.fetchCollation" +
          s"($collationId).collator.getCollationKey($c.toString()).toByteArray())")
      }
    case ArrayType(_: StringType, _) =>
      val expr = ctx.addReferenceObj("this", this)
      val arrData = ctx.freshName("arrData")
      val arrLength = ctx.freshName("arrLength")
      val arrResult = ctx.freshName("arrResult")
      val arrIndex = ctx.freshName("arrIndex")
      nullSafeCodeGen(ctx, ev, eval => {
        s"""
           |if ($eval instanceof GenericArrayData) {
           |  ArrayData $arrData = (ArrayData)$eval;
           |  int $arrLength = $arrData.numElements();
           |  UTF8String[] $arrResult = new UTF8String[$arrLength];
           |  for (int $arrIndex = 0; $arrIndex < $arrLength; $arrIndex++) {
           |    $arrResult[$arrIndex] = $expr.getCollationKey($arrData.getUTF8String($arrIndex));
           |  }
           |  ${ev.value} = new GenericArrayData($arrResult);
           |} else {
           |  ${ev.value} = null;
           |}
      """.stripMargin
      })
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(expr = newChild)
  }

  override def child: Expression = expr
}
