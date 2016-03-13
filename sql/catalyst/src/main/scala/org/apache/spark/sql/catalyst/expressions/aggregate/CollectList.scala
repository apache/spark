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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType, DataTypes, NullType}

case class CollectList(child: Expression) extends DeclarativeAggregate {

  override def children: scala.Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = DataTypes.createArrayType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  // Don't do partial aggregation for CollectList, similarly to RDD.groupBy().
  override def supportsPartial: Boolean = false

  private lazy val list = AttributeReference("list", DataTypes.createArrayType(child.dataType))()

  override lazy val aggBufferAttributes = list :: Nil

  override lazy val initialValues = Seq(Literal.default(DataTypes.createArrayType(child.dataType)))

  override lazy val updateExpressions = Seq(UpdateList(Seq(list, child)))

  override lazy val mergeExpressions = Seq(MergeList(Seq(list.left, list.right)))

  override lazy val evaluateExpression = Cast(list, dataType)

  override def defaultResult: Option[Literal] = Option(Literal(Array()))

}

// TODO proper code generation for this
private[sql] case class MergeList(children: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != 2) {
      TypeCheckResult.TypeCheckFailure(s"MERGELIST requires 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got MERGELIST (${children.map(_.dataType)}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    val left = children(0).eval(input).asInstanceOf[ArrayData];
    val right = children(1).eval(input).asInstanceOf[ArrayData];

    new GenericArrayData(left.array ++ right.array)
  }
}

// TODO proper code generation for this
private[sql] case class UpdateList(children: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != 2) {
      TypeCheckResult.TypeCheckFailure(s"UPDATELIST requires 2 arguments")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    val left = children(0).eval(input).asInstanceOf[ArrayData];
    val right = children(1).eval(input);

    new GenericArrayData(left.array :+ right)
  }
}
