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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback, CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.types._

/**
 * Given an array or map, returns its size.
 */
case class Size(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(ArrayType, MapType))

  override def nullSafeEval(value: Any): Int = child.dataType match {
    case _: ArrayType => value.asInstanceOf[ArrayData].numElements()
    case _: MapType => value.asInstanceOf[Map[Any, Any]].size
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sizeCall = child.dataType match {
      case _: ArrayType => "numElements()"
      case _: MapType => "size()"
    }
    nullSafeCodeGen(ctx, ev, c => s"${ev.primitive} = ($c).$sizeCall;")
  }
}

/**
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 */
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case _ @ ArrayType(n: AtomicType, _) => TypeCheckResult.TypeCheckSuccess
    case _ @ ArrayType(n, _) => TypeCheckResult.TypeCheckFailure(
                    s"Type $n is not the AtomicType, we can not perform the ordering operations")
    case other =>
      TypeCheckResult.TypeCheckFailure(s"ArrayType(AtomicType) is expected, but we got $other")
  }

  @transient
  private lazy val lt: (Any, Any) => Boolean = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
    }

    (left, right) => {
      if (left == null && right == null) {
        false
      } else if (left == null) {
        true
      } else if (right == null) {
        false
      } else {
        ordering.compare(left, right) < 0
      }
    }
  }

  @transient
  private lazy val gt: (Any, Any) => Boolean = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
    }

    (left, right) => {
      if (left == null && right == null) {
        true
      } else if (left == null) {
        false
      } else if (right == null) {
        true
      } else {
        ordering.compare(left, right) > 0
      }
    }
  }

  override def nullSafeEval(array: Any, ascending: Any): Seq[Any] = {
    array.asInstanceOf[Seq[Any]].sortWith(if (ascending.asInstanceOf[Boolean]) lt else gt)
  }

  override def prettyName: String = "sort_array"
}
