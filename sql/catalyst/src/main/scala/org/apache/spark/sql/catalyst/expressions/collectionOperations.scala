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
 * Sorts the input array in ascending order according to the natural ordering of
 * the array elements and returns it.
 */
case class SortArray(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {

  override def dataType: DataType = child.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient
  private lazy val lt: (Any, Any) => Boolean = {
    val ordering = child.dataType match {
      case ArrayType(elementType, _) => elementType match {
        case n: AtomicType => n.ordering.asInstanceOf[Ordering[Any]]
        case other => sys.error(s"Type $other does not support ordered operations")
      }
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

  override def nullSafeEval(value: Any): Seq[Any] = {
    value.asInstanceOf[Seq[Any]].sortWith(lt)
  }

  override def prettyName: String = "sort_array"
}
