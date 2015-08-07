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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class FromUnsafe(child: Expression) extends UnaryExpression
  with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(ArrayType, StructType, MapType))

  override def dataType: DataType = child.dataType

  private def convert(value: Any, dt: DataType): Any = dt match {
    case StructType(fields) =>
      val row = value.asInstanceOf[UnsafeRow]
      val result = new Array[Any](fields.length)
      fields.map(_.dataType).zipWithIndex.foreach { case (dt, i) =>
        if (!row.isNullAt(i)) {
          result(i) = convert(row.get(i, dt), dt)
        }
      }
      new GenericInternalRow(result)

    case ArrayType(elementType, _) =>
      val array = value.asInstanceOf[UnsafeArrayData]
      val length = array.numElements()
      val result = new Array[Any](length)
      var i = 0
      while (i < length) {
        if (!array.isNullAt(i)) {
          result(i) = convert(array.get(i, elementType), elementType)
        }
        i += 1
      }
      new GenericArrayData(result)

    case StringType => value.asInstanceOf[UTF8String].clone()

    case MapType(kt, vt, _) =>
      val map = value.asInstanceOf[UnsafeMapData]
      val safeKeyArray = convert(map.keys, ArrayType(kt)).asInstanceOf[GenericArrayData]
      val safeValueArray = convert(map.values, ArrayType(vt)).asInstanceOf[GenericArrayData]
      new ArrayBasedMapData(safeKeyArray, safeValueArray)

    case _ => value
  }

  override def nullSafeEval(input: Any): Any = {
    convert(input, dataType)
  }
}
