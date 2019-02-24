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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._


/**
 * An interpreted version of a safe projection.
 *
 * @param expressions that produces the resulting fields. These expressions must be bound
 *                    to a schema.
 */
class InterpretedSafeProjection(expressions: Seq[Expression]) extends Projection {

  private[this] val mutableRow = new SpecificInternalRow(expressions.map(_.dataType))

  private[this] val exprsWithWriters = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }.map { case (e, i) =>
    val converter = generateSafeValueConverter(e.dataType)
    val writer = InternalRow.getWriter(i, e.dataType)
    val f = if (!e.nullable) {
      (v: Any) => writer(mutableRow, converter(v))
    } else {
      (v: Any) => {
        if (v == null) {
          mutableRow.setNullAt(i)
        } else {
          writer(mutableRow, converter(v))
        }
      }
    }
    (e, f)
  }

  private def generateSafeValueConverter(dt: DataType): Any => Any = dt match {
    case ArrayType(elemType, _) =>
      val elementConverter = generateSafeValueConverter(elemType)
      v => {
        val arrayValue = v.asInstanceOf[ArrayData]
        val result = new Array[Any](arrayValue.numElements())
        arrayValue.foreach(elemType, (i, e) => {
          result(i) = elementConverter(e)
        })
        new GenericArrayData(result)
      }

    case st: StructType =>
      val fieldTypes = st.fields.map(_.dataType)
      val fieldConverters = fieldTypes.map(generateSafeValueConverter)
      v => {
        val row = v.asInstanceOf[InternalRow]
        val ar = new Array[Any](row.numFields)
        var idx = 0
        while (idx < row.numFields) {
          ar(idx) = fieldConverters(idx)(row.get(idx, fieldTypes(idx)))
          idx += 1
        }
        new GenericInternalRow(ar)
      }

    case MapType(keyType, valueType, _) =>
      lazy val keyConverter = generateSafeValueConverter(keyType)
      lazy val valueConverter = generateSafeValueConverter(valueType)
      v => {
        val mapValue = v.asInstanceOf[MapData]
        val keys = mapValue.keyArray().toArray[Any](keyType)
        val values = mapValue.valueArray().toArray[Any](valueType)
        val convertedKeys = keys.map(keyConverter)
        val convertedValues = values.map(valueConverter)
        ArrayBasedMapData(convertedKeys, convertedValues)
      }

    case udt: UserDefinedType[_] =>
      generateSafeValueConverter(udt.sqlType)

    case _ => identity
  }

  override def apply(row: InternalRow): InternalRow = {
    var i = 0
    while (i < exprsWithWriters.length) {
      val (expr, writer) = exprsWithWriters(i)
      writer(expr.eval(row))
      i += 1
    }
    mutableRow
  }
}

/**
 * Helper functions for creating an [[InterpretedSafeProjection]].
 */
object InterpretedSafeProjection {

  /**
   * Returns an [[SafeProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): Projection = {
    // We need to make sure that we do not reuse stateful expressions.
    val cleanedExpressions = exprs.map(_.transform {
      case s: Stateful => s.freshCopy()
    })
    new InterpretedSafeProjection(cleanedExpressions)
  }
}
