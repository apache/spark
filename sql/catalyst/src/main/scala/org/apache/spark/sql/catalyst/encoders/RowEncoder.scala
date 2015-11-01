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

package org.apache.spark.sql.catalyst.encoders

import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A factory for constructing encoders that convert external row to/from the Spark SQL
 * internal binary representation.
 */
object RowEncoder {
  def apply(schema: StructType): ExpressionEncoder[Row] = {
    val cls = classOf[Row]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val extractExpressions = extractorsFor(inputObject, schema)
    val constructExpression = constructorFor(schema)
    new ExpressionEncoder[Row](
      schema,
      flat = false,
      extractExpressions.asInstanceOf[CreateStruct].children,
      constructExpression,
      ClassTag(cls))
  }

  private def extractorsFor(
      inputObject: Expression,
      inputType: DataType): Expression = inputType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => inputObject

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils,
        TimestampType,
        "fromJavaTimestamp",
        inputObject :: Nil)

    case DateType =>
      StaticInvoke(
        DateTimeUtils,
        DateType,
        "fromJavaDate",
        inputObject :: Nil)

    case _: DecimalType =>
      StaticInvoke(
        Decimal,
        DecimalType.SYSTEM_DEFAULT,
        "apply",
        inputObject :: Nil)

    case StringType =>
      StaticInvoke(
        classOf[UTF8String],
        StringType,
        "fromString",
        inputObject :: Nil)

    case t @ ArrayType(et, _) => et match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        NewInstance(
          classOf[GenericArrayData],
          inputObject :: Nil,
          dataType = t)
      case _ => MapObjects(extractorsFor(_, et), inputObject, externalDataTypeFor(et))
    }

    case t @ MapType(kt, vt, valueNullable) =>
      val keys =
        Invoke(
          Invoke(inputObject, "keysIterator", ObjectType(classOf[scala.collection.Iterator[_]])),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]))
      val convertedKeys = extractorsFor(keys, ArrayType(kt, false))

      val values =
        Invoke(
          Invoke(inputObject, "valuesIterator", ObjectType(classOf[scala.collection.Iterator[_]])),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]))
      val convertedValues = extractorsFor(values, ArrayType(vt, valueNullable))

      NewInstance(
        classOf[ArrayBasedMapData],
        convertedKeys :: convertedValues :: Nil,
        dataType = t)

    case StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        If(
          Invoke(inputObject, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, f.dataType),
          extractorsFor(
            Invoke(inputObject, "get", externalDataTypeFor(f.dataType), Literal(i) :: Nil),
            f.dataType))
      }
      CreateStruct(convertedFields)
  }

  private def externalDataTypeFor(dt: DataType): DataType = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => dt
    case TimestampType => ObjectType(classOf[java.sql.Timestamp])
    case DateType => ObjectType(classOf[java.sql.Date])
    case _: DecimalType => ObjectType(classOf[java.math.BigDecimal])
    case StringType => ObjectType(classOf[java.lang.String])
    case _: ArrayType => ObjectType(classOf[scala.collection.Seq[_]])
    case _: MapType => ObjectType(classOf[scala.collection.Map[_, _]])
    case _: StructType => ObjectType(classOf[Row])
  }

  private def constructorFor(schema: StructType): Expression = {
    val fields = schema.zipWithIndex.map { case (f, i) =>
      val field = BoundReference(i, f.dataType, f.nullable)
      If(
        IsNull(field),
        Literal.create(null, externalDataTypeFor(f.dataType)),
        constructorFor(BoundReference(i, f.dataType, f.nullable), f.dataType)
      )
    }
    CreateExternalRow(fields)
  }

  private def constructorFor(input: Expression, dataType: DataType): Expression = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => input

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils,
        ObjectType(classOf[java.sql.Timestamp]),
        "toJavaTimestamp",
        input :: Nil)

    case DateType =>
      StaticInvoke(
        DateTimeUtils,
        ObjectType(classOf[java.sql.Date]),
        "toJavaDate",
        input :: Nil)

    case _: DecimalType =>
      Invoke(input, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]))

    case StringType =>
      Invoke(input, "toString", ObjectType(classOf[String]))

    case ArrayType(et, nullable) =>
      val arrayData =
        Invoke(
          MapObjects(constructorFor(_, et), input, et),
          "array",
          ObjectType(classOf[Array[_]]))
      StaticInvoke(
        scala.collection.mutable.WrappedArray,
        ObjectType(classOf[Seq[_]]),
        "make",
        arrayData :: Nil)

    case MapType(kt, vt, valueNullable) =>
      val keyArrayType = ArrayType(kt, false)
      val keyData = constructorFor(Invoke(input, "keyArray", keyArrayType), keyArrayType)

      val valueArrayType = ArrayType(vt, valueNullable)
      val valueData = constructorFor(Invoke(input, "valueArray", valueArrayType), valueArrayType)

      StaticInvoke(
        ArrayBasedMapData,
        ObjectType(classOf[Map[_, _]]),
        "toScalaMap",
        keyData :: valueData :: Nil)

    case StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        If(
          Invoke(input, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, externalDataTypeFor(f.dataType)),
          constructorFor(getField(input, i, f.dataType), f.dataType))
      }
      CreateExternalRow(convertedFields)
  }

  private def getField(
     row: Expression,
     ordinal: Int,
     dataType: DataType): Expression = dataType match {
    case BooleanType =>
      Invoke(row, "getBoolean", dataType, Literal(ordinal) :: Nil)
    case ByteType =>
      Invoke(row, "getByte", dataType, Literal(ordinal) :: Nil)
    case ShortType =>
      Invoke(row, "getShort", dataType, Literal(ordinal) :: Nil)
    case IntegerType | DateType =>
      Invoke(row, "getInt", dataType, Literal(ordinal) :: Nil)
    case LongType | TimestampType =>
      Invoke(row, "getLong", dataType, Literal(ordinal) :: Nil)
    case FloatType =>
      Invoke(row, "getFloat", dataType, Literal(ordinal) :: Nil)
    case DoubleType =>
      Invoke(row, "getDouble", dataType, Literal(ordinal) :: Nil)
    case t: DecimalType =>
      Invoke(row, "getDecimal", dataType, Seq(ordinal, t.precision, t.scale).map(Literal(_)))
    case StringType =>
      Invoke(row, "getUTF8String", dataType, Literal(ordinal) :: Nil)
    case BinaryType =>
      Invoke(row, "getBinary", dataType, Literal(ordinal) :: Nil)
    case CalendarIntervalType =>
      Invoke(row, "getInterval", dataType, Literal(ordinal) :: Nil)
    case t: StructType =>
      Invoke(row, "getStruct", dataType, Literal(ordinal) :: Literal(t.size) :: Nil)
    case _: ArrayType =>
      Invoke(row, "getArray", dataType, Literal(ordinal) :: Nil)
    case _: MapType =>
      Invoke(row, "getMap", dataType, Literal(ordinal) :: Nil)
  }
}
