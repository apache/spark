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
import org.apache.spark.sql.catalyst.ScalaReflection
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
    // We use an If expression to wrap extractorsFor result of StructType
    val extractExpressions = extractorsFor(inputObject, schema).asInstanceOf[If].falseValue
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
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => inputObject

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "serialize", udt.sqlType, inputObject :: Nil)

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        TimestampType,
        "fromJavaTimestamp",
        inputObject :: Nil)

    case DateType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        DateType,
        "fromJavaDate",
        inputObject :: Nil)

    case _: DecimalType =>
      StaticInvoke(
        Decimal.getClass,
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
        val method = if (f.dataType.isInstanceOf[StructType]) {
          "getStruct"
        } else {
          "get"
        }
        If(
          Invoke(inputObject, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, f.dataType),
          extractorsFor(
            Invoke(inputObject, method, externalDataTypeFor(f.dataType), Literal(i) :: Nil),
            f.dataType))
      }
      If(IsNull(inputObject),
        Literal.create(null, inputType),
        CreateStruct(convertedFields))
  }

  private def externalDataTypeFor(dt: DataType): DataType = dt match {
    case _ if ScalaReflection.isNativeType(dt) => dt
    case TimestampType => ObjectType(classOf[java.sql.Timestamp])
    case DateType => ObjectType(classOf[java.sql.Date])
    case _: DecimalType => ObjectType(classOf[java.math.BigDecimal])
    case StringType => ObjectType(classOf[java.lang.String])
    case _: ArrayType => ObjectType(classOf[scala.collection.Seq[_]])
    case _: MapType => ObjectType(classOf[scala.collection.Map[_, _]])
    case _: StructType => ObjectType(classOf[Row])
    case udt: UserDefinedType[_] => ObjectType(udt.userClass)
    case _: NullType => ObjectType(classOf[java.lang.Object])
  }

  private def constructorFor(schema: StructType): Expression = {
    val fields = schema.zipWithIndex.map { case (f, i) =>
      val field = BoundReference(i, f.dataType, f.nullable)
      If(
        IsNull(field),
        Literal.create(null, externalDataTypeFor(f.dataType)),
        constructorFor(BoundReference(i, f.dataType, f.nullable))
      )
    }
    CreateExternalRow(fields)
  }

  private def constructorFor(input: Expression): Expression = input.dataType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => input

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), input :: Nil)

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        ObjectType(classOf[java.sql.Timestamp]),
        "toJavaTimestamp",
        input :: Nil)

    case DateType =>
      StaticInvoke(
        DateTimeUtils.getClass,
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
          MapObjects(constructorFor(_), input, et),
          "array",
          ObjectType(classOf[Array[_]]))
      StaticInvoke(
        scala.collection.mutable.WrappedArray.getClass,
        ObjectType(classOf[Seq[_]]),
        "make",
        arrayData :: Nil)

    case MapType(kt, vt, valueNullable) =>
      val keyArrayType = ArrayType(kt, false)
      val keyData = constructorFor(Invoke(input, "keyArray", keyArrayType))

      val valueArrayType = ArrayType(vt, valueNullable)
      val valueData = constructorFor(Invoke(input, "valueArray", valueArrayType))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        ObjectType(classOf[Map[_, _]]),
        "toScalaMap",
        keyData :: valueData :: Nil)

    case StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        If(
          Invoke(input, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, externalDataTypeFor(f.dataType)),
          constructorFor(GetStructField(input, i)))
      }
      If(IsNull(input),
        Literal.create(null, externalDataTypeFor(input.dataType)),
        CreateExternalRow(convertedFields))
  }
}
