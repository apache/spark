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

import scala.annotation.tailrec
import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{ScalaReflection, WalkedTypePath}
import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.SerializerBuildHelper._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A factory for constructing encoders that convert external row to/from the Spark SQL
 * internal binary representation.
 *
 * The following is a mapping between Spark SQL types and its allowed external types:
 * {{{
 *   BooleanType -> java.lang.Boolean
 *   ByteType -> java.lang.Byte
 *   ShortType -> java.lang.Short
 *   IntegerType -> java.lang.Integer
 *   FloatType -> java.lang.Float
 *   DoubleType -> java.lang.Double
 *   StringType -> String
 *   DecimalType -> java.math.BigDecimal or scala.math.BigDecimal or Decimal
 *
 *   DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
 *   DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true
 *
 *   TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
 *   TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true
 *
 *   TimestampNTZType -> java.time.LocalDateTime
 *
 *   DayTimeIntervalType -> java.time.Duration
 *   YearMonthIntervalType -> java.time.Period
 *
 *   BinaryType -> byte array
 *   ArrayType -> scala.collection.Seq or Array
 *   MapType -> scala.collection.Map
 *   StructType -> org.apache.spark.sql.Row
 * }}}
 */
object RowEncoder {
  def apply(schema: StructType, lenient: Boolean): ExpressionEncoder[Row] = {
    val cls = classOf[Row]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val serializer = serializerFor(inputObject, schema, lenient)
    val deserializer = deserializerFor(GetColumnByOrdinal(0, serializer.dataType), schema)
    new ExpressionEncoder[Row](
      serializer,
      deserializer,
      ClassTag(cls))
  }
  def apply(schema: StructType): ExpressionEncoder[Row] = {
    apply(schema, lenient = false)
  }

  private def serializerFor(
      inputObject: Expression,
      inputType: DataType,
      lenient: Boolean): Expression = inputType match {
    case dt if ScalaReflection.isNativeType(dt) => inputObject

    case p: PythonUserDefinedType => serializerFor(inputObject, p.sqlType, lenient)

    case udt: UserDefinedType[_] =>
      val annotation = udt.userClass.getAnnotation(classOf[SQLUserDefinedType])
      val udtClass: Class[_] = if (annotation != null) {
        annotation.udt()
      } else {
        UDTRegistration.getUDTFor(udt.userClass.getName).getOrElse {
          throw QueryExecutionErrors.userDefinedTypeNotAnnotatedAndRegisteredError(udt)
        }
      }
      val obj = NewInstance(
        udtClass,
        Nil,
        dataType = ObjectType(udtClass), false)
      Invoke(obj, "serialize", udt, inputObject :: Nil, returnNullable = false)

    case TimestampType =>
      if (lenient) {
        createSerializerForAnyTimestamp(inputObject)
      } else if (SQLConf.get.datetimeJava8ApiEnabled) {
        createSerializerForJavaInstant(inputObject)
      } else {
        createSerializerForSqlTimestamp(inputObject)
      }

    case TimestampNTZType => createSerializerForLocalDateTime(inputObject)

    case DateType =>
      if (lenient) {
        createSerializerForAnyDate(inputObject)
      } else if (SQLConf.get.datetimeJava8ApiEnabled) {
        createSerializerForJavaLocalDate(inputObject)
      } else {
        createSerializerForSqlDate(inputObject)
      }

    case _: DayTimeIntervalType => createSerializerForJavaDuration(inputObject)

    case _: YearMonthIntervalType => createSerializerForJavaPeriod(inputObject)

    case d: DecimalType =>
      CheckOverflow(StaticInvoke(
        Decimal.getClass,
        d,
        "fromDecimal",
        inputObject :: Nil,
        returnNullable = false), d, !SQLConf.get.ansiEnabled)

    case StringType => createSerializerForString(inputObject)

    case t @ ArrayType(et, containsNull) =>
      et match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
          StaticInvoke(
            classOf[ArrayData],
            t,
            "toArrayData",
            inputObject :: Nil,
            returnNullable = false)

        case _ =>
          createSerializerForMapObjects(
            inputObject,
            ObjectType(classOf[Object]),
            element => {
              val value = serializerFor(ValidateExternalType(element, et, lenient), et, lenient)
              expressionWithNullSafety(value, containsNull, WalkedTypePath())
            })
      }

    case t @ MapType(kt, vt, valueNullable) =>
      val keys =
        Invoke(
          Invoke(inputObject, "keysIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedKeys = serializerFor(keys, ArrayType(kt, false), lenient)

      val values =
        Invoke(
          Invoke(inputObject, "valuesIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedValues = serializerFor(values, ArrayType(vt, valueNullable), lenient)

      val nonNullOutput = NewInstance(
        classOf[ArrayBasedMapData],
        convertedKeys :: convertedValues :: Nil,
        dataType = t,
        propagateNull = false)

      if (inputObject.nullable) {
        expressionForNullableExpr(inputObject, nonNullOutput)
      } else {
        nonNullOutput
      }

    case StructType(fields) =>
      val nonNullOutput = CreateNamedStruct(fields.zipWithIndex.flatMap { case (field, index) =>
        val fieldValue = serializerFor(
          ValidateExternalType(
            GetExternalRowField(inputObject, index, field.name),
            field.dataType,
            lenient),
          field.dataType,
          lenient)
        val convertedField = if (field.nullable) {
          If(
            Invoke(inputObject, "isNullAt", BooleanType, Literal(index) :: Nil),
            // Because we strip UDTs, `field.dataType` can be different from `fieldValue.dataType`.
            // We should use `fieldValue.dataType` here.
            Literal.create(null, fieldValue.dataType),
            fieldValue
          )
        } else {
          fieldValue
        }
        Literal(field.name) :: convertedField :: Nil
      })

      if (inputObject.nullable) {
        expressionForNullableExpr(inputObject, nonNullOutput)
      } else {
        nonNullOutput
      }
    // For other data types, return the internal catalyst value as it is.
    case _ => inputObject
  }

  /**
   * Returns the `DataType` that can be used when generating code that converts input data
   * into the Spark SQL internal format.  Unlike `externalDataTypeFor`, the `DataType` returned
   * by this function can be more permissive since multiple external types may map to a single
   * internal type.  For example, for an input with DecimalType in external row, its external types
   * can be `scala.math.BigDecimal`, `java.math.BigDecimal`, or
   * `org.apache.spark.sql.types.Decimal`.
   */
  def externalDataTypeForInput(dt: DataType, lenient: Boolean): DataType = dt match {
    // In order to support both Decimal and java/scala BigDecimal in external row, we make this
    // as java.lang.Object.
    case _: DecimalType => ObjectType(classOf[java.lang.Object])
    // In order to support both Array and Seq in external row, we make this as java.lang.Object.
    case _: ArrayType => ObjectType(classOf[java.lang.Object])
    case _: DateType | _: TimestampType if lenient => ObjectType(classOf[java.lang.Object])
    case _ => externalDataTypeFor(dt)
  }

  @tailrec
  def externalDataTypeFor(dt: DataType): DataType = dt match {
    case _ if ScalaReflection.isNativeType(dt) => dt
    case TimestampType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        ObjectType(classOf[java.time.Instant])
      } else {
        ObjectType(classOf[java.sql.Timestamp])
      }
    case TimestampNTZType =>
      ObjectType(classOf[java.time.LocalDateTime])
    case DateType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        ObjectType(classOf[java.time.LocalDate])
      } else {
        ObjectType(classOf[java.sql.Date])
      }
    case _: DayTimeIntervalType => ObjectType(classOf[java.time.Duration])
    case _: YearMonthIntervalType => ObjectType(classOf[java.time.Period])
    case p: PythonUserDefinedType => externalDataTypeFor(p.sqlType)
    case udt: UserDefinedType[_] => ObjectType(udt.userClass)
    case _ => dt.physicalDataType match {
      case _: PhysicalArrayType => ObjectType(classOf[scala.collection.Seq[_]])
      case _: PhysicalDecimalType => ObjectType(classOf[java.math.BigDecimal])
      case _: PhysicalMapType => ObjectType(classOf[scala.collection.Map[_, _]])
      case PhysicalStringType => ObjectType(classOf[java.lang.String])
      case _: PhysicalStructType => ObjectType(classOf[Row])
      // For other data types, return the data type as it is.
      case _ => dt
    }
  }

  private def deserializerFor(input: Expression, schema: StructType): Expression = {
    val fields = schema.zipWithIndex.map { case (f, i) =>
      deserializerFor(GetStructField(input, i))
    }
    CreateExternalRow(fields, schema)
  }

  private def deserializerFor(input: Expression): Expression = {
    deserializerFor(input, input.dataType)
  }

  @tailrec
  private def deserializerFor(input: Expression, dataType: DataType): Expression = dataType match {
    case dt if ScalaReflection.isNativeType(dt) => input

    case p: PythonUserDefinedType => deserializerFor(input, p.sqlType)

    case udt: UserDefinedType[_] =>
      val annotation = udt.userClass.getAnnotation(classOf[SQLUserDefinedType])
      val udtClass: Class[_] = if (annotation != null) {
        annotation.udt()
      } else {
        UDTRegistration.getUDTFor(udt.userClass.getName).getOrElse {
          throw QueryExecutionErrors.userDefinedTypeNotAnnotatedAndRegisteredError(udt)
        }
      }
      val obj = NewInstance(
        udtClass,
        Nil,
        dataType = ObjectType(udtClass))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), input :: Nil)

    case TimestampType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        createDeserializerForInstant(input)
      } else {
        createDeserializerForSqlTimestamp(input)
      }

    case TimestampNTZType =>
      createDeserializerForLocalDateTime(input)

    case DateType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        createDeserializerForLocalDate(input)
      } else {
        createDeserializerForSqlDate(input)
      }

    case _: DayTimeIntervalType => createDeserializerForDuration(input)

    case _: YearMonthIntervalType => createDeserializerForPeriod(input)

    case _: DecimalType => createDeserializerForJavaBigDecimal(input, returnNullable = false)

    case StringType => createDeserializerForString(input, returnNullable = false)

    case ArrayType(et, nullable) =>
      val arrayData =
        Invoke(
          MapObjects(deserializerFor(_), input, et),
          "array",
          ObjectType(classOf[Array[_]]), returnNullable = false)
      // TODO should use `scala.collection.immutable.ArrayDeq.unsafeMake` method to create
      //  `immutable.Seq` in Scala 2.13 when Scala version compatibility is no longer required.
      StaticInvoke(
        scala.collection.mutable.WrappedArray.getClass,
        ObjectType(classOf[scala.collection.Seq[_]]),
        "make",
        arrayData :: Nil,
        returnNullable = false)

    case MapType(kt, vt, valueNullable) =>
      val keyArrayType = ArrayType(kt, false)
      val keyData = deserializerFor(Invoke(input, "keyArray", keyArrayType))

      val valueArrayType = ArrayType(vt, valueNullable)
      val valueData = deserializerFor(Invoke(input, "valueArray", valueArrayType))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        ObjectType(classOf[Map[_, _]]),
        "toScalaMap",
        keyData :: valueData :: Nil,
        returnNullable = false)

    case schema @ StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        If(
          Invoke(input, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, externalDataTypeFor(f.dataType)),
          deserializerFor(GetStructField(input, i)))
      }
      If(IsNull(input),
        Literal.create(null, externalDataTypeFor(input.dataType)),
        CreateExternalRow(convertedFields, schema))

    // For other data types, return the internal catalyst value as it is.
    case _ => input
  }

  private def expressionForNullableExpr(
      expr: Expression,
      newExprWhenNotNull: Expression): Expression = {
    If(IsNull(expr), Literal.create(null, newExprWhenNotNull.dataType), newExprWhenNotNull)
  }
}
