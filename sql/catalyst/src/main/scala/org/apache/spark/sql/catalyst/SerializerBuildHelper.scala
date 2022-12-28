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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, CreateNamedStruct, Expression, IsNull, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData, IntervalUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object SerializerBuildHelper {

  private def nullOnOverflow: Boolean = !SQLConf.get.ansiEnabled

  def createSerializerForBoolean(inputObject: Expression): Expression = {
    Invoke(inputObject, "booleanValue", BooleanType)
  }

  def createSerializerForByte(inputObject: Expression): Expression = {
    Invoke(inputObject, "byteValue", ByteType)
  }

  def createSerializerForShort(inputObject: Expression): Expression = {
    Invoke(inputObject, "shortValue", ShortType)
  }

  def createSerializerForInteger(inputObject: Expression): Expression = {
    Invoke(inputObject, "intValue", IntegerType)
  }

  def createSerializerForLong(inputObject: Expression): Expression = {
    Invoke(inputObject, "longValue", LongType)
  }

  def createSerializerForFloat(inputObject: Expression): Expression = {
    Invoke(inputObject, "floatValue", FloatType)
  }

  def createSerializerForDouble(inputObject: Expression): Expression = {
    Invoke(inputObject, "doubleValue", DoubleType)
  }

  def createSerializerForString(inputObject: Expression): Expression = {
    StaticInvoke(
      classOf[UTF8String],
      StringType,
      "fromString",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForJavaInstant(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      TimestampType,
      "instantToMicros",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForScalaEnum(inputObject: Expression): Expression = {
    createSerializerForString(
      Invoke(
        inputObject,
        "toString",
        ObjectType(classOf[String]),
        returnNullable = false))
  }

  def createSerializerForJavaEnum(inputObject: Expression): Expression =
    createSerializerForString(Invoke(inputObject, "name", ObjectType(classOf[String])))

  def createSerializerForSqlTimestamp(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      TimestampType,
      "fromJavaTimestamp",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForAnyTimestamp(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      TimestampType,
      "anyToMicros",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForLocalDateTime(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      TimestampNTZType,
      "localDateTimeToMicros",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForJavaLocalDate(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      DateType,
      "localDateToDays",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForSqlDate(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      DateType,
      "fromJavaDate",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForAnyDate(inputObject: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      DateType,
      "anyToDays",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForJavaDuration(inputObject: Expression): Expression = {
    StaticInvoke(
      IntervalUtils.getClass,
      DayTimeIntervalType(),
      "durationToMicros",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForJavaPeriod(inputObject: Expression): Expression = {
    StaticInvoke(
      IntervalUtils.getClass,
      YearMonthIntervalType(),
      "periodToMonths",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForJavaBigDecimal(inputObject: Expression): Expression = {
    CheckOverflow(StaticInvoke(
      Decimal.getClass,
      DecimalType.SYSTEM_DEFAULT,
      "apply",
      inputObject :: Nil,
      returnNullable = false), DecimalType.SYSTEM_DEFAULT, nullOnOverflow)
  }

  def createSerializerForScalaBigDecimal(inputObject: Expression): Expression = {
    createSerializerForJavaBigDecimal(inputObject)
  }

  def createSerializerForJavaBigInteger(inputObject: Expression): Expression = {
    CheckOverflow(StaticInvoke(
      Decimal.getClass,
      DecimalType.BigIntDecimal,
      "apply",
      inputObject :: Nil,
      returnNullable = false), DecimalType.BigIntDecimal, nullOnOverflow)
  }

  def createSerializerForScalaBigInt(inputObject: Expression): Expression = {
    createSerializerForJavaBigInteger(inputObject)
  }

  def createSerializerForPrimitiveArray(
      inputObject: Expression,
      dataType: DataType): Expression = {
    StaticInvoke(
      classOf[UnsafeArrayData],
      ArrayType(dataType, false),
      "fromPrimitiveArray",
      inputObject :: Nil,
      returnNullable = false)
  }

  def createSerializerForGenericArray(
      inputObject: Expression,
      dataType: DataType,
      nullable: Boolean): Expression = {
    NewInstance(
      classOf[GenericArrayData],
      inputObject :: Nil,
      dataType = ArrayType(dataType, nullable))
  }

  def createSerializerForMapObjects(
      inputObject: Expression,
      dataType: ObjectType,
      funcForNewExpr: Expression => Expression): Expression = {
    MapObjects(funcForNewExpr, inputObject, dataType)
  }

  case class MapElementInformation(
      dataType: DataType,
      nullable: Boolean,
      funcForNewExpr: Expression => Expression)

  def createSerializerForMap(
      inputObject: Expression,
      keyInformation: MapElementInformation,
      valueInformation: MapElementInformation): Expression = {
    ExternalMapToCatalyst(
      inputObject,
      keyInformation.dataType,
      keyInformation.funcForNewExpr,
      keyNullable = keyInformation.nullable,
      valueInformation.dataType,
      valueInformation.funcForNewExpr,
      valueNullable = valueInformation.nullable
    )
  }

  private def argumentsForFieldSerializer(
      fieldName: String,
      serializerForFieldValue: Expression): Seq[Expression] = {
    expressions.Literal(fieldName) :: serializerForFieldValue :: Nil
  }

  def createSerializerForObject(
      inputObject: Expression,
      fields: Seq[(String, Expression)]): Expression = {
    val nonNullOutput = CreateNamedStruct(fields.flatMap { case(fieldName, fieldExpr) =>
      argumentsForFieldSerializer(fieldName, fieldExpr)
    })
    if (inputObject.nullable) {
      val nullOutput = expressions.Literal.create(null, nonNullOutput.dataType)
      expressions.If(IsNull(inputObject), nullOutput, nonNullOutput)
    } else {
      nonNullOutput
    }
  }

  def createSerializerForUserDefinedType(
      inputObject: Expression,
      udt: UserDefinedType[_],
      udtClass: Class[_]): Expression = {
    val obj = NewInstance(udtClass, Nil, dataType = ObjectType(udtClass))
    Invoke(obj, "serialize", udt, inputObject :: Nil)
  }
}
