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

import scala.language.existentials

import org.apache.spark.sql.catalyst.{expressions => exprs}
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.expressionWithNullSafety
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, BoxedBooleanEncoder, BoxedByteEncoder, BoxedDoubleEncoder, BoxedFloatEncoder, BoxedIntEncoder, BoxedLeafEncoder, BoxedLongEncoder, BoxedShortEncoder, DateEncoder, DayTimeIntervalEncoder, InstantEncoder, IterableEncoder, JavaBeanEncoder, JavaBigIntEncoder, JavaDecimalEncoder, JavaEnumEncoder, LocalDateEncoder, LocalDateTimeEncoder, MapEncoder, OptionEncoder, PrimitiveLeafEncoder, ProductEncoder, ScalaBigIntEncoder, ScalaDecimalEncoder, ScalaEnumEncoder, StringEncoder, TimestampEncoder, UDTEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.catalyst.encoders.EncoderUtils.{externalDataTypeFor, isNativeEncoder, lenientExternalDataTypeFor}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CheckOverflow, CreateNamedStruct, Expression, IsNull, KnownNotNull, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, GenericArrayData, IntervalUtils}
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

  def createSerializerForBigDecimal(inputObject: Expression): Expression = {
    createSerializerForBigDecimal(inputObject, DecimalType.SYSTEM_DEFAULT)
  }

  def createSerializerForBigDecimal(inputObject: Expression, dt: DecimalType): Expression = {
    CheckOverflow(StaticInvoke(
      Decimal.getClass,
      dt,
      "apply",
      inputObject :: Nil,
      returnNullable = false), dt, nullOnOverflow)
  }

  def createSerializerForAnyDecimal(inputObject: Expression, dt: DecimalType): Expression = {
    CheckOverflow(StaticInvoke(
      Decimal.getClass,
      dt,
      "fromDecimal",
      inputObject :: Nil,
      returnNullable = false), dt, nullOnOverflow)
  }

  def createSerializerForBigInteger(inputObject: Expression): Expression = {
    CheckOverflow(StaticInvoke(
      Decimal.getClass,
      DecimalType.BigIntDecimal,
      "apply",
      inputObject :: Nil,
      returnNullable = false), DecimalType.BigIntDecimal, nullOnOverflow)
  }

  def createSerializerForPrimitiveArray(
      inputObject: Expression,
      dataType: DataType): Expression = {
    StaticInvoke(
      classOf[UnsafeArrayData],
      ArrayType(dataType, containsNull = false),
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

  /**
   * Returns an expression for serializing an object into its Spark SQL form. The mapping
   * between the external and internal representations is described by encoder `enc`. The
   * input object is located at ordinal 0 of a row, i.e., `BoundReference(0, _)`.
   */
  def createSerializer(enc: AgnosticEncoder[_]): Expression = {
    val input = BoundReference(0, lenientExternalDataTypeFor(enc), nullable = enc.nullable)
    createSerializer(enc, input)
  }

  /**
   * Returns an expression for serializing the value of an input expression into its Spark SQL
   * representation. The mapping between the external and internal representations is described
   * by encoder `enc`.
   */
  private def createSerializer(enc: AgnosticEncoder[_], input: Expression): Expression = enc match {
    case _ if isNativeEncoder(enc) => input
    case BoxedBooleanEncoder => createSerializerForBoolean(input)
    case BoxedByteEncoder => createSerializerForByte(input)
    case BoxedShortEncoder => createSerializerForShort(input)
    case BoxedIntEncoder => createSerializerForInteger(input)
    case BoxedLongEncoder => createSerializerForLong(input)
    case BoxedFloatEncoder => createSerializerForFloat(input)
    case BoxedDoubleEncoder => createSerializerForDouble(input)
    case JavaEnumEncoder(_) => createSerializerForJavaEnum(input)
    case ScalaEnumEncoder(_, _) => createSerializerForScalaEnum(input)
    case StringEncoder => createSerializerForString(input)
    case ScalaDecimalEncoder(dt) => createSerializerForBigDecimal(input, dt)
    case JavaDecimalEncoder(dt, false) => createSerializerForBigDecimal(input, dt)
    case JavaDecimalEncoder(dt, true) => createSerializerForAnyDecimal(input, dt)
    case ScalaBigIntEncoder => createSerializerForBigInteger(input)
    case JavaBigIntEncoder => createSerializerForBigInteger(input)
    case DayTimeIntervalEncoder => createSerializerForJavaDuration(input)
    case YearMonthIntervalEncoder => createSerializerForJavaPeriod(input)
    case DateEncoder(true) | LocalDateEncoder(true) => createSerializerForAnyDate(input)
    case DateEncoder(false) => createSerializerForSqlDate(input)
    case LocalDateEncoder(false) => createSerializerForJavaLocalDate(input)
    case TimestampEncoder(true) | InstantEncoder(true) => createSerializerForAnyTimestamp(input)
    case TimestampEncoder(false) => createSerializerForSqlTimestamp(input)
    case InstantEncoder(false) => createSerializerForJavaInstant(input)
    case LocalDateTimeEncoder => createSerializerForLocalDateTime(input)
    case UDTEncoder(udt, udtClass) => createSerializerForUserDefinedType(input, udt, udtClass)
    case OptionEncoder(valueEnc) =>
      createSerializer(valueEnc, UnwrapOption(externalDataTypeFor(valueEnc), input))

    case ArrayEncoder(elementEncoder, containsNull) =>
      if (elementEncoder.isPrimitive) {
        createSerializerForPrimitiveArray(input, elementEncoder.dataType)
      } else {
        serializerForArray(elementEncoder, containsNull, input, lenientSerialization = false)
      }

    case IterableEncoder(ctag, elementEncoder, containsNull, lenientSerialization) =>
      val getter = if (classOf[scala.collection.Set[_]].isAssignableFrom(ctag.runtimeClass)) {
        // There's no corresponding Catalyst type for `Set`, we serialize a `Set` to Catalyst array.
        // Note that the property of `Set` is only kept when manipulating the data as domain object.
        Invoke(input, "toSeq", ObjectType(classOf[scala.collection.Seq[_]]))
      } else {
        input
      }
      serializerForArray(elementEncoder, containsNull, getter, lenientSerialization)

    case MapEncoder(_, keyEncoder, valueEncoder, valueContainsNull) =>
      createSerializerForMap(
        input,
        MapElementInformation(
          ObjectType(classOf[AnyRef]),
          nullable = keyEncoder.nullable,
          validateAndSerializeElement(keyEncoder, keyEncoder.nullable)),
        MapElementInformation(
          ObjectType(classOf[AnyRef]),
          nullable = valueContainsNull,
          validateAndSerializeElement(valueEncoder, valueContainsNull))
      )

    case ProductEncoder(_, fields, _) =>
      val serializedFields = fields.map { field =>
        // SPARK-26730 inputObject won't be null with If's guard below. And KnownNotNul
        // is necessary here. Because for a nullable nested inputObject with struct data
        // type, e.g. StructType(IntegerType, StringType), it will return nullable=true
        // for IntegerType without KnownNotNull. And that's what we do not expect to.
        val getter = Invoke(
          KnownNotNull(input),
          field.name,
          externalDataTypeFor(field.enc),
          returnNullable = field.nullable)
        field.name -> createSerializer(field.enc, getter)
      }
      createSerializerForObject(input, serializedFields)

    case AgnosticEncoders.RowEncoder(fields) =>
      val serializedFields = fields.zipWithIndex.map { case (field, index) =>
        val fieldValue = createSerializer(
          field.enc,
          ValidateExternalType(
            GetExternalRowField(input, index, field.name),
            field.enc.dataType,
            lenientExternalDataTypeFor(field.enc)))

        val convertedField = if (field.nullable) {
          exprs.If(
            Invoke(input, "isNullAt", BooleanType, exprs.Literal(index) :: Nil),
            // Because we strip UDTs, `field.dataType` can be different from `fieldValue.dataType`.
            // We should use `fieldValue.dataType` here.
            exprs.Literal.create(null, fieldValue.dataType),
            fieldValue
          )
        } else {
          AssertNotNull(fieldValue)
        }
        field.name -> convertedField
      }
      createSerializerForObject(input, serializedFields)

    case JavaBeanEncoder(_, fields) =>
      val serializedFields = fields.map { f =>
        val fieldValue = Invoke(
          KnownNotNull(input),
          f.readMethod.get,
          externalDataTypeFor(f.enc),
          propagateNull = f.nullable,
          returnNullable = f.nullable)
        f.name -> createSerializer(f.enc, fieldValue)
      }
      createSerializerForObject(input, serializedFields)
  }

  private def serializerForArray(
      elementEnc: AgnosticEncoder[_],
      elementNullable: Boolean,
      input: Expression,
      lenientSerialization: Boolean): Expression = {
    // Default serializer for Seq and generic Arrays. This does not work for primitive arrays.
    val genericSerializer = createSerializerForMapObjects(
      input,
      ObjectType(classOf[AnyRef]),
      validateAndSerializeElement(elementEnc, elementNullable))

    // Check if it is possible the user can pass a primitive array. This is the only case when it
    // is safe to directly convert to an array (for generic arrays and Seqs the type and the
    // nullability can be violated). If the user has passed a primitive array we create a special
    // code path to deal with these.
    val primitiveEncoderOption = elementEnc match {
      case _ if !lenientSerialization => None
      case enc: PrimitiveLeafEncoder[_] => Option(enc)
      case enc: BoxedLeafEncoder[_, _] => Option(enc.primitive)
      case _ => None
    }
    primitiveEncoderOption match {
      case Some(primitiveEncoder) =>
        val primitiveArrayClass = primitiveEncoder.clsTag.wrap.runtimeClass
        val check = Invoke(
          targetObject = exprs.Literal.fromObject(primitiveArrayClass),
          functionName = "isInstance",
          BooleanType,
          arguments = input :: Nil,
          propagateNull = false,
          returnNullable = false)
        exprs.If(
          check,
          // TODO replace this with `createSerializerForPrimitiveArray` as
          //  soon as Cast support ObjectType casts.
          StaticInvoke(
            classOf[ArrayData],
            ArrayType(elementEnc.dataType, containsNull = false),
            "toArrayData",
            input :: Nil,
            propagateNull = false,
            returnNullable = false),
          genericSerializer)
      case None =>
        genericSerializer
    }
  }

  private def validateAndSerializeElement(
      enc: AgnosticEncoder[_],
      nullable: Boolean): Expression => Expression = { input =>
    val expected = enc match {
      case OptionEncoder(_) => lenientExternalDataTypeFor(enc)
      case _ => enc.dataType
    }

    expressionWithNullSafety(
      createSerializer(
        enc,
        ValidateExternalType(input, expected, lenientExternalDataTypeFor(enc))),
      nullable,
      WalkedTypePath())
  }
}
