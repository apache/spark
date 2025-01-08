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

import org.apache.spark.sql.catalyst.{expressions => exprs}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders, Codec, JavaSerializationCodec, KryoSerializationCodec}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, BoxedLeafEncoder, CharEncoder, DateEncoder, DayTimeIntervalEncoder, InstantEncoder, IterableEncoder, JavaBeanEncoder, JavaBigIntEncoder, JavaDecimalEncoder, JavaEnumEncoder, LocalDateEncoder, LocalDateTimeEncoder, MapEncoder, OptionEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, ProductEncoder, ScalaBigIntEncoder, ScalaDecimalEncoder, ScalaEnumEncoder, StringEncoder, TimestampEncoder, TransformingEncoder, UDTEncoder, VarcharEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.catalyst.encoders.EncoderUtils.{externalDataTypeFor, isNativeEncoder}
import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField, IsNull, Literal, MapKeys, MapValues, UpCast}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, CreateExternalRow, DecodeUsingSerializer, InitializeJavaBean, Invoke, NewInstance, StaticInvoke, UnresolvedCatalystToExternalMap, UnresolvedMapObjects, WrapOption}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CharVarcharCodegenUtils, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.types._

object DeserializerBuildHelper {
  /** Returns the current path with a sub-field extracted. */
  def addToPath(
      path: Expression,
      part: String,
      dataType: DataType,
      walkedTypePath: WalkedTypePath): Expression = {
    val newPath = UnresolvedExtractValue(path, expressions.Literal(part))
    upCastToExpectedType(newPath, dataType, walkedTypePath)
  }

  /** Returns the current path with a field at ordinal extracted. */
  def addToPathOrdinal(
      path: Expression,
      ordinal: Int,
      dataType: DataType,
      walkedTypePath: WalkedTypePath): Expression = {
    val newPath = GetStructField(path, ordinal)
    upCastToExpectedType(newPath, dataType, walkedTypePath)
  }

  def deserializerForWithNullSafetyAndUpcast(
      expr: Expression,
      dataType: DataType,
      nullable: Boolean,
      walkedTypePath: WalkedTypePath,
      funcForCreatingDeserializer: Expression => Expression): Expression = {
    val casted = upCastToExpectedType(expr, dataType, walkedTypePath)
    expressionWithNullSafety(funcForCreatingDeserializer(casted), nullable, walkedTypePath)
  }

  def expressionWithNullSafety(
      expr: Expression,
      nullable: Boolean,
      walkedTypePath: WalkedTypePath): Expression = {
    if (nullable) {
      expr
    } else {
      AssertNotNull(expr, walkedTypePath.getPaths)
    }
  }

  def createDeserializerForTypesSupportValueOf(
      path: Expression,
      clazz: Class[_]): Expression = {
    StaticInvoke(
      clazz,
      ObjectType(clazz),
      "valueOf",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForChar(
      path: Expression,
      returnNullable: Boolean,
      length: Int): Expression = {
    val expr = StaticInvoke(
      classOf[CharVarcharCodegenUtils],
      StringType,
      "charTypeWriteSideCheck",
      path :: Literal(length) :: Nil,
      returnNullable = returnNullable)
    createDeserializerForString(expr, returnNullable)
  }

  def createDeserializerForVarchar(
      path: Expression,
      returnNullable: Boolean,
      length: Int): Expression = {
    val expr = StaticInvoke(
      classOf[CharVarcharCodegenUtils],
      StringType,
      "varcharTypeWriteSideCheck",
      path :: Literal(length) :: Nil,
      returnNullable = returnNullable)
    createDeserializerForString(expr, returnNullable)
  }

  def createDeserializerForString(path: Expression, returnNullable: Boolean): Expression = {
    Invoke(path, "toString", ObjectType(classOf[java.lang.String]),
      returnNullable = returnNullable)
  }

  def createDeserializerForSqlDate(path: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[java.sql.Date]),
      "toJavaDate",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForLocalDate(path: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[java.time.LocalDate]),
      "daysToLocalDate",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForInstant(path: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[java.time.Instant]),
      "microsToInstant",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForSqlTimestamp(path: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[java.sql.Timestamp]),
      "toJavaTimestamp",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForLocalDateTime(path: Expression): Expression = {
    StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[java.time.LocalDateTime]),
      "microsToLocalDateTime",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForJavaBigDecimal(
      path: Expression,
      returnNullable: Boolean): Expression = {
    Invoke(path, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]),
      returnNullable = returnNullable)
  }

  def createDeserializerForScalaBigDecimal(
      path: Expression,
      returnNullable: Boolean): Expression = {
    Invoke(path, "toBigDecimal", ObjectType(classOf[BigDecimal]), returnNullable = returnNullable)
  }

  def createDeserializerForJavaBigInteger(
      path: Expression,
      returnNullable: Boolean): Expression = {
    Invoke(path, "toJavaBigInteger", ObjectType(classOf[java.math.BigInteger]),
      returnNullable = returnNullable)
  }

  def createDeserializerForScalaBigInt(path: Expression): Expression = {
    Invoke(path, "toScalaBigInt", ObjectType(classOf[scala.math.BigInt]),
      returnNullable = false)
  }

  def createDeserializerForDuration(path: Expression): Expression = {
    StaticInvoke(
      IntervalUtils.getClass,
      ObjectType(classOf[java.time.Duration]),
      "microsToDuration",
      path :: Nil,
      returnNullable = false)
  }

  def createDeserializerForPeriod(path: Expression): Expression = {
    StaticInvoke(
      IntervalUtils.getClass,
      ObjectType(classOf[java.time.Period]),
      "monthsToPeriod",
      path :: Nil,
      returnNullable = false)
  }

  /**
   * When we build the `deserializer` for an encoder, we set up a lot of "unresolved" stuff
   * and lost the required data type, which may lead to runtime error if the real type doesn't
   * match the encoder's schema.
   * For example, we build an encoder for `case class Data(a: Int, b: String)` and the real type
   * is [a: int, b: long], then we will hit runtime error and say that we can't construct class
   * `Data` with int and long, because we lost the information that `b` should be a string.
   *
   * This method help us "remember" the required data type by adding a `UpCast`. Note that we
   * only need to do this for leaf nodes.
   */
  private[catalyst] def upCastToExpectedType(
      expr: Expression,
      expected: DataType,
      walkedTypePath: WalkedTypePath): Expression = expected match {
    case _: StructType => expr
    case _: ArrayType => expr
    case _: MapType => expr
    case _: DecimalType =>
      // For Scala/Java `BigDecimal`, we accept decimal types of any valid precision/scale.
      // Here we use the `DecimalType` object to indicate it.
      UpCast(expr, DecimalType, walkedTypePath.getPaths)
    case _ => UpCast(expr, expected, walkedTypePath.getPaths)
  }

  /**
   * Returns an expression for deserializing the Spark SQL representation of an object into its
   * external form. The mapping between the internal and external representations is
   * described by encoder `enc`. The Spark SQL representation is located at ordinal 0 of
   * a row, i.e., `GetColumnByOrdinal(0, _)`. Nested classes will have their fields accessed using
   * `UnresolvedExtractValue`.
   *
   * The returned expression is used by `ExpressionEncoder`. The encoder will resolve and bind this
   * deserializer expression when using it.
   *
   * @param enc encoder that describes the mapping between the Spark SQL representation and the
   *            external representation.
   */
  def createDeserializer[T](enc: AgnosticEncoder[T]): Expression = {
    val walkedTypePath = WalkedTypePath().recordRoot(enc.clsTag.runtimeClass.getName)
    // Assumes we are deserializing the first column of a row.
    val input = GetColumnByOrdinal(0, enc.dataType)
    enc match {
      case AgnosticEncoders.RowEncoder(fields) =>
        val children = fields.zipWithIndex.map { case (f, i) =>
          createDeserializer(f.enc, GetStructField(input, i), walkedTypePath)
        }
        CreateExternalRow(children, enc.schema)
      case _ =>
        val deserializer = createDeserializer(
          enc,
          upCastToExpectedType(input, enc.dataType, walkedTypePath),
          walkedTypePath)
        expressionWithNullSafety(deserializer, enc.nullable, walkedTypePath)
    }
  }

  /**
   * Returns an expression for deserializing the value of an input expression into its external
   * representation. The mapping between the internal and external representations is
   * described by encoder `enc`.
   *
   * @param enc encoder that describes the mapping between the Spark SQL representation and the
   *            external representation.
   * @param path The expression which can be used to extract serialized value.
   * @param walkedTypePath The paths from top to bottom to access current field when deserializing.
   */
  private def createDeserializer(
      enc: AgnosticEncoder[_],
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = enc match {
    case _ if isNativeEncoder(enc) =>
      path
    case _: BoxedLeafEncoder[_, _] =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case JavaEnumEncoder(tag) =>
      val toString = createDeserializerForString(path, returnNullable = false)
      createDeserializerForTypesSupportValueOf(toString, tag.runtimeClass)
    case ScalaEnumEncoder(parent, tag) =>
      StaticInvoke(
        parent,
        ObjectType(tag.runtimeClass),
        "withName",
        createDeserializerForString(path, returnNullable = false) :: Nil,
        returnNullable = false)
    case CharEncoder(length) =>
      createDeserializerForChar(path, returnNullable = false, length)
    case VarcharEncoder(length) =>
      createDeserializerForVarchar(path, returnNullable = false, length)
    case StringEncoder =>
      createDeserializerForString(path, returnNullable = false)
    case _: ScalaDecimalEncoder =>
      createDeserializerForScalaBigDecimal(path, returnNullable = false)
    case _: JavaDecimalEncoder =>
      createDeserializerForJavaBigDecimal(path, returnNullable = false)
    case ScalaBigIntEncoder =>
      createDeserializerForScalaBigInt(path)
    case JavaBigIntEncoder =>
      createDeserializerForJavaBigInteger(path, returnNullable = false)
    case DayTimeIntervalEncoder =>
      createDeserializerForDuration(path)
    case YearMonthIntervalEncoder =>
      createDeserializerForPeriod(path)
    case _: DateEncoder =>
      createDeserializerForSqlDate(path)
    case _: LocalDateEncoder =>
      createDeserializerForLocalDate(path)
    case _: TimestampEncoder =>
      createDeserializerForSqlTimestamp(path)
    case _: InstantEncoder =>
      createDeserializerForInstant(path)
    case LocalDateTimeEncoder =>
      createDeserializerForLocalDateTime(path)
    case UDTEncoder(udt, udtClass) =>
      val obj = NewInstance(udtClass, Nil, ObjectType(udtClass))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)
    case OptionEncoder(valueEnc) =>
      val newTypePath = walkedTypePath.recordOption(valueEnc.clsTag.runtimeClass.getName)
      val deserializer = createDeserializer(valueEnc, path, newTypePath)
      WrapOption(deserializer, externalDataTypeFor(valueEnc))

    case ArrayEncoder(elementEnc: AgnosticEncoder[_], containsNull) =>
      Invoke(
        deserializeArray(
          path,
          elementEnc,
          containsNull,
          None,
          walkedTypePath),
        toArrayMethodName(elementEnc),
        ObjectType(enc.clsTag.runtimeClass),
        returnNullable = false)

    case IterableEncoder(clsTag, elementEnc, containsNull, _) =>
      deserializeArray(
        path,
        elementEnc,
        containsNull,
        Option(clsTag.runtimeClass),
        walkedTypePath)

    case MapEncoder(tag, keyEncoder, valueEncoder, _)
      if classOf[java.util.Map[_, _]].isAssignableFrom(tag.runtimeClass) =>
      // TODO (hvanhovell) this is can be improved.
      val newTypePath = walkedTypePath.recordMap(
        keyEncoder.clsTag.runtimeClass.getName,
        valueEncoder.clsTag.runtimeClass.getName)

      val keyData =
        Invoke(
          UnresolvedMapObjects(
            p => createDeserializer(keyEncoder, p, newTypePath),
            MapKeys(path)),
          "array",
          ObjectType(classOf[Array[Any]]))

      val valueData =
        Invoke(
          UnresolvedMapObjects(
            p => createDeserializer(valueEncoder, p, newTypePath),
            MapValues(path)),
          "array",
          ObjectType(classOf[Array[Any]]))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        ObjectType(classOf[java.util.Map[_, _]]),
        "toJavaMap",
        keyData :: valueData :: Nil,
        returnNullable = false)

    case MapEncoder(tag, keyEncoder, valueEncoder, _) =>
      val newTypePath = walkedTypePath.recordMap(
        keyEncoder.clsTag.runtimeClass.getName,
        valueEncoder.clsTag.runtimeClass.getName)
      UnresolvedCatalystToExternalMap(
        path,
        createDeserializer(keyEncoder, _, newTypePath),
        createDeserializer(valueEncoder, _, newTypePath),
        tag.runtimeClass)

    case ProductEncoder(tag, fields, outerPointerGetter) =>
      val cls = tag.runtimeClass
      val dt = ObjectType(cls)
      val isTuple = cls.getName.startsWith("scala.Tuple")
      val arguments = fields.zipWithIndex.map {
        case (field, i) =>
          val newTypePath = walkedTypePath.recordField(
            field.enc.clsTag.runtimeClass.getName,
            field.name)
          // For tuples, we grab the inner fields by ordinal instead of name.
          val getter = if (isTuple) {
            addToPathOrdinal(path, i, field.enc.dataType, newTypePath)
          } else {
            addToPath(path, field.name, field.enc.dataType, newTypePath)
          }
          expressionWithNullSafety(
            createDeserializer(field.enc, getter, newTypePath),
            field.enc.nullable,
            newTypePath)
      }
      exprs.If(
        IsNull(path),
        exprs.Literal.create(null, dt),
        NewInstance(cls, arguments, Nil, propagateNull = false, dt, outerPointerGetter))

    case AgnosticEncoders.RowEncoder(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        val newTypePath = walkedTypePath.recordField(
          f.enc.clsTag.runtimeClass.getName,
          f.name)
        exprs.If(
          Invoke(path, "isNullAt", BooleanType, exprs.Literal(i) :: Nil),
          exprs.Literal.create(null, externalDataTypeFor(f.enc)),
          createDeserializer(f.enc, GetStructField(path, i), newTypePath))
      }
      exprs.If(IsNull(path),
        exprs.Literal.create(null, externalDataTypeFor(enc)),
        CreateExternalRow(convertedFields, enc.schema))

    case JavaBeanEncoder(tag, fields) =>
      val setters = fields
        .filter(_.writeMethod.isDefined)
        .map { f =>
        val newTypePath = walkedTypePath.recordField(
          f.enc.clsTag.runtimeClass.getName,
          f.name)
        val setter = expressionWithNullSafety(
          createDeserializer(
            f.enc,
            addToPath(path, f.name, f.enc.dataType, newTypePath),
            newTypePath),
          nullable = f.nullable,
          newTypePath)
        f.writeMethod.get -> setter
      }

      val cls = tag.runtimeClass
      val newInstance = NewInstance(cls, Nil, ObjectType(cls), propagateNull = false)
      val result = InitializeJavaBean(newInstance, setters.toMap)
      exprs.If(IsNull(path), exprs.Literal.create(null, ObjectType(cls)), result)

    case TransformingEncoder(tag, _, codec) if codec == JavaSerializationCodec =>
      DecodeUsingSerializer(path, tag, kryo = false)

    case TransformingEncoder(tag, _, codec) if codec == KryoSerializationCodec =>
      DecodeUsingSerializer(path, tag, kryo = true)

    case TransformingEncoder(tag, encoder, provider) =>
      Invoke(
        Literal.create(provider(), ObjectType(classOf[Codec[_, _]])),
        "decode",
        ObjectType(tag.runtimeClass),
        createDeserializer(encoder, path, walkedTypePath) :: Nil)
  }

  private def deserializeArray(
      path: Expression,
      elementEnc: AgnosticEncoder[_],
      containsNull: Boolean,
      cls: Option[Class[_]],
      walkedTypePath: WalkedTypePath): Expression = {
    val newTypePath = walkedTypePath.recordArray(elementEnc.clsTag.runtimeClass.getName)
    val mapFunction: Expression => Expression = element => {
      // upcast the array element to the data type the encoder expects.
      deserializerForWithNullSafetyAndUpcast(
        element,
        elementEnc.dataType,
        nullable = containsNull,
        newTypePath,
        createDeserializer(elementEnc, _, newTypePath))
    }
    UnresolvedMapObjects(mapFunction, path, cls)
  }

  private def toArrayMethodName(enc: AgnosticEncoder[_]): String = enc match {
    case PrimitiveBooleanEncoder => "toBooleanArray"
    case PrimitiveByteEncoder => "toByteArray"
    case PrimitiveShortEncoder => "toShortArray"
    case PrimitiveIntEncoder => "toIntArray"
    case PrimitiveLongEncoder => "toLongArray"
    case PrimitiveFloatEncoder => "toFloatArray"
    case PrimitiveDoubleEncoder => "toDoubleArray"
    case _ => "array"
  }
}
