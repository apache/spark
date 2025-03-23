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

import scala.collection.mutable
import scala.reflect.classTag

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BinaryEncoder, BoxedBooleanEncoder, BoxedByteEncoder, BoxedDoubleEncoder, BoxedFloatEncoder, BoxedIntEncoder, BoxedLongEncoder, BoxedShortEncoder, CalendarIntervalEncoder, CharEncoder, DateEncoder, DayTimeIntervalEncoder, EncoderField, InstantEncoder, IterableEncoder, JavaDecimalEncoder, LocalDateEncoder, LocalDateTimeEncoder, LocalTimeEncoder, MapEncoder, NullEncoder, RowEncoder => AgnosticRowEncoder, StringEncoder, TimestampEncoder, UDTEncoder, VarcharEncoder, VariantEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.errors.{DataTypeErrorsBase, ExecutionErrors}
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * A factory for constructing encoders that convert external row to/from the Spark SQL internal
 * binary representation.
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
 *   TimeType -> java.time.LocalTime
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
object RowEncoder extends DataTypeErrorsBase {
  def encoderFor(schema: StructType): AgnosticEncoder[Row] = {
    encoderFor(schema, lenient = false)
  }

  def encoderFor(schema: StructType, lenient: Boolean): AgnosticEncoder[Row] = {
    encoderForDataType(schema, lenient).asInstanceOf[AgnosticEncoder[Row]]
  }

  private[sql] def encoderForDataType(dataType: DataType, lenient: Boolean): AgnosticEncoder[_] =
    dataType match {
      case NullType => NullEncoder
      case BooleanType => BoxedBooleanEncoder
      case ByteType => BoxedByteEncoder
      case ShortType => BoxedShortEncoder
      case IntegerType => BoxedIntEncoder
      case LongType => BoxedLongEncoder
      case FloatType => BoxedFloatEncoder
      case DoubleType => BoxedDoubleEncoder
      case dt: DecimalType => JavaDecimalEncoder(dt, lenientSerialization = true)
      case BinaryType => BinaryEncoder
      case CharType(length) if SqlApiConf.get.preserveCharVarcharTypeInfo =>
        CharEncoder(length)
      case VarcharType(length) if SqlApiConf.get.preserveCharVarcharTypeInfo =>
        VarcharEncoder(length)
      case s: StringType if StringHelper.isPlainString(s) => StringEncoder
      case TimestampType if SqlApiConf.get.datetimeJava8ApiEnabled => InstantEncoder(lenient)
      case TimestampType => TimestampEncoder(lenient)
      case TimestampNTZType => LocalDateTimeEncoder
      case DateType if SqlApiConf.get.datetimeJava8ApiEnabled => LocalDateEncoder(lenient)
      case DateType => DateEncoder(lenient)
      case _: TimeType => LocalTimeEncoder
      case CalendarIntervalType => CalendarIntervalEncoder
      case _: DayTimeIntervalType => DayTimeIntervalEncoder
      case _: YearMonthIntervalType => YearMonthIntervalEncoder
      case _: VariantType => VariantEncoder
      case p: PythonUserDefinedType =>
        // TODO check if this works.
        encoderForDataType(p.sqlType, lenient)
      case udt: UserDefinedType[_] =>
        val annotation = udt.userClass.getAnnotation(classOf[SQLUserDefinedType])
        val udtClass: Class[_] = if (annotation != null) {
          annotation.udt()
        } else {
          UDTRegistration.getUDTFor(udt.userClass.getName).getOrElse {
            throw ExecutionErrors.userDefinedTypeNotAnnotatedAndRegisteredError(udt)
          }
        }
        UDTEncoder(udt, udtClass.asInstanceOf[Class[_ <: UserDefinedType[_]]])
      case ArrayType(elementType, containsNull) =>
        IterableEncoder(
          classTag[mutable.ArraySeq[_]],
          encoderForDataType(elementType, lenient),
          containsNull,
          lenientSerialization = true)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapEncoder(
          classTag[scala.collection.Map[_, _]],
          encoderForDataType(keyType, lenient),
          encoderForDataType(valueType, lenient),
          valueContainsNull)
      case StructType(fields) =>
        AgnosticRowEncoder(fields.map { field =>
          EncoderField(
            field.name,
            encoderForDataType(field.dataType, lenient),
            field.nullable,
            field.metadata)
        }.toImmutableArraySeq)

      case _ =>
        throw new AnalysisException(
          errorClass = "UNSUPPORTED_DATA_TYPE_FOR_ENCODER",
          messageParameters = Map("dataType" -> toSQLType(dataType)))
    }
}
