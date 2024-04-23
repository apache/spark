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
package org.apache.spark.sql.types

/**
 * Rule that defines which upcasts are allow in Spark.
 */
private[sql] object UpCastRule {
  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  val numericPrecedence: IndexedSeq[NumericType] = IndexedSeq(
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType)

  /**
   * Returns true iff we can safely up-cast the `from` type to `to` type without any truncating or
   * precision lose or possible runtime failures. For example, long to int, string to int are not
   * up-casts.
   */
  def canUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to => true
    case (VariantType, _) => false
    case (from: NumericType, to: DecimalType) if to.isWiderThan(from) => true
    case (from: DecimalType, to: NumericType) if from.isTighterThan(to) => true
    case (f, t) if legalNumericPrecedence(f, t) => true
    case (DateType, TimestampType) => true
    case (DateType, TimestampNTZType) => true
    case (TimestampNTZType, TimestampType) => true
    case (TimestampType, TimestampNTZType) => true
    case (_: AtomicType, StringType) => true
    case (_: CalendarIntervalType, StringType) => true
    case (NullType, _) => true

    // Spark supports casting between long and timestamp, please see `longToTimestamp` and
    // `timestampToLong` for details.
    case (TimestampType, LongType) => true
    case (LongType, TimestampType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      resolvableNullability(fn, tn) && canUpCast(fromType, toType)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      resolvableNullability(fn, tn) && canUpCast(fromKey, toKey) && canUpCast(fromValue, toValue)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (f1, f2) =>
            resolvableNullability(f1.nullable, f2.nullable) && canUpCast(f1.dataType, f2.dataType)
        }

    case (_: DayTimeIntervalType, _: DayTimeIntervalType) => true
    case (_: YearMonthIntervalType, _: YearMonthIntervalType) => true

    case (from: UserDefinedType[_], to: UserDefinedType[_]) if to.acceptsType(from) => true

    case _ => false
  }

  private def legalNumericPrecedence(from: DataType, to: DataType): Boolean = {
    val fromPrecedence = numericPrecedence.indexOf(from)
    val toPrecedence = numericPrecedence.indexOf(to)
    fromPrecedence >= 0 && fromPrecedence < toPrecedence
  }

  private def resolvableNullability(from: Boolean, to: Boolean): Boolean = !from || to
}
