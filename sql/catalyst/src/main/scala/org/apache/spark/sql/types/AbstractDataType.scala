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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * A non-concrete data type, reserved for internal uses.
 */
private[sql] abstract class AbstractDataType {
  /**
   * The default concrete type to use if we want to cast a null literal into this type.
   */
  private[sql] def defaultConcreteType: DataType

  /**
   * Returns true if `other` is an acceptable input type for a function that expects this,
   * possibly abstract DataType.
   *
   * {{{
   *   // this should return true
   *   DecimalType.acceptsType(DecimalType(10, 2))
   *
   *   // this should return true as well
   *   NumericType.acceptsType(DecimalType(10, 2))
   * }}}
   */
  private[sql] def acceptsType(other: DataType): Boolean

  /** Readable string representation for the type. */
  private[sql] def simpleString: String
}


/**
 * A collection of types that can be used to specify type constraints. The sequence also specifies
 * precedence: an earlier type takes precedence over a latter type.
 *
 * {{{
 *   TypeCollection(StringType, BinaryType)
 * }}}
 *
 * This means that we prefer StringType over BinaryType if it is possible to cast to StringType.
 */
private[sql] class TypeCollection(private val types: Seq[AbstractDataType])
  extends AbstractDataType {

  require(types.nonEmpty, s"TypeCollection ($types) cannot be empty")

  override private[sql] def defaultConcreteType: DataType = types.head.defaultConcreteType

  override private[sql] def acceptsType(other: DataType): Boolean =
    types.exists(_.acceptsType(other))

  override private[sql] def simpleString: String = {
    types.map(_.simpleString).mkString("(", " or ", ")")
  }
}


private[sql] object TypeCollection {

  /**
   * Types that include numeric types and ANSI interval types.
   */
  val NumericAndAnsiInterval = TypeCollection(
    NumericType,
    DayTimeIntervalType,
    YearMonthIntervalType)

  /**
   * Types that include numeric and ANSI interval types, and additionally the legacy interval type.
   * They are only used in unary_minus, unary_positive, add and subtract operations.
   */
  val NumericAndInterval = new TypeCollection(NumericAndAnsiInterval.types :+ CalendarIntervalType)

  def apply(types: AbstractDataType*): TypeCollection = new TypeCollection(types)

  def unapply(typ: AbstractDataType): Option[Seq[AbstractDataType]] = typ match {
    case typ: TypeCollection => Some(typ.types)
    case _ => None
  }
}


/**
 * An `AbstractDataType` that matches any concrete data types.
 */
protected[sql] object AnyDataType extends AbstractDataType with Serializable {

  // Note that since AnyDataType matches any concrete types, defaultConcreteType should never
  // be invoked.
  override private[sql] def defaultConcreteType: DataType =
    throw QueryExecutionErrors.unsupportedOperationExceptionError()

  override private[sql] def simpleString: String = "any"

  override private[sql] def acceptsType(other: DataType): Boolean = true
}


/**
 * An internal type used to represent everything that is not null, UDTs, arrays, structs, and maps.
 */
protected[sql] abstract class AtomicType extends DataType {
  private[sql] type InternalType
  private[sql] val tag: TypeTag[InternalType]
  private[sql] val ordering: Ordering[InternalType]
}

object AtomicType {
  /**
   * Enables matching against AtomicType for expressions:
   * {{{
   *   case Cast(child @ AtomicType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[AtomicType]
}


/**
 * Numeric data types.
 *
 * @since 1.3.0
 */
@Stable
abstract class NumericType extends AtomicType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer a no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[InternalType]

  private[sql] def exactNumeric: Numeric[InternalType] = numeric
}


private[spark] object NumericType extends AbstractDataType {
  /**
   * Enables matching against NumericType for expressions:
   * {{{
   *   case Cast(child @ NumericType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]

  override private[spark] def defaultConcreteType: DataType = DoubleType

  override private[spark] def simpleString: String = "numeric"

  override private[spark] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[NumericType]
}


private[sql] object IntegralType extends AbstractDataType {
  /**
   * Enables matching against IntegralType for expressions:
   * {{{
   *   case Cast(child @ IntegralType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[IntegralType]

  override private[sql] def defaultConcreteType: DataType = IntegerType

  override private[sql] def simpleString: String = "integral"

  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[IntegralType]
}


private[sql] abstract class IntegralType extends NumericType {
  private[sql] val integral: Integral[InternalType]
}


private[sql] object FractionalType {
  /**
   * Enables matching against FractionalType for expressions:
   * {{{
   *   case Cast(child @ FractionalType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[FractionalType]
}


private[sql] abstract class FractionalType extends NumericType {
  private[sql] val fractional: Fractional[InternalType]
  private[sql] val asIntegral: Integral[InternalType]
}

private[sql] object AnyTimestampType extends AbstractDataType with Serializable {
  override private[sql] def defaultConcreteType: DataType = TimestampType

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[TimestampType] || other.isInstanceOf[TimestampNTZType]

  override private[sql] def simpleString = "(timestamp or timestamp without time zone)"

  def unapply(e: Expression): Boolean = acceptsType(e.dataType)
}

private[sql] abstract class DatetimeType extends AtomicType

/**
 * The interval type which conforms to the ANSI SQL standard.
 */
private[sql] abstract class AnsiIntervalType extends AtomicType
