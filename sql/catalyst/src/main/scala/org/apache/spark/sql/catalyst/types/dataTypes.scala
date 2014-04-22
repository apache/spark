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

package org.apache.spark.sql.catalyst.types

import java.sql.Timestamp

import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.catalyst.expressions.Expression

abstract class DataType {
  /** Matches any expression that evaluates to this DataType */
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType == this => true
    case _ => false
  }
}

case object NullType extends DataType

abstract class NativeType extends DataType {
  type JvmType
  @transient val tag: TypeTag[JvmType]
  val ordering: Ordering[JvmType]
}

case object StringType extends NativeType {
  type JvmType = String
  @transient lazy val tag = typeTag[JvmType]
  val ordering = implicitly[Ordering[JvmType]]
}
case object BinaryType extends DataType {
  type JvmType = Array[Byte]
}
case object BooleanType extends NativeType {
  type JvmType = Boolean
  @transient lazy val tag = typeTag[JvmType]
  val ordering = implicitly[Ordering[JvmType]]
}

case object TimestampType extends NativeType {
  type JvmType = Timestamp

  @transient lazy val tag = typeTag[JvmType]

  val ordering = new Ordering[JvmType] {
    def compare(x: Timestamp, y: Timestamp) = x.compareTo(y)
  }
}

abstract class NumericType extends NativeType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer an no argument constructor and thus the JVM cannot serialize the object anymore.
  val numeric: Numeric[JvmType]
}

/** Matcher for any expressions that evaluate to [[IntegralType]]s */
object IntegralType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[IntegralType] => true
    case _ => false
  }
}

abstract class IntegralType extends NumericType {
  val integral: Integral[JvmType]
}

case object LongType extends IntegralType {
  type JvmType = Long
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Long]]
  val integral = implicitly[Integral[Long]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object IntegerType extends IntegralType {
  type JvmType = Int
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Int]]
  val integral = implicitly[Integral[Int]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object ShortType extends IntegralType {
  type JvmType = Short
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Short]]
  val integral = implicitly[Integral[Short]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object ByteType extends IntegralType {
  type JvmType = Byte
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Byte]]
  val integral = implicitly[Integral[Byte]]
  val ordering = implicitly[Ordering[JvmType]]
}

/** Matcher for any expressions that evaluate to [[FractionalType]]s */
object FractionalType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[FractionalType] => true
    case _ => false
  }
}
abstract class FractionalType extends NumericType {
  val fractional: Fractional[JvmType]
}

case object DecimalType extends FractionalType {
  type JvmType = BigDecimal
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[BigDecimal]]
  val fractional = implicitly[Fractional[BigDecimal]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object DoubleType extends FractionalType {
  type JvmType = Double
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Double]]
  val fractional = implicitly[Fractional[Double]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object FloatType extends FractionalType {
  type JvmType = Float
  @transient lazy val tag = typeTag[JvmType]
  val numeric = implicitly[Numeric[Float]]
  val fractional = implicitly[Fractional[Float]]
  val ordering = implicitly[Ordering[JvmType]]
}

case class ArrayType(elementType: DataType) extends DataType

case class StructField(name: String, dataType: DataType, nullable: Boolean)
case class StructType(fields: Seq[StructField]) extends DataType

case class MapType(keyType: DataType, valueType: DataType) extends DataType
