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

import scala.util.parsing.combinator.RegexParsers

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag, runtimeMirror}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.util.Utils

/**
 *
 */
object DataType extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "StringType" ^^^ StringType |
    "FloatType" ^^^ FloatType |
    "IntegerType" ^^^ IntegerType |
    "ByteType" ^^^ ByteType |
    "ShortType" ^^^ ShortType |
    "DoubleType" ^^^ DoubleType |
    "LongType" ^^^ LongType |
    "BinaryType" ^^^ BinaryType |
    "BooleanType" ^^^ BooleanType |
    "DecimalType" ^^^ DecimalType |
    "TimestampType" ^^^ TimestampType

  protected lazy val arrayType: Parser[DataType] =
    "ArrayType" ~> "(" ~> dataType <~ ")" ^^ ArrayType

  protected lazy val mapType: Parser[DataType] =
    "MapType" ~> "(" ~> dataType ~ "," ~ dataType <~ ")" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
      case name ~ tpe ~ nullable  =>
          StructField(name, tpe, nullable = nullable)
    }

  protected lazy val boolVal: Parser[Boolean] =
    "true" ^^^ true |
    "false" ^^^ false


  protected lazy val structType: Parser[DataType] =
    "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ {
      case fields => new StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
      mapType |
      structType |
      primitiveType

  /**
   * Parses a string representation of a DataType.
   *
   * TODO: Generate parser as pickler...
   */
  def apply(asString: String): DataType = parseAll(dataType, asString) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $asString, $failure")
  }
}

abstract class DataType {
  /** Matches any expression that evaluates to this DataType */
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType == this => true
    case _ => false
  }

  def isPrimitive: Boolean = false
}

case object NullType extends DataType

trait PrimitiveType extends DataType {
  override def isPrimitive = true
}

abstract class NativeType extends DataType {
  type JvmType
  @transient val tag: TypeTag[JvmType]
  val ordering: Ordering[JvmType]

  @transient val classTag = {
    val mirror = runtimeMirror(Utils.getSparkClassLoader)
    ClassTag[JvmType](mirror.runtimeClass(tag.tpe))
  }
}

case object StringType extends NativeType with PrimitiveType {
  type JvmType = String
  @transient lazy val tag = typeTag[JvmType]
  val ordering = implicitly[Ordering[JvmType]]
}
case object BinaryType extends DataType with PrimitiveType {
  type JvmType = Array[Byte]
}
case object BooleanType extends NativeType with PrimitiveType {
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

abstract class NumericType extends NativeType with PrimitiveType {
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

object StructType {
  def fromAttributes(attributes: Seq[Attribute]): StructType = {
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable)))
  }

  // def apply(fields: Seq[StructField]) = new StructType(fields.toIndexedSeq)
}

case class StructType(fields: Seq[StructField]) extends DataType {
  def toAttributes = fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
}

case class MapType(keyType: DataType, valueType: DataType) extends DataType
