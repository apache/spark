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

import java.sql.{Date, Timestamp}

import scala.math.Numeric.{BigDecimalAsIfIntegral, DoubleAsIfIntegral, FloatAsIfIntegral}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, runtimeMirror, typeTag}
import scala.util.parsing.combinator.RegexParsers

import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.util.Utils


object DataType {
  def fromJson(json: String): DataType = parseDataType(parse(json))

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      PrimitiveType.nameToType(name)

    case JSortedObject(
        ("containsNull", JBool(n)),
        ("elementType", t: JValue),
        ("type", JString("array"))) =>
      ArrayType(parseDataType(t), n)

    case JSortedObject(
        ("keyType", k: JValue),
        ("type", JString("map")),
        ("valueContainsNull", JBool(n)),
        ("valueType", v: JValue)) =>
      MapType(parseDataType(k), parseDataType(v), n)

    case JSortedObject(
        ("fields", JArray(fields)),
        ("type", JString("struct"))) =>
      StructType(fields.map(parseStructField))
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
        ("name", JString(name)),
        ("nullable", JBool(nullable)),
        ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
  }

  @deprecated("Use DataType.fromJson instead")
  def fromCaseClassString(string: String): DataType = CaseClassStringParser(string)

  private object CaseClassStringParser extends RegexParsers {
    protected lazy val primitiveType: Parser[DataType] =
      ( "StringType" ^^^ StringType
      | "FloatType" ^^^ FloatType
      | "IntegerType" ^^^ IntegerType
      | "ByteType" ^^^ ByteType
      | "ShortType" ^^^ ShortType
      | "DoubleType" ^^^ DoubleType
      | "LongType" ^^^ LongType
      | "BinaryType" ^^^ BinaryType
      | "BooleanType" ^^^ BooleanType
      | "DecimalType" ^^^ DecimalType
      | "DateType" ^^^ DateType
      | "TimestampType" ^^^ TimestampType
      )

    protected lazy val arrayType: Parser[DataType] =
      "ArrayType" ~> "(" ~> dataType ~ "," ~ boolVal <~ ")" ^^ {
        case tpe ~ _ ~ containsNull => ArrayType(tpe, containsNull)
      }

    protected lazy val mapType: Parser[DataType] =
      "MapType" ~> "(" ~> dataType ~ "," ~ dataType ~ "," ~ boolVal <~ ")" ^^ {
        case t1 ~ _ ~ t2 ~ _ ~ valueContainsNull => MapType(t1, t2, valueContainsNull)
      }

    protected lazy val structField: Parser[StructField] =
      ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
        case name ~ tpe ~ nullable  =>
          StructField(name, tpe, nullable = nullable)
      }

    protected lazy val boolVal: Parser[Boolean] =
      ( "true" ^^^ true
      | "false" ^^^ false
      )

    protected lazy val structType: Parser[DataType] =
      "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ {
        case fields => new StructType(fields)
      }

    protected lazy val dataType: Parser[DataType] =
      ( arrayType
      | mapType
      | structType
      | primitiveType
      )

    /**
     * Parses a string representation of a DataType.
     *
     * TODO: Generate parser as pickler...
     */
    def apply(asString: String): DataType = parseAll(dataType, asString) match {
      case Success(result, _) => result
      case failure: NoSuccess =>
        throw new IllegalArgumentException(s"Unsupported dataType: $asString, $failure")
    }

  }

  protected[types] def buildFormattedString(
      dataType: DataType,
      prefix: String,
      builder: StringBuilder): Unit = {
    dataType match {
      case array: ArrayType =>
        array.buildFormattedString(prefix, builder)
      case struct: StructType =>
        struct.buildFormattedString(prefix, builder)
      case map: MapType =>
        map.buildFormattedString(prefix, builder)
      case _ =>
    }
  }
}

abstract class DataType {
  /** Matches any expression that evaluates to this DataType */
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType == this => true
    case _ => false
  }

  def isPrimitive: Boolean = false

  def typeName: String = this.getClass.getSimpleName.stripSuffix("$").dropRight(4).toLowerCase

  private[sql] def jsonValue: JValue = typeName

  def json: String = compact(render(jsonValue))

  def prettyJson: String = pretty(render(jsonValue))
}

case object NullType extends DataType

object NativeType {
  val all = Seq(
    IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType)

  def unapply(dt: DataType): Boolean = all.contains(dt)

  val defaultSizeOf: Map[NativeType, Int] = Map(
    IntegerType -> 4,
    BooleanType -> 1,
    LongType -> 8,
    DoubleType -> 8,
    FloatType -> 4,
    ShortType -> 2,
    ByteType -> 1,
    StringType -> 4096)
}

trait PrimitiveType extends DataType {
  override def isPrimitive = true
}

object PrimitiveType {
  private[sql] val all = Seq(DecimalType, DateType, TimestampType, BinaryType) ++
    NativeType.all

  private[sql] val nameToType = all.map(t => t.typeName -> t).toMap
}

abstract class NativeType extends DataType {
  private[sql] type JvmType
  @transient private[sql] val tag: TypeTag[JvmType]
  private[sql] val ordering: Ordering[JvmType]

  @transient private[sql] val classTag = ScalaReflectionLock.synchronized {
    val mirror = runtimeMirror(Utils.getSparkClassLoader)
    ClassTag[JvmType](mirror.runtimeClass(tag.tpe))
  }
}

case object StringType extends NativeType with PrimitiveType {
  private[sql] type JvmType = String
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

case object BinaryType extends NativeType with PrimitiveType {
  private[sql] type JvmType = Array[Byte]
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      for (i <- 0 until x.length; if i < y.length) {
        val res = x(i).compareTo(y(i))
        if (res != 0) return res
      }
      x.length - y.length
    }
  }
}

case object BooleanType extends NativeType with PrimitiveType {
  private[sql] type JvmType = Boolean
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

case object TimestampType extends NativeType {
  private[sql] type JvmType = Timestamp

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Timestamp, y: Timestamp) = x.compareTo(y)
  }
}

case object DateType extends NativeType {
  private[sql] type JvmType = Date

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Date, y: Date) = x.compareTo(y)
  }
}

abstract class NumericType extends NativeType with PrimitiveType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer an no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[JvmType]
}

object NumericType {
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]
}

/** Matcher for any expressions that evaluate to [[IntegralType]]s */
object IntegralType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[IntegralType] => true
    case _ => false
  }
}

abstract class IntegralType extends NumericType {
  private[sql] val integral: Integral[JvmType]
}

case object LongType extends IntegralType {
  private[sql] type JvmType = Long
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Long]]
  private[sql] val integral = implicitly[Integral[Long]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

case object IntegerType extends IntegralType {
  private[sql] type JvmType = Int
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Int]]
  private[sql] val integral = implicitly[Integral[Int]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

case object ShortType extends IntegralType {
  private[sql] type JvmType = Short
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Short]]
  private[sql] val integral = implicitly[Integral[Short]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

case object ByteType extends IntegralType {
  private[sql] type JvmType = Byte
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Byte]]
  private[sql] val integral = implicitly[Integral[Byte]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}

/** Matcher for any expressions that evaluate to [[FractionalType]]s */
object FractionalType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[FractionalType] => true
    case _ => false
  }
}
abstract class FractionalType extends NumericType {
  private[sql] val fractional: Fractional[JvmType]
  private[sql] val asIntegral: Integral[JvmType]
}

case object DecimalType extends FractionalType {
  private[sql] type JvmType = BigDecimal
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[BigDecimal]]
  private[sql] val fractional = implicitly[Fractional[BigDecimal]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
  private[sql] val asIntegral = BigDecimalAsIfIntegral
}

case object DoubleType extends FractionalType {
  private[sql] type JvmType = Double
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Double]]
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
  private[sql] val asIntegral = DoubleAsIfIntegral
}

case object FloatType extends FractionalType {
  private[sql] type JvmType = Float
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Float]]
  private[sql] val fractional = implicitly[Fractional[Float]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
  private[sql] val asIntegral = FloatAsIfIntegral
}

object ArrayType {
  /** Construct a [[ArrayType]] object with the given element type. The `containsNull` is true. */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, true)
}

/**
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * @param elementType The data type of values.
 * @param containsNull Indicates if values have `null` values
 */
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {
  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(
      s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
    DataType.buildFormattedString(elementType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("elementType" -> elementType.jsonValue) ~
      ("containsNull" -> containsNull)
}

/**
 * A field inside a StructType.
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 */
case class StructField(name: String, dataType: DataType, nullable: Boolean) {

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- $name: ${dataType.typeName} (nullable = $nullable)\n")
    DataType.buildFormattedString(dataType, s"$prefix    |", builder)
  }

  private[sql] def jsonValue: JValue = {
    ("name" -> name) ~
      ("type" -> dataType.jsonValue) ~
      ("nullable" -> nullable)
  }
}

object StructType {
  protected[sql] def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable)))
}

case class StructType(fields: Seq[StructField]) extends DataType {

  /**
   * Returns all field names in a [[Seq]].
   */
  lazy val fieldNames: Seq[String] = fields.map(_.name)
  private lazy val fieldNamesSet: Set[String] = fieldNames.toSet
  private lazy val nameToField: Map[String, StructField] = fields.map(f => f.name -> f).toMap
  /**
   * Extracts a [[StructField]] of the given name. If the [[StructType]] object does not
   * have a name matching the given name, `null` will be returned.
   */
  def apply(name: String): StructField = {
    nameToField.getOrElse(name, throw new IllegalArgumentException(s"Field $name does not exist."))
  }

  /**
   * Returns a [[StructType]] containing [[StructField]]s of the given names.
   * Those names which do not have matching fields will be ignored.
   */
  def apply(names: Set[String]): StructType = {
    val nonExistFields = names -- fieldNamesSet
    if (nonExistFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Field ${nonExistFields.mkString(",")} does not exist.")
    }
    // Preserve the original order of fields.
    StructType(fields.filter(f => names.contains(f.name)))
  }

  protected[sql] def toAttributes =
    fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

  def treeString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    fields.foreach(field => field.buildFormattedString(prefix, builder))

    builder.toString()
  }

  def printTreeString(): Unit = println(treeString)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    fields.foreach(field => field.buildFormattedString(prefix, builder))
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("fields" -> fields.map(_.jsonValue))
}

object MapType {
  /**
   * Construct a [[MapType]] object with the given key type and value type.
   * The `valueContainsNull` is true.
   */
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType: DataType, valueType: DataType, true)
}

/**
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 */
case class MapType(
    keyType: DataType,
    valueType: DataType,
    valueContainsNull: Boolean) extends DataType {
  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- key: ${keyType.typeName}\n")
    builder.append(s"$prefix-- value: ${valueType.typeName} " +
      s"(valueContainsNull = $valueContainsNull)\n")
    DataType.buildFormattedString(keyType, s"$prefix    |", builder)
    DataType.buildFormattedString(valueType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue: JValue =
    ("type" -> typeName) ~
      ("keyType" -> keyType.jsonValue) ~
      ("valueType" -> valueType.jsonValue) ~
      ("valueContainsNull" -> valueContainsNull)
}
