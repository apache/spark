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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, runtimeMirror}
import scala.util.parsing.combinator.RegexParsers

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.util.Utils


/**
 * :: DeveloperApi ::
 * The base type of all Spark SQL data types.
 *
 * @group dataType
 */
@DeveloperApi
abstract class DataType {
  /**
   * Enables matching against DataType for expressions:
   * {{{
   *   case Cast(child @ BinaryType(), StringType) =>
   *     ...
   * }}}
   */
  private[sql] def unapply(e: Expression): Boolean = e.dataType == this

  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int

  /** Name of the type used in JSON serialization. */
  def typeName: String = this.getClass.getSimpleName.stripSuffix("$").dropRight(4).toLowerCase

  private[sql] def jsonValue: JValue = typeName

  /** The compact JSON representation of this data type. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this data type. */
  def prettyJson: String = pretty(render(jsonValue))

  /** Readable string representation for the type. */
  def simpleString: String = typeName

  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def sameType(other: DataType): Boolean =
    DataType.equalsIgnoreNullability(this, other)

  /**
   * Returns the same data type but set all nullability fields are true
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def asNullable: DataType
}


/**
 * An internal type used to represent everything that is not null, UDTs, arrays, structs, and maps.
 */
protected[sql] abstract class AtomicType extends DataType {
  private[sql] type InternalType
  @transient private[sql] val tag: TypeTag[InternalType]
  private[sql] val ordering: Ordering[InternalType]

  @transient private[sql] val classTag = ScalaReflectionLock.synchronized {
    val mirror = runtimeMirror(Utils.getSparkClassLoader)
    ClassTag[InternalType](mirror.runtimeClass(tag.tpe))
  }
}


/**
 * :: DeveloperApi ::
 * Numeric data types.
 *
 * @group dataType
 */
abstract class NumericType extends AtomicType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer an no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[InternalType]
}


private[sql] object NumericType {
  /**
   * Enables matching against NumericType for expressions:
   * {{{
   *   case Cast(child @ NumericType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]
}


private[sql] object IntegralType {
  /**
   * Enables matching against IntegralType for expressions:
   * {{{
   *   case Cast(child @ IntegralType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[IntegralType]
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


object DataType {

  def fromJson(json: String): DataType = parseDataType(parse(json))

  @deprecated("Use DataType.fromJson instead", "1.2.0")
  def fromCaseClassString(string: String): DataType = CaseClassStringParser(string)

  private val nonDecimalNameToType = {
    Seq(NullType, DateType, TimestampType, BinaryType,
      IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType)
      .map(t => t.typeName -> t).toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    name match {
      case "decimal" => DecimalType.Unlimited
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType(other)
    }
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      nameToType(name)

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

    case JSortedObject(
    ("class", JString(udtClass)),
    ("pyClass", _),
    ("sqlType", _),
    ("type", JString("udt"))) =>
      Class.forName(udtClass).newInstance().asInstanceOf[UserDefinedType[_]]
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
    ("metadata", metadata: JObject),
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable, Metadata.fromJObject(metadata))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
  }

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
        | "DateType" ^^^ DateType
        | "DecimalType()" ^^^ DecimalType.Unlimited
        | fixedDecimalType
        | "TimestampType" ^^^ TimestampType
        )

    protected lazy val fixedDecimalType: Parser[DataType] =
      ("DecimalType(" ~> "[0-9]+".r) ~ ("," ~> "[0-9]+".r <~ ")") ^^ {
        case precision ~ scale => DecimalType(precision.toInt, scale.toInt)
      }

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
        case fields => StructType(fields)
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

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  private[types] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
        equalsIgnoreNullability(leftElementType, rightElementType)
      case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) =>
        equalsIgnoreNullability(leftKeyType, rightKeyType) &&
          equalsIgnoreNullability(leftValueType, rightValueType)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
          leftFields.zip(rightFields).forall { case (l, r) =>
            l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
          }
      case (l, r) => l == r
    }
  }

  /**
   * Compares two types, ignoring compatible nullability of ArrayType, MapType, StructType.
   *
   * Compatible nullability is defined as follows:
   *   - If `from` and `to` are ArrayTypes, `from` has a compatible nullability with `to`
   *   if and only if `to.containsNull` is true, or both of `from.containsNull` and
   *   `to.containsNull` are false.
   *   - If `from` and `to` are MapTypes, `from` has a compatible nullability with `to`
   *   if and only if `to.valueContainsNull` is true, or both of `from.valueContainsNull` and
   *   `to.valueContainsNull` are false.
   *   - If `from` and `to` are StructTypes, `from` has a compatible nullability with `to`
   *   if and only if for all every pair of fields, `to.nullable` is true, or both
   *   of `fromField.nullable` and `toField.nullable` are false.
   */
  private[sql] def equalsIgnoreCompatibleNullability(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
        (tn || !fn) && equalsIgnoreCompatibleNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
        (tn || !fn) &&
          equalsIgnoreCompatibleNullability(fromKey, toKey) &&
          equalsIgnoreCompatibleNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (fromField, toField) =>
            fromField.name == toField.name &&
              (toField.nullable || !fromField.nullable) &&
              equalsIgnoreCompatibleNullability(fromField.dataType, toField.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }
}
