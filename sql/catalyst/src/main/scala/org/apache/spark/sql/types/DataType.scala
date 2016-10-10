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

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * The base type of all Spark SQL data types.
 */
@DeveloperApi
abstract class DataType extends AbstractDataType {
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
  def typeName: String = {
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type").stripSuffix("UDT").toLowerCase
  }

  private[sql] def jsonValue: JValue = typeName

  /** The compact JSON representation of this data type. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this data type. */
  def prettyJson: String = pretty(render(jsonValue))

  /** Readable string representation for the type. */
  def simpleString: String = typeName

  /** String representation for the type saved in external catalogs. */
  def catalogString: String = simpleString

  /** Readable string representation for the type with truncation */
  private[sql] def simpleString(maxNumberFields: Int): String = simpleString

  def sql: String = simpleString.toUpperCase

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

  /**
   * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
   */
  private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  override private[sql] def defaultConcreteType: DataType = this

  override private[sql] def acceptsType(other: DataType): Boolean = sameType(other)
}


object DataType {

  def fromJson(json: String): DataType = parseDataType(parse(json))

  private val nonDecimalNameToType = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      .map(t => t.typeName -> t).toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
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
  private[sql] def parseDataType(json: JValue): DataType = json match {
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

    // Scala/Java UDT
    case JSortedObject(
    ("class", JString(udtClass)),
    ("pyClass", _),
    ("sqlType", _),
    ("type", JString("udt"))) =>
      Utils.classForName(udtClass).newInstance().asInstanceOf[UserDefinedType[_]]

    // Python UDT
    case JSortedObject(
    ("pyClass", JString(pyClass)),
    ("serializedClass", JString(serialized)),
    ("sqlType", v: JValue),
    ("type", JString("udt"))) =>
        new PythonUserDefinedType(parseDataType(v), pyClass, serialized)
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
      case (l: DataType, r: UserDefinedType[_]) => r.acceptsType(l)
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
