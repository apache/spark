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

import java.util.Locale

import scala.util.control.NonFatal

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * The base type of all Spark SQL data types.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
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
    this.getClass.getSimpleName
      .stripSuffix("$").stripSuffix("Type").stripSuffix("UDT")
      .toLowerCase(Locale.ROOT)
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

  def sql: String = simpleString.toUpperCase(Locale.ROOT)

  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def sameType(other: DataType): Boolean =
    if (SQLConf.get.caseSensitiveAnalysis) {
      DataType.equalsIgnoreNullability(this, other)
    } else {
      DataType.equalsIgnoreCaseAndNullability(this, other)
    }

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


/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
object DataType {

  def fromDDL(ddl: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(ddl)
    } catch {
      case NonFatal(_) => CatalystSqlParser.parseTableSchema(ddl)
    }
  }

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
      case other => nonDecimalNameToType.getOrElse(
        other,
        throw new IllegalArgumentException(
          s"Failed to convert the JSON string '$name' to a data type."))
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

    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a data type.")
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
    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a field.")
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

  private val NoNameCheck = 0
  private val CaseSensitiveNameCheck = 1
  private val CaseInsensitiveNameCheck = 2
  private val NoNullabilityCheck = 0
  private val NullabilityCheck = 1
  private val CompatibleNullabilityCheck = 2

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  private[types] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    equalsDataTypes(left, right, CaseSensitiveNameCheck, NoNullabilityCheck)
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
    equalsDataTypes(from, to, CaseSensitiveNameCheck, CompatibleNullabilityCheck)
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  private[sql] def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean = {
    equalsDataTypes(from, to, CaseInsensitiveNameCheck, NoNullabilityCheck)
  }

  /**
   * Returns true if the two data types share the same "shape", i.e. the types
   * are the same, but the field names don't need to be the same.
   *
   * @param ignoreNullability whether to ignore nullability when comparing the types
   */
  def equalsStructurally(
      from: DataType,
      to: DataType,
      ignoreNullability: Boolean = false): Boolean = {
    if (ignoreNullability) {
      equalsDataTypes(from, to, NoNameCheck, NoNullabilityCheck)
    } else {
      equalsDataTypes(from, to, NoNameCheck, NullabilityCheck)
    }
  }

  /** Given the fieldNames compare for equality based on nameCheckType */
  private def isSameFieldName(left: String, right: String, nameCheckType: Int): Boolean = {
    nameCheckType match {
      case NoNameCheck => true
      case CaseSensitiveNameCheck => left == right
      case CaseInsensitiveNameCheck => left.toLowerCase == right.toLowerCase
    }
  }

  /** Given the nullability of two datatypes compare for equality based on nullabilityCheckType */
  private def isSameNullability(
      leftNullability: Boolean,
      rightNullability: Boolean,
      nullabilityCheckType: Int): Boolean = {
    nullabilityCheckType match {
      case NoNullabilityCheck => true
      case NullabilityCheck => leftNullability == rightNullability
      case CompatibleNullabilityCheck => rightNullability || !leftNullability
    }
  }

  /**
   * Compare two dataTypes based on -
   * nameCheckType - (NoNameCheck, CaseSensitiveNameCheck, CaseInsensitiveNameCheck)
   * nullabilityCheckType - (NoNullabilityCheck, NullabilityCheck, CompatibleNullabilityCheck)
   * @param left
   * @param right
   * @param nameCheckType
   * @param nullabilityCheckType
   * @return
   */
  private def equalsDataTypes(
      left: DataType,
      right: DataType,
      nameCheckType: Int,
      nullabilityCheckType: Int
      ): Boolean = {
    (left, right) match {
      case (left: ArrayType, right: ArrayType) =>
        val sameNullability = isSameNullability(left.containsNull, right.containsNull,
          nullabilityCheckType)
        val sameType = equalsDataTypes(left.elementType, right.elementType,
          nameCheckType, nullabilityCheckType)
        sameNullability && sameType

      case (left: MapType, right: MapType) =>
        val sameNullability = isSameNullability(left.valueContainsNull, right.valueContainsNull,
          nullabilityCheckType)
        val sameKeyType = equalsDataTypes(left.keyType, right.keyType,
          nameCheckType, nullabilityCheckType)
        val sameValueType = equalsDataTypes(left.valueType, right.valueType,
          nameCheckType, nullabilityCheckType)
        sameNullability && sameKeyType && sameValueType

      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
          leftFields.zip(rightFields).forall { case (lf, rf) =>
            val sameFieldName = isSameFieldName(lf.name, rf.name, nameCheckType)
            val sameNullability = isSameNullability(lf.nullable, rf.nullable, nullabilityCheckType)
            val sameType = equalsDataTypes(lf.dataType, rf.dataType,
              nameCheckType, nullabilityCheckType)

            sameFieldName && sameNullability && sameType
          }

      case (leftDataType, rightDataType) => leftDataType == rightDataType
    }
  }



}
