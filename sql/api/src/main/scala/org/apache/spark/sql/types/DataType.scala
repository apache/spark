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

import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkIllegalArgumentException, SparkThrowable}
import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.analysis.SqlApiAnalysis
import org.apache.spark.sql.catalyst.parser.DataTypeParser
import org.apache.spark.sql.catalyst.util.{CollationFactory, StringConcat}
import org.apache.spark.sql.catalyst.util.DataTypeJsonUtils.{DataTypeJsonDeserializer, DataTypeJsonSerializer}
import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.util.SparkClassUtils

/**
 * The base type of all Spark SQL data types.
 *
 * @since 1.3.0
 */

@Stable
@JsonSerialize(using = classOf[DataTypeJsonSerializer])
@JsonDeserialize(using = classOf[DataTypeJsonDeserializer])
abstract class DataType extends AbstractDataType {

  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int

  /** Name of the type used in JSON serialization. */
  def typeName: String = {
    this.getClass.getSimpleName
      .stripSuffix("$")
      .stripSuffix("Type")
      .stripSuffix("UDT")
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
    if (SqlApiConf.get.caseSensitiveAnalysis) {
      DataType.equalsIgnoreNullability(this, other)
    } else {
      DataType.equalsIgnoreCaseAndNullability(this, other)
    }

  /**
   * Returns the same data type but set all nullability fields are true (`StructField.nullable`,
   * `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def asNullable: DataType

  /**
   * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
   */
  private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  /**
   * Recursively applies the provided partial function `f` to transform this DataType tree.
   */
  private[spark] def transformRecursively(f: PartialFunction[DataType, DataType]): DataType = {
    this match {
      case _ if f.isDefinedAt(this) =>
        f(this)

      case ArrayType(elementType, containsNull) =>
        ArrayType(elementType.transformRecursively(f), containsNull)

      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          keyType.transformRecursively(f),
          valueType.transformRecursively(f),
          valueContainsNull)

      case StructType(fields) =>
        StructType(fields.map { field =>
          field.copy(dataType = field.dataType.transformRecursively(f))
        })

      case _ => this
    }
  }

  final override private[sql] def defaultConcreteType: DataType = this

  override private[sql] def acceptsType(other: DataType): Boolean = sameType(other)
}

/**
 * @since 1.3.0
 */
@Stable
object DataType {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  val COLLATIONS_METADATA_KEY = "__COLLATIONS"

  def fromDDL(ddl: String): DataType = {
    parseTypeWithFallback(
      ddl,
      DataTypeParser.parseDataType,
      fallbackParser = str => DataTypeParser.parseTableSchema(str))
  }

  /**
   * Parses data type from a string with schema. It calls `parser` for `schema`. If it fails,
   * calls `fallbackParser`. If the fallback function fails too, combines error message from
   * `parser` and `fallbackParser`.
   *
   * @param schema
   *   The schema string to parse by `parser` or `fallbackParser`.
   * @param parser
   *   The function that should be invoke firstly.
   * @param fallbackParser
   *   The function that is called when `parser` fails.
   * @return
   *   The data type parsed from the `schema` schema.
   */
  def parseTypeWithFallback(
      schema: String,
      parser: String => DataType,
      fallbackParser: String => DataType): DataType = {
    try {
      parser(schema)
    } catch {
      case NonFatal(e) =>
        try {
          fallbackParser(schema)
        } catch {
          case NonFatal(suppressed) =>
            e.addSuppressed(suppressed)
            if (e.isInstanceOf[SparkThrowable]) {
              throw e
            }
            throw DataTypeErrors.schemaFailToParseError(schema, e)
        }
    }
  }

  def fromJson(json: String): DataType = parseDataType(parse(json))

  private val otherTypes = {
    Seq(
      NullType,
      DateType,
      TimestampType,
      BinaryType,
      IntegerType,
      BooleanType,
      LongType,
      DoubleType,
      FloatType,
      ShortType,
      ByteType,
      StringType,
      CalendarIntervalType,
      DayTimeIntervalType(DAY),
      DayTimeIntervalType(DAY, HOUR),
      DayTimeIntervalType(DAY, MINUTE),
      DayTimeIntervalType(DAY, SECOND),
      DayTimeIntervalType(HOUR),
      DayTimeIntervalType(HOUR, MINUTE),
      DayTimeIntervalType(HOUR, SECOND),
      DayTimeIntervalType(MINUTE),
      DayTimeIntervalType(MINUTE, SECOND),
      DayTimeIntervalType(SECOND),
      YearMonthIntervalType(YEAR),
      YearMonthIntervalType(MONTH),
      YearMonthIntervalType(YEAR, MONTH),
      TimestampNTZType,
      VariantType)
      .map(t => t.typeName -> t)
      .toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case VARCHAR_TYPE(length) => VarcharType(length.toInt)
      // For backwards compatibility, previously the type name of NullType is "null"
      case "null" => NullType
      case "timestamp_ltz" => TimestampType
      case other =>
        otherTypes.getOrElse(
          other,
          throw new SparkIllegalArgumentException(
            errorClass = "INVALID_JSON_DATA_TYPE",
            messageParameters = Map("invalidType" -> name)))
    }
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.sortBy(_._1))
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  private[sql] def parseDataType(
      json: JValue,
      fieldPath: String = "",
      collationsMap: Map[String, String] = Map.empty): DataType = json match {
    case JString(name) =>
      collationsMap.get(fieldPath) match {
        case Some(collation) =>
          assertValidTypeForCollations(fieldPath, name, collationsMap)
          stringTypeWithCollation(collation)
        case _ => nameToType(name)
      }

    case JSortedObject(
          ("containsNull", JBool(n)),
          ("elementType", t: JValue),
          ("type", JString("array"))) =>
      assertValidTypeForCollations(fieldPath, "array", collationsMap)
      val elementType = parseDataType(t, appendFieldToPath(fieldPath, "element"), collationsMap)
      ArrayType(elementType, n)

    case JSortedObject(
          ("keyType", k: JValue),
          ("type", JString("map")),
          ("valueContainsNull", JBool(n)),
          ("valueType", v: JValue)) =>
      assertValidTypeForCollations(fieldPath, "map", collationsMap)
      val keyType = parseDataType(k, appendFieldToPath(fieldPath, "key"), collationsMap)
      val valueType = parseDataType(v, appendFieldToPath(fieldPath, "value"), collationsMap)
      MapType(keyType, valueType, n)

    case JSortedObject(("fields", JArray(fields)), ("type", JString("struct"))) =>
      assertValidTypeForCollations(fieldPath, "struct", collationsMap)
      StructType(fields.map(parseStructField))

    // Scala/Java UDT
    case JSortedObject(
          ("class", JString(udtClass)),
          ("pyClass", _),
          ("sqlType", _),
          ("type", JString("udt"))) =>
      SparkClassUtils.classForName[UserDefinedType[_]](udtClass).getConstructor().newInstance()

    // Python UDT
    case JSortedObject(
          ("pyClass", JString(pyClass)),
          ("serializedClass", JString(serialized)),
          ("sqlType", v: JValue),
          ("type", JString("udt"))) =>
      new PythonUserDefinedType(parseDataType(v), pyClass, serialized)

    case other =>
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_JSON_DATA_TYPE",
        messageParameters = Map("invalidType" -> compact(render(other))))
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
          ("metadata", JObject(metadataFields)),
          ("name", JString(name)),
          ("nullable", JBool(nullable)),
          ("type", dataType: JValue)) =>
      val collationsMap = getCollationsMap(metadataFields)
      val metadataWithoutCollations =
        JObject(metadataFields.filterNot(_._1 == COLLATIONS_METADATA_KEY))
      StructField(
        name,
        parseDataType(dataType, name, collationsMap),
        nullable,
        Metadata.fromJObject(metadataWithoutCollations))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
          ("name", JString(name)),
          ("nullable", JBool(nullable)),
          ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
    // Support reading schema when 'nullable' is missing.
    case JSortedObject(("name", JString(name)), ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType))
    case other =>
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_JSON_DATA_TYPE",
        messageParameters = Map("invalidType" -> compact(render(other))))
  }

  private def assertValidTypeForCollations(
      fieldPath: String,
      fieldType: String,
      collationMap: Map[String, String]): Unit = {
    if (collationMap.contains(fieldPath) && fieldType != "string") {
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_JSON_DATA_TYPE_FOR_COLLATIONS",
        messageParameters = Map("jsonType" -> fieldType))
    }
  }

  /**
   * Appends a field name to a given path, using a dot separator if the path is not empty.
   */
  private def appendFieldToPath(basePath: String, fieldName: String): String = {
    if (basePath.isEmpty) fieldName else s"$basePath.$fieldName"
  }

  /**
   * Returns a map of field path to collation name.
   */
  private def getCollationsMap(metadataFields: List[JField]): Map[String, String] = {
    val collationsJsonOpt = metadataFields.find(_._1 == COLLATIONS_METADATA_KEY).map(_._2)
    collationsJsonOpt match {
      case Some(JObject(fields)) =>
        fields.collect { case (fieldPath, JString(collation)) =>
          collation.split("\\.", 2) match {
            case Array(provider: String, collationName: String) =>
              CollationFactory.assertValidProvider(provider)
              fieldPath -> collationName
          }
        }.toMap

      case _ => Map.empty
    }
  }

  private def stringTypeWithCollation(collationName: String): StringType = {
    StringType(CollationFactory.collationNameToId(collationName))
  }

  protected[types] def buildFormattedString(
      dataType: DataType,
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int): Unit = {
    dataType match {
      case array: ArrayType =>
        array.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case struct: StructType =>
        struct.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case map: MapType =>
        map.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case _ =>
    }
  }

  /**
   * Compares two types, ignoring compatible nullability of ArrayType, MapType, StructType.
   *
   * Compatible nullability is defined as follows:
   *   - If `from` and `to` are ArrayTypes, `from` has a compatible nullability with `to` if and
   *     only if `to.containsNull` is true, or both of `from.containsNull` and `to.containsNull`
   *     are false.
   *   - If `from` and `to` are MapTypes, `from` has a compatible nullability with `to` if and
   *     only if `to.valueContainsNull` is true, or both of `from.valueContainsNull` and
   *     `to.valueContainsNull` are false.
   *   - If `from` and `to` are StructTypes, `from` has a compatible nullability with `to` if and
   *     only if for all every pair of fields, `to.nullable` is true, or both of
   *     `fromField.nullable` and `toField.nullable` are false.
   */
  private[sql] def equalsIgnoreCompatibleNullability(from: DataType, to: DataType): Boolean = {
    equalsIgnoreCompatibleNullability(from, to, ignoreName = false)
  }

  /**
   * Compares two types, ignoring compatible nullability of ArrayType, MapType, StructType, and
   * also the field name. It compares based on the position.
   *
   * Compatible nullability is defined as follows:
   *   - If `from` and `to` are ArrayTypes, `from` has a compatible nullability with `to` if and
   *     only if `to.containsNull` is true, or both of `from.containsNull` and `to.containsNull`
   *     are false.
   *   - If `from` and `to` are MapTypes, `from` has a compatible nullability with `to` if and
   *     only if `to.valueContainsNull` is true, or both of `from.valueContainsNull` and
   *     `to.valueContainsNull` are false.
   *   - If `from` and `to` are StructTypes, `from` has a compatible nullability with `to` if and
   *     only if for all every pair of fields, `to.nullable` is true, or both of
   *     `fromField.nullable` and `toField.nullable` are false.
   */
  private[sql] def equalsIgnoreNameAndCompatibleNullability(
      from: DataType,
      to: DataType): Boolean = {
    equalsIgnoreCompatibleNullability(from, to, ignoreName = true)
  }

  private def equalsIgnoreCompatibleNullability(
      from: DataType,
      to: DataType,
      ignoreName: Boolean = false): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
        (tn || !fn) && equalsIgnoreCompatibleNullability(fromElement, toElement, ignoreName)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
        (tn || !fn) &&
        equalsIgnoreCompatibleNullability(fromKey, toKey, ignoreName) &&
        equalsIgnoreCompatibleNullability(fromValue, toValue, ignoreName)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall { case (fromField, toField) =>
          (ignoreName || fromField.name == toField.name) &&
          (toField.nullable || !fromField.nullable) &&
          equalsIgnoreCompatibleNullability(fromField.dataType, toField.dataType, ignoreName)
        }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  /**
   * Check if `from` is equal to `to` type except for collations, which are checked to be
   * compatible so that data of type `from` can be interpreted as of type `to`.
   */
  private[sql] def equalsIgnoreCompatibleCollation(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      // String types with possibly different collations are compatible.
      case (_: StringType, _: StringType) => true

      case (ArrayType(fromElement, fromContainsNull), ArrayType(toElement, toContainsNull)) =>
        (fromContainsNull == toContainsNull) &&
        equalsIgnoreCompatibleCollation(fromElement, toElement)

      case (
            MapType(fromKey, fromValue, fromContainsNull),
            MapType(toKey, toValue, toContainsNull)) =>
        fromContainsNull == toContainsNull &&
        // Map keys cannot change collation.
        fromKey == toKey &&
        equalsIgnoreCompatibleCollation(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall { case (fromField, toField) =>
          fromField.name == toField.name &&
          fromField.nullable == toField.nullable &&
          fromField.metadata == toField.metadata &&
          equalsIgnoreCompatibleCollation(fromField.dataType, toField.dataType)
        }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  /**
   * Returns true if the two data types share the same "shape", i.e. the types are the same, but
   * the field names don't need to be the same.
   *
   * @param ignoreNullability
   *   whether to ignore nullability when comparing the types
   */
  def equalsStructurally(
      from: DataType,
      to: DataType,
      ignoreNullability: Boolean = false): Boolean = {
    (from, to) match {
      case (left: ArrayType, right: ArrayType) =>
        equalsStructurally(left.elementType, right.elementType, ignoreNullability) &&
        (ignoreNullability || left.containsNull == right.containsNull)

      case (left: MapType, right: MapType) =>
        equalsStructurally(left.keyType, right.keyType, ignoreNullability) &&
        equalsStructurally(left.valueType, right.valueType, ignoreNullability) &&
        (ignoreNullability || left.valueContainsNull == right.valueContainsNull)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields
          .zip(toFields)
          .forall { case (l, r) =>
            equalsStructurally(l.dataType, r.dataType, ignoreNullability) &&
            (ignoreNullability || l.nullable == r.nullable)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  /**
   * Returns true if the two data types have the same field names in order recursively.
   */
  def equalsStructurallyByName(
      from: DataType,
      to: DataType,
      resolver: SqlApiAnalysis.Resolver): Boolean = {
    (from, to) match {
      case (left: ArrayType, right: ArrayType) =>
        equalsStructurallyByName(left.elementType, right.elementType, resolver)

      case (left: MapType, right: MapType) =>
        equalsStructurallyByName(left.keyType, right.keyType, resolver) &&
        equalsStructurallyByName(left.valueType, right.valueType, resolver)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields
          .zip(toFields)
          .forall { case (l, r) =>
            resolver(l.name, r.name) && equalsStructurallyByName(l.dataType, r.dataType, resolver)
          }

      case _ => true
    }
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
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
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, _), ArrayType(toElement, _)) =>
        equalsIgnoreCaseAndNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
        equalsIgnoreCaseAndNullability(fromKey, toKey) &&
        equalsIgnoreCaseAndNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall { case (l, r) =>
          l.name.equalsIgnoreCase(r.name) &&
          equalsIgnoreCaseAndNullability(l.dataType, r.dataType)
        }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }
}
