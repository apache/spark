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

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.DataTypeJsonUtils.{DataTypeJsonDeserializer, DataTypeJsonSerializer}
import org.apache.spark.sql.catalyst.util.StringUtils.StringConcat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy.{ANSI, STRICT}
import org.apache.spark.util.Utils

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
@Stable
object DataType {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  def fromDDL(ddl: String): DataType = {
    parseTypeWithFallback(
      ddl,
      CatalystSqlParser.parseDataType,
      "Cannot parse the data type: ",
      fallbackParser = str => CatalystSqlParser.parseTableSchema(str))
  }

  /**
   * Parses data type from a string with schema. It calls `parser` for `schema`.
   * If it fails, calls `fallbackParser`. If the fallback function fails too, combines error message
   * from `parser` and `fallbackParser`.
   *
   * @param schema The schema string to parse by `parser` or `fallbackParser`.
   * @param parser The function that should be invoke firstly.
   * @param errorMsg The error message for `parser`.
   * @param fallbackParser The function that is called when `parser` fails.
   * @return The data type parsed from the `schema` schema.
   */
  def parseTypeWithFallback(
      schema: String,
      parser: String => DataType,
      errorMsg: String,
      fallbackParser: String => DataType): DataType = {
    try {
      parser(schema)
    } catch {
      case NonFatal(e1) =>
        try {
          fallbackParser(schema)
        } catch {
          case NonFatal(e2) =>
            throw new AnalysisException(
              message = s"$errorMsg${e1.getMessage}\nFailed fallback parsing: ${e2.getMessage}",
              cause = Some(e1.getCause))
        }
    }
  }

  def fromJson(json: String): DataType = parseDataType(parse(json))

  private val otherTypes = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      .map(t => t.typeName -> t).toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case VARCHAR_TYPE(length) => VarcharType(length.toInt)
      case other => otherTypes.getOrElse(
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
      Utils.classForName[UserDefinedType[_]](udtClass).getConstructor().newInstance()

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
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  private[sql] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
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
    equalsIgnoreCompatibleNullability(from, to, ignoreName = false)
  }

  /**
   * Compares two types, ignoring compatible nullability of ArrayType, MapType, StructType, and
   * also the field name. It compares based on the position.
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
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  private[sql] def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean = {
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
          fromFields.zip(toFields)
            .forall { case (l, r) =>
              equalsStructurally(l.dataType, r.dataType, ignoreNullability) &&
                (ignoreNullability || l.nullable == r.nullable)
            }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  private val SparkGeneratedName = """col\d+""".r
  private def isSparkGeneratedName(name: String): Boolean = name match {
    case SparkGeneratedName(_*) => true
    case _ => false
  }

  /**
   * Returns true if the write data type can be read using the read data type.
   *
   * The write type is compatible with the read type if:
   * - Both types are arrays, the array element types are compatible, and element nullability is
   *   compatible (read allows nulls or write does not contain nulls).
   * - Both types are maps and the map key and value types are compatible, and value nullability
   *   is compatible  (read allows nulls or write does not contain nulls).
   * - Both types are structs and have the same number of fields. The type and nullability of each
   *   field from read/write is compatible. If byName is true, the name of each field from
   *   read/write needs to be the same.
   * - Both types are atomic and the write type can be safely cast to the read type.
   *
   * Extra fields in write-side structs are not allowed to avoid accidentally writing data that
   * the read schema will not read, and to ensure map key equality is not changed when data is read.
   *
   * @param write a write-side data type to validate against the read type
   * @param read a read-side data type
   * @return true if data written with the write type can be read using the read type
   */
  def canWrite(
      write: DataType,
      read: DataType,
      byName: Boolean,
      resolver: Resolver,
      context: String,
      storeAssignmentPolicy: StoreAssignmentPolicy.Value,
      addError: String => Unit): Boolean = {
    (write, read) match {
      case (wArr: ArrayType, rArr: ArrayType) =>
        // run compatibility check first to produce all error messages
        val typesCompatible = canWrite(
          wArr.elementType, rArr.elementType, byName, resolver, context + ".element",
          storeAssignmentPolicy, addError)

        if (wArr.containsNull && !rArr.containsNull) {
          addError(s"Cannot write nullable elements to array of non-nulls: '$context'")
          false
        } else {
          typesCompatible
        }

      case (wMap: MapType, rMap: MapType) =>
        // map keys cannot include data fields not in the read schema without changing equality when
        // read. map keys can be missing fields as long as they are nullable in the read schema.

        // run compatibility check first to produce all error messages
        val keyCompatible = canWrite(
          wMap.keyType, rMap.keyType, byName, resolver, context + ".key",
          storeAssignmentPolicy, addError)
        val valueCompatible = canWrite(
          wMap.valueType, rMap.valueType, byName, resolver, context + ".value",
          storeAssignmentPolicy, addError)

        if (wMap.valueContainsNull && !rMap.valueContainsNull) {
          addError(s"Cannot write nullable values to map of non-nulls: '$context'")
          false
        } else {
          keyCompatible && valueCompatible
        }

      case (StructType(writeFields), StructType(readFields)) =>
        var fieldCompatible = true
        readFields.zip(writeFields).zipWithIndex.foreach {
          case ((rField, wField), i) =>
            val nameMatch = resolver(wField.name, rField.name) || isSparkGeneratedName(wField.name)
            val fieldContext = s"$context.${rField.name}"
            val typesCompatible = canWrite(
              wField.dataType, rField.dataType, byName, resolver, fieldContext,
              storeAssignmentPolicy, addError)

            if (byName && !nameMatch) {
              addError(s"Struct '$context' $i-th field name does not match " +
                s"(may be out of order): expected '${rField.name}', found '${wField.name}'")
              fieldCompatible = false
            } else if (!rField.nullable && wField.nullable) {
              addError(s"Cannot write nullable values to non-null field: '$fieldContext'")
              fieldCompatible = false
            } else if (!typesCompatible) {
              // errors are added in the recursive call to canWrite above
              fieldCompatible = false
            }
        }

        if (readFields.size > writeFields.size) {
          val missingFieldsStr = readFields.takeRight(readFields.size - writeFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          if (missingFieldsStr.nonEmpty) {
            addError(s"Struct '$context' missing fields: $missingFieldsStr")
            fieldCompatible = false
          }

        } else if (writeFields.size > readFields.size) {
          val extraFieldsStr = writeFields.takeRight(writeFields.size - readFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          addError(s"Cannot write extra fields to struct '$context': $extraFieldsStr")
          fieldCompatible = false
        }

        fieldCompatible

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == STRICT =>
        if (!Cast.canUpCast(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (_: NullType, _) if storeAssignmentPolicy == ANSI => true

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == ANSI =>
        if (!Cast.canANSIStoreAssign(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (w, r) if w.sameType(r) && !w.isInstanceOf[NullType] =>
        true

      case (w, r) =>
        addError(s"Cannot write '$context': " +
          s"${w.catalogString} is incompatible with ${r.catalogString}")
        false
    }
  }
}
