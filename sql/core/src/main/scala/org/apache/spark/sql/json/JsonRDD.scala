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

package org.apache.spark.sql.json

import java.io.StringWriter
import java.sql.{Date, Timestamp}

import scala.collection.Map
import scala.collection.convert.Wrappers.{JMapWrapper, JListWrapper}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.Logging

private[sql] object JsonRDD extends Logging {

  private[sql] def jsonStringToRow(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[Row] = {
    parseJson(json, columnNameOfCorruptRecords).map(parsed => asRow(parsed, schema))
  }

  private[sql] def inferSchema(
      json: RDD[String],
      samplingRatio: Double = 1.0,
      columnNameOfCorruptRecords: String): StructType = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) json else json.sample(false, samplingRatio, 1)
    val allKeys =
      parseJson(schemaData, columnNameOfCorruptRecords).map(allKeysWithValueTypes).reduce(_ ++ _)
    createSchema(allKeys)
  }

  private def createSchema(allKeys: Set[(String, DataType)]): StructType = {
    // Resolve type conflicts
    val resolved = allKeys.groupBy {
      case (key, dataType) => key
    }.map {
      // Now, keys and types are organized in the format of
      // key -> Set(type1, type2, ...).
      case (key, typeSet) => {
        val fieldName = key.substring(1, key.length - 1).split("`.`").toSeq
        val dataType = typeSet.map {
          case (_, dataType) => dataType
        }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

        (fieldName, dataType)
      }
    }

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (topLevel, structLike) = values.partition(_.size == 1)

      val topLevelFields = topLevel.filter {
        name => resolved.get(prefix ++ name).get match {
          case ArrayType(elementType, _) => {
            def hasInnerStruct(t: DataType): Boolean = t match {
              case s: StructType => true
              case ArrayType(t1, _) => hasInnerStruct(t1)
              case o => false
            }

            // Check if this array has inner struct.
            !hasInnerStruct(elementType)
          }
          case struct: StructType => false
          case _ => true
        }
      }.map {
        a => StructField(a.head, resolved.get(prefix ++ a).get, nullable = true)
      }
      val topLevelFieldNameSet = topLevelFields.map(_.name)

      val structFields: Seq[StructField] = structLike.groupBy(_(0)).filter {
        case (name, _) => !topLevelFieldNameSet.contains(name)
      }.map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          val dataType = resolved.get(prefix :+ name).get
          dataType match {
            case array: ArrayType =>
              // The pattern of this array is ArrayType(...(ArrayType(StructType))).
              // Since the inner struct of array is a placeholder (StructType(Nil)),
              // we need to replace this placeholder with the actual StructType (structType).
              def getActualArrayType(
                  innerStruct: StructType,
                  currentArray: ArrayType): ArrayType = currentArray match {
                case ArrayType(s: StructType, containsNull) =>
                  ArrayType(innerStruct, containsNull)
                case ArrayType(a: ArrayType, containsNull) =>
                  ArrayType(getActualArrayType(innerStruct, a), containsNull)
              }
              Some(StructField(name, getActualArrayType(structType, array), nullable = true))
            case struct: StructType => Some(StructField(name, structType, nullable = true))
            // dataType is StringType means that we have resolved type conflicts involving
            // primitive types and complex types. So, the type of name has been relaxed to
            // StringType. Also, this field should have already been put in topLevelFields.
            case StringType => None
          }
        }
      }.flatMap(field => field).toSeq

      StructType((topLevelFields ++ structFields).sortBy(_.name))
    }

    makeStruct(resolved.keySet.toSeq, Nil)
  }

  private[sql] def nullTypeToStringType(struct: StructType): StructType = {
    val fields = struct.fields.map {
      case StructField(fieldName, dataType, nullable, _) => {
        val newType = dataType match {
          case NullType => StringType
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case ArrayType(struct: StructType, containsNull) =>
            ArrayType(nullTypeToStringType(struct), containsNull)
          case struct: StructType =>nullTypeToStringType(struct)
          case other: DataType => other
        }
        StructField(fieldName, newType, nullable)
      }
    }

    StructType(fields)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private[json] def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonType(t1, t2) match {
      case Some(commonType) => commonType
      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) => {
            val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
              case (name, fieldTypes) => {
                val dataType = fieldTypes.map(field => field.dataType).reduce(
                  (type1: DataType, type2: DataType) => compatibleType(type1, type2))
                StructField(name, dataType, true)
              }
            }
            StructType(newFields.toSeq.sortBy(_.name))
          }
          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
          // TODO: We should use JsonObjectStringType to mark that values of field will be
          // strings and every string is a Json object.
          case (_, _) => StringType
        }
    }
  }

  private def typeOfPrimitiveValue: PartialFunction[Any, DataType] = {
    ScalaReflection.typeOfObject orElse {
      // Since we do not have a data type backed by BigInteger,
      // when we see a Java BigInteger, we use DecimalType.
      case value: java.math.BigInteger => DecimalType.Unlimited
      // DecimalType's JVMType is scala BigDecimal.
      case value: java.math.BigDecimal => DecimalType.Unlimited
      // Unexpected data type.
      case _ => StringType
    }
  }

  /**
   * Returns the element type of an JSON array. We go through all elements of this array
   * to detect any possible type conflict. We use [[compatibleType]] to resolve
   * type conflicts.
   */
  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.exists(v => v == null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements.map {
        e => e match {
          case map: Map[_, _] => StructType(Nil)
          // We have an array of arrays. If those element arrays do not have the same
          // element types, we will return ArrayType[StringType].
          case seq: Seq[_] =>  typeOfArray(seq)
          case value => typeOfPrimitiveValue(value)
        }
      }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

      ArrayType(elementType, containsNull)
    }
  }

  /**
   * Figures out all key names and data types of values from a parsed JSON object
   * (in the format of Map[Stirng, Any]). When the value of a key is an JSON object, we
   * only use a placeholder (StructType(Nil)) to mark that it should be a struct
   * instead of getting all fields of this struct because a field does not appear
   * in this JSON object can appear in other JSON objects.
   */
  private def allKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    val keyValuePairs = m.map {
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, value) => (s"`$key`", value)
    }.toSet
    keyValuePairs.flatMap {
      case (key: String, struct: Map[_, _]) => {
        // The value associated with the key is an JSON object.
        allKeysWithValueTypes(struct.asInstanceOf[Map[String, Any]]).map {
          case (k, dataType) => (s"$key.$k", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: Seq[_]) => {
        // The value associated with the key is an array.
        // Handle inner structs of an array.
        def buildKeyPathForInnerStructs(v: Any, t: DataType): Seq[(String, DataType)] = t match {
          case ArrayType(e: StructType, containsNull) => {
            // The elements of this arrays are structs.
            v.asInstanceOf[Seq[Map[String, Any]]].flatMap(Option(_)).flatMap {
              element => allKeysWithValueTypes(element)
            }.map {
              case (k, t) => (s"$key.$k", t)
            }
          }
          case ArrayType(t1, containsNull) =>
            v.asInstanceOf[Seq[Any]].flatMap(Option(_)).flatMap {
              element => buildKeyPathForInnerStructs(element, t1)
            }
          case other => Nil
        }
        val elementType = typeOfArray(array)
        buildKeyPathForInnerStructs(array, elementType) :+ (key, elementType)
      }
      // we couldn't tell what the type is if the value is null or empty string
      case (key: String, value) if value == "" || value == null => (key, NullType) :: Nil
      case (key: String, value) => (key, typeOfPrimitiveValue(value)) :: Nil
    }
  }

  /**
   * Converts a Java Map/List to a Scala Map/Seq.
   * We do not use Jackson's scala module at here because
   * DefaultScalaModule in jackson-module-scala will make
   * the parsing very slow.
   */
  private def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[_, _] =>
      // .map(identity) is used as a workaround of non-serializable Map
      // generated by .mapValues.
      // This issue is documented at https://issues.scala-lang.org/browse/SI-7005
      JMapWrapper(map).mapValues(scalafy).map(identity)
    case list: java.util.List[_] =>
      JListWrapper(list).map(scalafy)
    case atom => atom
  }

  private def parseJson(
      json: RDD[String],
      columnNameOfCorruptRecords: String): RDD[Map[String, Any]] = {
    // According to [Jackson-72: https://jira.codehaus.org/browse/JACKSON-72],
    // ObjectMapper will not return BigDecimal when
    // "DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS" is disabled
    // (see NumberDeserializer.deserialize for the logic).
    // But, we do not want to enable this feature because it will use BigDecimal
    // for every float number, which will be slow.
    // So, right now, we will have Infinity for those BigDecimal number.
    // TODO: Support BigDecimal.
    json.mapPartitions(iter => {
      // When there is a key appearing multiple times (a duplicate key),
      // the ObjectMapper will take the last value associated with this duplicate key.
      // For example: for {"key": 1, "key":2}, we will get "key"->2.
      val mapper = new ObjectMapper()
      iter.flatMap { record =>
        try {
          val parsed = mapper.readValue(record, classOf[Object]) match {
            case map: java.util.Map[_, _] => scalafy(map).asInstanceOf[Map[String, Any]] :: Nil
            case list: java.util.List[_] => scalafy(list).asInstanceOf[Seq[Map[String, Any]]]
          }

          parsed
        } catch {
          case e: JsonProcessingException => Map(columnNameOfCorruptRecords -> record) :: Nil
        }
      }
    })
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  private def toDecimal(value: Any): Decimal = {
    value match {
      case value: java.lang.Integer => Decimal(value)
      case value: java.lang.Long => Decimal(value)
      case value: java.math.BigInteger => Decimal(new java.math.BigDecimal(value))
      case value: java.lang.Double => Decimal(value)
      case value: java.math.BigDecimal => Decimal(value)
    }
  }

  private def toJsonArrayString(seq: Seq[Any]): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any]): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        val stringValue = if (value.isInstanceOf[String]) s"""\"$value\"""" else toString(value)
        builder.append(s"""\"${key}\":${stringValue}""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]])
      case value: Seq[_] => toJsonArrayString(value)
      case value => Option(value).map(_.toString).orNull
    }
  }

  private def toDate(value: Any): Int = {
    value match {
      // only support string as date
      case value: java.lang.String =>
        DateUtils.millisToDays(DataTypeConversions.stringToTime(value).getTime)
      case value: java.sql.Date => DateUtils.fromJavaDate(value)
    }
  }

  private def toTimestamp(value: Any): Timestamp = {
    value match {
      case value: java.lang.Integer => new Timestamp(value.asInstanceOf[Int].toLong)
      case value: java.lang.Long => new Timestamp(value)
      case value: java.lang.String => toTimestamp(DataTypeConversions.stringToTime(value).getTime)
    }
  }

  private[json] def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case StringType => toString(value)
        case _ if value == null || value == "" => null // guard the non string type
        case IntegerType => value.asInstanceOf[IntegerType.JvmType]
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case DecimalType() => toDecimal(value)
        case BooleanType => value.asInstanceOf[BooleanType.JvmType]
        case NullType => null
        case ArrayType(elementType, _) =>
          value.asInstanceOf[Seq[Any]].map(enforceCorrectType(_, elementType))
        case struct: StructType => asRow(value.asInstanceOf[Map[String, Any]], struct)
        case DateType => toDate(value)
        case TimestampType => toTimestamp(value)
      }
    }
  }

  private def asRow(json: Map[String,Any], schema: StructType): Row = {
    // TODO: Reuse the row instead of creating a new one for every record.
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      case (StructField(name, dataType, _, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          enforceCorrectType(_, dataType)).orNull)
    }

    row
  }

  /** Transforms a single Row to JSON using Jackson
    *
    * @param jsonFactory a JsonFactory object to construct a JsonGenerator
    * @param rowSchema the schema object used for conversion
    * @param row The row to convert
    */
  private[sql] def rowToJSON(rowSchema: StructType, jsonFactory: JsonFactory)(row: Row): String = {
    val writer = new StringWriter()
    val gen = jsonFactory.createGenerator(writer)

    def valWriter: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _)  => gen.writeNull()
      case (StringType, v: String) => gen.writeString(v)
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: java.math.BigDecimal) => gen.writeNumber(v)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, v)

      case (ArrayType(ty, _), v: Seq[_] ) =>
        gen.writeStartArray()
        v.foreach(valWriter(ty,_))
        gen.writeEndArray()

      case (MapType(kv,vv, _), v: Map[_,_]) =>
        gen.writeStartObject()
        v.foreach { p =>
          gen.writeFieldName(p._1.toString)
          valWriter(vv,p._2)
        }
        gen.writeEndObject()

      case (StructType(ty), v: Row) =>
        gen.writeStartObject()
        ty.zip(v.toSeq).foreach {
          case (_, null) =>
          case (field, v) =>
            gen.writeFieldName(field.name)
            valWriter(field.dataType, v)
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
    gen.close()
    writer.toString
  }

}
