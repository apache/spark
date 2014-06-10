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

import scala.collection.JavaConversions._
import scala.math.BigDecimal

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.Logging

sealed trait SchemaResolutionMode

case object EagerSchemaResolution extends SchemaResolutionMode
case class EagerSchemaResolutionWithSampling(fraction: Double) extends SchemaResolutionMode

/**
 * :: Experimental ::
 * Converts a JSON file to a SparkSQL logical query plan.  This implementation is only designed to
 * work on JSON files that have mostly uniform schema.  The conversion suffers from the following
 * limitation:
 *  - The data is optionally sampled to determine all of the possible fields. Any fields that do
 *    not appear in this sample will not be included in the final output.
 */
@Experimental
object JsonTable extends Logging {
  def inferSchema(json: RDD[String], samplingRatio: Double = 1.0): LogicalPlan = {
    val schemaData = if (samplingRatio > 0.99) json.sample(false, samplingRatio, 1) else json
    val allKeys = parseJson(schemaData).map(getAllKeysWithValueTypes).reduce(_ ++ _)

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
        }.reduce((type1: DataType, type2: DataType) => getCompatibleType(type1, type2))

        // Finally, we replace all NullType to StringType. We do not need to take care
        // StructType because all fields with a StructType are represented by a placeholder
        // StructType(Nil).
        dataType match {
          case NullType => (fieldName, StringType)
          case ArrayType(NullType) => (fieldName, ArrayType(StringType))
          case other => (fieldName, other)
        }
      }
    }

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (topLevel, structLike) = values.partition(_.size == 1)
      val topLevelFields = topLevel.filter {
        name => resolved.get(prefix ++ name).get match {
          case ArrayType(StructType(Nil)) => false
          case ArrayType(_) => true
          case struct: StructType => false
          case _ => true
        }
      }.map {
        a => StructField(a.head, resolved.get(prefix ++ a).get, nullable = true)
      }.sortBy {
        case StructField(name, _, _) => name
      }

      val structFields: Seq[StructField] = structLike.groupBy(_(0)).map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          val dataType = resolved.get(prefix :+ name).get
          dataType match {
            case array: ArrayType => Some(StructField(name, ArrayType(structType), nullable = true))
            case struct: StructType => Some(StructField(name, structType, nullable = true))
            // dataType is StringType means that we have resolved type conflicts involving
            // primitive types and complex types. So, the type of name has been relaxed to
            // StringType. Also, this field should have already been put in topLevelFields.
            case StringType => None
          }
        }
      }.flatMap(field => field).toSeq.sortBy {
        case StructField(name, _, _) => name
      }

      StructType(topLevelFields ++ structFields)
    }

    val schema = makeStruct(resolved.keySet.toSeq, Nil)

    SparkLogicalPlan(ExistingRdd(asAttributes(schema), parseJson(json).map(asRow(_, schema))))
  }

  /**
   * Returns the most general data type for two given data types.
   */
  protected def getCompatibleType(t1: DataType, t2: DataType): DataType = {
    // Try and find a promotion rule that contains both types in question.
    val applicableConversion = HiveTypeCoercion.allPromotions.find(p => p.contains(t1) && p
      .contains(t2))

    // If found return the widest common type, otherwise None
    val returnType = applicableConversion.map(_.filter(t => t == t1 || t == t2).last)

    if (returnType.isDefined) {
      returnType.get
    } else {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (other: DataType, NullType) => other
        case (NullType, other: DataType) => other
        // TODO: Returns the union of fields1 and fields2?
        case (StructType(fields1), StructType(fields2))
          if (fields1 == fields2) => StructType(fields1)
        case (ArrayType(elementType1), ArrayType(elementType2)) =>
          ArrayType(getCompatibleType(elementType1, elementType2))
        case (_, _) => StringType
      }
    }
  }

  protected def getPrimitiveType(value: Any): DataType = {
    value match {
      case value: java.lang.String => StringType
      case value: java.lang.Integer => IntegerType
      case value: java.lang.Long => LongType
      // Since we do not have a data type backed by BigInteger,
      // when we see a Java BigInteger, we use DecimalType.
      case value: java.math.BigInteger => DecimalType
      case value: java.lang.Double => DoubleType
      case value: java.math.BigDecimal => DecimalType
      case value: java.lang.Boolean => BooleanType
      case null => NullType
      // Unexpected data type.
      case _ => StringType
    }
  }

  /**
   * Returns the element type of an JSON array. We go through all elements of this array
   * to detect any possible type conflict. We use [[getCompatibleType]] to resolve
   * type conflicts. Right now, when the element of an array is another array, we
   * treat the element as String.
   */
  protected def getTypeOfArray(l: Seq[Any]): ArrayType = {
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType)
    } else {
      val elementType = elements.map {
        e => e match {
          case map: Map[_, _] => StructType(Nil)
          // We have an array of arrays. If those element arrays do not have the same
          // element types, we will return ArrayType[StringType].
          case seq: Seq[_] =>  getTypeOfArray(seq)
          case value => getPrimitiveType(value)
        }
      }.reduce((type1: DataType, type2: DataType) => getCompatibleType(type1, type2))

      ArrayType(elementType)
    }
  }

  /**
   * Figures out all key names and data types of values from a parsed JSON object
   * (in the format of Map[Stirng, Any]). When the value of a key is an JSON object, we
   * only use a placeholder (StructType(Nil)) to mark that it should be a struct
   * instead of getting all fields of this struct because a field does not appear
   * in this JSON object can appear in other JSON objects.
   */
  protected def getAllKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    m.map{
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, dataType) => (s"`$key`", dataType)
    }.flatMap {
      case (key: String, struct: Map[String, Any]) => {
        // The value associted with the key is an JSON object.
        getAllKeysWithValueTypes(struct).map {
          case (k, dataType) => (s"$key.$k", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: List[Any]) => {
        // The value associted with the key is an array.
        getTypeOfArray(array) match {
          case ArrayType(StructType(Nil)) => {
            // The elements of this arrays are structs.
            array.asInstanceOf[List[Map[String, Any]]].flatMap {
              element => getAllKeysWithValueTypes(element)
            }.map {
              case (k, dataType) => (s"$key.$k", dataType)
            } :+ (key, ArrayType(StructType(Nil)))
          }
          case ArrayType(elementType) => (key, ArrayType(elementType)) :: Nil
        }
      }
      case (key: String, value) => (key, getPrimitiveType(value)) :: Nil
    }.toSet
  }

  /**
   * Converts a Java Map/List to a Scala Map/List.
   * We do not use Jackson's scala module at here because
   * DefaultScalaModule in jackson-module-scala will make
   * the parsing very slow.
   */
  protected def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[String, Object] =>
      // .map(identity) is used as a workaround of non-serializable Map
      // generated by .mapValues.
      // This issue is documented at https://issues.scala-lang.org/browse/SI-7005
      map.toMap.mapValues(scalafy).map(identity)
    case list: java.util.List[Object] =>
      list.toList.map(scalafy)
    case atom => atom
  }

  protected def parseJson(json: RDD[String]): RDD[Map[String, Any]] = {
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
      iter.map(record => mapper.readValue(record, classOf[Object]))
    }).map(scalafy).map(_.asInstanceOf[Map[String, Any]])
  }

  protected def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
    }
  }

  protected def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  protected def toDecimal(value: Any): BigDecimal = {
    value match {
      case value: java.lang.Integer => BigDecimal(value)
      case value: java.lang.Long => BigDecimal(value)
      case value: java.math.BigInteger => BigDecimal(value)
      case value: java.lang.Double => BigDecimal(value)
      case value: java.math.BigDecimal => BigDecimal(value)
    }
  }

  protected[json] def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case ArrayType(elementType) =>
          value.asInstanceOf[Seq[Any]].map(enforceCorrectType(_, elementType))
        case StringType => value.toString
        case IntegerType => value.asInstanceOf[IntegerType.JvmType]
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case DecimalType => toDecimal(value)
        case BooleanType => value.asInstanceOf[BooleanType.JvmType]
        case NullType => null
      }
    }
  }

  protected def asRow(json: Map[String,Any], schema: StructType): Row = {
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      // StructType
      case (StructField(name, fields: StructType, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          v => asRow(v.asInstanceOf[Map[String, Any]], fields)).orNull)

      // ArrayType(StructType)
      case (StructField(name, ArrayType(structType: StructType), _), i) =>
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[Any]].map(
              e => asRow(e.asInstanceOf[Map[String, Any]], structType))).orNull)

      // Other cases
      case (StructField(name, dataType, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          enforceCorrectType(_, dataType)).getOrElse(null))
    }

    row
  }

  protected def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)())
  }
}
