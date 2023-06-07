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

package org.apache.spark.sql.util

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Base64

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.JacksonUtils


object ToJsonUtil {
  /**
   * The compact JSON representation of this row.
   * @since 3.5
   */
  @deprecated
  def json(row: Row): String = JacksonUtils.writeValueAsString(jsonNode(row))

  /**
   * The pretty (i.e. indented) JSON representation of this row.
   * @since 3.5
   */
  @deprecated
  def prettyJson(row: Row): String = JacksonUtils.writeValuePrettyAsString(jsonNode(row))

  /**
   * JSON representation of the row.
   *
   * Note that this only supports the data types that are also supported by
   * [[org.apache.spark.sql.catalyst.encoders.RowEncoder]].
   *
   * @return the JSON representation of the row.
   */
  private[sql] def jsonNode(row: Row): JsonNode = {
    require(row.schema != null, "JSON serialization requires a non-null schema.")
    val nodeFactory = JacksonUtils.defaultNodeFactory

    lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
    lazy val dateFormatter = DateFormatter()
    lazy val timestampFormatter = TimestampFormatter(zoneId)

    // Convert an iterator of values to a json array
    def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): ArrayNode = {
      val data = iterator.map(toJson(_, elementType)).toList
      val arrayNode = nodeFactory.arrayNode(data.length)
      arrayNode.addAll(data.asJava)
      arrayNode
    }

    // Convert a value to json.
    def toJson(value: Any, dataType: DataType): JsonNode = (value, dataType) match {
      case (null, _) => nodeFactory.nullNode()
      case (b: Boolean, _) => nodeFactory.booleanNode(b)
      case (b: Byte, _) => nodeFactory.numberNode(b.toLong)
      case (s: Short, _) => nodeFactory.numberNode(s.toLong)
      case (i: Int, _) => nodeFactory.numberNode(i.toLong)
      case (l: Long, _) => nodeFactory.numberNode(l)
      case (f: Float, _) => nodeFactory.numberNode(f.toDouble)
      case (d: Double, _) => nodeFactory.numberNode(d)
      case (d: BigDecimal, _) => nodeFactory.numberNode(d.bigDecimal)
      case (d: java.math.BigDecimal, _) => nodeFactory.numberNode(d)
      case (d: Decimal, _) => nodeFactory.numberNode(d.toBigDecimal.bigDecimal)
      case (s: String, _) => nodeFactory.textNode(s)
      case (b: Array[Byte], BinaryType) =>
        nodeFactory.textNode(Base64.getEncoder.encodeToString(b))
      case (d: LocalDate, _) => nodeFactory.textNode(dateFormatter.format(d))
      case (d: Date, _) => nodeFactory.textNode(dateFormatter.format(d))
      case (i: Instant, _) => nodeFactory.textNode(timestampFormatter.format(i))
      case (t: Timestamp, _) => nodeFactory.textNode(timestampFormatter.format(t))
      case (i: CalendarInterval, _) => nodeFactory.textNode(i.toString)
      case (a: Array[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (a: mutable.ArraySeq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (s: Seq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(s.iterator, elementType)
      case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
        val obj = nodeFactory.objectNode()
        m.toList.sortBy(_._1).map {
          case (k, v) => obj.set[JsonNode](k, toJson(v, valueType))
        }
        obj
      case (m: Map[_, _], MapType(keyType, valueType, _)) =>
        val objs = m.iterator.map {
          case (k, v) =>
            val obj = nodeFactory.objectNode()
            obj.set[JsonNode]("key", toJson(k, keyType))
            obj.set[JsonNode]("value", toJson(v, valueType))
            obj
        }.toList
        val arrayNode = nodeFactory.arrayNode(objs.length)
        arrayNode.addAll(objs.asJava)
        arrayNode
      case (r: Row, _) => ToJsonUtil.jsonNode(r)
      case (v: Any, udt: UserDefinedType[Any @unchecked]) =>
        val dataType = udt.sqlType
        toJson(CatalystTypeConverters.convertToScala(udt.serialize(v), dataType), dataType)
      case _ =>
        throw new IllegalArgumentException(s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
    }

    // Convert the row fields to json
    var n = 0
    val elements = new mutable.ListBuffer[(String, JsonNode)]
    val len = row.length
    while (n < len) {
      val field = row.schema(n)
      elements += (field.name -> toJson(row.apply(n), field.dataType))
      n += 1
    }
    val obj = nodeFactory.objectNode()
    elements.foreach {
      case (name, node) => obj.set[JsonNode](name, node)
    }
    obj
  }
}
