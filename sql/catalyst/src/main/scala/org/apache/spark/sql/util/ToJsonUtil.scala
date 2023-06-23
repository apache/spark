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

import scala.collection.mutable

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.{compact, pretty, render}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval


object ToJsonUtil {
  /**
   * The compact JSON representation of this row.
   * @since 3.5
   */
  @deprecated
  def json(row: Row): String = compact(jsonValue(row))

  /**
   * The pretty (i.e. indented) JSON representation of this row.
   * @since 3.5
   */
  @deprecated
  def prettyJson(row: Row): String = pretty(render(jsonValue(row)))

  /**
   * JSON representation of the row.
   *
   * Note that this only supports the data types that are also supported by
   * [[org.apache.spark.sql.catalyst.encoders.RowEncoder]].
   *
   * @return the JSON representation of the row.
   */
  private[sql] def jsonValue(row: Row): JValue = {
    require(row.schema != null, "JSON serialization requires a non-null schema.")

    lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
    lazy val dateFormatter = DateFormatter()
    lazy val timestampFormatter = TimestampFormatter(zoneId)

    // Convert an iterator of values to a json array
    def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): JArray = {
      JArray(iterator.map(toJson(_, elementType)).toList)
    }

    // Convert a value to json.
    def toJson(value: Any, dataType: DataType): JValue = (value, dataType) match {
      case (null, _) => JNull
      case (b: Boolean, _) => JBool(b)
      case (b: Byte, _) => JLong(b)
      case (s: Short, _) => JLong(s)
      case (i: Int, _) => JLong(i)
      case (l: Long, _) => JLong(l)
      case (f: Float, _) => JDouble(f)
      case (d: Double, _) => JDouble(d)
      case (d: BigDecimal, _) => JDecimal(d)
      case (d: java.math.BigDecimal, _) => JDecimal(d)
      case (d: Decimal, _) => JDecimal(d.toBigDecimal)
      case (s: String, _) => JString(s)
      case (b: Array[Byte], BinaryType) =>
        JString(Base64.getEncoder.encodeToString(b))
      case (d: LocalDate, _) => JString(dateFormatter.format(d))
      case (d: Date, _) => JString(dateFormatter.format(d))
      case (i: Instant, _) => JString(timestampFormatter.format(i))
      case (t: Timestamp, _) => JString(timestampFormatter.format(t))
      case (i: CalendarInterval, _) => JString(i.toString)
      case (a: Array[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (a: mutable.ArraySeq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (s: Seq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(s.iterator, elementType)
      case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
        new JObject(m.toList.sortBy(_._1).map {
          case (k, v) => k -> toJson(v, valueType)
        })
      case (m: Map[_, _], MapType(keyType, valueType, _)) =>
        new JArray(m.iterator.map {
          case (k, v) =>
            new JObject("key" -> toJson(k, keyType) :: "value" -> toJson(v, valueType) :: Nil)
        }.toList)
      case (r: Row, _) => jsonValue(r)
      case (v: Any, udt: UserDefinedType[Any @unchecked]) =>
        val dataType = udt.sqlType
        toJson(CatalystTypeConverters.convertToScala(udt.serialize(v), dataType), dataType)
      case _ =>
        throw new IllegalArgumentException(s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
    }

    // Convert the row fields to json
    var n = 0
    val elements = new mutable.ListBuffer[JField]
    val len = row.length
    while (n < len) {
      val field = row.schema(n)
      elements += (field.name -> toJson(row.apply(n), field.dataType))
      n += 1
    }
    new JObject(elements.toList)
  }
}
