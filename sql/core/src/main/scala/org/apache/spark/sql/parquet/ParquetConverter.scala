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

package org.apache.spark.sql.parquet

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}

import jodd.datetime.JDateTime
import parquet.io.api.Binary

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.timestamp.NanoTime
import org.apache.spark.sql.types._

private[sql] object CatalystConverter {
  // The type internally used for fields
  type FieldType = StructField

  // This is mostly Parquet convention (see, e.g., `ConversionPatterns`).
  // Note that "array" for the array elements is chosen by ParquetAvro.
  // Using a different value will result in Parquet silently dropping columns.
  val ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME = "bag"
  val ARRAY_ELEMENTS_SCHEMA_NAME = "array"
  // SPARK-4520: Thrift generated parquet files have different array element
  // schema names than avro. Thrift parquet uses array_schema_name + "_tuple"
  // as opposed to "array" used by default. For more information, check
  // TestThriftSchemaConverter.java in parquet.thrift.
  val THRIFT_ARRAY_ELEMENTS_SCHEMA_NAME_SUFFIX = "_tuple"
  val MAP_KEY_SCHEMA_NAME = "key"
  val MAP_VALUE_SCHEMA_NAME = "value"
  val MAP_SCHEMA_NAME = "map"

  // TODO: consider using Array[T] for arrays to avoid boxing of primitive types
  type ArrayScalaType[T] = Seq[T]
  type StructScalaType[T] = Row
  type MapScalaType[K, V] = Map[K, V]
}

private[parquet] object CatalystTimestampConverter {
  // TODO most part of this comes from Hive-0.14
  // Hive code might have some issues, so we need to keep an eye on it.
  // Also we use NanoTime and Int96Values from parquet-examples.
  // We utilize jodd to convert between NanoTime and Timestamp
  val parquetTsCalendar = new ThreadLocal[Calendar]
  def getCalendar: Calendar = {
    // this is a cache for the calendar instance.
    if (parquetTsCalendar.get == null) {
      parquetTsCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")))
    }
    parquetTsCalendar.get
  }
  val NANOS_PER_SECOND: Long = 1000000000
  val SECONDS_PER_MINUTE: Long = 60
  val MINUTES_PER_HOUR: Long = 60
  val NANOS_PER_MILLI: Long = 1000000

  def convertToTimestamp(value: Binary): Timestamp = {
    val nt = NanoTime.fromBinary(value)
    val timeOfDayNanos = nt.getTimeOfDayNanos
    val julianDay = nt.getJulianDay
    val jDateTime = new JDateTime(julianDay.toDouble)
    val calendar = getCalendar
    calendar.set(Calendar.YEAR, jDateTime.getYear)
    calendar.set(Calendar.MONTH, jDateTime.getMonth - 1)
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay)

    // written in command style
    var remainder = timeOfDayNanos
    calendar.set(
      Calendar.HOUR_OF_DAY,
      (remainder / (NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)).toInt)
    remainder = remainder % (NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)
    calendar.set(
      Calendar.MINUTE, (remainder / (NANOS_PER_SECOND * SECONDS_PER_MINUTE)).toInt)
    remainder = remainder % (NANOS_PER_SECOND * SECONDS_PER_MINUTE)
    calendar.set(Calendar.SECOND, (remainder / NANOS_PER_SECOND).toInt)
    val nanos = remainder % NANOS_PER_SECOND
    val ts = new Timestamp(calendar.getTimeInMillis)
    ts.setNanos(nanos.toInt)
    ts
  }

  def convertFromTimestamp(ts: Timestamp): Binary = {
    val calendar = getCalendar
    calendar.setTime(ts)
    val jDateTime = new JDateTime(calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
    // Hive-0.14 didn't set hour before get day number, while the day number should
    // has something to do with hour, since julian day number grows at 12h GMT
    // here we just follow what hive does.
    val julianDay = jDateTime.getJulianDayNumber

    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    val minute = calendar.get(Calendar.MINUTE)
    val second = calendar.get(Calendar.SECOND)
    val nanos = ts.getNanos
    // Hive-0.14 would use hours directly, that might be wrong, since the day starts
    // from 12h in Julian. here we just follow what hive does.
    val nanosOfDay = nanos + second * NANOS_PER_SECOND +
      minute * NANOS_PER_SECOND * SECONDS_PER_MINUTE +
      hour * NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR
    NanoTime(julianDay, nanosOfDay).toBinary
  }
}
