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

package org.apache.spark.sql.types.ops

import java.time.LocalTime

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.util.{FractionTimeFormatter, SparkDateTimeUtils, TimeFormatter}
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Client-side (spark-api) operations for TimeType.
 *
 * This class implements all TypeApiOps methods for the TIME data type:
 *   - String formatting: uses FractionTimeFormatter for consistent output
 *   - Row encoding: uses LocalTimeEncoder for java.time.LocalTime
 *   - Arrow conversion (ArrowUtils)
 *   - Python interop (EvaluatePython)
 *   - Hive formatting (HiveResult)
 *   - Thrift type mapping (SparkExecuteStatementOperation)
 *
 * RELATIONSHIP TO TimeTypeOps: TimeTypeOps (in catalyst package) extends this class to inherit
 * client-side operations while adding server-side operations (physical type, literals, etc.).
 *
 * @param t
 *   The TimeType with precision information
 * @since 4.2.0
 */
class TimeTypeApiOps(val t: TimeType) extends TypeApiOps {

  override def dataType: DataType = t

  // ==================== String Formatting ====================

  @transient
  private lazy val timeFormatter: TimeFormatter = new FractionTimeFormatter()

  override def format(v: Any): String = {
    timeFormatter.format(v.asInstanceOf[Long])
  }

  override def toSQLValue(v: Any): String = {
    s"TIME '${format(v)}'"
  }

  // ==================== Row Encoding ====================

  override def getEncoder: AgnosticEncoder[_] = LocalTimeEncoder

  // ==================== Optional Operations ====================

  override def toArrowType(timeZoneId: String): Option[ArrowType] = {
    Some(new ArrowType.Time(TimeUnit.NANOSECOND, 8 * 8))
  }

  override def needConversionInPython: Option[Boolean] = Some(true)

  override def makeFromJava: Option[Any => Any] = Some((obj: Any) =>
    nullSafeConvert(obj) {
      case c: Long => c
      // Py4J serializes values between MIN_INT and MAX_INT as Ints, not Longs
      case c: Int => c.toLong
    })

  override def formatExternal(value: Any): Option[String] = {
    val nanos = SparkDateTimeUtils.localTimeToNanos(value.asInstanceOf[LocalTime])
    Some(timeFormatter.format(nanos))
  }

  override def thriftTypeName: Option[String] = Some("STRING_TYPE")
}
