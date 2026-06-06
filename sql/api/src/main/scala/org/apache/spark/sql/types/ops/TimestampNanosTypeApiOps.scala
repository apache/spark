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

import java.time.{ZoneId, ZoneOffset}

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.errors.{DataTypeErrors, DataTypeErrorsBase}
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Client-side (spark-api) operations shared by the nanosecond timestamp types
 * (TimestampNTZNanosType and TimestampLTZNanosType).
 *
 * Internal values are [[org.apache.spark.unsafe.types.TimestampNanosVal]] (epoch micros + nanos
 * within the micro). The two concrete subclasses differ only in their DataType, SQL-literal
 * prefix, and time-zone handling; storage is identical.
 *
 * CAST to STRING (SPARK-57285): the Types Framework is the single integration point for
 * nanosecond timestamp cast-to-string. ToStringBase routes both the interpreted and codegen paths
 * through the zone-aware [[format(v, zoneId)]] / [[formatUTF8(v, zoneId)]] hook here, passing the
 * session time zone. NTZ rendering is zone-independent; LTZ rendering uses the session zone.
 *
 * The remaining zone-less callers (EXPLAIN plan output and SQL-literal rendering via toSQLValue)
 * have no session zone: NTZ can still format directly, while LTZ raises the user-facing
 * UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_TO_STRING error rather than silently truncating or guessing
 * a zone.
 *
 * Dataset encoders are wired here to the precision-aware leaves added by SPARK-57033
 * (LocalDateTimeNanosEncoder / InstantNanosEncoder), so that turning on the Types Framework
 * matches the legacy RowEncoder.encoderForDataTypeDefault behavior rather than regressing it.
 *
 * @since 4.3.0
 */
abstract class TimestampNanosTypeApiOps extends TypeApiOps with DataTypeErrorsBase {

  /** SQL literal prefix for this type, e.g. "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ". */
  protected def sqlTypeName: String

  /** The fractional-second precision in [7, 9] of this type. */
  protected def precision: Int

  // ==================== String Formatting ====================

  // Row JSON (Row.json / Row.prettyJson) holds the external Row value (java.time.Instant for LTZ,
  // java.time.LocalDateTime for NTZ), but rendering nanosecond timestamps from this zone-less path
  // is not supported yet (same reason as format above), so raise the user-facing
  // UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_TO_STRING error here rather than silently truncating.
  override def formatExternal(value: Any): Option[String] =
    throw DataTypeErrors.cannotConvertNanosTimestampToStringError(dataType)

  // The Hive result path (HiveResult.toHiveString) is zone-aware and renders nanosecond timestamps
  // through its own default formatter, so return None here to fall through to it rather than raise
  // the unsupported-rendering error that the zone-less single-arg formatExternal above throws. This
  // is a temporary override until nanos external rendering is unified across the two paths.
  override def formatExternal(value: Any, nested: Boolean): Option[String] = None

  override def toSQLValue(v: Any): String = s"$sqlTypeName '${format(v)}'"

  // ==================== Row Encoding ====================

  // Honor the spark.sql.timestampNanosTypes.enabled gate just like the legacy
  // RowEncoder.encoderForDataTypeDefault path, so enabling the Types Framework does not bypass
  // the feature flag.
  final override def getEncoder: AgnosticEncoder[_] = {
    DataTypeErrors.checkTimestampNanosTypesEnabled()
    nanosEncoder
  }

  /** The precision-aware encoder for this type (SPARK-57033). */
  protected def nanosEncoder: AgnosticEncoder[_]
}

/**
 * Client-side operations for [[org.apache.spark.sql.types.TimestampNTZNanosType]].
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
class TimestampNTZNanosTypeApiOps(val t: TimestampNTZNanosType) extends TimestampNanosTypeApiOps {
  override def dataType: TimestampNTZNanosType = t
  override protected def sqlTypeName: String = "TIMESTAMP_NTZ"
  override protected def precision: Int = t.precision

  // NTZ rendering is zone-independent: the value is the UTC-grid wall clock, so a UTC formatter
  // produces the same string regardless of the session zone.
  @transient private lazy val formatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)

  override def format(v: Any): String =
    formatter.formatWithoutTimeZoneNanos(v.asInstanceOf[TimestampNanosVal], precision)

  override def format(v: Any, zoneId: ZoneId): String = format(v)

  // Mirrors RowEncoder.encoderForDataTypeDefault for TimestampNTZNanosType (SPARK-57033):
  // maps to java.time.LocalDateTime with the column precision.
  override protected def nanosEncoder: AgnosticEncoder[_] = LocalDateTimeNanosEncoder(t.precision)
}

/**
 * Client-side operations for [[org.apache.spark.sql.types.TimestampLTZNanosType]].
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
class TimestampLTZNanosTypeApiOps(val t: TimestampLTZNanosType) extends TimestampNanosTypeApiOps {
  override def dataType: TimestampLTZNanosType = t
  override protected def sqlTypeName: String = "TIMESTAMP_LTZ"
  override protected def precision: Int = t.precision

  // Zone-less callers (EXPLAIN, SQL-literal) have no session zone to render LTZ in, so they still
  // raise rather than guessing a default.
  override def format(v: Any): String =
    throw DataTypeErrors.cannotConvertNanosTimestampToStringError(dataType)

  // LTZ rendering depends on the session time zone. The fraction formatter has its own internal
  // cache, so build it per call rather than caching it here.
  override def format(v: Any, zoneId: ZoneId): String = {
    val formatter = TimestampFormatter.getFractionFormatter(zoneId)
    formatter.formatNanos(v.asInstanceOf[TimestampNanosVal], precision)
  }

  // Mirrors RowEncoder.encoderForDataTypeDefault for TimestampLTZNanosType (SPARK-57033):
  // maps to java.time.Instant with the column precision.
  override protected def nanosEncoder: AgnosticEncoder[_] = InstantNanosEncoder(t.precision)
}
