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

import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

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
 * nanosecond timestamp cast-to-string, via the zone-less [[format]] / [[formatUTF8]]. NTZ
 * rendering is zone-independent (the value is the UTC-grid wall clock). LTZ rendering depends on
 * the session time zone, so the LTZ ops carries a ZoneId and builds its formatter once per
 * instance. The zone is threaded in via TypeApiOps.apply(dt, zoneId): CAST passes the cast's
 * resolved session zone, while zone-less callers (Row JSON via formatExternal) accept the
 * default, the session-local time zone config.
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

  // Both external-value consumers share the single-arg formatExternal each subclass overrides:
  //   - Row JSON (Row.json / Row.prettyJson) holds the external Row value (java.time.Instant for
  //     LTZ, java.time.LocalDateTime for NTZ).
  //   - The Hive result path (HiveResult.toHiveString) calls the two-arg overload, whose default
  //     delegates to the single-arg renderer; `nested` does not affect timestamp formatting.
  // Each subclass renders the external value through the same formatter as its zone-aware
  // cast-to-string, so both paths show the nanosecond value rather than silently truncating to
  // microseconds via the legacy path.

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

  // Row JSON holds the external java.time.LocalDateTime (LocalDateTimeNanosEncoder, SPARK-57033)
  // already at the column precision; render it zone-independently through the same UTC formatter
  // as the internal-value format above.
  override def formatExternal(value: Any): Option[String] =
    Some(formatter.format(value.asInstanceOf[LocalDateTime]))

  // Mirrors RowEncoder.encoderForDataTypeDefault for TimestampNTZNanosType (SPARK-57033):
  // maps to java.time.LocalDateTime with the column precision.
  override protected def nanosEncoder: AgnosticEncoder[_] = LocalDateTimeNanosEncoder(t.precision)
}

/**
 * Client-side operations for [[org.apache.spark.sql.types.TimestampLTZNanosType]].
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @param zoneId
 *   The time zone LTZ values are rendered in (LTZ is zone-aware). `TypeApiOps.apply` threads in
 *   the session zone: the cast's resolved zone for CAST, or the session-local time zone config
 *   for zone-less render callers (Row JSON via formatExternal).
 * @since 4.3.0
 */
class TimestampLTZNanosTypeApiOps(val t: TimestampLTZNanosType, zoneId: ZoneId)
    extends TimestampNanosTypeApiOps {
  override def dataType: TimestampLTZNanosType = t
  override protected def sqlTypeName: String = "TIMESTAMP_LTZ"
  override protected def precision: Int = t.precision

  // LTZ rendering depends on the session time zone, carried by this ops instance (the cast's
  // resolved zone, or the session-local time zone config for the zone-less framework lookup). The
  // fraction formatter is built once here (per cast / per ops) rather than per row.
  @transient private lazy val formatter = TimestampFormatter.getFractionFormatter(zoneId)

  override def format(v: Any): String =
    formatter.formatNanos(v.asInstanceOf[TimestampNanosVal], precision)

  // Row JSON holds the external java.time.Instant (InstantNanosEncoder, SPARK-57033) already at the
  // column precision; render it in the ops' session zone through the same formatter as the
  // internal-value format above.
  override def formatExternal(value: Any): Option[String] =
    Some(formatter.format(value.asInstanceOf[Instant]))

  // Mirrors RowEncoder.encoderForDataTypeDefault for TimestampLTZNanosType (SPARK-57033):
  // maps to java.time.Instant with the column precision.
  override protected def nanosEncoder: AgnosticEncoder[_] = InstantNanosEncoder(t.precision)
}
