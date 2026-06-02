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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder}
import org.apache.spark.sql.errors.{DataTypeErrors, DataTypeErrorsBase}
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}

/**
 * Client-side (spark-api) operations shared by the nanosecond timestamp types
 * (TimestampNTZNanosType and TimestampLTZNanosType).
 *
 * Internal values are [[org.apache.spark.unsafe.types.TimestampNanosVal]] (epoch micros + nanos
 * within the micro). The two concrete subclasses differ only in their DataType and SQL-literal
 * prefix; storage and formatting are identical.
 *
 * SCOPE (SPARK-57207): this issue wires physical representation, literals, row accessors, and
 * codegen class selection. Dedicated fractional-second string formatting is not yet implemented;
 * until it lands, format() follows the legacy CAST-to-string behavior and renders the internal
 * TimestampNanosVal via its toString, so enabling the Types Framework does not change
 * CAST-to-string / display output.
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

  // ==================== String Formatting ====================

  // Dedicated fractional-second formatting is not yet implemented (follow-up after SPARK-57207).
  // Mirror the legacy ToStringBase fallback (UTF8String.fromString(value.toString)) so that
  // CAST-to-string and display output are identical whether or not the Types Framework is enabled.
  override def format(v: Any): String = v.toString

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

  // Mirrors RowEncoder.encoderForDataTypeDefault for TimestampLTZNanosType (SPARK-57033):
  // maps to java.time.Instant with the column precision.
  override protected def nanosEncoder: AgnosticEncoder[_] = InstantNanosEncoder(t.precision)
}
