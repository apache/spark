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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Client-side (spark-api) operations shared by the nanosecond timestamp types
 * (TimestampNTZNanosType and TimestampLTZNanosType).
 *
 * Internal values are [[org.apache.spark.unsafe.types.TimestampNanosVal]] (epoch micros + nanos
 * within the micro). The two concrete subclasses differ only in their DataType and SQL-literal
 * prefix; storage and formatting are identical.
 *
 * SCOPE (SPARK-57207): this issue wires physical representation, literals, row accessors, and
 * codegen class selection. String formatting here is an interim implementation until dedicated
 * fractional-second formatters land in a follow-up issue; Dataset encoders are out of scope
 * (SPARK-57033 and related), so getEncoder reports the type as unsupported, matching the legacy
 * RowEncoder behavior.
 *
 * @since 4.3.0
 */
abstract class TimestampNanosTypeApiOps extends TypeApiOps with DataTypeErrorsBase {

  /** SQL literal prefix for this type, e.g. "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ". */
  protected def sqlTypeName: String

  // ==================== String Formatting (interim) ====================

  override def format(v: Any): String = v.asInstanceOf[TimestampNanosVal].toString

  override def toSQLValue(v: Any): String = s"$sqlTypeName '${format(v)}'"

  // ==================== Row Encoding ====================

  // Encoders are handled in a follow-up issue (SPARK-57033). Until then, report the type as
  // unsupported with the same error as the legacy RowEncoder fallback to preserve parity.
  override def getEncoder: AgnosticEncoder[_] =
    throw new AnalysisException(
      errorClass = "UNSUPPORTED_DATA_TYPE_FOR_ENCODER",
      messageParameters = Map("dataType" -> toSQLType(dataType)))
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
}
