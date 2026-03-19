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

import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional client-side type operations for the Types Framework.
 *
 * This trait extends TypeApiOps with operations needed by client-facing infrastructure:
 * Arrow conversion (ArrowUtils), JDBC mapping (JdbcUtils), Python interop (EvaluatePython),
 * Hive formatting (HiveResult), and Thrift type mapping (SparkExecuteStatementOperation).
 *
 * Lives in sql/api so it's visible from sql/core and sql/hive-thriftserver.
 *
 * USAGE - integration points use ClientTypeOps(dt) which returns Option[ClientTypeOps]:
 * {{{
 * // Forward lookup (most files):
 * ClientTypeOps(dt).map(_.toArrowType(timeZoneId)).getOrElse { ... }
 *
 * // Reverse lookup (ArrowUtils.fromArrowType):
 * ClientTypeOps.fromArrowType(at).getOrElse { ... }
 * }}}
 *
 * @see
 *   TimeTypeApiOps for a reference implementation
 * @since 4.2.0
 */
trait ClientTypeOps { self: TypeApiOps =>

  // ==================== Arrow Conversion ====================

  /**
   * Converts this DataType to its Arrow representation.
   *
   * Used by ArrowUtils.toArrowType.
   *
   * @param timeZoneId
   *   the session timezone (needed by some temporal types)
   * @return
   *   the corresponding ArrowType
   */
  def toArrowType(timeZoneId: String): ArrowType

  // ==================== JDBC Mapping ====================

  /**
   * Returns the java.sql.Types constant for this type.
   *
   * Used by JdbcUtils.getCommonJDBCType for JDBC write path.
   *
   * @return
   *   java.sql.Types constant (e.g., java.sql.Types.TIME)
   */
  def getJdbcType: Int

  /**
   * Returns the DDL type name string for this type.
   *
   * Used by JdbcUtils for CREATE TABLE DDL generation.
   *
   * @return
   *   DDL type string (e.g., "TIME")
   */
  def jdbcTypeName: String

  // ==================== Python Interop ====================

  /**
   * Returns true if values of this type need conversion when passed to/from Python.
   *
   * Used by EvaluatePython.needConversionInPython.
   */
  def needConversionInPython: Boolean

  /**
   * Creates a converter function for Python/Py4J interop.
   *
   * Used by EvaluatePython.makeFromJava. The returned function handles null-safe
   * conversion of Java/Py4J values to the internal Catalyst representation.
   *
   * @return
   *   a function that converts a Java value to the internal representation
   */
  def makeFromJava: Any => Any

  // ==================== Hive Formatting ====================

  /**
   * Formats an external-type value for Hive output.
   *
   * Used by HiveResult.toHiveString. The input is an external-type value
   * (e.g., java.time.LocalTime for TimeType), NOT the internal representation.
   *
   * @param value
   *   the external-type value to format
   * @return
   *   formatted string representation
   */
  def formatExternal(value: Any): String

  // ==================== Thrift Mapping ====================

  /**
   * Returns the Thrift TTypeId name for this type.
   *
   * Used by SparkExecuteStatementOperation.toTTypeId. Returns a String that maps
   * to a TTypeId enum value (e.g., "STRING_TYPE") since TTypeId is only available
   * in the hive-thriftserver module.
   *
   * @return
   *   TTypeId enum name (e.g., "STRING_TYPE")
   */
  def thriftTypeName: String
}

/**
 * Factory object for ClientTypeOps lookup.
 *
 * Delegates to TypeApiOps and narrows via collect to find implementations that mix in
 * ClientTypeOps.
 */
object ClientTypeOps {

  /**
   * Returns a ClientTypeOps instance for the given DataType, if available.
   *
   * @param dt
   *   the DataType to get operations for
   * @return
   *   Some(ClientTypeOps) if supported, None otherwise
   */
  def apply(dt: DataType): Option[ClientTypeOps] =
    TypeApiOps(dt).collect { case co: ClientTypeOps => co }

  /**
   * Reverse lookup: converts an Arrow type to a Spark DataType, if it belongs to a
   * framework-managed type.
   *
   * Used by ArrowUtils.fromArrowType. Returns None if the Arrow type doesn't correspond
   * to any framework-managed type, or the framework is disabled.
   *
   * @param at
   *   the ArrowType to convert
   * @return
   *   Some(DataType) if recognized, None otherwise
   */
  def fromArrowType(at: ArrowType): Option[DataType] = {
    import org.apache.arrow.vector.types.TimeUnit
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    at match {
      case t: ArrowType.Time
        if t.getUnit == TimeUnit.NANOSECOND && t.getBitWidth == 8 * 8 =>
          Some(TimeType(TimeType.MICROS_PRECISION))
      // Add new framework types here
      case _ => None
    }
  }
}
