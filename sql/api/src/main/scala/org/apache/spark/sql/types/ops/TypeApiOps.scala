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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, TimeType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Client-side (spark-api) type operations for the Types Framework.
 *
 * This trait consolidates all client-side operations that a data type must implement to be usable
 * in the Spark SQL API layer. Mandatory methods (format, toSQLValue, getEncoder) must be
 * implemented by every type. Optional methods (Arrow, Python, Hive, Thrift) return Option and
 * default to None - types implement them as they expand their integration coverage.
 *
 * RELATIONSHIP TO TypeOps:
 *   - TypeOps (catalyst): Server-side operations - physical types, literals, conversions
 *   - TypeApiOps (spark-api): Client-side operations - formatting, encoding
 *
 * The split exists because sql/api cannot depend on sql/catalyst. For TimeType, TimeTypeOps
 * (catalyst) extends TimeTypeApiOps (sql-api) to inherit both sets of operations.
 *
 * @see
 *   TimeTypeApiOps for reference implementation
 * @since 4.2.0
 */
trait TypeApiOps extends Serializable {

  /** The DataType this Ops instance handles. */
  def dataType: DataType

  // ==================== String Formatting ====================

  /**
   * Formats an internal value as a display string.
   *
   * Used by CAST to STRING, EXPLAIN output, SHOW commands.
   *
   * @param v
   *   the internal value (e.g., Long nanoseconds for TimeType)
   * @return
   *   formatted string (e.g., "10:30:45.123456")
   */
  def format(v: Any): String

  /**
   * Formats an internal value as a UTF8String.
   *
   * Default implementation wraps format(). Override for performance if needed.
   */
  def formatUTF8(v: Any): UTF8String = UTF8String.fromString(format(v))

  /**
   * Formats an internal value as a SQL literal string.
   *
   * @param v
   *   the internal value
   * @return
   *   SQL literal string (e.g., "TIME '10:30:00'")
   */
  def toSQLValue(v: Any): String

  // ==================== Row Encoding ====================

  /**
   * Returns the AgnosticEncoder for this type.
   *
   * Used by RowEncoder for Dataset[T] operations.
   *
   * @return
   *   AgnosticEncoder instance (e.g., LocalTimeEncoder for TimeType)
   */
  def getEncoder: AgnosticEncoder[_]

  // ==================== Utilities ====================

  /**
   * Null-safe conversion helper. Returns null for null input, applies the partial function for
   * non-null input, and returns null for unmatched values.
   */
  protected def nullSafeConvert(input: Any)(f: PartialFunction[Any, Any]): Any = {
    if (input == null) {
      null
    } else {
      f.applyOrElse(input, (_: Any) => null)
    }
  }

  // ==================== Arrow Conversion (optional) ====================

  /** Converts this DataType to its Arrow representation. Returns None if not supported. */
  def toArrowType(timeZoneId: String): Option[ArrowType] = None

  // ==================== Python Interop (optional) ====================

  /** Returns true if values of this type need conversion when passed to/from Python. */
  def needConversionInPython: Option[Boolean] = None

  /** Creates a converter function for Python/Py4J interop. */
  def makeFromJava: Option[Any => Any] = None

  // ==================== Hive Formatting (optional) ====================

  /**
   * Formats an external-type value for Hive output. Most types override this simple version.
   * Types that need different formatting when nested should override the 2-param overload.
   */
  def formatExternal(value: Any): Option[String] = None

  /** Formats with nesting context. Default delegates to the simple version. */
  def formatExternal(value: Any, nested: Boolean): Option[String] = formatExternal(value)

  // ==================== Thrift Mapping (optional) ====================

  /** Returns the Thrift TTypeId name for this type (e.g., "STRING_TYPE"). */
  def thriftTypeName: Option[String] = None
}

/**
 * Factory object for creating TypeApiOps instances.
 *
 * Returns Option to serve as both lookup and existence check - callers use getOrElse to fall
 * through to legacy handling. The feature flag check is inside apply(), so callers don't need to
 * check it separately.
 */
object TypeApiOps {

  /**
   * Returns a TypeApiOps instance for the given DataType, if supported by the framework.
   *
   * Returns None if the type is not supported or the framework is disabled. This is the single
   * registration point for all client-side type operations.
   *
   * @param dt
   *   the DataType to get operations for
   * @return
   *   Some(TypeApiOps) if supported, None otherwise
   */
  def apply(dt: DataType): Option[TypeApiOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(new TimeTypeApiOps(tt))
      // Add new types here - single registration point
      case _ => None
    }
  }

  /**
   * Reverse lookup: converts an Arrow type to a Spark DataType.
   */
  def fromArrowType(at: ArrowType): Option[DataType] = {
    import org.apache.arrow.vector.types.TimeUnit
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    at match {
      case t: ArrowType.Time if t.getUnit == TimeUnit.NANOSECOND && t.getBitWidth == 8 * 8 =>
        Some(TimeType(TimeType.MICROS_PRECISION))
      // Add new framework types here
      case _ => None
    }
  }
}
