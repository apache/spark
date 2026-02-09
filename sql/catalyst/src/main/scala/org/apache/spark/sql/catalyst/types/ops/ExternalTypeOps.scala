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

package org.apache.spark.sql.catalyst.types.ops

import javax.annotation.Nullable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

/**
 * Operations for converting between external (Java/Scala) and internal (Catalyst) representations.
 *
 * PURPOSE: Handles bidirectional conversion between user-facing types (e.g., java.time.LocalTime)
 * and internal Catalyst types (e.g., Long representing nanoseconds since midnight).
 * This is essential for Dataset[T] operations, UDFs, and collect() operations.
 *
 * USAGE CONTEXT - examples of use:
 *   - CatalystTypeConverters.scala - main conversion utilities
 *   - Dataset[T] encoders - converting user objects to/from internal rows
 *   - UDF execution - converting arguments and return values
 *   - collect() operations - converting results back to user types
 *   - Cast expressions - some cast operations use external type conversion
 *
 * TYPE PARAMETERS:
 *   - ScalaInputType: The external type accepted as input (e.g., LocalTime)
 *   - ScalaOutputType: The external type produced as output (e.g., LocalTime)
 *   - CatalystType: The internal representation type (e.g., Long)
 *
 * In most cases, ScalaInputType and ScalaOutputType are the same type.
 *
 * @see
 *   TimeTypeOps for reference implementation
 * @since 4.1.0
 */
trait ExternalTypeOps extends TypeOps {

  /**
   * Converts an external (Scala/Java) value to its internal Catalyst representation.
   *
   * This method handles null checking and Option unwrapping automatically via toCatalyst().
   * Implementations should handle the non-null case in toCatalystImpl().
   *
   * @param maybeScalaValue
   *   The external value (may be null or Option)
   * @return
   *   The internal Catalyst representation, or null if input was null/None
   */
  final def toCatalyst(@Nullable maybeScalaValue: Any): Any = {
    maybeScalaValue match {
      case null | None => null
      case opt: Some[_] => toCatalystImpl(opt.get)
      case other => toCatalystImpl(other)
    }
  }

  /**
   * Converts an external value to its internal representation.
   *
   * This method is called by toCatalyst() after null checking.
   * Implementations should assume the input is non-null.
   *
   * @param scalaValue
   *   The external value (guaranteed non-null)
   * @return
   *   The internal Catalyst representation
   * @example
   *   LocalTime.of(10, 30, 0) -> 37800000000000L (nanoseconds since midnight)
   */
  def toCatalystImpl(scalaValue: Any): Any

  /**
   * Converts an internal Catalyst value to its external representation.
   *
   * @param catalystValue The internal value (may be null)
   * @return The external representation, or null if input was null
   * @example 37800000000000L -> LocalTime.of(10, 30, 0)
   */
  def toScala(@Nullable catalystValue: Any): Any

  /**
   * Extracts a value from an InternalRow and converts to external representation.
   *
   * This is a convenience method used when reading values from rows.
   * The caller should check for null before calling this method.
   *
   * @param row
   *   The InternalRow containing the value
   * @param column
   *   The column index
   * @return
   *   The external representation of the value at the given column
   */
  def toScalaImpl(row: InternalRow, column: Int): Any

  /**
   * Extracts a value from an InternalRow with null checking.
   *
   * @param row
   *   The InternalRow containing the value
   * @param column
   *   The column index
   * @return
   *   The external representation, or null if the column is null
   */
  final def toScala(row: InternalRow, column: Int): Any = {
    if (row.isNullAt(column)) null else toScalaImpl(row, column)
  }
}

/**
 * Companion object providing factory methods for ExternalTypeOps.
 */
object ExternalTypeOps {

  /**
   * Creates an ExternalTypeOps instance for the given DataType.
   *
   * @param dt
   *   The DataType to get external conversion operations for
   * @return
   *   ExternalTypeOps instance
   * @throws SparkException
   *   if the type doesn't support ExternalTypeOps
   */
  def apply(dt: DataType): ExternalTypeOps = TypeOps(dt).asInstanceOf[ExternalTypeOps]

  /**
   * Checks if a DataType supports ExternalTypeOps operations.
   *
   * @param dt
   *   The DataType to check
   * @return
   *   true if the type supports ExternalTypeOps
   */
  def supports(dt: DataType): Boolean = TypeOps.supports(dt)
}
