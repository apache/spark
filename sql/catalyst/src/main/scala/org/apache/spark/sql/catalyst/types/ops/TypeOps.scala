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
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableValue}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Server-side (catalyst) type operations for the Types Framework.
 *
 * This trait consolidates all server-side operations that a data type must implement to function
 * in the Spark SQL engine. All methods are mandatory because without any of them the type would
 * fail at runtime - physical type mapping is needed for storage, literals for the optimizer,
 * and external type conversion for user-facing operations like collect() and UDFs.
 *
 * This single-interface design was chosen over separate PhyTypeOps/LiteralTypeOps/ExternalTypeOps
 * traits to make it clear what a new type must implement. There is one mandatory interface with
 * everything required. Optional capabilities (e.g., proto serialization, client integration)
 * are defined as separate traits that can be mixed in incrementally as a type's support expands.
 *
 * USAGE - integration points use TypeOps(dt) which returns Option[TypeOps]:
 * {{{
 * def getPhysicalType(dt: DataType): PhysicalDataType =
 *   TypeOps(dt).map(_.getPhysicalType).getOrElse {
 *     dt match {
 *       case DateType => PhysicalIntegerType
 *       // ... legacy types
 *     }
 *   }
 * }}}
 *
 * IMPLEMENTATION - to add a new type to the framework:
 *   1. Create a case class extending TypeOps (and optionally TypeApiOps for client-side ops)
 *   2. Register it in TypeOps.apply() below - single registration point
 *   3. No other file modifications needed - all integration points automatically work
 *
 * @see TimeTypeOps for a reference implementation
 * @since 4.2.0
 */
trait TypeOps extends Serializable {

  /** The DataType this Ops instance handles. */
  def dataType: DataType

  // ==================== Physical Type Representation ====================

  /**
   * Returns the physical data type representation.
   *
   * Determines how values are stored in memory and accessed from InternalRow.
   *
   * @return PhysicalDataType (e.g., PhysicalLongType for TimeType)
   */
  def getPhysicalType: PhysicalDataType

  /**
   * Returns the Java class used for code generation.
   *
   * @return Java class (e.g., classOf[Long] for TimeType)
   */
  def getJavaClass: Class[_]

  /**
   * Returns a MutableValue instance for use in SpecificInternalRow.
   *
   * @return MutableValue instance (e.g., MutableLong for TimeType)
   */
  def getMutableValue: MutableValue

  /**
   * Returns a writer function for setting values in an InternalRow.
   *
   * @param ordinal the column index to write to
   * @return writer function (InternalRow, Any) => Unit
   */
  def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit

  // ==================== Literal Creation ====================

  /**
   * Returns the default literal value for this type.
   *
   * Used by Literal.default() for ALTER TABLE ADD COLUMN, optimizer, etc.
   *
   * @return Literal with the default value and correct type
   */
  def getDefaultLiteral: Literal

  /**
   * Returns the Java literal representation for code generation.
   *
   * @param v the internal value to represent
   * @return Java literal string (e.g., "37800000000000L")
   */
  def getJavaLiteral(v: Any): String

  // ==================== External Type Conversion ====================

  /**
   * Converts an external (Scala/Java) value to its internal Catalyst representation.
   *
   * Handles null checking and Option unwrapping automatically.
   *
   * @param maybeScalaValue the external value (may be null or Option)
   * @return the internal representation, or null if input was null/None
   */
  final def toCatalyst(@Nullable maybeScalaValue: Any): Any = {
    maybeScalaValue match {
      case null | None => null
      case opt: Some[_] => toCatalystImpl(opt.get)
      case other => toCatalystImpl(other)
    }
  }

  /**
   * Converts a non-null external value to its internal representation.
   *
   * @param scalaValue the external value (guaranteed non-null)
   * @return the internal Catalyst representation
   */
  def toCatalystImpl(scalaValue: Any): Any

  /**
   * Converts an internal Catalyst value to its external representation.
   *
   * @param catalystValue the internal value (may be null)
   * @return the external representation, or null if input was null
   */
  def toScala(@Nullable catalystValue: Any): Any

  /**
   * Extracts a value from an InternalRow and converts to external representation.
   *
   * @param row the InternalRow containing the value
   * @param column the column index
   * @return the external representation
   */
  def toScalaImpl(row: InternalRow, column: Int): Any

  /**
   * Extracts a value from an InternalRow with null checking.
   */
  final def toScala(row: InternalRow, column: Int): Any = {
    if (row.isNullAt(column)) null else toScalaImpl(row, column)
  }
}

/**
 * Factory object for creating TypeOps instances.
 *
 * Returns Option to serve as both lookup and existence check - callers use
 * getOrElse to fall through to legacy handling. The feature flag check is
 * inside apply(), so callers don't need to check it separately.
 *
 * Uses pattern matching (not Set enumeration) to support parameterized types
 * like TimeType(precision) or DecimalType(precision, scale).
 */
object TypeOps {

  /**
   * Returns a TypeOps instance for the given DataType, if supported by the framework.
   *
   * Returns None if the type is not supported or the framework is disabled.
   * This is the single registration point for all server-side type operations.
   *
   * @param dt the DataType to get operations for
   * @return Some(TypeOps) if supported, None otherwise
   */
  def apply(dt: DataType): Option[TypeOps] = {
    if (!SQLConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(TimeTypeOps(tt))
      // Add new types here - single registration point
      case _ => None
    }
  }
}
