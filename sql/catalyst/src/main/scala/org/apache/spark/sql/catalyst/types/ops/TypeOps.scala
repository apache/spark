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

import org.apache.spark.SparkException
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Base trait for all type operations in the Types Framework.
 *
 * PURPOSE: TypeOps centralizes type-specific operations that were previously scattered across
 * 40+ files in pattern matching expressions like `case _: TimeType => ...`. By implementing
 * TypeOps for a new type, all integration points automatically work without modifying those files.
 *
 * USAGE - integration points use the check-and-delegate pattern:
 * {{{
 * def getPhysicalType(dt: DataType): PhysicalDataType = dt match {
 *   case _ if TypeOps.supports(dt) => TypeOps(dt).asInstanceOf[PhyTypeOps].getPhysicalType
 *   case DateType => PhysicalIntegerType
 *   // ... legacy types
 * }
 * }}}
 *
 * IMPLEMENTATION - to add a new type to the framework:
 *   1. Create a case class extending TypeOps and the relevant traits (PhyTypeOps, etc.)
 *   2. Register it in the TypeOps.apply() and TypeOps.supports() methods below
 *   3. No other file modifications needed - all integration points automatically work
 *
 * @see
 *   TimeTypeOps for a reference implementation
 * @since 4.1.0
 */
trait TypeOps extends Serializable {

  /** The DataType this Ops instance handles */
  def dataType: DataType
}

/**
 * Factory object for creating TypeOps instances.
 *
 * Uses pattern matching rather than Set enumeration to support parameterized types
 * like TimeType(precision) or DecimalType(precision, scale).
 */
object TypeOps {

  /**
   * Creates a TypeOps instance for the given DataType.
   *
   * @param dt
   *   The DataType to get operations for
   * @return
   *   TypeOps instance for the type
   * @throws SparkException
   *   if no TypeOps implementation exists for the type
   */
  def apply(dt: DataType): TypeOps = dt match {
    case tt: TimeType => TimeTypeOps(tt)
    // Future types will be added here:
    // case dt: DecimalType => DecimalTypeOps(dt)
    // case _: DurationType => DurationTypeOps(dt)
    case _ => throw SparkException.internalError(
      s"No TypeOps implementation for ${dt.typeName}. " +
        "This type is not yet supported by the Types Framework.")
  }

  /**
   * Checks if a DataType is supported by the Types Framework.
   *
   * This method should be used in the check-and-delegate pattern at integration points:
   * {{{
   * case _ if TypeOps.supports(dt) => TypeOps(dt).someMethod()
   * }}}
   *
   * @param dt
   *   The DataType to check
   * @return
   *   true if the type is supported by the framework
   */
  def supports(dt: DataType): Boolean = dt match {
    case _: TimeType => true
    // Future types will be added here
    case _ => false
  }
}
