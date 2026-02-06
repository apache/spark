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

import org.apache.spark.sql.catalyst.expressions.MutableValue
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.DataType

/**
 * Operations related to physical type representation.
 *
 * PURPOSE:
 * Defines how a logical type (e.g., TimeType) is physically represented in memory.
 * This includes the physical data type, Java class for code generation, and mutable
 * value type for SpecificInternalRow.
 *
 * USAGE CONTEXT:
 * Used by:
 * - PhysicalDataType.scala - determines physical type mapping
 * - CodeGenerator.scala - determines Java class for codegen
 * - SpecificInternalRow.scala - determines MutableValue type
 * - ColumnVector operations
 *
 * @see TimeTypeOps for reference implementation
 * @since 4.1.0
 */
trait PhyTypeOps extends TypeOps {
  /**
   * Returns the physical data type representation.
   *
   * The physical type determines how values are stored in memory and accessed
   * from InternalRow and ColumnVector.
   *
   * @return PhysicalDataType (e.g., PhysicalLongType for TimeType)
   * @example TimeType -> PhysicalLongType (stored as Long nanoseconds)
   * @example DecimalType -> PhysicalDecimalType (stored as Decimal object)
   */
  def getPhysicalType: PhysicalDataType

  /**
   * Returns the Java class used for code generation.
   *
   * This class is used when generating Java/Scala code for expressions
   * that operate on this type.
   *
   * @return Java class (e.g., classOf[Long] for TimeType)
   */
  def getJavaClass: Class[_]

  /**
   * Returns a MutableValue instance for use in SpecificInternalRow.
   *
   * MutableValue is a mutable wrapper that can hold values of the physical type.
   * It's used for efficient row mutation without boxing/unboxing overhead.
   *
   * @return MutableValue instance (e.g., MutableLong for TimeType)
   */
  def getMutableValue: MutableValue
}

/**
 * Companion object providing factory methods for PhyTypeOps.
 */
object PhyTypeOps {
  /**
   * Creates a PhyTypeOps instance for the given DataType.
   *
   * @param dt The DataType to get physical operations for
   * @return PhyTypeOps instance
   * @throws SparkException if the type doesn't support PhyTypeOps
   */
  def apply(dt: DataType): PhyTypeOps = TypeOps(dt).asInstanceOf[PhyTypeOps]

  /**
   * Checks if a DataType supports PhyTypeOps operations.
   *
   * Note: All types in the framework support PhyTypeOps, so this is equivalent
   * to TypeOps.supports(dt). This method exists for consistency with the
   * check-and-delegate pattern.
   *
   * @param dt The DataType to check
   * @return true if the type supports PhyTypeOps
   */
  def supports(dt: DataType): Boolean = TypeOps.supports(dt)
}
