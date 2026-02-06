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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DataType

/**
 * Operations related to literal creation and code generation.
 *
 * PURPOSE:
 * Provides default literal values and Java literal representations for types.
 * This is used when a column needs a default value (e.g., ALTER TABLE ADD COLUMN)
 * or when generating code that includes literal values.
 *
 * USAGE CONTEXT:
 * Used by:
 * - literals.scala - Literal.default() method
 * - CodeGenerator.scala - generating Java literal strings
 * - ALTER TABLE ADD COLUMN - default column values
 * - Query optimization - constant folding
 *
 * @see TimeTypeOps for reference implementation
 * @since 4.1.0
 */
trait LiteralTypeOps extends TypeOps {
  /**
   * Returns the default literal value for this type.
   *
   * This is used when a default value is needed, such as when adding
   * a new column without specifying a default.
   *
   * @return Literal with the default value and correct type
   * @example TimeType -> Literal(0L, TimeType(precision)) representing "00:00:00"
   * @example DecimalType(10,2) -> Literal(Decimal(0), DecimalType(10,2))
   */
  def getDefaultLiteral: Literal

  /**
   * Returns the Java literal representation for code generation.
   *
   * When generating Java/Scala code, literal values need to be represented
   * as strings that the compiler can parse. This method provides that
   * representation.
   *
   * @param v The internal value to represent
   * @return Java literal string (e.g., "123456L" for a Long value)
   * @example 123456L -> "123456L" (for TimeType nanoseconds)
   * @example Decimal(10.5) -> "Decimal(10.5)" (for DecimalType)
   */
  def getJavaLiteral(v: Any): String
}

/**
 * Companion object providing factory methods for LiteralTypeOps.
 */
object LiteralTypeOps {
  /**
   * Creates a LiteralTypeOps instance for the given DataType.
   *
   * @param dt The DataType to get literal operations for
   * @return LiteralTypeOps instance
   * @throws SparkException if the type doesn't support LiteralTypeOps
   */
  def apply(dt: DataType): LiteralTypeOps = TypeOps(dt).asInstanceOf[LiteralTypeOps]

  /**
   * Checks if a DataType supports LiteralTypeOps operations.
   *
   * @param dt The DataType to check
   * @return true if the type supports LiteralTypeOps
   */
  def supports(dt: DataType): Boolean = TypeOps.supports(dt)
}
