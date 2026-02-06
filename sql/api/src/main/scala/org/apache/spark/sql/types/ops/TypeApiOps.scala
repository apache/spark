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

import org.apache.spark.SparkException
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Base trait for client-side (spark-api) type operations.
 *
 * PURPOSE:
 * TypeApiOps handles operations that require spark-api internals (e.g., AgnosticEncoder)
 * that are not available in the catalyst package. This separation prevents circular
 * dependencies between sql/api and sql/catalyst modules.
 *
 * USAGE:
 * TypeApiOps is used for:
 * - Row encoding/decoding (EncodeTypeOps)
 * - String formatting (FormatTypeOps)
 *
 * RELATIONSHIP TO TypeOps:
 * - TypeOps (catalyst): Server-side operations - physical types, literals, conversions
 * - TypeApiOps (spark-api): Client-side operations - encoding, formatting
 *
 * For TimeType, TimeTypeOps extends TimeTypeApiOps to inherit both sets of operations.
 *
 * @see TimeTypeApiOps for a reference implementation
 * @since 4.1.0
 */
trait TypeApiOps extends Serializable {
  /** The DataType this Ops instance handles */
  def dataType: DataType
}

/**
 * Factory object for creating TypeApiOps instances.
 */
object TypeApiOps {
  /**
   * Creates a TypeApiOps instance for the given DataType.
   *
   * @param dt The DataType to get operations for
   * @return TypeApiOps instance for the type
   * @throws SparkException if no TypeApiOps implementation exists for the type
   */
  def apply(dt: DataType): TypeApiOps = dt match {
    case tt: TimeType => new TimeTypeApiOps(tt)
    // Future types will be added here
    case _ => throw SparkException.internalError(
      s"No TypeApiOps implementation for ${dt.typeName}. " +
        "This type is not yet supported by the Types Framework.")
  }

  /**
   * Checks if a DataType is supported by the Types Framework (client-side).
   *
   * @param dt The DataType to check
   * @return true if the type is supported by the framework
   */
  def supports(dt: DataType): Boolean = dt match {
    case _: TimeType => true
    // Future types will be added here
    case _ => false
  }
}
