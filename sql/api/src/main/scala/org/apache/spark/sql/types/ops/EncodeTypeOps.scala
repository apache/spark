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
import org.apache.spark.sql.types.DataType

/**
 * Operations for row encoding and decoding.
 *
 * PURPOSE:
 * Provides the encoder needed for Dataset[T] operations. The encoder handles
 * serialization and deserialization of user objects to/from internal rows.
 *
 * USAGE CONTEXT:
 * Used by:
 * - RowEncoder.scala - creates encoders for schema fields
 * - EncoderUtils.scala - encoder utility functions
 * - Spark Connect - client-side encoding
 * - Dataset[T] API - all typed dataset operations
 *
 * @see TimeTypeApiOps for reference implementation
 * @since 4.1.0
 */
trait EncodeTypeOps extends TypeApiOps {
  /**
   * Returns the AgnosticEncoder for this type.
   *
   * The encoder handles serialization (external -> internal) and deserialization
   * (internal -> external) for Dataset[T] operations.
   *
   * @return AgnosticEncoder instance (e.g., LocalTimeEncoder for TimeType)
   * @example TimeType -> LocalTimeEncoder (handles java.time.LocalTime)
   * @example DateType -> LocalDateEncoder or DateEncoder (depending on config)
   */
  def getEncoder: AgnosticEncoder[_]
}

/**
 * Companion object providing factory methods for EncodeTypeOps.
 */
object EncodeTypeOps {
  /**
   * Creates an EncodeTypeOps instance for the given DataType.
   *
   * @param dt The DataType to get encoding operations for
   * @return EncodeTypeOps instance
   * @throws SparkException if the type doesn't support EncodeTypeOps
   */
  def apply(dt: DataType): EncodeTypeOps = TypeApiOps(dt).asInstanceOf[EncodeTypeOps]

  /**
   * Checks if a DataType supports EncodeTypeOps operations.
   *
   * @param dt The DataType to check
   * @return true if the type supports EncodeTypeOps
   */
  def supports(dt: DataType): Boolean = TypeApiOps.supports(dt)
}
