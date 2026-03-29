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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.QueryContext
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper functions for casting string to numeric values.
 */
object UTF8StringUtils {

  def toLongExact(s: UTF8String, context: QueryContext): Long =
    withException(s.toLongExact, context, LongType, s)

  def toIntExact(s: UTF8String, context: QueryContext): Int =
    withException(s.toIntExact, context, IntegerType, s)

  def toShortExact(s: UTF8String, context: QueryContext): Short =
    withException(s.toShortExact, context, ShortType, s)

  def toByteExact(s: UTF8String, context: QueryContext): Byte =
    withException(s.toByteExact, context, ByteType, s)

  private def withException[A](
      f: => A,
      context: QueryContext,
      to: DataType,
      s: UTF8String): A = {
    try {
      f
    } catch {
      case e: NumberFormatException =>
        throw QueryExecutionErrors.invalidInputInCastToNumberError(to, s, context)
    }
  }
}
