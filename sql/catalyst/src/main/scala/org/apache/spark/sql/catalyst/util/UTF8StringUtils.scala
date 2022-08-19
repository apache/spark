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

import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.{IntWrapper, LongWrapper}

/**
 * Helper functions for casting string to numeric values.
 */
object UTF8StringUtils {

  // Parses UTF8String(trimmed if needed) to long. This method is used for implicit casting
  // when ANSI is enabled. It doesn't allow any decimal point in the string.
  def toLongExact(s: UTF8String, context: SQLQueryContext): Long =
    withException(s.toLongExact, context, LongType, s)

  // Parses UTF8String(trimmed if needed) to int. This method is used for implicit casting
  // when ANSI is enabled. It doesn't allow any decimal point in the string.
  def toIntExact(s: UTF8String, context: SQLQueryContext): Int =
    withException(s.toIntExact, context, IntegerType, s)

  // Parses UTF8String(trimmed if needed) to short integer. This method is used for implicit casting
  // when ANSI is enabled. It doesn't allow any decimal point in the string.
  def toShortExact(s: UTF8String, context: SQLQueryContext): Short =
    withException(s.toShortExact, context, ShortType, s)

  // Parses UTF8String(trimmed if needed) to tiny integer. This method is used for implicit casting
  // when ANSI is enabled. It doesn't allow any decimal point in the string.
  def toByteExact(s: UTF8String, context: SQLQueryContext): Byte =
    withException(s.toByteExact, context, ByteType, s)

  // Parses UTF8String(trimmed if needed) to long. This method is used for explicit casting
  // when ANSI is enabled. It allows a decimal point in the string.
  def toLongAnsi(s: UTF8String, context: SQLQueryContext): Long = {
    val wrapper = new LongWrapper()
    if (s.toLong(wrapper)) {
      wrapper.value
    } else {
      throw QueryExecutionErrors.invalidInputInCastToNumberError(LongType, s, context)
    }
  }

  // Parses UTF8String(trimmed if needed) to int. This method is used for explicit casting
  // when ANSI is enabled. It allows a decimal point in the string.
  def toIntAnsi(s: UTF8String, context: SQLQueryContext): Int = {
    val wrapper = new IntWrapper()
    if (s.toInt(wrapper)) {
      wrapper.value
    } else {
      throw QueryExecutionErrors.invalidInputInCastToNumberError(IntegerType, s, context)
    }
  }

  // Parses UTF8String(trimmed if needed) to short. This method is used for explicit casting
  // when ANSI is enabled. It allows a decimal point in the string.
  def toShortAnsi(s: UTF8String, context: SQLQueryContext): Short = {
    val wrapper = new IntWrapper()
    if (s.toShort(wrapper)) {
      wrapper.value.toShort
    } else {
      throw QueryExecutionErrors.invalidInputInCastToNumberError(ShortType, s, context)
    }
  }

  // Parses UTF8String(trimmed if needed) to byte. This method is used for explicit casting
  // when ANSI is enabled. It allows a decimal point in the string.
  def toByteAnsi(s: UTF8String, context: SQLQueryContext): Byte = {
    val wrapper = new IntWrapper()
    if (s.toByte(wrapper)) {
      wrapper.value.toByte
    } else {
      throw QueryExecutionErrors.invalidInputInCastToNumberError(ByteType, s, context)
    }
  }

  private def withException[A](
      f: => A,
      context: SQLQueryContext,
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
