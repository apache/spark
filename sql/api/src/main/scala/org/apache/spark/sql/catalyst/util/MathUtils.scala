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
import org.apache.spark.sql.errors.ExecutionErrors

/**
 * Helper functions for arithmetic operations with overflow.
 */
object MathUtils {

  def addExact(a: Int, b: Int): Int = withOverflow(Math.addExact(a, b))

  def addExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.addExact(a, b), hint = "try_add", context)
  }

  def addExact(a: Long, b: Long): Long = withOverflow(Math.addExact(a, b))

  def addExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.addExact(a, b), hint = "try_add", context)
  }

  def subtractExact(a: Int, b: Int): Int = withOverflow(Math.subtractExact(a, b))

  def subtractExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.subtractExact(a, b), hint = "try_subtract", context)
  }

  def subtractExact(a: Long, b: Long): Long = withOverflow(Math.subtractExact(a, b))

  def subtractExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.subtractExact(a, b), hint = "try_subtract", context)
  }

  def multiplyExact(a: Int, b: Int): Int = withOverflow(Math.multiplyExact(a, b))

  def multiplyExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.multiplyExact(a, b), hint = "try_multiply", context)
  }

  def multiplyExact(a: Long, b: Long): Long = withOverflow(Math.multiplyExact(a, b))

  def multiplyExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.multiplyExact(a, b), hint = "try_multiply", context)
  }

  def negateExact(a: Byte): Byte = {
    if (a == Byte.MinValue) { // if and only if x is Byte.MinValue, overflow can happen
      throw ExecutionErrors.arithmeticOverflowError("byte overflow")
    }
    (-a).toByte
  }

  def negateExact(a: Short): Short = {
    if (a == Short.MinValue) { // if and only if x is Short.MinValue, overflow can happen
      throw ExecutionErrors.arithmeticOverflowError("short overflow")
    }
    (-a).toShort
  }

  def negateExact(a: Int): Int = withOverflow(Math.negateExact(a))

  def negateExact(a: Long): Long = withOverflow(Math.negateExact(a))

  def toIntExact(a: Long): Int = withOverflow(Math.toIntExact(a))

  def floorDiv(a: Int, b: Int): Int = withOverflow(Math.floorDiv(a, b), hint = "try_divide")

  def floorDiv(a: Long, b: Long): Long = withOverflow(Math.floorDiv(a, b), hint = "try_divide")

  // TODO(SPARK-49631): When used, please provide appropriate hint.
  def floorMod(a: Int, b: Int): Int = withOverflow(Math.floorMod(a, b))

  // TODO(SPARK-49631): When used, please provide appropriate hint.
  def floorMod(a: Long, b: Long): Long = withOverflow(Math.floorMod(a, b))

  def withOverflow[A](f: => A, hint: String = "", context: QueryContext = null): A = {
    try {
      f
    } catch {
      case e: ArithmeticException =>
        throw ExecutionErrors.arithmeticOverflowError(e.getMessage, hint, context)
    }
  }

  def withOverflowCode(evalCode: String, context: String): String = {
    s"""
       |try {
       |  $evalCode
       |} catch (ArithmeticException e) {
       |  throw QueryExecutionErrors.arithmeticOverflowError(e.getMessage(), "", $context);
       |}
       |""".stripMargin
  }
}
