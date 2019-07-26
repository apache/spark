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

package org.apache.spark.sql.types

import scala.math.Numeric.{ByteIsIntegral, IntIsIntegral, LongIsIntegral, ShortIsIntegral}
import scala.math.Ordering


object ByteExactNumeric extends ByteIsIntegral with Ordering.ByteOrdering {
  private def checkOverflow(res: Int, x: Byte, y: Byte, op: String): Unit = {
    if (x > Byte.MaxValue || x < Byte.MinValue) {
      throw new ArithmeticException(s"$x $op $y caused overflow.")
    }
  }

  override def plus(x: Byte, y: Byte): Byte = {
    val tmp = x + y
    checkOverflow(tmp, x, y, "+")
    tmp.toByte
  }

  override def minus(x: Byte, y: Byte): Byte = {
    val tmp = x - y
    checkOverflow(tmp, x, y, "-")
    tmp.toByte
  }

  override def times(x: Byte, y: Byte): Byte = {
    val tmp = x * y
    checkOverflow(tmp, x, y, "*")
    tmp.toByte
  }
}


object ShortExactNumeric extends ShortIsIntegral with Ordering.ShortOrdering {
  private def checkOverflow(res: Int, x: Short, y: Short, op: String): Unit = {
    if (x > Short.MaxValue || x < Short.MinValue) {
      throw new ArithmeticException(s"$x $op $y caused overflow.")
    }
  }

  override def plus(x: Short, y: Short): Short = {
    val tmp = x + y
    checkOverflow(tmp, x, y, "+")
    tmp.toShort
  }

  override def minus(x: Short, y: Short): Short = {
    val tmp = x - y
    checkOverflow(tmp, x, y, "-")
    tmp.toShort
  }

  override def times(x: Short, y: Short): Short = {
    val tmp = x * y
    checkOverflow(tmp, x, y, "*")
    tmp.toShort
  }
}


object IntegerExactNumeric extends IntIsIntegral with Ordering.IntOrdering {
  override def plus(x: Int, y: Int): Int = Math.addExact(x, y)

  override def minus(x: Int, y: Int): Int = Math.subtractExact(x, y)

  override def times(x: Int, y: Int): Int = Math.multiplyExact(x, y)
}

object LongExactNumeric extends LongIsIntegral with Ordering.LongOrdering {
  override def plus(x: Long, y: Long): Long = Math.addExact(x, y)

  override def minus(x: Long, y: Long): Long = Math.subtractExact(x, y)

  override def times(x: Long, y: Long): Long = Math.multiplyExact(x, y)
}
