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

trait BigDecimalConverter[T] {
  def toBigDecimal(in: T) : BigDecimal
  def fromBigDecimal(bd: BigDecimal) : T
}

/**
 * Helper type converters to work with BigDecimal
 * from http://stackoverflow.com/a/30979266/1115193
 */
object BigDecimalConverter {

  implicit object ByteConverter extends BigDecimalConverter[Byte] {
    def toBigDecimal(in: Byte) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toByte
  }

  implicit object ShortConverter extends BigDecimalConverter[Short] {
    def toBigDecimal(in: Short) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toShort
  }

  implicit object IntConverter extends BigDecimalConverter[Int] {
    def toBigDecimal(in: Int) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toInt
  }

  implicit object LongConverter extends BigDecimalConverter[Long] {
    def toBigDecimal(in: Long) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toLong
  }

  implicit object FloatConverter extends BigDecimalConverter[Float] {
    def toBigDecimal(in: Float) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toFloat
  }

  implicit object DoubleConverter extends BigDecimalConverter[Double] {
    def toBigDecimal(in: Double) = BigDecimal(in)
    def fromBigDecimal(bd: BigDecimal) = bd.toDouble
  }
}
