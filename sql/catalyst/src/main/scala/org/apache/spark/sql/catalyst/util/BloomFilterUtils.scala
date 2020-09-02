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

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat}

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

object BloomFilterUtils {

  private def toBytes(decimal: Decimal): Array[Byte] = {
    decimal.toJavaBigDecimal.unscaledValue().toByteArray
  }

  private def toBytes(str: UTF8String): Array[Byte] = {
    str.getBytes
  }

  private def toLong(boolean: Boolean): Long = {
    JBoolean.compare(boolean, false).toLong
  }

  private def toLong(float: Float): Long = {
    JFloat.floatToIntBits(float.asInstanceOf[Float]).toLong
  }

  private def toLong(double: Double): Long = {
    JDouble.doubleToLongBits(double)
  }

  private def toLong(number: Number): Long = {
    number.asInstanceOf[Number].longValue()
  }

  def putValue(bloomFilter: BloomFilter, value: Any): Unit = value match {
    case float: Float => bloomFilter.putLong(toLong(float))
    case double: Double => bloomFilter.putLong(toLong(double))
    case number: Number => bloomFilter.putLong(toLong(number))
    case str: UTF8String => bloomFilter.putBinary(toBytes(str))
    case bytes: Array[Byte] => bloomFilter.putBinary(bytes)
    case decimal: Decimal => bloomFilter.putBinary(toBytes(decimal))
    case boolean: Boolean => bloomFilter.putLong(toLong(boolean))
    case other =>
      throw new IllegalArgumentException(
        s"Bloom filter only supports atomic types, but got ${other.getClass.getCanonicalName}.")
  }

  def mightContain(bloomFilter: BloomFilter, value: Any): Boolean = value match {
    case float: Float => bloomFilter.mightContainLong(toLong(float))
    case double: Double => bloomFilter.mightContainLong(toLong(double))
    case number: Number => bloomFilter.mightContainLong(toLong(number))
    case str: UTF8String => bloomFilter.mightContainBinary(toBytes(str))
    case bytes: Array[Byte] => bloomFilter.mightContainBinary(bytes)
    case decimal: Decimal => bloomFilter.mightContainBinary(toBytes(decimal))
    case boolean: Boolean => bloomFilter.mightContainLong(toLong(boolean))
    case other =>
      throw new IllegalArgumentException(
        s"Bloom filter only supports atomic types, but got ${other.getClass.getCanonicalName}.")
  }
}
