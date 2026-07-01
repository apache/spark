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

package org.apache.spark.sql.execution.columnar

import java.math.{BigInteger => JBigInteger}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo.KRYO_REGISTRATION_REQUIRED
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.Decimal

/**
 * Regression tests for Kryo round-trip of types that appear in
 * [[DefaultCachedBatch]] stats rows when `kryo.registrationRequired`
 * is enabled.
 */
class DefaultCachedBatchKryoSerializerSuite extends SparkFunSuite {

  private def newSerializer(): SerializerInstance = {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)
    new KryoSerializer(conf).newInstance()
  }

  test("SPARK-56411: round-trip Decimal in strict Kryo mode") {
    val ser = newSerializer()
    val value = Decimal(123456789L, 10, 2)
    val deserialized = ser.deserialize[Decimal](ser.serialize(value))
    assert(deserialized === value)
  }

  test("SPARK-56411: round-trip GenericInternalRow containing Decimal") {
    val ser = newSerializer()
    val row = new GenericInternalRow(
      Array[Any](42, Decimal(987654321L, 18, 6), 3.14D))
    val bytes = ser.serialize(row)
    val back = ser.deserialize[GenericInternalRow](bytes)
    assert(back.numFields === 3)
    assert(back.getInt(0) === 42)
    assert(back.getDecimal(1, 18, 6) === Decimal(987654321L, 18, 6))
    assert(back.getDouble(2) === 3.14D)
  }

  test("SPARK-56411: round-trip DefaultCachedBatch with Decimal stats") {
    val ser = newSerializer()
    val stats = new GenericInternalRow(
      Array[Any](Decimal(0L, 10, 2), Decimal(100000000L, 10, 2), 0L, 0L))
    val batch = DefaultCachedBatch(
      numRows = 10,
      buffers = Array(Array[Byte](1, 2, 3)),
      stats = stats)
    val bytes = ser.serialize(batch)
    val back = ser.deserialize[DefaultCachedBatch](bytes)
    assert(back.numRows === 10)
    assert(back.buffers.length === 1)
    assert(back.stats.getDecimal(0, 10, 2) === Decimal(0L, 10, 2))
    assert(back.stats.getDecimal(1, 10, 2) === Decimal(100000000L, 10, 2))
  }

  test("SPARK-56411: round-trip high-precision Decimal (BigDecimal-backed)") {
    val ser = newSerializer()
    // 30-digit value forces BigDecimal representation (overflows Long).
    val bigBacked =
      Decimal(BigDecimal("123456789012345678901234567890.123"), 33, 3)
    assert(bigBacked.precision > 18,
      "test precondition: precision must exceed Long range")
    val back = ser.deserialize[Decimal](ser.serialize(bigBacked))
    assert(back === bigBacked)
  }

  test("SPARK-56411: round-trip java.math.BigInteger in strict Kryo mode") {
    // BigInteger is serialized transitively when Kryo walks
    // java.math.BigDecimal's internal `intVal` field.
    val ser = newSerializer()
    val value = new JBigInteger("123456789012345678901234567890")
    val back = ser.deserialize[JBigInteger](ser.serialize(value))
    assert(back === value)
  }

  test("SPARK-56411: round-trip DefaultCachedBatch with BigDecimal-backed stats") {
    val ser = newSerializer()
    val stats = new GenericInternalRow(Array[Any](
      Decimal(BigDecimal("1.0"), 38, 6),
      Decimal(
        BigDecimal("99999999999999999999999999999999.999999"), 38, 6),
      100L, 0L))
    val batch = DefaultCachedBatch(
      numRows = 1000,
      buffers = Array(Array[Byte](10, 20, 30)),
      stats = stats)
    val bytes = ser.serialize(batch)
    val back = ser.deserialize[DefaultCachedBatch](bytes)
    assert(back.numRows === 1000)
    assert(back.stats.getDecimal(0, 38, 6) ===
      Decimal(BigDecimal("1.0"), 38, 6))
    assert(back.stats.getDecimal(1, 38, 6) ===
      Decimal(
        BigDecimal("99999999999999999999999999999999.999999"), 38, 6))
  }
}
