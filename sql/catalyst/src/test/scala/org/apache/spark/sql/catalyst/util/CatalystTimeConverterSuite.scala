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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.TimeType

class CatalystTimeConverterSuite extends SparkFunSuite {

  test("LocalTimeConverter - LocalTime to catalyst") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    val input = java.time.LocalTime.of(12, 30, 45)
    val result = converter(input).asInstanceOf[Long]

    assert(result === 45045000000L)
  }

  test("LocalTimeConverter - String to catalyst") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    val result = converter("12:30:45").asInstanceOf[Long]

    assert(result === 45045000000L)
  }

  test("LocalTimeConverter - invalid string throws") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    intercept[IllegalArgumentException] {
      converter("invalid-time")
    }
  }

  test("LocalTimeConverter - unsupported type throws") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    intercept[IllegalArgumentException] {
      converter(12345)
    }
  }

  test("LocalTimeConverter - catalyst to scala string") {
    val converter = CatalystTypeConverters.createToScalaConverter(TimeType)

    val result = converter(45045000000L)

    assert(result === "12:30:45.000000")
  }

  test("LocalTimeConverter - null handling") {
    val converter = CatalystTypeConverters.createToScalaConverter(TimeType)

    val result = converter(null)

    assert(result == null)
  }

  test("LocalTimeConverter - InternalRow conversion") {
    val row = new GenericInternalRow(1)
    row.setLong(0, 45045000000L)

    val converter = CatalystTypeConverters.createToScalaConverter(TimeType)
    val result = converter(row.get(0, TimeType))

    assert(result === "12:30:45.000000")
  }

  test("LocalTimeConverter - roundtrip") {
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(TimeType)
    val toScala = CatalystTypeConverters.createToScalaConverter(TimeType)

    val input = "09:15:30"
    val micros = toCatalyst(input)
    val output = toScala(micros)

    assert(output === "09:15:30.000000")
  }

  test("LocalTimeConverter - microsecond precision") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    val result = converter("12:30:45.123456").asInstanceOf[Long]

    assert(result === 45045123456L)
  }

  test("LocalTimeConverter - boundary values") {
    val converter = CatalystTypeConverters.createToCatalystConverter(TimeType)

    val min = converter("00:00:00")
    val max = converter("23:59:59.999999")

    assert(min === 0L)
    assert(max === 86399999999L)
  }
}
