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

import java.time.format.DateTimeParseException

import org.apache.spark.SparkFunSuite

class TimeFormatterSuite extends SparkFunSuite {

  test("Basic Parsing (Happy Path)") {
    val tf = TimeFormatter("HH:mm:ss")
    val micros = tf.parse("12:30:45")
    assert(micros == 45045000000L)
  }

  test("Parsing with Microseconds") {
    val tf = TimeFormatter("HH:mm:ss.SSSSSS")
    val micros = tf.parse("12:30:45.123456")
    assert(micros == 45045123456L)
  }

  test("Formatting") {
    val tf = TimeFormatter("HH:mm:ss.SSSSSS")
    val formatted = tf.format(45045123456L)
    assert(formatted == "12:30:45.123456")
  }

  test("Round-trip Consistency") {
    val tf = TimeFormatter("HH:mm:ss.SSSSSS")
    val s = "12:30:45.123456"
    val parsed = tf.parse(s)
    val formatted = tf.format(parsed)
    assert(formatted == s)
  }

  test("Default Formatter Behavior") {
    val tf = TimeFormatter(None)
    val micros = tf.parse("12:30:45")
    assert(micros == 45045000000L)
  }

  test("Invalid Inputs") {
    val tf = TimeFormatter("HH:mm:ss")
    intercept[DateTimeParseException] {
      tf.parse("abc")
    }
    intercept[DateTimeParseException] {
      tf.parse("25:00:00")
    }
  }

  test("Pattern Validation") {
    intercept[IllegalArgumentException] {
      TimeFormatter("invalid-pattern")
    }
  }

  test("Custom Pattern Support") {
    val tf = TimeFormatter("HH:mm")
    val micros = tf.parse("12:30")
    assert(micros == 45000000000L)
  }

  test("Boundary Values") {
    val tf = TimeFormatter("HH:mm:ss.SSSSSS")
    assert(tf.parse("00:00:00.000000") == 0L)
    assert(tf.parse("23:59:59.999999") == 86399999999L)
  }

  test("Microsecond Precision Loss") {
    val tf = TimeFormatter("HH:mm:ss.S")
    val micros = tf.parse("12:30:45.1")
    assert(micros == 45045100000L)
  }

  test("Serialization Test") {
    val tf = TimeFormatter("HH:mm:ss")
    val baos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(baos)
    oos.writeObject(tf)
    oos.close()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois = new java.io.ObjectInputStream(bais)
    val deserializedTf = ois.readObject().asInstanceOf[TimeFormatter]
    ois.close()
    val micros = deserializedTf.parse("12:30:45")
    assert(micros == 45045000000L)
  }
}
