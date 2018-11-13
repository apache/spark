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

package org.apache.spark.internal.config

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.SparkConf.buildConf
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.SparkConfWithEnv

class ConfigEntrySuite extends SparkFunSuite {

  private val PREFIX = "spark.ConfigEntrySuite"

  private def testKey(name: String): String = s"$PREFIX.$name"

  test("conf entry: int") {
    val conf = new SparkConf()
    val iConf = buildConf(testKey("int")).intConf.createWithDefault(1)
    assert(conf.get(iConf) === 1)
    conf.set(iConf, 2)
    assert(conf.get(iConf) === 2)
  }

  test("conf entry: long") {
    val conf = new SparkConf()
    val lConf = buildConf(testKey("long")).longConf.createWithDefault(0L)
    conf.set(lConf, 1234L)
    assert(conf.get(lConf) === 1234L)
  }

  test("conf entry: double") {
    val conf = new SparkConf()
    val dConf = buildConf(testKey("double")).doubleConf.createWithDefault(0.0)
    conf.set(dConf, 20.0)
    assert(conf.get(dConf) === 20.0)
  }

  test("conf entry: boolean") {
    val conf = new SparkConf()
    val bConf = buildConf(testKey("boolean")).booleanConf.createWithDefault(false)
    assert(!conf.get(bConf))
    conf.set(bConf, true)
    assert(conf.get(bConf))
  }

  test("conf entry: optional") {
    val conf = new SparkConf()
    val optionalConf = buildConf(testKey("optional")).intConf.createOptional
    assert(conf.get(optionalConf) === None)
    conf.set(optionalConf, 1)
    assert(conf.get(optionalConf) === Some(1))
  }

  test("conf entry: fallback") {
    val conf = new SparkConf()
    val parentConf = buildConf(testKey("parent")).intConf.createWithDefault(1)
    val confWithFallback = buildConf(testKey("fallback")).fallbackConf(parentConf)
    assert(conf.get(confWithFallback) === 1)
    conf.set(confWithFallback, 2)
    assert(conf.get(parentConf) === 1)
    assert(conf.get(confWithFallback) === 2)
  }

  test("conf entry: time") {
    val conf = new SparkConf()
    val time = buildConf(testKey("time")).timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("1h")
    assert(conf.get(time) === 3600L)
    conf.set(time.key, "1m")
    assert(conf.get(time) === 60L)
  }

  test("conf entry: bytes") {
    val conf = new SparkConf()
    val bytes = buildConf(testKey("bytes")).bytesConf(ByteUnit.KiB)
      .createWithDefaultString("1m")
    assert(conf.get(bytes) === 1024L)
    conf.set(bytes.key, "1k")
    assert(conf.get(bytes) === 1L)
  }

  test("conf entry: regex") {
    val conf = new SparkConf()
    val rConf = buildConf(testKey("regex")).regexConf.createWithDefault(".*".r)

    conf.set(rConf, "[0-9a-f]{8}".r)
    assert(conf.get(rConf).toString === "[0-9a-f]{8}")

    conf.set(rConf.key, "[0-9a-f]{4}")
    assert(conf.get(rConf).toString === "[0-9a-f]{4}")

    val e = intercept[IllegalArgumentException](conf.set(rConf.key, "[."))
    assert(e.getMessage.contains("regex should be a regex, but was"))
  }

  test("conf entry: string seq") {
    val conf = new SparkConf()
    val seq = buildConf(testKey("seq")).stringConf.toSequence.createWithDefault(Seq())
    conf.set(seq.key, "1,,2, 3 , , 4")
    assert(conf.get(seq) === Seq("1", "2", "3", "4"))
    conf.set(seq, Seq("1", "2"))
    assert(conf.get(seq) === Seq("1", "2"))
  }

  test("conf entry: int seq") {
    val conf = new SparkConf()
    val seq = buildConf(testKey("intSeq")).intConf.toSequence.createWithDefault(Seq())
    conf.set(seq.key, "1,,2, 3 , , 4")
    assert(conf.get(seq) === Seq(1, 2, 3, 4))
    conf.set(seq, Seq(1, 2))
    assert(conf.get(seq) === Seq(1, 2))
  }

  test("conf entry: transformation") {
    val conf = new SparkConf()
    val transformationConf = buildConf(testKey("transformation"))
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createWithDefault("FOO")

    assert(conf.get(transformationConf) === "foo")
    conf.set(transformationConf, "BAR")
    assert(conf.get(transformationConf) === "bar")
  }

  test("conf entry: checkValue()") {
    def createEntry(default: Int): ConfigEntry[Int] =
      buildConf(testKey("checkValue"))
        .intConf
        .checkValue(value => value >= 0, "value must be non-negative")
        .createWithDefault(default)

    val conf = new SparkConf()

    val entry = createEntry(10)
    val e1 = intercept[IllegalArgumentException] {
      conf.set(entry, -1)
    }
    assert(e1.getMessage == "value must be non-negative")

    val e2 = intercept[IllegalArgumentException] {
      createEntry(-1)
    }
    assert(e2.getMessage == "value must be non-negative")
  }

  test("conf entry: valid values check") {
    val conf = new SparkConf()
    val enum = buildConf(testKey("enum"))
      .stringConf
      .checkValues(Set("a", "b", "c"))
      .createWithDefault("a")
    assert(conf.get(enum) === "a")

    conf.set(enum, "b")
    assert(conf.get(enum) === "b")

    val enumError = intercept[IllegalArgumentException] {
      conf.set(enum, "d")
    }
    assert(enumError.getMessage === s"The value of ${enum.key} should be one of a, b, c, but was d")
  }

  test("conf entry: conversion error") {
    val conf = new SparkConf()
    val conversionTest = buildConf(testKey("conversionTest")).doubleConf.createOptional
    val conversionError = intercept[IllegalArgumentException] {
      conf.set(conversionTest.key, "abc")
    }
    assert(conversionError.getMessage === s"${conversionTest.key} should be double, but was abc")
  }

  test("default value handling is null-safe") {
    val conf = new SparkConf()
    val stringConf = buildConf(testKey("string")).stringConf.createWithDefault(null)
    assert(conf.get(stringConf) === null)
  }

  test("variable expansion of spark config entries") {
    val env = Map("ENV1" -> "env1")
    val conf = new SparkConfWithEnv(env)

    val stringConf = buildConf(testKey("stringForExpansion"))
      .stringConf
      .createWithDefault("string1")
    val optionalConf = buildConf(testKey("optionForExpansion"))
      .stringConf
      .createOptional
    val intConf = buildConf(testKey("intForExpansion"))
      .intConf
      .createWithDefault(42)
    val fallbackConf = buildConf(testKey("fallbackForExpansion"))
      .fallbackConf(intConf)

    val refConf = buildConf(testKey("configReferenceTest"))
      .stringConf
      .createWithDefault(null)

    def ref(entry: ConfigEntry[_]): String = "${" + entry.key + "}"

    def testEntryRef(entry: ConfigEntry[_], expected: String): Unit = {
      conf.set(refConf, ref(entry))
      assert(conf.get(refConf) === expected)
    }

    testEntryRef(stringConf, "string1")
    testEntryRef(intConf, "42")
    testEntryRef(fallbackConf, "42")

    testEntryRef(optionalConf, ref(optionalConf))

    conf.set(optionalConf, ref(stringConf))
    testEntryRef(optionalConf, "string1")

    conf.set(optionalConf, ref(fallbackConf))
    testEntryRef(optionalConf, "42")

    // Default string values with variable references.
    val parameterizedStringConf = buildConf(testKey("stringWithParams"))
      .stringConf
      .createWithDefault(ref(stringConf))
    assert(conf.get(parameterizedStringConf) === conf.get(stringConf))

    // Make sure SparkConf's env override works.
    conf.set(refConf, "${env:ENV1}")
    assert(conf.get(refConf) === env("ENV1"))

    // Conf with null default value is not expanded.
    val nullConf = buildConf(testKey("nullString"))
      .stringConf
      .createWithDefault(null)
    testEntryRef(nullConf, ref(nullConf))
  }

  test("conf entry : default function") {
    var data = 0
    val conf = new SparkConf()
    val iConf = buildConf(testKey("intval")).intConf.createWithDefaultFunction(() => data)
    assert(conf.get(iConf) === 0)
    data = 2
    assert(conf.get(iConf) === 2)
  }

  test("conf entry: alternative keys") {
    val conf = new SparkConf()
    val iConf = buildConf(testKey("a"))
      .withAlternative(testKey("b"))
      .withAlternative(testKey("c"))
      .intConf.createWithDefault(0)

    // no key is set, return default value.
    assert(conf.get(iConf) === 0)

    // the primary key is set, the alternative keys are not set, return the value of primary key.
    conf.set(testKey("a"), "1")
    assert(conf.get(iConf) === 1)

    // the primary key and alternative keys are all set, return the value of primary key.
    conf.set(testKey("b"), "2")
    conf.set(testKey("c"), "3")
    assert(conf.get(iConf) === 1)

    // the primary key is not set, (some of) the alternative keys are set, return the value of the
    // first alternative key that is set.
    conf.remove(testKey("a"))
    assert(conf.get(iConf) === 2)
    conf.remove(testKey("b"))
    assert(conf.get(iConf) === 3)
  }

  test("onCreate") {
    var onCreateCalled = false
    buildConf(testKey("oc1")).onCreate(_ => onCreateCalled = true).intConf.createWithDefault(1)
    assert(onCreateCalled)

    onCreateCalled = false
    buildConf(testKey("oc2")).onCreate(_ => onCreateCalled = true).intConf.createOptional
    assert(onCreateCalled)

    onCreateCalled = false
    buildConf(testKey("oc3")).onCreate(_ => onCreateCalled = true).intConf
      .createWithDefaultString("1.0")
    assert(onCreateCalled)

    val fallback = buildConf(testKey("oc4")).intConf.createWithDefault(1)
    onCreateCalled = false
    buildConf(testKey("oc5")).onCreate(_ => onCreateCalled = true).fallbackConf(fallback)
    assert(onCreateCalled)
  }
}
