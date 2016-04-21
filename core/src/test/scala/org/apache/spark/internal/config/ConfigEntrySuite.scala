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

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.util.ByteUnit

class ConfigEntrySuite extends SparkFunSuite {

  test("conf entry: int") {
    val conf = new SparkConf()
    val iConf = ConfigBuilder("spark.int").intConf.createWithDefault(1)
    assert(conf.get(iConf) === 1)
    conf.set(iConf, 2)
    assert(conf.get(iConf) === 2)
  }

  test("conf entry: long") {
    val conf = new SparkConf()
    val lConf = ConfigBuilder("spark.long").longConf.createWithDefault(0L)
    conf.set(lConf, 1234L)
    assert(conf.get(lConf) === 1234L)
  }

  test("conf entry: double") {
    val conf = new SparkConf()
    val dConf = ConfigBuilder("spark.double").doubleConf.createWithDefault(0.0)
    conf.set(dConf, 20.0)
    assert(conf.get(dConf) === 20.0)
  }

  test("conf entry: boolean") {
    val conf = new SparkConf()
    val bConf = ConfigBuilder("spark.boolean").booleanConf.createWithDefault(false)
    assert(!conf.get(bConf))
    conf.set(bConf, true)
    assert(conf.get(bConf))
  }

  test("conf entry: optional") {
    val conf = new SparkConf()
    val optionalConf = ConfigBuilder("spark.optional").intConf.createOptional
    assert(conf.get(optionalConf) === None)
    conf.set(optionalConf, 1)
    assert(conf.get(optionalConf) === Some(1))
  }

  test("conf entry: fallback") {
    val conf = new SparkConf()
    val parentConf = ConfigBuilder("spark.int").intConf.createWithDefault(1)
    val confWithFallback = ConfigBuilder("spark.fallback").fallbackConf(parentConf)
    assert(conf.get(confWithFallback) === 1)
    conf.set(confWithFallback, 2)
    assert(conf.get(parentConf) === 1)
    assert(conf.get(confWithFallback) === 2)
  }

  test("conf entry: time") {
    val conf = new SparkConf()
    val time = ConfigBuilder("spark.time").timeConf(TimeUnit.SECONDS).createWithDefaultString("1h")
    assert(conf.get(time) === 3600L)
    conf.set(time.key, "1m")
    assert(conf.get(time) === 60L)
  }

  test("conf entry: bytes") {
    val conf = new SparkConf()
    val bytes = ConfigBuilder("spark.bytes").bytesConf(ByteUnit.KiB).createWithDefaultString("1m")
    assert(conf.get(bytes) === 1024L)
    conf.set(bytes.key, "1k")
    assert(conf.get(bytes) === 1L)
  }

  test("conf entry: string seq") {
    val conf = new SparkConf()
    val seq = ConfigBuilder("spark.seq").stringConf.toSequence.createWithDefault(Seq())
    conf.set(seq.key, "1,,2, 3 , , 4")
    assert(conf.get(seq) === Seq("1", "2", "3", "4"))
    conf.set(seq, Seq("1", "2"))
    assert(conf.get(seq) === Seq("1", "2"))
  }

  test("conf entry: int seq") {
    val conf = new SparkConf()
    val seq = ConfigBuilder("spark.seq").intConf.toSequence.createWithDefault(Seq())
    conf.set(seq.key, "1,,2, 3 , , 4")
    assert(conf.get(seq) === Seq(1, 2, 3, 4))
    conf.set(seq, Seq(1, 2))
    assert(conf.get(seq) === Seq(1, 2))
  }

  test("conf entry: transformation") {
    val conf = new SparkConf()
    val transformationConf = ConfigBuilder("spark.transformation")
      .stringConf
      .transform(_.toLowerCase())
      .createWithDefault("FOO")

    assert(conf.get(transformationConf) === "foo")
    conf.set(transformationConf, "BAR")
    assert(conf.get(transformationConf) === "bar")
  }

  test("conf entry: valid values check") {
    val conf = new SparkConf()
    val enum = ConfigBuilder("spark.enum")
      .stringConf
      .checkValues(Set("a", "b", "c"))
      .createWithDefault("a")
    assert(conf.get(enum) === "a")

    conf.set(enum, "b")
    assert(conf.get(enum) === "b")

    conf.set(enum, "d")
    val enumError = intercept[IllegalArgumentException] {
      conf.get(enum)
    }
    assert(enumError.getMessage === s"The value of ${enum.key} should be one of a, b, c, but was d")
  }

  test("conf entry: conversion error") {
    val conf = new SparkConf()
    val conversionTest = ConfigBuilder("spark.conversionTest").doubleConf.createOptional
    conf.set(conversionTest.key, "abc")
    val conversionError = intercept[IllegalArgumentException] {
      conf.get(conversionTest)
    }
    assert(conversionError.getMessage === s"${conversionTest.key} should be double, but was abc")
  }

  test("default value handling is null-safe") {
    val conf = new SparkConf()
    val stringConf = ConfigBuilder("spark.string").stringConf.createWithDefault(null)
    assert(conf.get(stringConf) === null)
  }

}
