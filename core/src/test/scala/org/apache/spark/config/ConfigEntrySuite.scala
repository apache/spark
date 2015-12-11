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

package org.apache.spark.config

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.util.ByteUnit

class ConfigEntrySuite extends SparkFunSuite {

  test("conf entry: int") {
    val conf = new SparkConf()
    val iConf = ConfigEntry.intConf("spark.int", defaultValue = Some(1))
    assert(conf.get(iConf) === 1)
    conf.set(iConf, 2)
    assert(conf.get(iConf) === 2)
  }

  test("conf entry: long") {
    val conf = new SparkConf()
    val lConf = ConfigEntry.longConf("spark.long")
    conf.set(lConf, 1234L)
    assert(conf.get(lConf) === 1234L)
  }

  test("conf entry: double") {
    val conf = new SparkConf()
    val dConf = ConfigEntry.doubleConf("spark.double")
    conf.set(dConf, 20.0)
    assert(conf.get(dConf) === 20.0)
  }

  test("conf entry: boolean") {
    val conf = new SparkConf()
    val bConf = ConfigEntry.booleanConf("spark.boolean", defaultValue = Some(false))
    assert(!conf.get(bConf))
    conf.set(bConf, true)
    assert(conf.get(bConf))
  }

  test("conf entry: required") {
    val conf = new SparkConf()
    val requiredConf = ConfigEntry.longConf("spark.noDefault")
    intercept[NoSuchElementException] {
      conf.get(requiredConf)
    }
  }

  test("conf entry: optional") {
    val conf = new SparkConf()
    val optionalConf = ConfigEntry.intConf("spark.optional").optional
    assert(conf.get(optionalConf) === None)
    conf.set(optionalConf, 1)
    assert(conf.get(optionalConf) === Some(1))
  }

  test("conf entry: fallback") {
    val conf = new SparkConf()
    val parentConf = ConfigEntry.intConf("spark.int", defaultValue = Some(1))
    val confWithFallback = ConfigEntry.fallbackConf("spark.fallback", parentConf)
    assert(conf.get(confWithFallback) === 1)
    conf.set(confWithFallback, 2)
    assert(conf.get(parentConf) === 1)
    assert(conf.get(confWithFallback) === 2)
  }

  test("conf entry: time") {
    val conf = new SparkConf()
    val time = ConfigEntry.timeConf("spark.time", TimeUnit.SECONDS, defaultValue = Some("1h"))
    assert(conf.get(time) === 3600L)
    conf.set(time.key, "1m")
    assert(conf.get(time) === 60L)
  }

  test("conf entry: bytes") {
    val conf = new SparkConf()
    val bytes = ConfigEntry.bytesConf("spark.bytes", ByteUnit.KiB, defaultValue = Some("1m"))
    assert(conf.get(bytes) === 1024L)
    conf.set(bytes.key, "1k")
    assert(conf.get(bytes) === 1L)
  }

  test("conf entry: string seq") {
    val conf = new SparkConf()
    val seq = ConfigEntry.stringSeqConf("spark.seq")
    conf.set(seq.key, "1,,2, 3 , , 4")
    assert(conf.get(seq) === Seq("1", "2", "3", "4"))
    conf.set(seq, Seq("1", "2"))
    assert(conf.get(seq) === Seq("1", "2"))
  }

  test("conf entry: enum") {
    val conf = new SparkConf()
    val enum = ConfigEntry.enumConf("spark.enum", v => v, Set("a", "b", "c"),
      defaultValue = Some("a"))
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
    val conversionTest = ConfigEntry.doubleConf("spark.conversionTest")
    conf.set(conversionTest.key, "abc")
    val conversionError = intercept[IllegalArgumentException] {
      conf.get(conversionTest)
    }
    assert(conversionError.getMessage === s"${conversionTest.key} should be double, but was abc")
  }

}
