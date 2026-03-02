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

package org.apache.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config._
import org.apache.spark.internal.config.ConfigEntryType._
import org.apache.spark.network.util.ByteUnit

/**
 * Tests for the ConfigEntry.configEntryType enum and its assignment
 * through ConfigBuilder, ensuring that each config entry carries its
 * declared type without runtime type probing or exception handling.
 */
class RecordConfigAccessSuite extends SparkFunSuite {

  // --- configEntryType for builtin entries created via ConfigBuilder ---

  test("configEntryType is BooleanEntry for boolean config entries") {
    assert(DRIVER_USER_CLASS_PATH_FIRST.configEntryType === BooleanEntry)
  }

  test("configEntryType is IntEntry for int config entries") {
    assert(DRIVER_CORES.configEntryType === IntEntry)
  }

  test("configEntryType is LongEntry for long config entries") {
    assert(STORAGE_UNROLL_MEMORY_THRESHOLD.configEntryType === LongEntry)
  }

  test("configEntryType is DoubleEntry for double config entries") {
    assert(MEMORY_STORAGE_FRACTION.configEntryType === DoubleEntry)
  }

  test("configEntryType is StringEntry for string config entries") {
    assert(DRIVER_LIBRARY_PATH.configEntryType === StringEntry)
  }

  test("configEntryType is BytesEntry for bytes config entries") {
    assert(DRIVER_MEMORY.configEntryType === BytesEntry)
  }

  test("configEntryType is TimeEntry for time config entries") {
    assert(EXECUTOR_HEARTBEAT_INTERVAL.configEntryType === TimeEntry)
  }

  // --- configEntryType propagation through fallbackConf ---

  test("fallbackConf inherits configEntryType from the fallback entry") {
    assert(DYN_ALLOCATION_INITIAL_EXECUTORS.configEntryType === IntEntry)
    assert(DYN_ALLOCATION_INITIAL_EXECUTORS.configEntryType ===
      DYN_ALLOCATION_MIN_EXECUTORS.configEntryType)
  }

  test("time-typed fallbackConf inherits TimeEntry from fallback") {
    assert(DRIVER_METRICS_POLLING_INTERVAL.configEntryType === TimeEntry)
    assert(DRIVER_METRICS_POLLING_INTERVAL.configEntryType ===
      EXECUTOR_HEARTBEAT_INTERVAL.configEntryType)
  }

  // --- configEntryType through all create* variants ---

  test("createWithDefault preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.createWithDefault.bool")
      .booleanConf
      .createWithDefault(false)
    assert(entry.configEntryType === BooleanEntry)
  }

  test("createWithDefaultString preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.createWithDefaultString.int")
      .intConf
      .createWithDefaultString("42")
    assert(entry.configEntryType === IntEntry)
  }

  test("createWithDefaultFunction preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.createWithDefaultFunction.long")
      .longConf
      .createWithDefaultFunction(() => 100L)
    assert(entry.configEntryType === LongEntry)
  }

  test("createOptional preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.createOptional.double")
      .doubleConf
      .createOptional
    assert(entry.configEntryType === DoubleEntry)
  }

  // --- configEntryType survives transform / checkValue / toSequence ---

  test("transform preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.transform.int")
      .intConf
      .transform(v => v * 2)
      .createWithDefault(5)
    assert(entry.configEntryType === IntEntry)
  }

  test("checkValue preserves configEntryType") {
    val entry = ConfigBuilder("spark.test.checkValue.double")
      .doubleConf
      .checkValue(_ > 0, "must be positive")
      .createWithDefault(1.0)
    assert(entry.configEntryType === DoubleEntry)
  }

  test("toSequence preserves configEntryType of element type") {
    val entry = ConfigBuilder("spark.test.toSequence.string")
      .stringConf
      .toSequence
      .createWithDefault(Nil)
    assert(entry.configEntryType === StringEntry)
  }

  // --- each ConfigBuilder.*Conf method assigns the correct enum variant ---

  test("intConf assigns IntEntry") {
    val entry = ConfigBuilder("spark.test.intConf")
      .intConf
      .createWithDefault(0)
    assert(entry.configEntryType === IntEntry)
  }

  test("longConf assigns LongEntry") {
    val entry = ConfigBuilder("spark.test.longConf")
      .longConf
      .createWithDefault(0L)
    assert(entry.configEntryType === LongEntry)
  }

  test("doubleConf assigns DoubleEntry") {
    val entry = ConfigBuilder("spark.test.doubleConf")
      .doubleConf
      .createWithDefault(0.0)
    assert(entry.configEntryType === DoubleEntry)
  }

  test("booleanConf assigns BooleanEntry") {
    val entry = ConfigBuilder("spark.test.booleanConf")
      .booleanConf
      .createWithDefault(false)
    assert(entry.configEntryType === BooleanEntry)
  }

  test("stringConf assigns StringEntry") {
    val entry = ConfigBuilder("spark.test.stringConf")
      .stringConf
      .createWithDefault("default")
    assert(entry.configEntryType === StringEntry)
  }

  test("timeConf assigns TimeEntry") {
    val entry = ConfigBuilder("spark.test.timeConf")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10L)
    assert(entry.configEntryType === TimeEntry)
  }

  test("bytesConf assigns BytesEntry") {
    val entry = ConfigBuilder("spark.test.bytesConf")
      .bytesConf(ByteUnit.MiB)
      .createWithDefault(256L)
    assert(entry.configEntryType === BytesEntry)
  }

  test("regexConf assigns RegexEntry") {
    val entry = ConfigBuilder("spark.test.regexConf")
      .regexConf
      .createWithDefault(".*".r)
    assert(entry.configEntryType === RegexEntry)
  }

  // --- non-boolean entries are not BooleanEntry ---

  test("non-boolean config entries do not have BooleanEntry type") {
    assert(DRIVER_CORES.configEntryType !== BooleanEntry)
    assert(DRIVER_MEMORY.configEntryType !== BooleanEntry)
    assert(EXECUTOR_HEARTBEAT_INTERVAL.configEntryType !== BooleanEntry)
    assert(STORAGE_UNROLL_MEMORY_THRESHOLD.configEntryType !== BooleanEntry)
    assert(MEMORY_STORAGE_FRACTION.configEntryType !== BooleanEntry)
  }
}
