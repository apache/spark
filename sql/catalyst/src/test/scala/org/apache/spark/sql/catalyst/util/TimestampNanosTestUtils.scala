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

import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS
import org.apache.spark.sql.types.TimestampNTZNanosType
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Helpers for tests involving nanosecond-capable timestamp types
 * ([[org.apache.spark.sql.types.TimestampNTZNanosType]] /
 * [[org.apache.spark.sql.types.TimestampLTZNanosType]], precision in [7, 9]).
 *
 * Provides three things:
 *   - Fixed-value builders that take readable `(year, month, day, ..., nanoOfSec)` arguments and
 *     return external-representation `java.time` values ([[LocalDateTime]] for NTZ,
 *     [[Instant]] for LTZ) -- same convention as
 *     [[org.apache.spark.sql.RandomDataGenerator]] for microsecond timestamps.
 *   - Conversion between `java.time` external types and the physical composite
 *     [[TimestampNanosVal]] (`epochMicros` + `nanosWithinMicro in [0, 999]`).
 *   - A shared edge-case corpus [[specialNanosTs]] extending the microsecond `specialTs` set
 *     with 7-to-9 fractional digits.
 */
object TimestampNanosTestUtils {

  /**
   * Builds a [[LocalDateTime]] suitable for `TIMESTAMP_NTZ(p)` tests.
   *
   * @param nanoOfSec nanoseconds within the second, in [0, 999_999_999]; matches
   *                  [[LocalDateTime.of]]'s `nanoOfSecond` argument and
   *                  [[LocalDateTime.getNano]].
   */
  def timestampNTZ(
      year: Int,
      month: Int,
      day: Int,
      hour: Int = 0,
      minute: Int = 0,
      sec: Int = 0,
      nanoOfSec: Int = 0): LocalDateTime = {
    LocalDateTime.of(year, month, day, hour, minute, sec, nanoOfSec)
  }

  /**
   * Builds an [[Instant]] suitable for `TIMESTAMP_LTZ(p)` tests.
   *
   * @param nanoOfSec nanoseconds within the second, in [0, 999_999_999].
   * @param zoneId    zone used to interpret the local time when computing the instant; defaults
   *                  to UTC so the wall-clock fields above match the resulting epoch instant.
   */
  def timestampLTZ(
      year: Int,
      month: Int,
      day: Int,
      hour: Int = 0,
      minute: Int = 0,
      sec: Int = 0,
      nanoOfSec: Int = 0,
      zoneId: ZoneId = ZoneOffset.UTC): Instant = {
    LocalDateTime.of(year, month, day, hour, minute, sec, nanoOfSec).atZone(zoneId).toInstant
  }

  /**
   * Builds a [[TimestampNanosVal]] from raw components. Delegates to
   * [[TimestampNanosVal.fromParts]] which enforces `nanosWithinMicro in [0, 999]`.
   */
  def nanosVal(epochMicros: Long, nanosWithinMicro: Int): TimestampNanosVal = {
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }

  /**
   * Converts a [[LocalDateTime]] (interpreted under `zoneId`, default UTC) to its composite
   * physical representation `(epochMicros, nanosWithinMicro)`.
   */
  def localDateTimeToNanosVal(
      ldt: LocalDateTime,
      zoneId: ZoneId = ZoneOffset.UTC): TimestampNanosVal = {
    instantToNanosVal(ldt.atZone(zoneId).toInstant)
  }

  /**
   * Converts an [[Instant]] to its composite physical representation
   * `(epochMicros, nanosWithinMicro)`.
   */
  def instantToNanosVal(instant: Instant): TimestampNanosVal = {
    val epochMicros = DateTimeUtils.instantToMicros(instant)
    val nanosWithinMicro = (instant.getNano % NANOS_PER_MICROS).toShort
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro)
  }

  /**
   * Converts a composite [[TimestampNanosVal]] back to a [[LocalDateTime]] at UTC. The result
   * preserves nanosecond precision: round-tripping through
   * [[localDateTimeToNanosVal]] / [[nanosValToLocalDateTime]] is the identity (modulo zone).
   */
  def nanosValToLocalDateTime(v: TimestampNanosVal): LocalDateTime = {
    DateTimeUtils.microsToLocalDateTime(v.epochMicros).plusNanos(v.nanosWithinMicro.toLong)
  }

  /**
   * Converts a composite [[TimestampNanosVal]] back to an [[Instant]]. The result preserves
   * nanosecond precision: round-tripping through [[instantToNanosVal]] / [[nanosValToInstant]]
   * is the identity.
   */
  def nanosValToInstant(v: TimestampNanosVal): Instant = {
    DateTimeUtils.microsToInstant(v.epochMicros).plusNanos(v.nanosWithinMicro.toLong)
  }

  /**
   * Edge-case corpus for nanosecond timestamps, extending the microsecond `specialTs` set used
   * by [[org.apache.spark.sql.RandomDataGenerator]] with sub-microsecond fractional digits.
   * Each entry is an ISO-like `"yyyy-MM-dd HH:mm:ss.nnnnnnnnn"` string.
   *
   * Covers the four canonical micro dates (0001/1582/1970/9999) each at
   * `nanosWithinMicro in {0, 1, 999}`, plus an arbitrary mid-range string spanning all 9
   * fractional digits.
   */
  val specialNanosTs: Seq[String] = Seq(
    "0001-01-01 00:00:00.000000000",
    "0001-01-01 00:00:00.000000001",
    "0001-01-01 00:00:00.000000999",
    "1582-10-15 23:59:59.123456789",
    "1970-01-01 00:00:00.000000000",
    "1970-01-01 00:00:00.000000001",
    "1970-01-01 00:00:00.000000999",
    "9999-12-31 23:59:59.999999000",
    "9999-12-31 23:59:59.999999001",
    "9999-12-31 23:59:59.999999999")

  /** Parses an entry from [[specialNanosTs]] into a [[LocalDateTime]] (NTZ external rep). */
  def parseSpecialNanosNTZ(s: String): LocalDateTime = LocalDateTime.parse(s.replace(' ', 'T'))

  /**
   * Parses an entry from [[specialNanosTs]] into an [[Instant]] (LTZ external rep).
   *
   * Defaults to [[ZoneId.systemDefault]] (not UTC) to mirror the LTZ special-value corpus in
   * [[org.apache.spark.sql.RandomDataGenerator]]'s `TimestampType` case.
   */
  def parseSpecialNanosLTZ(s: String, zoneId: ZoneId = ZoneId.systemDefault()): Instant = {
    parseSpecialNanosNTZ(s).atZone(zoneId).toInstant
  }

  /**
   * Runs `body` once for each valid nanosecond timestamp precision (currently 7, 8, 9).
   * Both [[org.apache.spark.sql.types.TimestampNTZNanosType]] and
   * [[org.apache.spark.sql.types.TimestampLTZNanosType]] share the same precision band, so a
   * single iterator is enough.
   */
  def foreachNanosPrecision(body: Int => Unit): Unit = {
    TimestampNTZNanosType.MIN_PRECISION to TimestampNTZNanosType.MAX_PRECISION foreach body
  }
}
