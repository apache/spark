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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils._
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

class TimestampNanosTestUtilsSuite extends SparkFunSuite {

  test("timestampNTZ / timestampLTZ builders return java.time values with the requested nanos") {
    val ldt = timestampNTZ(2024, 1, 15, 10, 30, 45, 123_456_789)
    assert(ldt === LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123_456_789))
    assert(ldt.getNano === 123_456_789)

    val instant = timestampLTZ(2024, 1, 15, 10, 30, 45, 123_456_789)
    assert(instant === LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123_456_789)
      .atZone(ZoneOffset.UTC).toInstant)
    assert(instant.getNano === 123_456_789)
  }

  test("nanosVal enforces the [0, 999] invariant on nanosWithinMicro") {
    // Boundary values accepted.
    Seq(0, 1, 499, TimestampNanosVal.MAX_NANOS_WITHIN_MICRO).foreach { n =>
      val v = nanosVal(1234L, n)
      assert(v.epochMicros === 1234L)
      assert(v.nanosWithinMicro === n.toShort)
    }
    // Out-of-range values rejected.
    Seq(-1, 1000, 1001, Int.MaxValue).foreach { n =>
      val e = intercept[Exception](nanosVal(0L, n))
      assert(e.getMessage.contains("nanosWithinMicro"))
    }
  }

  test("LocalDateTime <-> TimestampNanosVal round-trip preserves nanosecond precision") {
    Seq(
      timestampNTZ(1970, 1, 1, 0, 0, 0, 0),
      timestampNTZ(1970, 1, 1, 0, 0, 0, 1),
      timestampNTZ(1970, 1, 1, 0, 0, 0, TimestampNanosVal.MAX_NANOS_WITHIN_MICRO),
      // 1582 Julian/Gregorian cutover -- the date most sensitive to any change in
      // microsToInstant / instantToMicros calendar handling.
      timestampNTZ(1582, 10, 15, 23, 59, 59, 123_456_789),
      timestampNTZ(2024, 1, 15, 10, 30, 45, 123_456_789),
      timestampNTZ(9999, 12, 31, 23, 59, 59, 999_999_999)).foreach { ldt =>
      val v = localDateTimeToNanosVal(ldt)
      assert(v.nanosWithinMicro >= 0 &&
        v.nanosWithinMicro <= TimestampNanosVal.MAX_NANOS_WITHIN_MICRO,
        s"nanosWithinMicro out of range for $ldt: ${v.nanosWithinMicro}")
      assert(nanosValToLocalDateTime(v) === ldt)
    }
  }

  test("Instant <-> TimestampNanosVal round-trip preserves nanosecond precision") {
    Seq(
      Instant.EPOCH,
      Instant.EPOCH.plusNanos(1),
      Instant.EPOCH.plusNanos(TimestampNanosVal.MAX_NANOS_WITHIN_MICRO.toLong),
      timestampLTZ(1582, 10, 15, 23, 59, 59, 123_456_789),
      timestampLTZ(2024, 1, 15, 10, 30, 45, 123_456_789),
      timestampLTZ(9999, 12, 31, 23, 59, 59, 999_999_999)).foreach { instant =>
      val v = instantToNanosVal(instant)
      assert(v.nanosWithinMicro >= 0 &&
        v.nanosWithinMicro <= TimestampNanosVal.MAX_NANOS_WITHIN_MICRO,
        s"nanosWithinMicro out of range for $instant: ${v.nanosWithinMicro}")
      assert(nanosValToInstant(v) === instant)
    }
  }

  test("LDT split: epochMicros truncates, nanosWithinMicro keeps remainder") {
    // ".000000789" -> epochMicros at .000000 boundary, nanosWithinMicro = 789.
    val ldt = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 789)
    val v = localDateTimeToNanosVal(ldt)
    assert(v.epochMicros === 0L)
    assert(v.nanosWithinMicro === 789.toShort)

    // ".000001789" -> epochMicros = 1, nanosWithinMicro = 789.
    val ldt2 = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1789)
    val v2 = localDateTimeToNanosVal(ldt2)
    assert(v2.epochMicros === 1L)
    assert(v2.nanosWithinMicro === 789.toShort)
  }

  test("specialNanosTs entries parse without exception via both NTZ and LTZ helpers") {
    val zone = ZoneId.of("America/Los_Angeles")
    specialNanosTs.foreach { s =>
      val ldt = parseSpecialNanosNTZ(s)
      // Sanity-check nano-of-second range across the corpus.
      assert(ldt.getNano >= 0 && ldt.getNano <= 999_999_999)

      val instant = parseSpecialNanosLTZ(s, zone)
      assert(instant.getNano === ldt.getNano)
    }
    // Corpus invariant: at least one entry must carry sub-microsecond digits, otherwise the
    // suite degenerates to exercising the micros path only.
    assert(specialNanosTs.exists(s => parseSpecialNanosNTZ(s).getNano % NANOS_PER_MICROS != 0),
      "specialNanosTs corpus has no sub-micro entries")
  }

  test("nanoOfSecTruncator zeros (9 - precision) low decimal digits") {
    // precision 9: identity on every nano-of-second.
    val truncP9 = nanoOfSecTruncator(9)
    Seq(0, 1, 999, 123_456_789, 999_999_999).foreach { n =>
      assert(truncP9(n) === n, s"precision 9 truncator should be identity, but $n -> ${truncP9(n)}")
    }

    // precision 8: zeroes the last digit (factor 10).
    val truncP8 = nanoOfSecTruncator(8)
    assert(truncP8(123_456_789) === 123_456_780)
    assert(truncP8(123_456_780) === 123_456_780)
    assert(truncP8(999_999_999) === 999_999_990)
    assert(truncP8(9) === 0)
    assert(truncP8(0) === 0)

    // precision 7: zeroes the last two digits (factor 100).
    val truncP7 = nanoOfSecTruncator(7)
    assert(truncP7(123_456_789) === 123_456_700)
    assert(truncP7(123_456_700) === 123_456_700)
    assert(truncP7(999_999_999) === 999_999_900)
    assert(truncP7(99) === 0)
    assert(truncP7(100) === 100)
  }

  test("RandomDataGenerator honors precision for TimestampNTZNanosType") {
    foreachNanosPrecision { p =>
      val factor = math.pow(10.0, (9 - p).toDouble).toInt
      val gen = RandomDataGenerator.forType(TimestampNTZNanosType(p), nullable = false,
        rand = new Random(42L))
        .getOrElse(fail(s"No generator for TimestampNTZNanosType($p)"))
      Iterator.fill(200)(gen()).foreach {
        case ldt: LocalDateTime =>
          assert(ldt.getNano % factor === 0,
            s"p=$p generated $ldt with nanoOfSec ${ldt.getNano}; not divisible by $factor")
        case other => fail(s"Expected LocalDateTime, got $other: ${other.getClass}")
      }
    }
  }

  test("RandomDataGenerator honors precision for TimestampLTZNanosType") {
    foreachNanosPrecision { p =>
      val factor = math.pow(10.0, (9 - p).toDouble).toInt
      val gen = RandomDataGenerator.forType(TimestampLTZNanosType(p), nullable = false,
        rand = new Random(42L))
        .getOrElse(fail(s"No generator for TimestampLTZNanosType($p)"))
      Iterator.fill(200)(gen()).foreach {
        case ins: Instant =>
          assert(ins.getNano % factor === 0,
            s"p=$p generated $ins with nanoOfSec ${ins.getNano}; not divisible by $factor")
        case other => fail(s"Expected Instant, got $other: ${other.getClass}")
      }
    }
  }

  test("foreachNanosPrecision iterates [MIN_PRECISION, MAX_PRECISION] inclusive") {
    val seen = scala.collection.mutable.ArrayBuffer.empty[Int]
    foreachNanosPrecision(seen.append(_))
    assert(seen.toSeq === (TimestampNTZNanosType.MIN_PRECISION
      to TimestampNTZNanosType.MAX_PRECISION))
    assert(seen.toSeq === (TimestampLTZNanosType.MIN_PRECISION
      to TimestampLTZNanosType.MAX_PRECISION),
      "NTZ and LTZ should share the same precision band")
  }

  test("RandomDataGenerator produces non-null LocalDateTime for TimestampNTZNanosType") {
    foreachNanosPrecision { p =>
      val gen = RandomDataGenerator.forType(TimestampNTZNanosType(p), nullable = false,
        rand = new Random(42L))
        .getOrElse(fail(s"No generator for TimestampNTZNanosType($p)"))
      val values = Iterator.fill(200)(gen()).toList
      assert(!values.contains(null), "nullable = false should never produce null")
      values.foreach(v => assert(v.isInstanceOf[LocalDateTime], s"got $v: ${v.getClass}"))
      // Sub-microsecond variation is the whole point: at least one sample must carry
      // nanosWithinMicro > 0.
      assert(values.exists { case ldt: LocalDateTime => ldt.getNano % NANOS_PER_MICROS != 0 },
        "Random generator never produced sub-microsecond nanos in 200 samples")
    }
  }

  test("RandomDataGenerator produces non-null Instant for TimestampLTZNanosType") {
    foreachNanosPrecision { p =>
      val gen = RandomDataGenerator.forType(TimestampLTZNanosType(p), nullable = false,
        rand = new Random(42L))
        .getOrElse(fail(s"No generator for TimestampLTZNanosType($p)"))
      val values = Iterator.fill(200)(gen()).toList
      assert(!values.contains(null), "nullable = false should never produce null")
      values.foreach(v => assert(v.isInstanceOf[Instant], s"got $v: ${v.getClass}"))
      assert(values.exists { case ins: Instant => ins.getNano % NANOS_PER_MICROS != 0 },
        "Random generator never produced sub-microsecond nanos in 200 samples")
    }
  }

  test("RandomDataGenerator nullable = true mixes in nulls for nanos timestamp types") {
    val gen = RandomDataGenerator.forType(TimestampNTZNanosType(9), nullable = true,
      rand = new Random(42L))
      .getOrElse(fail("No generator for TimestampNTZNanosType(9)"))
    // Couples to RandomDataGenerator.PROBABILITY_OF_NULL (~10%). 500 samples gives a vanishing
    // false-fail probability at the current rate; if the constant ever drops below ~1% this
    // could flake.
    assert(Iterator.fill(500)(gen()).contains(null), "Expected at least one null in 500 samples")
  }

  test("Seeded RandomDataGenerator -> composite -> java.time round-trip is the identity") {
    // The seeded smoke test required by the JIRA: generate java.time values from a seeded
    // RandomDataGenerator, push them through the composite physical representation and back,
    // and assert equality.
    Seq[(DataType, Any => TimestampNanosVal, TimestampNanosVal => Any)](
      (TimestampNTZNanosType(9),
        v => localDateTimeToNanosVal(v.asInstanceOf[LocalDateTime]),
        nanosValToLocalDateTime),
      (TimestampLTZNanosType(9),
        v => instantToNanosVal(v.asInstanceOf[Instant]),
        nanosValToInstant)).foreach { case (dt, toComposite, fromComposite) =>
      val gen = RandomDataGenerator.forType(dt, nullable = false, rand = new Random(42L))
        .getOrElse(fail(s"No generator for $dt"))
      (1 to 1000).foreach { i =>
        val original = gen()
        val composite = toComposite(original)
        assert(composite.nanosWithinMicro >= 0 && composite.nanosWithinMicro <= 999,
          s"iter=$i value=$original produced out-of-range nanosWithinMicro " +
            s"${composite.nanosWithinMicro}")
        val roundTripped = fromComposite(composite)
        assert(roundTripped === original,
          s"iter=$i round-trip failed: original=$original, composite=$composite, " +
            s"roundTripped=$roundTripped")
      }
    }
  }
}
