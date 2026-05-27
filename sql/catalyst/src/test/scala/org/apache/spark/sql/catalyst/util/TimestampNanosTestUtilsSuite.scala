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
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

class TimestampNanosTestUtilsSuite extends SparkFunSuite {

  test("timestampNTZ / timestampLTZ builders return java.time values with the requested nanos") {
    val ldt = timestampNTZ(2024, 1, 15, 10, 30, 45, 123456789)
    assert(ldt === LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456789))
    assert(ldt.getNano === 123456789)

    val instant = timestampLTZ(2024, 1, 15, 10, 30, 45, 123456789)
    assert(instant === LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456789)
      .atZone(ZoneOffset.UTC).toInstant)
    assert(instant.getNano === 123456789)
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
      timestampNTZ(2024, 1, 15, 10, 30, 45, 123456789),
      timestampNTZ(9999, 12, 31, 23, 59, 59, 999999999)).foreach { ldt =>
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
      timestampLTZ(2024, 1, 15, 10, 30, 45, 123456789),
      timestampLTZ(9999, 12, 31, 23, 59, 59, 999999999)).foreach { instant =>
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
      // Corpus invariant: every entry carries sub-microsecond digits (so the suite actually
      // exercises the nanos path, not just the micros path).
      assert(ldt.getNano >= 0 && ldt.getNano <= 999999999)

      val instant = parseSpecialNanosLTZ(s, zone)
      assert(instant.getNano === ldt.getNano)
    }
    // At least one entry must have nanos that aren't a clean micro-multiple, otherwise the
    // corpus degenerates to a micro-only set.
    assert(specialNanosTs.exists(s => parseSpecialNanosNTZ(s).getNano % NANOS_PER_MICROS != 0),
      "specialNanosTs corpus has no sub-micro entries")
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
    assert(Iterator.fill(500)(gen()).contains(null), "Expected at least one null in 500 samples")
  }

  test("Seeded RandomDataGenerator -> composite -> java.time round-trip is the identity") {
    // The seeded smoke test required by the JIRA: generate java.time values from a seeded
    // RandomDataGenerator, push them through the composite physical representation and back,
    // and assert equality.
    Seq[(org.apache.spark.sql.types.DataType, Any => TimestampNanosVal, TimestampNanosVal => Any)](
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
