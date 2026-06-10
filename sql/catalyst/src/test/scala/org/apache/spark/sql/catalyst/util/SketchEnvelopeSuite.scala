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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.types.{LongType, StringType}

class SketchEnvelopeSuite extends SparkFunSuite {

  private val prettyName = "test_fn"

  /** Builds a profile with collation-independent numeric defaults that callers can override. */
  private def profile(
      sketchKind: Byte = SketchKind.HLL,
      keyEncoding: Byte = KeyEncoding.NUMERIC_OR_BINARY,
      collationId: Int = SketchEnvelope.NO_COLLATION_ID,
      icuMajor: Short = 0,
      icuMinor: Short = 0,
      dsLibMajor: Short = 6,
      dsLibMinor: Short = 2,
      sparkCollationRev: Short = 1): SketchProfile =
    SketchProfile(
      sketchKind = sketchKind,
      keyEncoding = keyEncoding,
      engineOrigin = EngineOrigin.JVM_SPARK,
      collationId = collationId,
      icuMajor = icuMajor,
      icuMinor = icuMinor,
      dsLibMajor = dsLibMajor,
      dsLibMinor = dsLibMinor,
      sparkCollationRev = sparkCollationRev,
      featureFlags = 0.toByte)

  // -- Round-tripping ---------------------------------------------------------------------------

  test("wrap/unwrap round-trips the payload and the profile") {
    val payload = Array[Byte](1, 2, 3, 4, 5, 6, 7)
    val p = profile(
      sketchKind = SketchKind.THETA,
      keyEncoding = KeyEncoding.ICU_SORTKEY,
      collationId = 42,
      icuMajor = 75,
      icuMinor = 1)

    val wrapped = SketchEnvelope.wrap(payload, p)
    assert(SketchEnvelope.hasEnvelope(wrapped))
    assert(wrapped.length == payload.length + SketchEnvelope.HEADER_LEN)

    val (profileOpt, recovered) = SketchEnvelope.unwrap(wrapped)
    assert(profileOpt.contains(p))
    assert(recovered.sameElements(payload))

    assert(SketchEnvelope.profileOf(wrapped).contains(p))
    assert(SketchEnvelope.payloadOf(wrapped).sameElements(payload))
  }

  test("wrap/unwrap round-trips an empty payload") {
    val payload = Array.empty[Byte]
    val wrapped = SketchEnvelope.wrap(payload, profile())
    assert(SketchEnvelope.hasEnvelope(wrapped))
    val (profileOpt, recovered) = SketchEnvelope.unwrap(wrapped)
    assert(profileOpt.isDefined)
    assert(recovered.isEmpty)
  }

  test("every sketch kind and key encoding round-trips") {
    val payload = Array[Byte](9, 8, 7)
    val kinds = Seq(
      SketchKind.HLL, SketchKind.THETA, SketchKind.TUPLE_DOUBLE, SketchKind.TUPLE_INTEGER,
      SketchKind.KLL_LONG, SketchKind.KLL_FLOAT, SketchKind.KLL_DOUBLE, SketchKind.ITEMS)
    val encodings = Seq(
      KeyEncoding.NUMERIC_OR_BINARY, KeyEncoding.RAW_BYTES, KeyEncoding.ICU_SORTKEY,
      KeyEncoding.UTF8_LCASE_CPS, KeyEncoding.ICU_COLLATION_KEY_STRING)
    for (k <- kinds; e <- encodings) {
      val p = profile(sketchKind = k, keyEncoding = e, collationId = 3, icuMajor = 70)
      val (profileOpt, recovered) = SketchEnvelope.unwrap(SketchEnvelope.wrap(payload, p))
      assert(profileOpt.contains(p), s"kind=$k encoding=$e")
      assert(recovered.sameElements(payload), s"kind=$k encoding=$e")
    }
  }

  // -- Backward compatibility (legacy buffers) --------------------------------------------------

  test("legacy buffers without an envelope pass through unchanged") {
    val legacy = Array.tabulate[Byte](40)(i => (i * 7 + 1).toByte)
    assert(!SketchEnvelope.hasEnvelope(legacy))
    val (profileOpt, recovered) = SketchEnvelope.unwrap(legacy)
    assert(profileOpt.isEmpty)
    assert(recovered.eq(legacy))
    assert(SketchEnvelope.profileOf(legacy).isEmpty)
    assert(SketchEnvelope.payloadOf(legacy).eq(legacy))
  }

  test("buffers shorter than the header are treated as legacy") {
    assert(!SketchEnvelope.hasEnvelope(Array.empty[Byte]))
    assert(!SketchEnvelope.hasEnvelope(Array[Byte](1, 2, 3)))
    assert(!SketchEnvelope.hasEnvelope(new Array[Byte](SketchEnvelope.HEADER_LEN - 1)))
    assert(!SketchEnvelope.hasEnvelope(null))
  }

  test("magic prefix with an inconsistent payload length is treated as legacy") {
    // A buffer that happens to start with the magic bytes but whose embedded payload-length field
    // does not match the actual length must NOT be mistaken for an envelope.
    val buf = new Array[Byte](30)
    val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
    bb.put(0xDB.toByte)
    bb.put(0x53.toByte)
    bb.put(0x4B.toByte)
    bb.put(0x01.toByte)
    // Write a bogus payload length at the payload_length offset (real inner length would be
    // 30 - HEADER_LEN), so the length cross-check must reject this buffer.
    bb.position(SketchEnvelope.HEADER_LEN - 4)
    bb.putInt(999)
    assert(!SketchEnvelope.hasEnvelope(buf))
    val (profileOpt, recovered) = SketchEnvelope.unwrap(buf)
    assert(profileOpt.isEmpty)
    assert(recovered.eq(buf))
  }

  test("wrap output is never mistaken for a native DataSketches preamble") {
    // Byte index 2 of the magic is 0x4B == 75, which is outside the valid DataSketches FamilyId
    // range (1..14), guaranteeing the magic cannot collide with a native preamble.
    val wrapped = SketchEnvelope.wrap(Array[Byte](1, 2, 3), profile())
    assert((wrapped(2) & 0xFF) > 14)
  }

  // -- Compatibility policy ---------------------------------------------------------------------

  test("identical profiles are compatible") {
    val p = profile(keyEncoding = KeyEncoding.ICU_SORTKEY, collationId = 1, icuMajor = 75)
    // Should not throw.
    SketchEnvelope.assertCompatible(p, p, prettyName, allowMismatch = false)
  }

  test("numeric encodings ignore collation and ICU differences") {
    val observed = profile(collationId = 1, icuMajor = 70)
    val runtime = profile(collationId = 2, icuMajor = 99)
    // Numeric key encodings carry no collation/ICU semantics, so these are compatible.
    SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
  }

  test("key encoding mismatch is a hard error") {
    val observed = profile(keyEncoding = KeyEncoding.RAW_BYTES)
    val runtime = profile(keyEncoding = KeyEncoding.ICU_SORTKEY)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_KEY_ENCODING_MISMATCH",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> "raw_bytes",
        "right" -> "icu_sortkey"))
  }

  test("collation mismatch is a hard error") {
    val observed = profile(keyEncoding = KeyEncoding.ICU_SORTKEY, collationId = 1, icuMajor = 75)
    val runtime = profile(keyEncoding = KeyEncoding.ICU_SORTKEY, collationId = 2, icuMajor = 75)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_COLLATION_MISMATCH",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> "1",
        "right" -> "2"))
  }

  test("ICU version mismatch is a hard error") {
    val observed =
      profile(keyEncoding = KeyEncoding.ICU_SORTKEY, collationId = 5, icuMajor = 75, icuMinor = 1)
    val runtime =
      profile(keyEncoding = KeyEncoding.ICU_SORTKEY, collationId = 5, icuMajor = 76, icuMinor = 2)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_ICU_VERSION_MISMATCH",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> "75.1",
        "right" -> "76.2"))
  }

  test("differing Spark collation revision is a soft warning, not an error") {
    val observed = profile(sparkCollationRev = 1)
    val runtime = profile(sparkCollationRev = 2)
    // Numeric encoding + only the revision differs: this only logs a warning, it must not throw.
    SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
  }

  test("allowMismatch suppresses hard errors") {
    val observed = profile(keyEncoding = KeyEncoding.RAW_BYTES)
    val runtime = profile(keyEncoding = KeyEncoding.ICU_SORTKEY)
    // Should not throw when version mismatches are explicitly allowed.
    SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = true)
  }

  // -- currentProfile / currentItemsProfile -----------------------------------------------------

  test("currentProfile for numeric types uses the numeric encoding") {
    val p = SketchEnvelope.currentProfile(SketchKind.KLL_LONG, LongType)
    assert(p.sketchKind == SketchKind.KLL_LONG)
    assert(p.keyEncoding == KeyEncoding.NUMERIC_OR_BINARY)
    assert(p.collationId == SketchEnvelope.NO_COLLATION_ID)
    assert(p.icuMajor == 0 && p.icuMinor == 0)
    assert(p.engineOrigin == EngineOrigin.JVM_SPARK)
  }

  test("currentProfile for UTF8_BINARY strings uses the raw-bytes encoding") {
    val p = SketchEnvelope.currentProfile(SketchKind.HLL, StringType)
    assert(p.keyEncoding == KeyEncoding.RAW_BYTES)
    assert(p.collationId == CollationFactory.UTF8_BINARY_COLLATION_ID)
  }

  test("currentItemsProfile for UTF8_BINARY strings still uses ITEMS kind") {
    val p = SketchEnvelope.currentItemsProfile(StringType)
    assert(p.sketchKind == SketchKind.ITEMS)
  }

  test("profile helper string accessors") {
    assert(profile(icuMajor = 75, icuMinor = 1).icuVersionString == "75.1")
    assert(profile(icuMajor = 0, icuMinor = 0).icuVersionString == null)
    assert(profile(dsLibMajor = 6, dsLibMinor = 2).datasketchesVersionString == "6.2")
    assert(profile().engineName == "jvm_spark")
  }
}
