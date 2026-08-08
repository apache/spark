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
import org.apache.spark.sql.catalyst.util.sketch.SketchEnvelopeProtos.SketchMetadata
import org.apache.spark.sql.types.{LongType, StringType}

class SketchEnvelopeSuite extends SparkFunSuite {

  private val prettyName = "test_fn"

  // A couple of real collation IDs used to exercise the derived key-encoding / collation checks.
  private val utf8Binary: Int = CollationFactory.UTF8_BINARY_COLLATION_ID
  private val unicode: Int = CollationFactory.collationNameToId("UNICODE")
  private val unicodeCi: Int = CollationFactory.collationNameToId("UNICODE_CI")

  /** Builds metadata with collation-independent numeric defaults that callers can override. */
  private def metadata(
      collationId: Int = SketchEnvelope.NO_COLLATION_ID,
      icuMajor: Int = 0,
      icuMinor: Int = 0,
      dsLibMajor: Int = 6,
      dsLibMinor: Int = 2): SketchMetadata =
    SketchMetadata.newBuilder()
      .setCollationId(collationId)
      .setIcuMajor(icuMajor)
      .setIcuMinor(icuMinor)
      .setDatasketchesMajor(dsLibMajor)
      .setDatasketchesMinor(dsLibMinor)
      .build()

  // -- Round-tripping ---------------------------------------------------------------------------

  test("wrap/unwrap round-trips the payload and the metadata") {
    val payload = Array[Byte](1, 2, 3, 4, 5, 6, 7)
    val m = metadata(collationId = unicode, icuMajor = 75, icuMinor = 1)

    val wrapped = SketchEnvelope.wrap(payload, m)
    assert(SketchEnvelope.hasEnvelope(wrapped))
    // The magic byte plus the 4-byte length prefix come before the protobuf and payload.
    assert((wrapped(0) & 0xFF) == 0xFF)

    val (metaOpt, recovered) = SketchEnvelope.unwrap(wrapped)
    assert(metaOpt.contains(m))
    assert(recovered.sameElements(payload))

    assert(SketchEnvelope.metadataOf(wrapped).contains(m))
    assert(SketchEnvelope.payloadOf(wrapped).sameElements(payload))
  }

  test("wrap/unwrap round-trips an empty payload") {
    val payload = Array.empty[Byte]
    val wrapped = SketchEnvelope.wrap(payload, metadata())
    assert(SketchEnvelope.hasEnvelope(wrapped))
    val (metaOpt, recovered) = SketchEnvelope.unwrap(wrapped)
    assert(metaOpt.isDefined)
    assert(recovered.isEmpty)
  }

  test("every collation and icu version round-trips") {
    val payload = Array[Byte](9, 8, 7)
    val collations = Seq(SketchEnvelope.NO_COLLATION_ID, utf8Binary, unicode, unicodeCi)
    for (c <- collations; icu <- Seq(0, 70, 75)) {
      val m = metadata(collationId = c, icuMajor = icu, icuMinor = 1)
      val (metaOpt, recovered) = SketchEnvelope.unwrap(SketchEnvelope.wrap(payload, m))
      assert(metaOpt.contains(m), s"collation=$c icu=$icu")
      assert(recovered.sameElements(payload), s"collation=$c icu=$icu")
    }
  }

  // -- Backward compatibility (legacy buffers) --------------------------------------------------

  test("legacy buffers without an envelope pass through unchanged") {
    val legacy = Array.tabulate[Byte](40)(i => (i * 7 + 1).toByte)
    assert(!SketchEnvelope.hasEnvelope(legacy))
    val (metaOpt, recovered) = SketchEnvelope.unwrap(legacy)
    assert(metaOpt.isEmpty)
    assert(recovered.eq(legacy))
    assert(SketchEnvelope.metadataOf(legacy).isEmpty)
    assert(SketchEnvelope.payloadOf(legacy).eq(legacy))
  }

  test("buffers shorter than the header are treated as legacy") {
    assert(!SketchEnvelope.hasEnvelope(Array.empty[Byte]))
    assert(!SketchEnvelope.hasEnvelope(Array[Byte](0xFF.toByte, 0, 0)))
    assert(!SketchEnvelope.hasEnvelope(null))
  }

  test("magic byte with an out-of-range length is treated as legacy") {
    // A buffer that starts with the magic byte but whose embedded protobuf length does not fit
    // inside the buffer must NOT be mistaken for an envelope.
    val buf = new Array[Byte](20)
    val bb = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN)
    bb.put(0xFF.toByte)
    bb.putInt(999) // bogus protobuf length, far larger than the buffer
    assert(!SketchEnvelope.hasEnvelope(buf))
    val (metaOpt, recovered) = SketchEnvelope.unwrap(buf)
    assert(metaOpt.isEmpty)
    assert(recovered.eq(buf))
  }

  test("magic byte with an unparseable protobuf falls back to legacy") {
    // protobuf length fits, but the bytes are not a valid SketchMetadata, so unwrap must not throw.
    val buf = Array[Byte](0xFF.toByte, 0, 0, 0, 1, 0x08.toByte)
    assert(SketchEnvelope.hasEnvelope(buf))
    val (metaOpt, recovered) = SketchEnvelope.unwrap(buf)
    assert(metaOpt.isEmpty)
    assert(recovered.eq(buf))
  }

  // -- Compatibility policy ---------------------------------------------------------------------

  test("identical metadata is compatible") {
    val m = metadata(collationId = unicode, icuMajor = 75)
    // Should not throw.
    SketchEnvelope.assertCompatible(m, m, prettyName, allowMismatch = false)
  }

  test("numeric encodings ignore collation and ICU differences") {
    val observed = metadata(collationId = SketchEnvelope.NO_COLLATION_ID, icuMajor = 70)
    val runtime = metadata(collationId = SketchEnvelope.NO_COLLATION_ID, icuMajor = 99)
    // Numeric key encodings carry no collation/ICU semantics, so these are compatible.
    SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
  }

  test("key encoding mismatch is a hard error") {
    // Numeric (no collation) vs UTF8_BINARY (raw bytes): the derived key encodings differ.
    val observed = metadata(collationId = SketchEnvelope.NO_COLLATION_ID)
    val runtime = metadata(collationId = utf8Binary)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_INPUT_MISMATCH.KEY_ENCODING",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> "numeric_or_binary",
        "right" -> "raw_bytes"))
  }

  test("collation mismatch is a hard error") {
    // Two distinct ICU collations share the icu_sortkey encoding, so the collation IDs must match.
    val observed = metadata(collationId = unicode, icuMajor = 75)
    val runtime = metadata(collationId = unicodeCi, icuMajor = 75)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_INPUT_MISMATCH.COLLATION",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> unicode.toString,
        "right" -> unicodeCi.toString))
  }

  test("ICU version mismatch is a hard error") {
    val observed = metadata(collationId = unicode, icuMajor = 75, icuMinor = 1)
    val runtime = metadata(collationId = unicode, icuMajor = 76, icuMinor = 2)
    checkError(
      exception = intercept[SparkRuntimeException] {
        SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = false)
      },
      condition = "SKETCH_INPUT_MISMATCH.ICU_VERSION",
      parameters = Map(
        "function" -> s"`$prettyName`",
        "left" -> "75.1",
        "right" -> "76.2"))
  }

  test("allowMismatch suppresses hard errors") {
    val observed = metadata(collationId = SketchEnvelope.NO_COLLATION_ID)
    val runtime = metadata(collationId = utf8Binary)
    // Should not throw when version mismatches are explicitly allowed.
    SketchEnvelope.assertCompatible(observed, runtime, prettyName, allowMismatch = true)
  }

  // -- currentMetadata --------------------------------------------------------------------------

  test("currentMetadata for numeric types uses the numeric encoding") {
    val m = SketchEnvelope.currentMetadata(LongType)
    assert(m.getCollationId == SketchEnvelope.NO_COLLATION_ID)
    assert(m.getIcuMajor == 0 && m.getIcuMinor == 0)
    assert(SketchEnvelope.keyEncodingForCollation(m.getCollationId) == KeyEncoding.NUMERIC_OR_BINARY)
  }

  test("currentMetadata for UTF8_BINARY strings uses the raw-bytes encoding") {
    val m = SketchEnvelope.currentMetadata(StringType)
    assert(m.getCollationId == CollationFactory.UTF8_BINARY_COLLATION_ID)
    assert(SketchEnvelope.keyEncodingForCollation(m.getCollationId) == KeyEncoding.RAW_BYTES)
  }

  test("metadata string accessors") {
    assert(SketchEnvelope.icuVersionString(metadata(icuMajor = 75, icuMinor = 1)) == "75.1")
    assert(SketchEnvelope.icuVersionString(metadata(icuMajor = 0, icuMinor = 0)) == null)
    assert(
      SketchEnvelope.datasketchesVersionString(metadata(dsLibMajor = 6, dsLibMinor = 2)) == "6.2")
  }
}
