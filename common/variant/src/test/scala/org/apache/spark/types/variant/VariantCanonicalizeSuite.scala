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

package org.apache.spark.types.variant

import java.util.Arrays

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.SparkRuntimeException

/**
 * Direct unit tests for `VariantBuilder.canonicalize`.
 *
 * The canonical form is the contract that `canonicalize(a)` and `canonicalize(b)` are byte-equal
 * iff `a` and `b` are semantically equal. It currently covers structural canonicalization
 * (metadata dictionary key order, unused-key stripping, object field-id remapping)
 * and value normalization for integers, decimals, float/double, and strings
 * (integer width, integer-promotion, trailing-zero strip, -0.0 -> +0.0, canonical NaN, short-string
 * encoding). The `isCanonical` read-side predicate is complete and checked against `canonicalize`
 * by a soundness oracle over a mixed corpus.
 */
class VariantCanonicalizeSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  private def parse(json: String): Variant =
    VariantBuilder.parseJson(
      json,
      /* allowDuplicateKeys = */ false)

  private def canon(v: Variant): Variant = VariantBuilder.canonicalize(v)

  private def bytesEqual(a: Variant, b: Variant): Boolean =
    Arrays.equals(a.getValue, b.getValue) && Arrays.equals(a.getMetadata, b.getMetadata)

  private def isCanon(v: Variant): Boolean =
    VariantBuilder.isCanonical(v)

  private def buildDouble(d: Double): Variant = {
    val b = new VariantBuilder(false)
    b.appendDouble(d)
    b.result()
  }

  private def buildFloat(f: Float): Variant = {
    val b = new VariantBuilder(false)
    b.appendFloat(f)
    b.result()
  }

  // Copy a variant through the public `appendVariant` (the non-canonical copy path) into a fresh
  // builder, like `buildDouble`/`buildFloat` above. This is the method under test; `canon` is its
  // canonical-mode counterpart.
  private def appendCopy(v: Variant): Variant = {
    val b = new VariantBuilder(false)
    b.appendVariant(v)
    b.result()
  }

  // offset-list integer width stored in an object/array value header at position 0
  private def valueOffsetWidth(v: Variant): Int =
    ((v.getValue()(0) >> VariantUtil.BASIC_TYPE_BITS) & 0x3) + 1

  // dictionary offset width stored in the metadata header byte
  private def metaOffsetWidth(v: Variant): Int =
    ((v.getMetadata()(0) >> 6) & 0x3) + 1

  // object field-id integer width stored in an object value header at position 0
  private def idWidth(v: Variant): Int =
    ((v.getValue()(0) >> (VariantUtil.BASIC_TYPE_BITS + 2)) & 0x3) + 1

  // basic-type bits (0..3) of the scalar value header at position 0
  private def basicType(v: Variant): Int = v.getValue()(0) & VariantUtil.BASIC_TYPE_MASK

  test("object key order does not affect the canonical form") {
    val a = canon(parse("""{"a":1,"b":2}"""))
    val b = canon(parse("""{"b":2,"a":1}"""))
    assert(bytesEqual(a, b), "objects equal up to key order must canonicalize to equal bytes")
  }

  test("nested object key order does not affect the canonical form") {
    val a = canon(parse("""{"outer":{"a":1,"b":2},"z":3}"""))
    val b = canon(parse("""{"z":3,"outer":{"b":2,"a":1}}"""))
    assert(bytesEqual(a, b), "nested object key order must be normalized recursively")
  }

  test("canonical form is independent of the incoming metadata dictionary order") {
    val a = canon(parse("""{"m":{"a":1},"a":{"b":2}}"""))
    val b = canon(parse("""{"a":{"b":2},"m":{"a":1}}"""))
    assert(bytesEqual(a, b), "canonical metadata must not depend on incoming dictionary order")
  }

  test("object key order inside array elements is normalized, array element order is preserved") {
    val a = canon(parse("""[{"a":1,"b":2},{"c":3}]"""))
    val b = canon(parse("""[{"b":2,"a":1},{"c":3}]"""))
    assert(bytesEqual(a, b), "object key order within array elements must be normalized")

    val c = canon(parse("""[1,2]"""))
    val d = canon(parse("""[2,1]"""))
    assert(!bytesEqual(c, d), "array element order is significant and must be preserved")
  }

  test("canonicalize is idempotent") {
    val inputs = Seq(
      """{"b":2,"a":1}""",
      """{"a":1,"b":2}""",
      """{"outer":{"z":1,"a":2},"m":[1,2,3]}""",
      "[1,2,3]",
      "\"hello\"",
      "true",
      "null",
      "1",
      "{}",
      "[]")
    for (json <- inputs) {
      val once = canon(parse(json))
      val twice = canon(once)
      assert(bytesEqual(once, twice), s"canonicalize must be idempotent for input $json")
    }
  }

  test("empty object and empty array canonicalize without error") {
    assert(bytesEqual(canon(parse("{}")), canon(parse("{}"))))
    assert(bytesEqual(canon(parse("[]")), canon(parse("[]"))))
  }

  // ----- Value normalization: DECIMAL -----

  test("integer-valued decimal canonicalizes to the integer encoding") {
    assert(!bytesEqual(parse("1.0"), parse("1")), "1.0 and 1 should differ before canon")
    assert(bytesEqual(canon(parse("1.0")), canon(parse("1"))), "1.0 must canonicalize to 1")
    assert(bytesEqual(canon(parse("1.000")), canon(parse("1"))), "1.000 must canonicalize to 1")
  }

  test("decimal trailing zeros are stripped") {
    assert(!bytesEqual(parse("1.50"), parse("1.5")), "1.50 and 1.5 should differ before canon")
    assert(bytesEqual(canon(parse("1.50")), canon(parse("1.5"))), "1.50 must canonicalize to 1.5")
    assert(bytesEqual(canon(parse("1.500")), canon(parse("1.5"))), "1.500 must canonicalize to 1.5")
  }

  test("decimal normalization applies inside nested objects and arrays") {
    assert(bytesEqual(canon(parse("""{"a":1.0}""")), canon(parse("""{"a":1}"""))))
    assert(bytesEqual(
      canon(parse("""{"a":[1.0, 2.50]}""")),
      canon(parse("""{"a":[1, 2.5]}"""))))
  }

  test("non-integer decimal is not promoted to an integer") {
    assert(!bytesEqual(canon(parse("1.5")), canon(parse("1"))), "1.5 must not collapse to 1")
    assert(!bytesEqual(canon(parse("1.5")), canon(parse("2"))), "1.5 must not collapse to 2")
  }

  test("integer decimal too large for a long is not promoted (stays a decimal)") {
    val big = "100000000000000000000"
    assert(VariantUtil.getType(parse(big).getValue, 0) == VariantUtil.Type.DECIMAL,
      "sanity: 10^20 should parse as a DECIMAL")
    val canonBig = canon(parse(big))
    assert(VariantUtil.getType(canonBig.getValue, 0) == VariantUtil.Type.DECIMAL,
      "10^20 must remain a DECIMAL, not be wrapped into a long")
    val actual = VariantUtil.getDecimal(canonBig.getValue, 0)
    assert(actual.compareTo(new java.math.BigDecimal(big)) == 0, "10^20 value must be preserved")
  }

  test("integer-valued decimal at the Long.MIN/MAX boundary promotes to a minimal long") {
    // A decimal whose integer value is exactly Long.MAX_VALUE / Long.MIN_VALUE must promote to the
    // integer encoding (INT8, the smallest width that holds it); the decimal and the plain-long
    // spellings must converge to identical bytes. The ".0" suffix forces a DECIMAL encoding -- a
    // bare in-range integer literal would already parse as a long.
    val cases = Seq(Long.MaxValue.toString + ".0" -> Long.MaxValue,
      Long.MinValue.toString + ".0" -> Long.MinValue)
    for ((decimalJson, longValue) <- cases) {
      assert(VariantUtil.getType(parse(decimalJson).getValue, 0) == VariantUtil.Type.DECIMAL,
        s"sanity: $decimalJson parses as a DECIMAL")
      assert(!isCanon(parse(decimalJson)),
        s"$decimalJson strips to a promotable long, so it is not canonical as a decimal")
      val promoted = canon(parse(decimalJson))
      assert(VariantUtil.getType(promoted.getValue, 0) == VariantUtil.Type.LONG,
        s"$decimalJson must promote to a long at the boundary")
      assert(VariantUtil.getTypeInfo(promoted.getValue, 0) == VariantUtil.INT8,
        s"$longValue needs the full INT8 width")
      assert(VariantUtil.getLong(promoted.getValue, 0) == longValue,
        s"$decimalJson must preserve the value $longValue")
      assert(isCanon(promoted), "the promoted long must be recognized as canonical")
      assert(bytesEqual(promoted, canon(parse(longValue.toString))),
        s"$decimalJson and $longValue must canonicalize to identical bytes")
    }
  }

  test("integer decimal just outside the long range stays a decimal (no promotion)") {
    // Long.MAX_VALUE + 1 and Long.MIN_VALUE - 1 sit one unit outside the promotable range, so canon
    // must keep them as DECIMAL -- promoting would overflow the long and corrupt the value. Both
    // are 19-digit integers, i.e. DECIMAL16.
    val cases = Seq((BigInt(Long.MaxValue) + 1).toString, (BigInt(Long.MinValue) - 1).toString)
    for (intJson <- cases) {
      val expected = new java.math.BigDecimal(intJson)
      // The bare integer literal is already a canonical DECIMAL: isCanonical must NOT demand a
      // promotion that would not fit a long.
      val bare = parse(intJson)
      assert(VariantUtil.getType(bare.getValue, 0) == VariantUtil.Type.DECIMAL,
        s"sanity: $intJson parses as a DECIMAL")
      assert(isCanon(bare), s"$intJson is already canonical as an out-of-long-range decimal")
      // A non-canonical (trailing-zero) spelling canonicalizes back to that decimal, not a long.
      val canonicalized = canon(parse(intJson + ".0"))
      assert(VariantUtil.getType(canonicalized.getValue, 0) == VariantUtil.Type.DECIMAL,
        s"$intJson must remain a DECIMAL, not be wrapped into a long")
      assert(VariantUtil.getDecimal(canonicalized.getValue, 0).compareTo(expected) == 0,
        s"$intJson value must be preserved")
      assert(bytesEqual(canonicalized, bare),
        s"the trailing-zero and bare spellings of $intJson must converge")
    }
  }

  test("canonicalize and isCanonical handle the DECIMAL8 and DECIMAL16 branches") {
    // 12 significant digits -> DECIMAL8.
    val nonCanon8 = parse("1234567.890120")
    assert(!isCanon(nonCanon8), "trailing-zero DECIMAL8 is not canonical")
    val d8 = canon(nonCanon8)
    assert(VariantUtil.getTypeInfo(d8.getValue, 0) == VariantUtil.DECIMAL8, "emits DECIMAL8")
    assert(isCanon(d8), "canon output is a canonical DECIMAL8")
    val expected8 = new java.math.BigDecimal("1234567.89012")
    assert(VariantUtil.getDecimal(d8.getValue, 0).compareTo(expected8) == 0, "value preserved")

    // 19 significant digits -> DECIMAL16.
    val nonCanon16 = parse("1234567890.1234567890")
    assert(!isCanon(nonCanon16), "trailing-zero DECIMAL16 is not canonical")
    val d16 = canon(nonCanon16)
    assert(VariantUtil.getTypeInfo(d16.getValue, 0) == VariantUtil.DECIMAL16, "emits DECIMAL16")
    assert(isCanon(d16), "canon output is a canonical DECIMAL16")
    val expected16 = new java.math.BigDecimal("1234567890.123456789")
    assert(VariantUtil.getDecimal(d16.getValue, 0).compareTo(expected16) == 0, "value preserved")
  }

  private def assertDecimalReduces(
      wide: Variant, stored: Int, minimal: Int, value: String): Unit = {
    assert(VariantUtil.getTypeInfo(wide.getValue, 0) == stored, "stored in the wide type")
    assert(!isCanon(wide), "an over-wide decimal is not canonical")
    val reduced = canon(wide)
    assert(VariantUtil.getTypeInfo(reduced.getValue, 0) == minimal, "reduced to minimal")
    val actual = VariantUtil.getDecimal(reduced.getValue, 0)
    assert(actual.compareTo(new java.math.BigDecimal(value)) == 0, "value preserved")
  }

  test("canonicalize reduces an over-wide decimal to the minimal type") {
    // 11 stored digits -> DECIMAL8, reduces to DECIMAL4.
    assertDecimalReduces(
      parse("123.45600000"), VariantUtil.DECIMAL8, VariantUtil.DECIMAL4, "123.456")
    // 19 stored digits -> DECIMAL16, reduces to DECIMAL4.
    assertDecimalReduces(
      parse("1.500000000000000000"), VariantUtil.DECIMAL16, VariantUtil.DECIMAL4, "1.5")
    // 19 stored digits -> DECIMAL16, reduces to DECIMAL8.
    assertDecimalReduces(
      parse("1234567.890120000000"), VariantUtil.DECIMAL16, VariantUtil.DECIMAL8, "1234567.89012")
  }

  test("isCanonical rejects a decimal stored in a wider type than its value needs") {
    def wideDecimal(wideType: Int, scale: Int, unscaled: Long): Variant = {
      val header = ((wideType << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
      val width = if (wideType == VariantUtil.DECIMAL8) 8 else 16
      val buf = java.nio.ByteBuffer.allocate(2 + width).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      buf.put(header).put(scale.toByte).putLong(unscaled)
      if (width == 16) buf.putLong(if (unscaled < 0) -1L else 0L) // sign-extend to 16 bytes
      new Variant(buf.array(), parse("1").getMetadata)
    }

    // DECIMAL8 holding 1.5 -> reduces to DECIMAL4.
    assertDecimalReduces(
      wideDecimal(VariantUtil.DECIMAL8, 1, 15L),
      VariantUtil.DECIMAL8, VariantUtil.DECIMAL4, "1.5")
    // DECIMAL16 holding 1.5 -> reduces to DECIMAL4.
    assertDecimalReduces(
      wideDecimal(VariantUtil.DECIMAL16, 1, 15L),
      VariantUtil.DECIMAL16, VariantUtil.DECIMAL4, "1.5")
    // DECIMAL16 holding 1234567.89012 -> reduces to DECIMAL8.
    assertDecimalReduces(
      wideDecimal(VariantUtil.DECIMAL16, 5, 123456789012L),
      VariantUtil.DECIMAL16, VariantUtil.DECIMAL8, "1234567.89012")
  }

  // ----- Value normalization: integer width -----

  test("non-minimal integer width is reduced to the smallest") {
    val int8Header =
      ((VariantUtil.INT8 << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val int8One =
      new Variant(Array[Byte](int8Header, 1, 0, 0, 0, 0, 0, 0, 0), parse("1").getMetadata)
    assert(!bytesEqual(int8One, parse("1")), "sanity: INT8(1) and INT1(1) differ before canon")
    assert(bytesEqual(canon(int8One), canon(parse("1"))), "INT8(1) must reduce to INT1(1)")
  }

  // ----- Value normalization: float / double -----

  test("negative zero canonicalizes to positive zero") {
    assert(!bytesEqual(buildDouble(-0.0d), buildDouble(0.0d)), "sanity: -0.0d and +0.0d differ")
    assert(bytesEqual(canon(buildDouble(-0.0d)), canon(buildDouble(0.0d))), "double -0.0 -> +0.0")
    assert(!bytesEqual(buildFloat(-0.0f), buildFloat(0.0f)), "sanity: -0.0f and +0.0f differ")
    assert(bytesEqual(canon(buildFloat(-0.0f)), canon(buildFloat(0.0f))), "float -0.0 -> +0.0")
  }

  test("non-canonical NaN canonicalizes to the canonical NaN") {
    val doubleHeader =
      ((VariantUtil.DOUBLE << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val bytes = java.nio.ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(doubleHeader).putLong(0x7ff8000000000001L).array()
    val nonCanonicalNaN = new Variant(bytes, parse("1").getMetadata)
    assert(!bytesEqual(nonCanonicalNaN, buildDouble(Double.NaN)), "sanity: NaN encodings differ")
    assert(bytesEqual(canon(nonCanonicalNaN), canon(buildDouble(Double.NaN))),
      "all NaN bit patterns must canonicalize to the same bytes")
  }

  // ----- Value normalization: string encoding -----

  test("a short string stored as long_str is re-encoded as a short string") {
    val longStrHeader =
      ((VariantUtil.LONG_STR << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val text = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val bytes = java.nio.ByteBuffer.allocate(1 + 4 + text.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(longStrHeader).putInt(text.length).put(text).array()
    val longEncoded = new Variant(bytes, parse("1").getMetadata)
    assert(!bytesEqual(longEncoded, parse("\"hi\"")), "sanity: long_str and short_str 'hi' differ")
    assert(bytesEqual(canon(longEncoded), parse("\"hi\"")), "long_str 'hi' -> short_str")
  }

  test("short-string boundary: the SHORT_STR/LONG_STR cut is at 63/64 UTF-8 bytes") {
    val utf8 = java.nio.charset.StandardCharsets.UTF_8

    // 63 bytes -> SHORT_STR on parse, and canonical. The same 63 bytes stored as a LONG_STR is
    // not canonical and canonicalizes (via a real rebuild) back to the short form.
    val s63 = "x" * VariantUtil.MAX_SHORT_STR_SIZE // 63 ASCII bytes
    assert(s63.getBytes(utf8).length == 63, "sanity: 63 bytes")
    val short63 = parse("\"" + s63 + "\"")
    assert(basicType(short63) == VariantUtil.SHORT_STR, "a 63-byte string short-encodes")
    assert(isCanon(short63), "a 63-byte short string is canonical")
    val p63 = s63.getBytes(utf8)
    val longStrHeader =
      ((VariantUtil.LONG_STR << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val long63 = new Variant(
      java.nio.ByteBuffer.allocate(1 + 4 + p63.length).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(longStrHeader).putInt(p63.length).put(p63).array(),
      parse("1").getMetadata)
    assert(!isCanon(long63), "a 63-byte string stored as long_str is not canonical")
    assert(bytesEqual(canon(long63), short63), "63-byte long_str canonicalizes to short")

    // 64 bytes cannot be short-encoded (max short payload is 63), so parse long-encodes it. That
    // long encoding is canonical, and the rebuild re-emits it as long_str unchanged.
    val s64 = "x" * (VariantUtil.MAX_SHORT_STR_SIZE + 1) // 64 ASCII bytes
    assert(s64.getBytes(utf8).length == 64, "sanity: 64 bytes")
    val long64 = parse("\"" + s64 + "\"")
    assert(basicType(long64) == VariantUtil.PRIMITIVE &&
      VariantUtil.getTypeInfo(long64.getValue, 0) == VariantUtil.LONG_STR,
      "a 64-byte string long-encodes")
    assert(isCanon(long64), "a 64-byte long string is canonical (cannot be short-encoded)")
    assert(bytesEqual(VariantBuilder.doCanonicalize(long64), long64),
      "the rebuild re-emits a 64-byte string as long_str (writer honors the 63/64 cut)")

    // The cut is on UTF-8 BYTES, not Java char count: two 32-char strings fall on opposite sides.
    val twoByte = new String(Character.toChars(0x00E9)) // U+00E9, 2 UTF-8 bytes
    val cp63 = twoByte * 31 + "x" // 31*2 + 1 = 63 bytes, 32 chars
    val cp64 = twoByte * 32       // 32*2     = 64 bytes, 32 chars
    assert(cp63.getBytes(utf8).length == 63, "sanity: cp63 is 63 bytes")
    assert(cp64.getBytes(utf8).length == 64, "sanity: cp64 is 64 bytes")
    assert(basicType(parse("\"" + cp63 + "\"")) == VariantUtil.SHORT_STR,
      "63 UTF-8 bytes short-encode even though the string is 32 chars")
    val cp64Variant = parse("\"" + cp64 + "\"")
    assert(basicType(cp64Variant) == VariantUtil.PRIMITIVE &&
      VariantUtil.getTypeInfo(cp64Variant.getValue, 0) == VariantUtil.LONG_STR,
      "64 UTF-8 bytes long-encode (same char count, one more byte)")
  }

  // ----- isCanonical: metadata dictionary -----

  test("isCanonical accepts a sorted metadata dictionary and rejects an unsorted one") {
    assert(isCanon(parse("""{"a":1,"b":2}""")), "ascending dictionary is canonical")
    assert(!isCanon(parse("""{"b":2,"a":1}""")), "descending dictionary is not canonical")
    assert(!isCanon(parse("""{"z":1,"a":2,"m":3}""")), "unsorted dictionary is not canonical")
    assert(isCanon(canon(parse("""{"b":2,"a":1}"""))), "canon output has a sorted dictionary")
  }

  test("isCanonical accepts an empty dictionary") {
    assert(isCanon(parse("1")), "a scalar's empty dictionary is canonical")
    assert(isCanon(parse("[1,2,3]")), "an array of scalars has an empty dictionary")
  }

  test("isCanonical rejects a non-minimal metadata offset width") {
    val emptyMeta = parse("1").getMetadata
    val version = emptyMeta(0)
    val header2 = (version | (1 << 6)).toByte // offset width = 2 bytes
    val meta = java.nio.ByteBuffer.allocate(1 + 2 + 2 + 2 + 1)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header2)
      .put(1.toByte).put(0.toByte) // numKeys = 1
      .put(0.toByte).put(0.toByte) // offset[0] = 0
      .put(1.toByte).put(0.toByte) // offset[1] = 1 (one key byte)
      .put('a'.toByte) // key "a"
      .array()
    val referencingValue = parse("""{"a":1}""").getValue
    assert(isCanon(parse("""{"a":1}""")), "sanity: a minimal-width dict is canonical")
    assert(!VariantBuilder.isCanonical(new Variant(referencingValue, meta)),
      "a 2-byte offset width where 1 byte fits is not canonical")
  }

  test("isCanonical rejects a dictionary whose first offset is not 0") {
    val canonical = parse("""{"a":1}""")
    val srcMeta = canonical.getMetadata
    val header = srcMeta(0) // VERSION byte, offset width 1
    val badMeta = java.nio.ByteBuffer.allocate(1 + 1 + 2 + 2)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .put(1.toByte)   // numKeys = 1
      .put(1.toByte)   // offset[0] = 1  (canonical would be 0)
      .put(2.toByte)   // offset[1] = 2
      .put('x'.toByte) // padding byte at string-region[0]
      .put('a'.toByte) // key "a" at string-region[1]
      .array()
    assert(isCanon(canonical), "sanity: the object is canonical")
    assert(!VariantBuilder.isCanonical(new Variant(canonical.getValue, badMeta)),
      "a dictionary with offset[0] != 0 is not canonical")
  }

  test("isCanonical rejects metadata with trailing bytes") {
    val canonical = parse("""{"a":1}""")
    val meta = canonical.getMetadata
    val trailing = Arrays.copyOf(meta, meta.length + 1) // canonical metadata + one stray 0 byte
    assert(isCanon(canonical), "sanity: the object is canonical")
    val nonCanonical = new Variant(canonical.getValue, trailing)
    assert(!VariantBuilder.isCanonical(nonCanonical),
      "metadata with a trailing byte is not canonical")
  }

  test("isCanonical rejects a value with trailing bytes") {
    val canonical = parse("1")
    val value = canonical.getValue
    val trailing = Arrays.copyOf(value, value.length + 1) // value + one stray 0 byte
    assert(isCanon(canonical), "sanity: 1 is canonical")
    val nonCanonical = new Variant(trailing, canonical.getMetadata)
    assert(!VariantBuilder.isCanonical(nonCanonical),
      "a value with a trailing byte is not canonical")
  }

  // ----- isCanonical: object / array structure -----

  test("isCanonical rejects a dictionary with an unused key") {
    val b = new VariantBuilder(false)
    b.addKey("unused")
    b.appendLong(1)
    val withUnusedKey = b.result()
    assert(!VariantBuilder.isCanonical(withUnusedKey),
      "a dictionary with an unreferenced key is not canonical")
  }

  test("isCanonical rejects an object with duplicate field ids") {
    val a = parse("1").getValue // INT1(1)
    val b = parse("2").getValue // INT1(2)
    val header = VariantUtil.objectHeader(false, 1, 1) // 1-byte size, idSize 1, offsetSize 1
    val value = java.nio.ByteBuffer.allocate(1 + 1 + 2 + 3 + a.length + b.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .put(2.toByte)                           // size = 2 fields
      .put(0.toByte).put(0.toByte)             // id list [0, 0] -- both key "a" (duplicate)
      .put(0.toByte).put(a.length.toByte).put((a.length + b.length).toByte) // offsets [0, 2, 4]
      .put(a).put(b)
      .array()
    val dup = new Variant(value, parse("""{"a":1}""").getMetadata)
    assert(!isCanon(dup), "an object with duplicate field ids is not canonical")
  }

  test("isCanonical accepts already-canonical nested objects and arrays") {
    assert(isCanon(parse("""{"a":{"b":1}}""")), "a canonical nested object is accepted")
    assert(isCanon(parse("""{"a":[1,2,3]}""")), "a canonical object-of-array is accepted")
    assert(isCanon(parse("[1,2]")), "a scalar array is accepted")
    assert(isCanon(parse("{}")), "an empty object is accepted")
    assert(isCanon(parse("[]")), "an empty array is accepted")
  }

  test("isCanonical accepts canonicalize's output for nested structures") {
    val inputs = Seq(
      """{"b":2,"a":1}""",
      """{"z":{"b":1,"a":2},"m":3}""",
      "[1,2,3]",
      """{"a":[{"y":1,"x":2}],"b":{"d":4,"c":5}}""")
    for (json <- inputs) {
      assert(isCanon(canon(parse(json))), s"canon output must be structurally canonical: $json")
    }
  }

  test("isCanonical rejects a non-minimal array offset width") {
    val elem = parse("1").getValue // INT1(1), a 2-byte canonical scalar, reused as the element
    val arrayHeader = ((0 << (VariantUtil.BASIC_TYPE_BITS + 2)) |
      ((2 - 1) << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.ARRAY).toByte // 2-byte offset width
    val arr = java.nio.ByteBuffer.allocate(1 + 1 + 2 + 2 + elem.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(arrayHeader)
      .put(1.toByte) // size = 1
      .put(0.toByte).put(0.toByte) // offset[0] = 0
      .put(elem.length.toByte).put(0.toByte) // offset[1] = data size
      .put(elem) // element: INT1(1)
      .array()
    val nonMinimalArray = new Variant(arr, parse("1").getMetadata)
    assert(isCanon(parse("[1]")), "sanity: a minimal-width array is canonical")
    assert(!VariantBuilder.isCanonical(nonMinimalArray),
      "an array with a 2-byte offset width where 1 byte fits is not canonical")
  }

  test("isCanonical rejects an object whose size field is stored wide") {
    val elem = parse("1").getValue // INT1(1), the field value
    // largeSize = true forces a 4-byte size field where 1 byte fits (idSize = offsetSize = 1).
    val header = VariantUtil.objectHeader(true, 1, 1)
    val obj = java.nio.ByteBuffer.allocate(
        1 + // header byte
        4 + // size field (forced wide: 4 bytes)
        1 + // id list: size(1) * idSize(1)
        2 + // offset list: (size + 1) * offsetSize(1)
        elem.length) // field data: INT1(1)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .putInt(1) // size = 1, stored in 4 bytes (canonical uses 1)
      .put(0.toByte) // field id list: [0] -> key "a"
      .put(0.toByte) // offset[0] = 0
      .put(elem.length.toByte) // offset[1] = data size
      .put(elem) // field value: INT1(1)
      .array()
    val wideSizeObject = new Variant(obj, parse("""{"a":1}""").getMetadata)
    assert(isCanon(parse("""{"a":1}""")), "sanity: a minimal-width object is canonical")
    assert(!VariantBuilder.isCanonical(wideSizeObject),
      "an object with a 4-byte size field where 1 byte fits is not canonical")
  }

  test("isCanonical rejects an array whose size field is stored wide") {
    val elem = parse("1").getValue // INT1(1), a 2-byte canonical scalar
    // largeSize = true forces a 4-byte size field where 1 byte fits (offsetSize = 1).
    val header = VariantUtil.arrayHeader(true, 1)
    val arr = java.nio.ByteBuffer.allocate(
        1 + // header byte
        4 + // size field (forced wide: 4 bytes)
        2 + // offset list: (size + 1) * offsetSize(1)
        elem.length) // element data: INT1(1)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .putInt(1) // size = 1, stored in 4 bytes (canonical uses 1)
      .put(0.toByte) // offset[0] = 0
      .put(elem.length.toByte) // offset[1] = data size
      .put(elem) // element: INT1(1)
      .array()
    val wideSizeArray = new Variant(arr, parse("1").getMetadata)
    assert(isCanon(parse("[1]")), "sanity: a minimal-width array is canonical")
    assert(!VariantBuilder.isCanonical(wideSizeArray),
      "an array with a 4-byte size field where 1 byte fits is not canonical")
  }

  test("isCanonical rejects reserved container header bits and canonicalize clears them") {
    // The object/array header byte has reserved type-info bits that the parser ignores:
    // the object's top bit (0x80) and the array's top three bits (0xE0). A foreign writer can
    // set them on an otherwise-canonical container, so two containers differing only in reserved
    // bits would both pass a width-only fast path and split hash-agg buckets. isCanonical must
    // reject them, and canonicalize (which always emits reserved = 0) must clear them.

    // OBJECT: flip on the single reserved bit (0x80) of a canonical {"a":1}.
    val canonicalObj = parse("""{"a":1}""")
    assert(isCanon(canonicalObj), "sanity: {\"a\":1} is canonical")
    val objValue = canonicalObj.getValue.clone()
    objValue(0) = (objValue(0) | 0x80).toByte
    val reservedObj = new Variant(objValue, canonicalObj.getMetadata)
    assert(VariantUtil.isValidVariant(objValue, canonicalObj.getMetadata),
      "sanity: a reserved bit keeps the object structurally valid")
    assert(!bytesEqual(reservedObj, canonicalObj), "sanity: the reserved bit changed the bytes")
    assert(!isCanon(reservedObj), "an object with a reserved header bit set is not canonical")
    assert(bytesEqual(canon(reservedObj), canonicalObj),
      "canonicalize must clear the object's reserved bit")
    assert(isCanon(canon(reservedObj)), "the cleared canonical form is canonical")

    // ARRAY: flip on all three reserved bits (0xE0) of a canonical [1,2].
    val canonicalArr = parse("[1,2]")
    assert(isCanon(canonicalArr), "sanity: [1,2] is canonical")
    val arrValue = canonicalArr.getValue.clone()
    arrValue(0) = (arrValue(0) | 0xE0).toByte
    val reservedArr = new Variant(arrValue, canonicalArr.getMetadata)
    assert(VariantUtil.isValidVariant(arrValue, canonicalArr.getMetadata),
      "sanity: reserved bits keep the array structurally valid")
    assert(!bytesEqual(reservedArr, canonicalArr), "sanity: the reserved bits changed the bytes")
    assert(!isCanon(reservedArr), "an array with reserved header bits set is not canonical")
    assert(bytesEqual(canon(reservedArr), canonicalArr),
      "canonicalize must clear the array's reserved bits")
    assert(isCanon(canon(reservedArr)), "the cleared canonical form is canonical")
  }

  // ----- isCanonical + canonicalize: 2/3-byte offset and id widths -----

  test("canonicalize and isCanonical handle 2-byte and 3-byte array offset widths") {
    // ~200 small ints exceed 255 B of element data -> a 2-byte offset list.
    val twoByte = canon(parse("[" + (0 until 200).mkString(",") + "]"))
    assert(valueOffsetWidth(twoByte) == 2, "array data > 255 B -> 2-byte offset width")
    assert(isCanon(twoByte), "the 2-byte-wide array is canonical")

    // A single ~70 KB string element pushes data past 65535 B -> a 3-byte offset list.
    val threeByte = canon(parse("[\"" + ("x" * 70000) + "\"]"))
    assert(valueOffsetWidth(threeByte) == 3, "array data > 65535 B -> 3-byte offset width")
    assert(isCanon(threeByte), "the 3-byte-wide array is canonical")
  }

  test("canonicalize and isCanonical handle 2-byte and 3-byte object offset widths") {
    // {"a": <~470 B array>}: object data > 255 B -> 2-byte offset width
    val twoByte = canon(parse("{\"a\":[" + (0 until 200).mkString(",") + "]}"))
    assert(valueOffsetWidth(twoByte) == 2, "object data > 255 B -> 2-byte offset width")
    assert(isCanon(twoByte), "the 2-byte-wide object is canonical")

    // {"a": <~70 KB string>} -> object data > 65535 B -> a 3-byte offset width.
    val threeByte = canon(parse("{\"a\":\"" + ("x" * 70000) + "\"}"))
    assert(valueOffsetWidth(threeByte) == 3, "object data > 65535 B -> 3-byte offset width")
    assert(isCanon(threeByte), "the 3-byte-wide object is canonical")
  }

  test("object id width crosses from 1 to 2 bytes at 257 keys") {
    // id width = minIntWidth(maxId); maxId = numKeys-1 -> width 1 up to 256 keys, 2 at 257.
    def objectWithKeys(n: Int): Variant =
      canon(parse("{" + (0 until n).map(i => "\"k%03d\":%d".format(i, i)).mkString(",") + "}"))
    val at256 = objectWithKeys(256) // maxId 255
    val at257 = objectWithKeys(257) // maxId 256
    assert(idWidth(at256) == 1, "256 keys (maxId 255) -> 1-byte id width")
    assert(idWidth(at257) == 2, "257 keys (maxId 256) -> 2-byte id width")
    assert(isCanon(at256), "the 256-key object is canonical")
    assert(isCanon(at257), "the 257-key object is canonical")
  }

  test("canonicalize and isCanonical handle 2-byte and 3-byte metadata offset widths") {
    // 50 seven-char keys make the dict key-region > 255 B -> a 2-byte dict offset width.
    val manyKeys = (0 until 50).map(i => "\"field%02d\":%d".format(i, i)).mkString(",")
    val twoByte = canon(parse("{" + manyKeys + "}"))
    assert(metaOffsetWidth(twoByte) == 2, "keys > 255 B -> 2-byte dict offset width")
    assert(isCanon(twoByte), "the 2-byte-wide dictionary is canonical")

    // Two ~40 KB keys make the key-region > 65535 B -> a 3-byte dict offset width.
    // (A single key > 50000 chars is rejected by Jackson's max-name-length limit.)
    val bigKeys = "\"" + ("a" * 40000) + "\":1,\"" + ("b" * 40000) + "\":2"
    val threeByte = canon(parse("{" + bigKeys + "}"))
    assert(metaOffsetWidth(threeByte) == 3, "keys > 65535 B -> 3-byte dict offset width")
    assert(isCanon(threeByte), "the 3-byte-wide dictionary is canonical")
  }

  // ----- isCanonical: scalar values -----

  test("isCanonical rejects a non-minimally-encoded integer") {
    val int8Header =
      ((VariantUtil.INT8 << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val int8One =
      new Variant(Array[Byte](int8Header, 1, 0, 0, 0, 0, 0, 0, 0), parse("1").getMetadata)
    assert(!VariantBuilder.isCanonical(int8One),
      "INT8(1) is not minimal-width")
    assert(isCanon(parse("1")), "INT1(1) is canonical")
  }

  test("isCanonical rejects integer-valued and trailing-zero decimals") {
    assert(!isCanon(parse("1.0")), "1.0 canonicalizes to the integer 1")
    assert(!isCanon(parse("1.50")), "1.50 has a trailing zero")
    assert(isCanon(parse("1.5")), "1.5 is a minimal fractional decimal")
  }

  test("isCanonical rejects -0.0 and non-canonical NaN") {
    assert(!isCanon(buildFloat(-0.0f)), "float -0.0 is not canonical")
    assert(!isCanon(buildDouble(-0.0d)), "double -0.0 is not canonical")
    assert(isCanon(buildFloat(0.0f)), "float +0.0 is canonical")
    assert(isCanon(buildDouble(1.5d)), "an ordinary double is canonical")
    assert(isCanon(buildDouble(Double.NaN)), "the canonical double NaN is canonical")

    // Hand-craft a double NaN whose mantissa differs from the canonical 0x7ff8000000000000.
    val doubleHeader =
      ((VariantUtil.DOUBLE << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val nanBytes = java.nio.ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(doubleHeader).putLong(0x7ff8000000000001L).array()
    val nonCanonicalNaN = new Variant(nanBytes, parse("1").getMetadata)
    assert(!VariantBuilder.isCanonical(nonCanonicalNaN),
      "a non-canonical NaN bit pattern is not canonical")
  }

  test("isCanonical rejects a short string stored as long_str") {
    val longStrHeader =
      ((VariantUtil.LONG_STR << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val text = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val bytes = java.nio.ByteBuffer.allocate(1 + 4 + text.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(longStrHeader).putInt(text.length).put(text).array()
    val longEncoded = new Variant(bytes, parse("1").getMetadata)
    assert(!VariantBuilder.isCanonical(longEncoded),
      "a short string stored as long_str is not canonical")
    assert(isCanon(parse("\"hi\"")), "a short-encoded string is canonical")
    // A genuinely long string (> MAX_SHORT_STR_SIZE bytes) is canonical as long_str.
    val bigString = "\"" + ("x" * 70) + "\""
    assert(isCanon(parse(bigString)), "a >63-byte string is canonical as long_str")
  }

  // ----- NaN and pass-through type coverage -----

  test("canonicalize and isCanonical handle a non-canonical float NaN") {
    val floatHeader =
      ((VariantUtil.FLOAT << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val bytes = java.nio.ByteBuffer.allocate(1 + 4).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(floatHeader).putInt(0x7fc00001).array()
    val nonCanonicalNaN = new Variant(bytes, parse("1").getMetadata)
    assert(!bytesEqual(nonCanonicalNaN, buildFloat(Float.NaN)), "sanity: NaN encodings differ")
    assert(!VariantBuilder.isCanonical(nonCanonicalNaN),
      "a non-canonical float NaN is not canonical")
    assert(bytesEqual(canon(nonCanonicalNaN), canon(buildFloat(Float.NaN))),
      "all float NaN bit patterns canonicalize to the same bytes")
    assert(isCanon(buildFloat(Float.NaN)), "the canonical float NaN is canonical")
  }

  test("canonicalize passes through date, timestamp, binary, and uuid unchanged") {
    def build(f: VariantBuilder => Unit): Variant = {
      val b = new VariantBuilder(false)
      f(b)
      b.result()
    }
    val samples = Seq(
      build(_.appendDate(19000)),
      build(_.appendTimestamp(1234567890123L)),
      build(_.appendTimestampNtz(1234567890123L)),
      build(_.appendBinary(Array[Byte](1, 2, 3, 4))),
      build(_.appendUuid(new java.util.UUID(1L, 2L))))
    for (v <- samples) {
      assert(bytesEqual(v, VariantBuilder.doCanonicalize(v)),
        "a pass-through scalar must be unchanged by the rebuild")
      assert(isCanon(v), "a pass-through scalar must be recognized as canonical")
    }
  }

  // ----- isCanonical soundness oracle -----

  test("isCanonical soundness oracle: a true result guarantees canonicalize is a no-op") {
    // The crown-jewel invariant. For every sample:
    //   (soundness) isCanonical(v) == true  =>  the full canonicalization (doCanonicalize, no fast
    //     path) is byte-identical to v. A false positive would let a non-canonical Variant through
    //     and silently split hash-agg buckets.
    //   (completeness) canonicalize(v) is always recognized as canonical (writer and recognizer
    //     agree).
    val parsed = Seq(
      "0", "1", "-1", "127", "128", "-128", "100000",
      "1.0", "1.5", "1.50", "1.000", "0.0",
      "100000000000000000000", "-100000000000000000000",
      "true", "false", "null",
      "\"\"", "\"hi\"", "\"" + ("x" * 70) + "\"",
      "{}", "[]", "[1,2,3]", "[3,2,1]", "[1,[2,[3]]]",
      """{"a":1,"b":2}""", """{"b":2,"a":1}""", """{"a":1.0,"b":1.50}""",
      """{"z":{"b":1,"a":2},"m":[1,2,3]}""",
      """{"a":[{"y":1,"x":2}],"b":{"d":4,"c":5}}""").map(parse)

    // Hand-crafted non-canonical encodings that parse_json never produces.
    val emptyMeta = parse("1").getMetadata
    def prim(t: Int): Byte = ((t << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val int8One = new Variant(
      Array[Byte](prim(VariantUtil.INT8), 1, 0, 0, 0, 0, 0, 0, 0), emptyMeta)
    val nonCanonicalNaN = new Variant(
      java.nio.ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.DOUBLE)).putLong(0x7ff8000000000001L).array(), emptyMeta)
    val nonCanonicalFloatNaN = new Variant(
      java.nio.ByteBuffer.allocate(5).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.FLOAT)).putInt(0x7fc00001).array(), emptyMeta)
    val hi = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val longStrHi = new Variant(
      java.nio.ByteBuffer.allocate(1 + 4 + hi.length).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.LONG_STR)).putInt(hi.length).put(hi).array(), emptyMeta)
    val unusedKey = {
      val b = new VariantBuilder(false)
      b.addKey("unused")
      b.appendLong(1)
      b.result()
    }
    val handCrafted =
      Seq(int8One, buildFloat(-0.0f), buildDouble(-0.0d), nonCanonicalNaN, nonCanonicalFloatNaN,
        longStrHi, unusedKey)

    for (v <- parsed ++ handCrafted) {
      // Soundness: a true isCanonical(v) must mean the FULL canonicalization is a no-op. canon(v)
      // short-circuits on isCanonical, which would make this vacuous (comparing v against itself),
      // so compare against doCanonicalize -- the rebuild with no fast path.
      if (isCanon(v)) {
        assert(bytesEqual(v, VariantBuilder.doCanonicalize(v)),
          "SOUNDNESS VIOLATION: isCanonical was true but the full rebuild changed the bytes")
      }
      // Completeness: canonicalize's output is always recognized as canonical (writer and
      // recognizer agree).
      assert(isCanon(canon(v)),
        "canonicalize output must be recognized as canonical")
    }
  }

  // ----- canonicalize: sub-variant (pos != 0) -----

  test("canonicalize treats a sub-variant (pos != 0) as a standalone value") {
    val parent = parse("""{"outer":{"b":2,"a":1},"z":3}""")
    val sub = parent.getFieldByKey("outer") // the object {"b":2,"a":1} as a pos != 0 view
    assert(sub.getType == VariantUtil.Type.OBJECT, "sanity: sub is the nested object")
    assert(bytesEqual(canon(sub), canon(parse("""{"a":1,"b":2}"""))),
      "a sub-variant canonicalizes to the standalone canonical form of that element")
    // It must NOT canonicalize the whole parent root instead.
    assert(!bytesEqual(canon(sub), canon(parent)),
      "canonicalizing a sub-variant must not canonicalize the whole parent")
    // A doubly-nested array-element sub-variant works too.
    val elem = parse("""{"arr":[{"y":2,"x":1}]}""").getFieldByKey("arr").getElementAtIndex(0)
    assert(bytesEqual(canon(elem), canon(parse("""{"x":1,"y":2}"""))),
      "a nested array-element sub-variant canonicalizes standalone")
  }

  // ----- non-ASCII (UTF-8 vs UTF-16) key ordering -----

  test("object key ordering uses UTF-8 byte order, not Java String (UTF-16) order") {
    // U+E000 is a single UTF-16 code unit (0xE000); U+1F600 is a surrogate pair (0xD83D 0xDE00).
    // Java String (UTF-16) order puts high surrogate 0xD83D before 0xE000 (k2 < k1), whereas
    // UTF-8 / code-point order has U+E000 < U+1F600 (k1 < k2). canonicalize must sort by UTF-8
    // (the order finishWritingObject emits fields and getFieldByKey binary-searches) so field ids
    // come out ascending and isCanonical accepts the result; a UTF-16 sort would break both.
    val k1 = new String(Character.toChars(0xE000))
    val k2 = new String(Character.toChars(0x1F600))
    val a = canon(parse(s"""{"$k1":1,"$k2":2}"""))
    val b = canon(parse(s"""{"$k2":2,"$k1":1}"""))
    assert(bytesEqual(a, b), "canonical form must not depend on incoming non-ASCII key order")
    assert(isCanon(a), "canon output with non-ASCII keys must be recognized as canonical")
    // Fields must stay retrievable, i.e. laid out in the UTF-8 order getFieldByKey binary-searches.
    assert(bytesEqual(canon(a.getFieldByKey(k1)), canon(parse("1"))), "k1 -> 1")
    assert(bytesEqual(canon(a.getFieldByKey(k2)), canon(parse("2"))), "k2 -> 2")
  }

  test("isCanonical uses UTF-8, not UTF-16, order for non-ASCII dictionary keys") {
    val k1 = new String(Character.toChars(0xE000))
    val k2 = new String(Character.toChars(0x1F600))
    // [k1, k2] is ascending in UTF-8 (U+E000 < U+1F600), so it is canonical.
    assert(isCanon(parse(s"""{"$k1":1,"$k2":2}""")), "UTF-8-ascending non-ASCII dictionary")
    // [k2, k1] is ascending in UTF-16 (0xD83D < 0xE000) but DESCENDING in UTF-8. A UTF-16 check
    // would wrongly accept it; the UTF-8 check must reject it (canon re-sorts it to [k1, k2]).
    assert(!isCanon(parse(s"""{"$k2":2,"$k1":1}""")),
      "dictionary ordered by UTF-16 rather than UTF-8 must be rejected")
  }

  test("object stored in legacy UTF-16 field order canonicalizes to the modern UTF-8 bytes") {
    // U+E000 and U+1F600 sort oppositely in UTF-8 (k1 < k2) vs UTF-16 (k2 < k1). Older Spark
    // stored object fields in UTF-16 order, so {k1:1, k2:2} can arrive physically laid out as
    // [k2, k1]. finishWritingObject reorders the id/offset lists but not the field data, so canon
    // must lay the data out in UTF-8 key order; this checks it converges with the modern encoding.
    val k1 = new String(Character.toChars(0xE000))
    val k2 = new String(Character.toChars(0x1F600))
    val modern = parse(s"""{"$k1":1,"$k2":2}""") // parse stores fields UTF-8-sorted: [k1, k2]
    val meta = modern.getMetadata
    val idk1 = if (VariantUtil.getMetadataKey(meta, 0) == k1) 0 else 1
    val idk2 = 1 - idk1
    val v1 = parse("1").getValue // k1's value, INT1(1)
    val v2 = parse("2").getValue // k2's value, INT1(2)
    // Hand-build the object with fields physically stored in UTF-16 order [k2, k1].
    val header = VariantUtil.objectHeader(false, 1, 1) // 1-byte size, idSize 1, offsetSize 1
    val legacyValue = java.nio.ByteBuffer.allocate(1 + 1 + 2 + 3 + v1.length + v2.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .put(2.toByte) // size = 2 fields
      .put(idk2.toByte).put(idk1.toByte) // id list [k2, k1] (UTF-16 order)
      .put(0.toByte).put(v2.length.toByte).put((v2.length + v1.length).toByte) // offsets [0, 2, 4]
      .put(v2).put(v1) // data stored as [k2's value, k1's value]
      .array()
    val legacy = new Variant(legacyValue, meta)
    assert(VariantUtil.isValidVariant(legacyValue, meta), "sanity: legacy object is valid")
    assert(!isCanon(legacy), "sanity: legacy object is not canonical before canon")
    assert(!bytesEqual(legacy, modern), "sanity: legacy and modern bytes differ before canon")
    assert(bytesEqual(canon(legacy), canon(modern)),
      "a legacy UTF-16-ordered object must canonicalize to the modern UTF-8 bytes")
    assert(isCanon(canon(legacy)), "canon of a legacy-ordered object must be canonical")
    assert(bytesEqual(canon(canon(legacy)), canon(legacy)), "canonicalize must be idempotent on it")
  }

  test("getMetadataKeyBytes returns the raw key bytes, matching encodeKey(getMetadataKey)") {
    val v = parse("""{"zebra":1,"apple":2,"c":3,"a-longer-key-name":4}""")
    val meta = v.getMetadata
    Seq(0, 1, 2, 3).foreach { id =>
      assert(
        Arrays.equals(
          VariantUtil.getMetadataKeyBytes(meta, id),
          VariantUtil.encodeKey(VariantUtil.getMetadataKey(meta, id))),
        s"getMetadataKeyBytes must equal encodeKey(getMetadataKey) at id $id")
    }
  }

  // ----- appendVariant (copy mode) byte-preservation -----
  //
  // `appendVariant` shares its re-emit walk with `canonicalize` (appendVariantImpl, gated by
  // `needCanonicalization`). These tests pin that the copy path leaves bytes untouched -- the
  // guarantee schema_of_variant, shredding, and stored-value byte-equality rely on -- so a future
  // slip in the shared walk cannot silently canonicalize copied bytes. The fixtures reuse the
  // non-canonical encodings from the isCanonical tests above but assert a DIFFERENT property (that
  // copy preserves them); non-canonicalness is guarded with `!bytesEqual(canon(v), v)` so the copy
  // assertion can never pass vacuously, without re-treading the `isCanonical rejects ...` tests.

  test("appendVariant copy mode never canonicalizes a scalar, at any depth or builder state") {
    def prim(t: Int): Byte = ((t << VariantUtil.BASIC_TYPE_BITS) | VariantUtil.PRIMITIVE).toByte
    val emptyMeta = parse("1").getMetadata
    val int8One = new Variant(
      Array[Byte](prim(VariantUtil.INT8), 1, 0, 0, 0, 0, 0, 0, 0), emptyMeta) // INT8 holding 1
    val hi = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val longStrHi = new Variant(
      java.nio.ByteBuffer.allocate(1 + 4 + hi.length).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.LONG_STR)).putInt(hi.length).put(hi).array(), emptyMeta)
    val nonCanonicalNaN = new Variant(
      java.nio.ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.DOUBLE)).putLong(0x7ff8000000000001L).array(), emptyMeta)
    val nonCanonicalFloatNaN = new Variant(
      java.nio.ByteBuffer.allocate(5).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(prim(VariantUtil.FLOAT)).putInt(0x7fc00001).array(), emptyMeta)

    // Top-level scalars.
    val scalars = Seq(
      "INT8(1)"         -> int8One,
      "decimal 1.50"    -> parse("1.50"),
      "float -0.0"      -> buildFloat(-0.0f),
      "double -0.0"     -> buildDouble(-0.0d),
      "double NaN"      -> nonCanonicalNaN,
      "float NaN"       -> nonCanonicalFloatNaN,
      "long_str \"hi\"" -> longStrHi)
    for ((label, v) <- scalars) {
      assert(!bytesEqual(canon(v), v), s"$label fixture must be non-canonical")
      assert(bytesEqual(appendCopy(v), v), s"copy must preserve $label verbatim")
    }

    // Nested one and two levels deep: pins that the copy flag is carried through the recursive
    // descent into object fields and array elements, not just applied at the root.
    val int8 = int8One.getValue
    val arrInt8 = new Variant(
      java.nio.ByteBuffer.allocate(1 + 1 + 2 + int8.length)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(VariantUtil.arrayHeader(false, 1))
        .put(1.toByte)                         // size = 1
        .put(0.toByte).put(int8.length.toByte) // offsets [0, 9]
        .put(int8).array(),
      emptyMeta)
    val objInt8 = new Variant(
      java.nio.ByteBuffer.allocate(1 + 1 + 1 + 2 + int8.length)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(VariantUtil.objectHeader(false, 1, 1))
        .put(1.toByte)                         // size = 1
        .put(0.toByte)                         // id list [0] -> key "a"
        .put(0.toByte).put(int8.length.toByte) // offsets [0, 9]
        .put(int8).array(),
      parse("""{"a":1}""").getMetadata)
    val arrInner = arrInt8.getValue
    val arrArrInt8 = new Variant(
      java.nio.ByteBuffer.allocate(1 + 1 + 2 + arrInner.length)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .put(VariantUtil.arrayHeader(false, 1))
        .put(1.toByte)                             // size = 1
        .put(0.toByte).put(arrInner.length.toByte) // offsets [0, len]
        .put(arrInner).array(),
      emptyMeta)
    val nested = Seq(
      "[INT8(1)]"       -> arrInt8,
      "{\"a\":INT8(1)}" -> objInt8,
      "[[INT8(1)]]"     -> arrArrInt8)
    for ((label, v) <- nested) {
      assert(!bytesEqual(canon(v), v), s"$label fixture must be non-canonical")
      assert(bytesEqual(appendCopy(v), v), s"copy must preserve the nested scalar in $label")
    }

    // Real callers (CAST AS VARIANT, to_variant, shredding) append into a NON-EMPTY builder. A
    // scalar never references the dictionary, so its value bytes must stay verbatim even when the
    // builder already holds a key.
    val seeded = {
      val b = new VariantBuilder(false)
      b.addKey("preexisting")
      b.appendVariant(int8One)
      b.result()
    }
    assert(Arrays.equals(seeded.getValue, int8One.getValue),
      "appending a scalar into a non-empty builder must preserve its value bytes verbatim")
  }

  test("appendVariant copy mode reproduces canonical and pass-through values byte-for-byte") {
    def build(f: VariantBuilder => Unit): Variant = {
      val b = new VariantBuilder(false); f(b); b.result()
    }
    val wideArray = "[" + (0 until 200).mkString(",") + "]" // data > 255 B -> 2-byte offset width
    val byteStable = Seq(
      "1"                -> parse("1"),
      "-1"               -> parse("-1"),
      "1.5"              -> parse("1.5"),
      "\"hi\""           -> parse("\"hi\""),
      "70-char long_str" -> parse("\"" + ("x" * 70) + "\""),
      "true"             -> parse("true"),
      "null"             -> parse("null"),
      "date"             -> build(_.appendDate(19000)),
      "timestamp"        -> build(_.appendTimestamp(1234567890123L)),
      "timestamp_ntz"    -> build(_.appendTimestampNtz(1234567890123L)),
      "binary"           -> build(_.appendBinary(Array[Byte](1, 2, 3, 4))),
      "uuid"             -> build(_.appendUuid(new java.util.UUID(1L, 2L))),
      "empty array"      -> parse("[]"),
      "empty object"     -> parse("{}"),
      "scalar array"     -> parse("[1,2,3]"),
      "nested array"     -> parse("[1,[2,[3]]]"),
      "mixed array"      -> parse("[true,null,\"hi\"]"),
      "wide array"       -> parse(wideArray))
    for ((label, v) <- byteStable) {
      assert(bytesEqual(appendCopy(v), v), s"copy of $label must be byte-identical")
    }
  }

  test("appendVariant copy mode keeps stored object field order (canonical sort gated)") {
    // appendVariant renumbers the dictionary into walk order, so a raw round-trip is NOT byte
    // identity for keyed objects. Pin the gating against the Issue #1 legacy fixture instead: an
    // object whose field DATA is physically stored in UTF-16 order while the canonical layout is
    // UTF-8 order. The canon-convergence line below deliberately echoes the legacy-order test -- it
    // is the contrast anchor: canon() must converge legacy and modern, while copy() must NOT.
    val k1 = new String(Character.toChars(0xE000))
    val k2 = new String(Character.toChars(0x1F600))
    val modern = parse(s"""{"$k1":1,"$k2":2}""") // parse stores fields UTF-8-sorted: [k1, k2]
    val meta = modern.getMetadata
    val idk1 = if (VariantUtil.getMetadataKey(meta, 0) == k1) 0 else 1
    val idk2 = 1 - idk1
    val v1 = parse("1").getValue // INT1(1)
    val v2 = parse("2").getValue // INT1(2)
    val header = VariantUtil.objectHeader(false, 1, 1) // 1-byte size, idSize 1, offsetSize 1
    val legacyValue = java.nio.ByteBuffer.allocate(1 + 1 + 2 + 3 + v1.length + v2.length)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put(header)
      .put(2.toByte) // size = 2 fields
      .put(idk2.toByte).put(idk1.toByte) // id list in UTF-16 order [k2, k1]
      .put(0.toByte).put(v2.length.toByte).put((v2.length + v1.length).toByte) // offsets [0, 2, 4]
      .put(v2).put(v1) // data stored as [k2's value, k1's value]
      .array()
    val legacy = new Variant(legacyValue, meta)
    assert(!isCanon(legacy), "sanity: legacy object is not canonical")
    assert(bytesEqual(canon(legacy), canon(modern)), "contrast anchor: canon converges the two")
    // The gate: copy must NOT apply the canonical field sort, so the two stay byte-distinct.
    assert(!bytesEqual(appendCopy(legacy), appendCopy(modern)),
      "copy must preserve stored field-data order")
    // A canonical, flat object round-trips through copy unchanged (walk order == dict order here).
    assert(bytesEqual(appendCopy(modern), modern),
      "a canonical flat object copies to identical bytes")
    // copy preserves the key -> value mapping even though it does not re-sort the data.
    assert(appendCopy(legacy).getFieldByKey(k1).getLong == 1, "k1 -> 1 preserved by copy")
    assert(appendCopy(legacy).getFieldByKey(k2).getLong == 2, "k2 -> 2 preserved by copy")
  }

  test("appendVariant copy mode preserves meaning and validity for arbitrary shapes") {
    // Whole-function net, independent of the two gated differences above: for ANY input shape --
    // nested, keyed, unsorted, wide objects/arrays -- copy() must emit a VALID variant that is
    // semantically equal to the input. Equality is canon's contract (byte-equal canonical forms),
    // so canon serves here only as a semantic-equality oracle.
    val wide = "{\"a\":[" + (0 until 200).mkString(",") + "]}" // forces 2-byte offset widths
    val corpus = Seq(
      "1", "1.50", "\"hi\"", "true", "null", "[1,2,3]", "[1,[2,[3]]]", "{}", "[]",
      """{"a":1,"b":2}""", """{"b":2,"a":1}""",
      """{"z":{"b":1,"a":2},"m":[1,2,3]}""",
      """{"a":[{"y":1,"x":2}],"b":{"d":4,"c":5}}""",
      wide)
    for (json <- corpus) {
      val v = parse(json)
      val c = appendCopy(v)
      assert(VariantUtil.isValidVariant(c.getValue, c.getMetadata), s"copy must be valid: $json")
      assert(bytesEqual(canon(c), canon(v)), s"copy must preserve meaning: $json")
    }

    // A sub-variant (pos != 0) views a parent's bytes; copy must emit a standalone value.
    val sub = parse("""{"outer":{"b":2,"a":1},"z":3}""").getFieldByKey("outer")
    val subCopy = appendCopy(sub)
    assert(VariantUtil.isValidVariant(subCopy.getValue, subCopy.getMetadata),
      "copy of a sub-variant must be a valid standalone variant")
    assert(bytesEqual(canon(subCopy), canon(sub)),
      "copy of a sub-variant must preserve its meaning")
  }

  // ----- canonicalize: malformed metadata (robustness) -----

  test("canonicalize rejects a dictionary-size header too large for the metadata (no OOM)") {
    // A corrupt/foreign metadata can set the dictionary-size header up to Integer.MAX_VALUE in a
    // few bytes; canonicalize must reject the inconsistent header instead of OOMing on it.
    val value = parse("1").getValue // scalar: the rebuild never reads the bogus dictionary
    val meta = java.nio.ByteBuffer.allocate(1 + 4 + 4) // header + numKeys(4B) + offset[0](4B)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .put((VariantUtil.VERSION | (3 << 6)).toByte) // version 1, 4-byte offset width
      .putInt(1 << 20) // dictionary size: far more keys than 9 bytes can hold
      .putInt(1) // offset[0] != 0 so isCanonical returns false and the rebuild runs
      .array()
    val malformed = new Variant(value, meta)
    val eDirect = intercept[SparkRuntimeException](VariantBuilder.doCanonicalize(malformed))
    assert(eDirect.getCondition == "MALFORMED_VARIANT", eDirect.getCondition)
    val eCanon = intercept[SparkRuntimeException](VariantBuilder.canonicalize(malformed))
    assert(eCanon.getCondition == "MALFORMED_VARIANT", eCanon.getCondition)
  }

  // ----- fuzz: random legal-but-non-canonical variants -----
  //
  // Build a random value, render it to JSON two ways (object keys shuffled, and numbers spelled
  // equivalently, e.g. 5 / 5.0), parse both into byte-different-but-equal Variants, and check:
  //   equivalence  -- the two encodings canonicalize to identical bytes
  //   completeness -- canonicalize's output is recognized as canonical
  //   soundness    -- canonicalizing an already-canonical value is a no-op (via doCanonicalize)

  private sealed trait Node
  private case class Obj(fields: Seq[(String, Node)]) extends Node
  private case class Arr(elems: Seq[Node]) extends Node
  private case class Scalar(spellings: Seq[String]) extends Node

  private val fuzzKeys = Seq("", "a", "b", "z", "key")
  private val fuzzStrings = Seq("", "x", "hello", "y" * 70)   // incl. a >63-byte long_str

  // one roll: leaf 1/6, array 2/6, object 3/6. Recurses until the depth budget is spent, so
  // arrays and objects nest (arrays of objects, objects of arrays, and so on).
  private def gen(rand: Random, depth: Int): Node =
    if (depth <= 0) genLeaf(rand)
    else rand.nextInt(6) match {
      case 0 => genLeaf(rand)
      case 1 | 2 => Arr(Seq.fill(1 + rand.nextInt(3))(gen(rand, depth - 1)))
      case _ =>
        Obj(rand.shuffle(fuzzKeys.toList).take(1 + rand.nextInt(3)).map(_ -> gen(rand, depth - 1)))
    }

  private def genLeaf(rand: Random): Node = rand.nextInt(7) match {
    case 0 => Scalar(Seq(if (rand.nextBoolean()) "true" else "false"))
    case 1 => Scalar(Seq("null"))
    case 2 => Scalar(Seq("\"" + fuzzStrings(rand.nextInt(fuzzStrings.size)) + "\""))
    case 3 => val n = rand.nextInt(200) - 100; Scalar(Seq(s"$n", s"$n.0", s"$n.00"))
    case 4 => val w = rand.nextInt(99) + 1;    Scalar(Seq(s"$w.5", s"$w.50"))
    case 5 => Obj(Nil)
    case _ => Arr(Nil)
  }

  // two calls => two legal encodings of one value (keys shuffled + equivalent number spellings)
  private def render(n: Node, rand: Random): String = n match {
    case Obj(fs) => rand.shuffle(fs.toList).map(f => "\"" + f._1 + "\":" + render(f._2, rand))
                         .mkString("{", ",", "}")
    case Arr(es) => es.map(render(_, rand)).mkString("[", ",", "]")
    case Scalar(sp) => sp(rand.nextInt(sp.size))
  }

  test("fuzz: canonicalize is consistent and complete over random variants") {
    (0 until 300).foreach { i =>
      val rand = new Random(0xC0FFEEL + i)
      val tree = gen(rand, 2 + rand.nextInt(4))    // max depth 2..5
      val v1 = parse(render(tree, rand))
      val v2 = parse(render(tree, rand))
      val clue = s"seed=${0xC0FFEEL + i}"
      assert(bytesEqual(canon(v1), canon(v2)), s"equivalence: $clue")
      assert(isCanon(canon(v1)), s"completeness: $clue")
      if (isCanon(v1)) {
        assert(bytesEqual(v1, VariantBuilder.doCanonicalize(v1)), s"soundness: $clue")
      }
    }
  }
}
