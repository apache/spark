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

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

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

  // offset-list integer width stored in an object/array value header at position 0
  private def valueOffsetWidth(v: Variant): Int =
    ((v.getValue()(0) >> VariantUtil.BASIC_TYPE_BITS) & 0x3) + 1

  // dictionary offset width stored in the metadata header byte
  private def metaOffsetWidth(v: Variant): Int =
    ((v.getMetadata()(0) >> 6) & 0x3) + 1

  // object field-id integer width stored in an object value header at position 0
  private def idWidth(v: Variant): Int =
    ((v.getValue()(0) >> (VariantUtil.BASIC_TYPE_BITS + 2)) & 0x3) + 1

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
    // isCanonical inspects only the metadata at this step, so any value bytes suffice.
    assert(!VariantBuilder.isCanonical(new Variant(parse("1").getValue, meta)),
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
      assert(bytesEqual(v, canon(v)), "a pass-through scalar must be unchanged by canonicalize")
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
}
