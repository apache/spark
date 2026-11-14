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

import java.nio.charset.StandardCharsets

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.SparkRuntimeException
import org.apache.spark.types.variant.VariantUtil._

class VariantCanonicalizeSuite extends AnyFunSuite { // scalastyle:ignore funsuite
  private val emptyMetadata = Array[Byte](VERSION, 0, 0)
  private val nullValue = Array(primitiveHeader(NULL))
  private val trueValue = Array(primitiveHeader(TRUE))
  private val falseValue = Array(primitiveHeader(FALSE))

  private def bytes(values: Int*): Array[Byte] = values.map(_.toByte).toArray

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def littleEndian(value: BigInt, width: Int): Array[Byte] =
    Array.tabulate(width)(index => ((value >> (8 * index)) & 0xff).toByte)

  private def concatenate(parts: Seq[Array[Byte]]): Array[Byte] =
    parts.iterator.flatMap(_.iterator).toArray

  private def integer(value: Long, typeInfo: Int = INT1, width: Int = 1): Array[Byte] =
    Array(primitiveHeader(typeInfo)) ++ littleEndian(BigInt(value), width)

  private def decimal(
      unscaled: String,
      scale: Int,
      typeInfo: Int,
      width: Int): Array[Byte] = {
    Array(primitiveHeader(typeInfo), scale.toByte) ++ littleEndian(BigInt(unscaled), width)
  }

  private def floating(rawBits: Long, typeInfo: Int, width: Int): Array[Byte] =
    Array(primitiveHeader(typeInfo)) ++ littleEndian(BigInt(rawBits), width)

  private def string(value: Array[Byte], longEncoding: Boolean = false): Array[Byte] = {
    if (longEncoding) {
      Array(primitiveHeader(LONG_STR)) ++ littleEndian(BigInt(value.length), 4) ++ value
    } else {
      Array(shortStrHeader(value.length)) ++ value
    }
  }

  // Metadata binary: header, key count, `keys.size + 1` offsets, then the key bytes.
  private def metadata(
      keys: Seq[Array[Byte]],
      offsetSize: Int = 1,
      flags: Int = 0): Array[Byte] = {
    val offsets = keys.scanLeft(0)((offset, key) => offset + key.length)
    bytes(metadataHeader(offsetSize) | flags) ++ littleEndian(BigInt(keys.size), offsetSize) ++
      concatenate(offsets.map(offset => littleEndian(BigInt(offset), offsetSize))) ++
      concatenate(keys)
  }

  private def dictionary(names: String*): Array[Byte] = metadata(names.map(utf8))

  // Array value: header, element count, `elements.size + 1` offsets, then the elements.
  private def array(
      elements: Seq[Array[Byte]],
      offsetSize: Int = 1,
      largeSize: Boolean = false): Array[Byte] = {
    val offsets = elements.scanLeft(0)((offset, element) => offset + element.length)
    Array(arrayHeader(largeSize, offsetSize)) ++
      littleEndian(BigInt(elements.size), if (largeSize) 4 else 1) ++
      concatenate(offsets.map(offset => littleEndian(BigInt(offset), offsetSize))) ++
      concatenate(elements)
  }

  // Object value from (key id, value bytes) pairs in field order. `physicalOrder` stores the values
  // in another order than the field list, which the spec allows.
  private def obj(
      fields: Seq[(Int, Array[Byte])],
      idSize: Int = 1,
      offsetSize: Int = 1,
      largeSize: Boolean = false,
      physicalOrder: Seq[Int] = Seq.empty): Array[Byte] = {
    val order = if (physicalOrder.isEmpty) fields.indices else physicalOrder
    val offsets = new Array[Int](fields.size)
    var dataSize = 0
    order.foreach { fieldIndex =>
      offsets(fieldIndex) = dataSize
      dataSize += fields(fieldIndex)._2.length
    }
    Array(objectHeader(largeSize, idSize, offsetSize)) ++
      littleEndian(BigInt(fields.size), if (largeSize) 4 else 1) ++
      concatenate(fields.map { case (keyId, _) => littleEndian(BigInt(keyId), idSize) }) ++
      concatenate((offsets.toSeq :+ dataSize).map(offset =>
        littleEndian(BigInt(offset), offsetSize))) ++
      concatenate(order.map(fieldIndex => fields(fieldIndex)._2))
  }

  private def assertBytes(actual: Array[Byte], expected: Array[Byte], label: String): Unit = {
    assert(actual.sameElements(expected),
      s"$label: expected ${expected.length} bytes, got ${actual.length} bytes; " +
        s"expected prefix ${expected.take(32).mkString(",")}, " +
        s"actual prefix ${actual.take(32).mkString(",")}")
  }

  // Canonicalize `input` and compare with the expected bytes. The input is canonical exactly when
  // it already has those bytes. The input bytes must not change.
  private def check(
      input: Variant,
      expectedValue: Array[Byte],
      expectedMetadata: Array[Byte] = emptyMetadata): Variant = {
    val canonicalInput = input.pos == 0 && input.value.sameElements(expectedValue) &&
      input.metadata.sameElements(expectedMetadata)
    val originalValue = input.value.clone()
    val originalMetadata = input.metadata.clone()
    assert(VariantBuilder.isCanonical(input) == canonicalInput)
    val actual = VariantBuilder.canonicalize(input)
    assert((actual eq input) == canonicalInput)
    assert(actual.pos == 0)
    assertBytes(actual.value, expectedValue, "canonical value")
    assertBytes(actual.metadata, expectedMetadata, "canonical metadata")
    assert(VariantBuilder.isCanonical(actual))
    assert(VariantBuilder.canonicalize(actual) eq actual)
    Seq(input, actual).foreach { source =>
      val rebuilt = VariantBuilder.doCanonicalize(source)
      assertBytes(rebuilt.value, expectedValue, "unconditional rebuild value")
      assertBytes(rebuilt.metadata, expectedMetadata, "unconditional rebuild metadata")
    }
    assertBytes(input.value, originalValue, "unchanged input value")
    assertBytes(input.metadata, originalMetadata, "unchanged input metadata")
    actual
  }

  private def checkKeyless(inputValue: Array[Byte], expectedValue: Array[Byte]): Unit =
    check(new Variant(inputValue, emptyMetadata), expectedValue)

  private def checkError(condition: String)(body: => Any): Unit = {
    assert(intercept[SparkRuntimeException](body).getCondition == condition)
  }

  // Pins the exact bytes of a few canonical values independently of the helpers above.
  test("canonical encoding has pinned value and metadata bytes") {
    // INT8 1 becomes INT1 1.
    checkKeyless(bytes(24, 1, 0, 0, 0, 0, 0, 0, 0), bytes(12, 1))
    // Long string "x" becomes short string "x".
    checkKeyless(bytes(64, 1, 0, 0, 0, 120), bytes(5, 120))
    // {"b":2,"a":1}: value is header, 2 fields, ids [0, 1], offsets [0, 2, 4], INT1 1, INT1 2;
    // metadata is header, 2 keys, offsets [0, 1, 2], "ab".
    check(VariantBuilder.parseJson("{\"b\":2,\"a\":1}", false),
      bytes(2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 2), bytes(1, 2, 0, 1, 2, 97, 98))
    // Empty object and empty array.
    checkKeyless(bytes(2, 0, 0), bytes(2, 0, 0))
    checkKeyless(bytes(3, 0, 0), bytes(3, 0, 0))
  }

  test("metadata helpers read every offset width and reject an overflowing dictionary size") {
    Seq((1, 0x01), (2, 0x41), (3, 0x81), (4, 0xc1)).foreach {
      case (width, header) =>
        // Keys "a" and "b" with a `width`-byte key count and offsets.
        val input = bytes(header) ++ littleEndian(BigInt(2), width) ++
          concatenate(Seq(0, 1, 2).map(offset => littleEndian(BigInt(offset), width))) ++
          bytes(97, 98)
        assert(metadataHeader(width) == header.toByte)
        assert(getMetadataOffsetSize(input) == width)
        assert(getMetadataNumKeys(input, width) == 2)
        assert(getMetadataStringStart(input, 2, width) == 1 + 4 * width)
        assert(getMetadataKey(input, 0) == "a")
        assert(getMetadataKey(input, 1) == "b")
    }
    // A key count near Int.MaxValue used to overflow the string start and return a wrong key.
    val forged = Array(metadataHeader(4)) ++ littleEndian(BigInt(Int.MaxValue - 1), 4) ++
      littleEndian(BigInt(0), 4) ++ littleEndian(BigInt(1), 4) ++ utf8("a")
    checkError("MALFORMED_VARIANT") { getMetadataKey(forged, 0) }
  }

  test("isCanonical rejects decreasing or out-of-bounds dictionary offsets") {
    val value = obj(Seq(0 -> integer(1), 1 -> integer(2), 2 -> integer(3)))
    assert(VariantBuilder.isCanonical(new Variant(value, dictionary("a", "b", "c"))))

    // Offsets [0, 2, 1, 3] decrease and [0, 4, 2, 3] exceed the last offset. [0, 4, 4, 3] would
    // reach the key comparison with an out-of-range slice if the bound were not checked.
    val decreasingOffsets = bytes(VERSION, 3, 0, 2, 1, 3) ++ utf8("abc")
    val outOfBoundsOffset = bytes(VERSION, 3, 0, 4, 2, 3) ++ utf8("abc")
    val outOfBoundsBeforeCompare = bytes(VERSION, 3, 0, 4, 4, 3) ++ utf8("abc")
    Seq(decreasingOffsets, outOfBoundsOffset, outOfBoundsBeforeCompare).foreach { invalidMetadata =>
      val variant = new Variant(value, invalidMetadata)
      assert(!VariantBuilder.isCanonical(variant))
      checkError("MALFORMED_VARIANT") { VariantBuilder.canonicalize(variant) }
    }
  }

  test("dictionary permutation and object value order canonicalize independently") {
    // The spec sorts field ids by key name, not by id, allows the values in any order and allows an
    // unsorted dictionary, so these are four legal encodings of {"a":1,"b":2}.
    val expectedValue = obj(Seq(0 -> integer(1), 1 -> integer(2)))
    Seq(Seq("a", "b"), Seq("b", "a")).foreach { keys =>
      Seq(Seq(0, 1), Seq(1, 0)).foreach { order =>
        val value = obj(Seq(keys.indexOf("a") -> integer(1), keys.indexOf("b") -> integer(2)),
          physicalOrder = order)
        check(new Variant(value, dictionary(keys: _*)), expectedValue, dictionary("a", "b"))
      }
    }
  }

  test("unused dictionary entries are removed and repeated names share an ID") {
    // Keys "b", "unused", "a", "a": the objects use ids 2, 3 and 0, so "unused" is dropped and
    // both "a" entries become one key.
    val sourceMetadata = dictionary("b", "unused", "a", "a")
    val sourceValue = array(Seq(
      obj(Seq(2 -> integer(1))), obj(Seq(3 -> integer(2))), obj(Seq(0 -> integer(3)))))
    val expectedValue = array(Seq(
      obj(Seq(0 -> integer(1))), obj(Seq(0 -> integer(2))), obj(Seq(1 -> integer(3)))))
    check(new Variant(sourceValue, sourceMetadata), expectedValue, dictionary("a", "b"))
    // A scalar root keeps no keys at all.
    check(new Variant(integer(1), sourceMetadata), integer(1))
    // The empty key is a key like any other.
    val emptyKey = obj(Seq(0 -> nullValue, 1 -> integer(1)))
    check(new Variant(emptyKey, dictionary("", "a")), emptyKey, dictionary("", "a"))
  }

  // scalastyle:off nonascii
  test("keys are sorted by unsigned UTF-8 bytes rather than UTF-16 string order") {
    // U+E000 (EE 80 80) sorts before U+10000 (F0 90 80 80) in UTF-8 bytes but after it in UTF-16,
    // the order of String.compareTo that Spark used before SPARK-58949. The first input lists the
    // fields in that old order.
    val bmpKey = utf8("\ue000")
    val supplementaryKey = utf8("\ud800\udc00")
    val expectedMetadata = metadata(Seq(bmpKey, supplementaryKey))
    val expectedValue = obj(Seq(0 -> integer(10), 1 -> integer(20)))
    Seq(Seq(0 -> integer(20), 1 -> integer(10)),
      Seq(1 -> integer(10), 0 -> integer(20))).foreach { fields =>
      check(new Variant(obj(fields), metadata(Seq(supplementaryKey, bmpKey))),
        expectedValue, expectedMetadata)
    }
    // Multi-byte keys keep their bytes: "cafe" < "caf" U+00E9 (C3 A9) < U+4F60 (E4 BD A0).
    check(new Variant(obj(Seq(2 -> integer(1), 1 -> integer(2), 0 -> integer(3))),
      dictionary("\u4f60", "caf\u00e9", "cafe")),
      obj(Seq(0 -> integer(1), 1 -> integer(2), 2 -> integer(3))),
      dictionary("cafe", "caf\u00e9", "\u4f60"))
  }
  // scalastyle:on nonascii

  test("all independent metadata ID offset and count widths converge") {
    // Every combination of metadata offset width, id width, offset width and is_large encodes the
    // same value and must end up with the minimal widths.
    val fields = Seq(0 -> trueValue, 1 -> integer(258, INT2, 2), 2 -> nullValue)
    val elements = fields.map(_._2)
    val keys = Seq("a", "b", "c").map(utf8)
    for {
      metadataWidth <- 1 to 4
      idWidth <- 1 to 4
      offsetWidth <- 1 to 4
      largeSize <- Seq(false, true)
    } {
      withClue(s"metadata=$metadataWidth id=$idWidth offset=$offsetWidth large=$largeSize: ") {
        check(new Variant(obj(fields, idWidth, offsetWidth, largeSize),
          metadata(keys, metadataWidth)), obj(fields), metadata(keys))
      }
    }
    for {
      metadataWidth <- 1 to 4
      offsetWidth <- 1 to 4
      largeSize <- Seq(false, true)
    } {
      val keylessMetadata = metadata(Seq.empty, metadataWidth)
      check(new Variant(array(elements, offsetWidth, largeSize), keylessMetadata),
        array(elements))
      check(new Variant(array(Seq.empty, offsetWidth, largeSize), keylessMetadata),
        array(Seq.empty))
      check(new Variant(obj(Seq.empty, 4, offsetWidth, largeSize), keylessMetadata),
        obj(Seq.empty))
    }
  }

  test("metadata flag bits are not part of canonical output") {
    // sorted_strings (bit 4) and the reserved bit 5 are cleared.
    Seq(1 << 4, 1 << 5, (1 << 4) | (1 << 5)).foreach { flags =>
      check(new Variant(integer(1), metadata(Seq.empty, flags = flags)), integer(1))
    }
  }

  test("reserved container header bits are cleared during normalization") {
    // Readers must ignore the reserved header bits (object bit 7, array bits 5 to 7), so values
    // that differ only there are equal and must converge.
    val objectValue = obj(Seq(0 -> integer(1)))
    val objectWithReservedBit = objectValue.clone()
    objectWithReservedBit(0) = (objectWithReservedBit(0) | 0x80).toByte
    check(new Variant(objectWithReservedBit, dictionary("a")), objectValue, dictionary("a"))
    val arrayValue = array(Seq(integer(1)))
    Seq(0x20, 0x40, 0x80, 0xe0).foreach { reservedBits =>
      val arrayWithReservedBits = arrayValue.clone()
      arrayWithReservedBits(0) = (arrayWithReservedBits(0) | reservedBits).toByte
      checkKeyless(arrayWithReservedBits, arrayValue)
    }
  }

  test("array elements stored out of order are rewritten in element order") {
    // [1, 2] with the second element stored first: the spec fixes the offsets, not where the
    // values sit.
    val outOfOrder = bytes(arrayHeader(false, 1), 2, 2, 0, 4) ++ integer(2) ++ integer(1)
    checkKeyless(outOfOrder, array(Seq(integer(1), integer(2))))
  }

  test("aliased nested values fail with the size limit") {
    // Robustness coverage for malformed input: stored bytes reach canonicalize without validation,
    // and 60 nested two-element arrays whose offsets both point at the same child reach 2^60
    // values from a few hundred bytes. Collecting the keys counts the values it reaches and gives
    // up at SIZE_LIMIT, so canonicalize fails instead of walking all of them.
    val nested = (0 until 60).foldLeft(integer(1)) { (child, _) =>
      Array(arrayHeader(false, 4)) ++ bytes(2) ++ littleEndian(BigInt(0), 4) ++
        littleEndian(BigInt(0), 4) ++ littleEndian(BigInt(child.length), 4) ++ child
    }
    val input = new Variant(nested, emptyMetadata)
    assert(!VariantBuilder.isCanonical(input))
    intercept[VariantSizeLimitException] { VariantBuilder.canonicalize(input) }
  }

  test("container and metadata widths change exactly at their boundaries") {
    // 256 fields or elements need is_large and, with one-byte values, 2-byte offsets; 2-byte ids
    // start at id 256.
    Seq((255, false, 1, 1), (256, true, 1, 2), (257, true, 2, 2)).foreach {
      case (count, largeSize, idWidth, offsetWidth) =>
        val keys = (0 until count).map(index => utf8(f"key$index%03d"))
        val fields = (0 until count).map(index => index -> nullValue)
        val expectedMetadata = metadata(keys, 2)
        val expectedObject = obj(fields, idWidth, offsetWidth, largeSize)
        check(new Variant(obj(fields, 4, 4, largeSize = true), metadata(keys, 4)),
          expectedObject, expectedMetadata)
        check(new Variant(expectedObject, expectedMetadata), expectedObject, expectedMetadata)
        val elements = Seq.fill(count)(nullValue)
        checkKeyless(array(elements, 4, largeSize = true), array(elements, offsetWidth, largeSize))
    }
    // The id width follows the largest id in the canonical dictionary, not the field count: the
    // object using key 256 needs 2-byte ids inside the array but 1-byte ids on its own.
    val globalKeys = (0 to 256).map(index => utf8(f"key$index%03d"))
    val sourceElements = globalKeys.indices.map { index =>
      obj(Seq(index -> nullValue), 4, 4, largeSize = true)
    }
    val expectedElements = globalKeys.indices.map { index =>
      obj(Seq(index -> nullValue), idSize = if (index <= U8_MAX) 1 else 2)
    }
    check(new Variant(array(sourceElements, 4, largeSize = true), metadata(globalKeys, 4)),
      array(expectedElements, 2, largeSize = true), metadata(globalKeys, 2))
    check(new Variant(obj(Seq(256 -> integer(1)), 2), metadata(globalKeys, 2)),
      obj(Seq(0 -> integer(1))), metadata(Seq(globalKeys.last)))
    // Offsets of 1, 2 and 3 bytes for metadata, arrays and objects, from one key or one binary
    // value of `size` bytes.
    Seq((255, 1), (256, 2), (65535, 2), (65536, 3)).foreach {
      case (size, expectedWidth) =>
        withClue(s"size=$size width=$expectedWidth: ") {
          val key = Array.fill[Byte](size)('a'.toByte)
          val objectValue = obj(Seq(0 -> nullValue))
          val expectedMetadata = metadata(Seq(key), expectedWidth)
          check(new Variant(objectValue, metadata(Seq(key), 4)), objectValue, expectedMetadata)
          check(new Variant(objectValue, expectedMetadata), objectValue, expectedMetadata)
          val payload = Array(primitiveHeader(BINARY)) ++ littleEndian(BigInt(size - 5), 4) ++
            Array.fill[Byte](size - 5)(7)
          checkKeyless(array(Seq(payload), 4), array(Seq(payload), expectedWidth))
          val expectedObject = obj(Seq(0 -> payload), offsetSize = expectedWidth)
          check(new Variant(obj(Seq(0 -> payload), offsetSize = 4), dictionary("a")),
            expectedObject, dictionary("a"))
        }
    }
  }

  test("signed integer boundaries choose the smallest encoding") {
    // Each boundary value, fed in every integer width that holds it, ends in the smallest type.
    val boundaries = Seq(
      (Long.MinValue, INT8, 8), (Int.MinValue.toLong - 1, INT8, 8),
      (Int.MinValue.toLong, INT4, 4), (Short.MinValue.toLong - 1, INT4, 4),
      (Short.MinValue.toLong, INT2, 2), (Byte.MinValue.toLong - 1, INT2, 2),
      (Byte.MinValue.toLong, INT1, 1), (-1L, INT1, 1), (0L, INT1, 1), (1L, INT1, 1),
      (Byte.MaxValue.toLong, INT1, 1), (Byte.MaxValue.toLong + 1, INT2, 2),
      (Short.MaxValue.toLong, INT2, 2), (Short.MaxValue.toLong + 1, INT4, 4),
      (Int.MaxValue.toLong, INT4, 4), (Int.MaxValue.toLong + 1, INT8, 8),
      (Long.MaxValue, INT8, 8))
    boundaries.foreach { case (number, expectedType, expectedWidth) =>
      Seq((INT1, 1), (INT2, 2), (INT4, 4), (INT8, 8)).foreach { case (typeInfo, width) =>
        if (width >= expectedWidth) {
          withClue(s"integer=$number width=$width: ") {
            checkKeyless(integer(number, typeInfo, width),
              integer(number, expectedType, expectedWidth))
          }
        }
      }
    }
  }

  test("decimal normalization respects precision scale and signed long promotion limits") {
    // Trailing zeros are stripped, integral values that fit a long become integers and the rest
    // take the smallest decimal type. Each case is fed in every decimal width that holds it.
    val cases = Seq(
      ("0", 30, integer(0)),
      ("100", 2, integer(1)),
      ("-100", 2, integer(-1)),
      ("100000", 2, integer(1000, INT2, 2)),
      ("5", 0, integer(5)),
      ("-5", 0, integer(-5)),
      ("100", 0, integer(100)),
      ("9223372036854775807", 0, integer(Long.MaxValue, INT8, 8)),
      ("-9223372036854775808", 0, integer(Long.MinValue, INT8, 8)),
      ("1200", 3, decimal("12", 1, DECIMAL4, 4)),
      ("-1200", 3, decimal("-12", 1, DECIMAL4, 4)),
      ("999999999", 1, decimal("999999999", 1, DECIMAL4, 4)),
      ("1000000001", 1, decimal("1000000001", 1, DECIMAL8, 8)),
      ("999999999999999999", 1, decimal("999999999999999999", 1, DECIMAL8, 8)),
      ("1000000000000000001", 1, decimal("1000000000000000001", 1, DECIMAL16, 16)),
      ("1", 9, decimal("1", 9, DECIMAL4, 4)),
      ("1", 10, decimal("1", 10, DECIMAL8, 8)),
      ("1", 18, decimal("1", 18, DECIMAL8, 8)),
      ("1", 19, decimal("1", 19, DECIMAL16, 16)),
      ("1", 38, decimal("1", 38, DECIMAL16, 16)),
      ("92233720368547758070", 1, integer(Long.MaxValue, INT8, 8)),
      ("-92233720368547758080", 1, integer(Long.MinValue, INT8, 8)),
      ("92233720368547758080", 1, decimal("9223372036854775808", 0, DECIMAL16, 16)),
      ("-92233720368547758090", 1, decimal("-9223372036854775809", 0, DECIMAL16, 16)),
      ("100000000000000000000", 0, decimal("100000000000000000000", 0, DECIMAL16, 16)))
    cases.foreach { case (unscaled, scale, expected) =>
      Seq((DECIMAL4, 4, MAX_DECIMAL4_PRECISION), (DECIMAL8, 8, MAX_DECIMAL8_PRECISION),
        (DECIMAL16, 16, MAX_DECIMAL16_PRECISION)).foreach {
        case (typeInfo, width, maxPrecision) =>
          if (unscaled.stripPrefix("-").length <= maxPrecision && scale <= maxPrecision) {
            withClue(s"decimal=$unscaled scale=$scale width=$width: ") {
              checkKeyless(decimal(unscaled, scale, typeInfo, width), expected)
            }
          }
      }
    }
  }

  test("float and double normalize negative zero and NaN within their type") {
    // Raw bits to canonical bits: -0.0 becomes 0.0 and every NaN payload the canonical NaN, while
    // infinities, subnormals and finite values keep their bits. A float never becomes a double.
    val floatCases = Seq(
      (0x00000000L, 0x00000000L), (0x80000000L, 0x00000000L),
      (0x7fc00000L, 0x7fc00000L), (0x7f800001L, 0x7fc00000L),
      (0x7fffffffL, 0x7fc00000L), (0xffc01234L, 0x7fc00000L),
      (0x7f800000L, 0x7f800000L), (0xff800000L, 0xff800000L),
      (0x00000001L, 0x00000001L), (0x00800000L, 0x00800000L),
      (0x7f7fffffL, 0x7f7fffffL), (0x3f800000L, 0x3f800000L),
      (0xbf800000L, 0xbf800000L))
    floatCases.foreach { case (rawBits, expectedBits) =>
      checkKeyless(floating(rawBits, FLOAT, 4), floating(expectedBits, FLOAT, 4))
    }
    val doubleCases = Seq(
      (0x0000000000000000L, 0x0000000000000000L),
      (0x8000000000000000L, 0x0000000000000000L),
      (0x7ff8000000000000L, 0x7ff8000000000000L),
      (0x7ff0000000000001L, 0x7ff8000000000000L),
      (0x7fffffffffffffffL, 0x7ff8000000000000L),
      (0xfff8123456789abcL, 0x7ff8000000000000L),
      (0x7ff0000000000000L, 0x7ff0000000000000L),
      (0xfff0000000000000L, 0xfff0000000000000L),
      (0x0000000000000001L, 0x0000000000000001L),
      (0x0010000000000000L, 0x0010000000000000L),
      (0x7fefffffffffffffL, 0x7fefffffffffffffL),
      (0x3ff0000000000000L, 0x3ff0000000000000L),
      (0xbff0000000000000L, 0xbff0000000000000L))
    doubleCases.foreach { case (rawBits, expectedBits) =>
      checkKeyless(floating(rawBits, DOUBLE, 8), floating(expectedBits, DOUBLE, 8))
    }
  }

  // scalastyle:off nonascii
  test("strings choose headers by UTF-8 byte length and preserve valid bytes") {
    // Up to MAX_SHORT_STR_SIZE UTF-8 bytes use the short-string header, longer strings the 4-byte
    // length header. The bytes themselves are kept.
    Seq("", "a", "a" * 63, "a" * 64, "\u00e9" * 31 + "a", "\u00e9" * 32,
      "\ud800\udc00", "prefix\u0000suffix", "caf\u00e9", "\u20ac",
      "\u4f60\u597d").foreach { text =>
      val payload = utf8(text)
      val expected = string(payload, longEncoding = payload.length > MAX_SHORT_STR_SIZE)
      checkKeyless(string(payload, longEncoding = true), expected)
      checkKeyless(expected, expected)
    }
    val value = obj(Seq(1 -> string(utf8("\u00e9" * 40), longEncoding = true),
      0 -> string(utf8("\ud800\udc00"))))
    check(new Variant(value, dictionary("b", "a")),
      obj(Seq(0 -> string(utf8("\u00e9" * 40), longEncoding = true),
        1 -> string(utf8("\ud800\udc00")))), dictionary("a", "b"))
  }
  // scalastyle:on nonascii

  test("deeply nested containers canonicalize at every level") {
    // 500 single-element arrays with 4-byte offsets around DECIMAL4 1.00 become 1- or 2-byte
    // offsets around INT1 1, so every level is rewritten.
    def nest(leaf: Array[Byte], minimalWidths: Boolean): Array[Byte] =
      (0 until 500).foldLeft(leaf) { (child, _) =>
        array(Seq(child), if (!minimalWidths) 4 else if (child.length <= U8_MAX) 1 else 2)
      }
    checkKeyless(nest(decimal("100", 2, DECIMAL4, 4), minimalWidths = false),
      nest(integer(1), minimalWidths = true))
  }

  test("single-encoding scalar types keep their bytes through a rebuild") {
    // These types have one encoding each. The unused key forces a rebuild, which must copy them.
    val values = Seq(nullValue, trueValue, falseValue,
      Array(primitiveHeader(DATE)) ++ littleEndian(BigInt(-12345), 4),
      Array(primitiveHeader(TIMESTAMP)) ++ littleEndian(BigInt(-123456789L), 8),
      Array(primitiveHeader(TIMESTAMP_NTZ)) ++ littleEndian(BigInt(123456789L), 8),
      Array(primitiveHeader(BINARY)) ++ littleEndian(BigInt(3), 4) ++ bytes(0, 0xff, 1),
      Array(primitiveHeader(UUID)) ++ (0 until 16).map(_.toByte).toArray)
    values.foreach { value =>
      checkKeyless(value, value)
      check(new Variant(value, dictionary("unused")), value)
    }
  }

  test("malformed structures fail with the documented error classes") {
    val malformed = Seq(
      // Scalars shorter than their header announces.
      new Variant(bytes(primitiveHeader(INT8), 0), emptyMetadata),
      new Variant(bytes(shortStrHeader(2), 97), emptyMetadata),
      new Variant(bytes(primitiveHeader(LONG_STR), 2, 0, 0, 0, 97),
        metadata(Seq.empty, 2)),
      new Variant(bytes(primitiveHeader(DECIMAL16), 0), emptyMetadata),
      // Decimal scale 39, and ten digits in a DECIMAL4.
      new Variant(decimal("1", 39, DECIMAL16, 16), emptyMetadata),
      new Variant(decimal("1000000000", 0, DECIMAL4, 4), emptyMetadata),
      // Containers whose child is truncated, uses a key the dictionary lacks, or lies outside the
      // value.
      new Variant(array(Seq(bytes(primitiveHeader(INT8), 0))), emptyMetadata),
      new Variant(obj(Seq(0 -> nullValue)), emptyMetadata),
      new Variant(bytes(arrayHeader(false, 1), 1, 5, 6, 0), emptyMetadata),
      // Metadata that is truncated, declares more key bytes than it has, has decreasing offsets,
      // or declares a dictionary larger than itself.
      new Variant(integer(1), bytes(1)),
      new Variant(obj(Seq(0 -> nullValue)), bytes(1, 1, 0)),
      new Variant(obj(Seq(0 -> nullValue)), bytes(1, 1, 0, 2, 97)),
      new Variant(obj(Seq(0 -> nullValue, 1 -> nullValue)), bytes(1, 2, 0, 2, 1, 97, 98)),
      new Variant(integer(1), bytes(0x41, 0, 4)),
      new Variant(integer(1), bytes(0xc1, 0xff, 0xff, 0xff, 0xff)))
    malformed.zipWithIndex.foreach { case (input, index) =>
      withClue(s"malformed case $index: ") {
        checkError("MALFORMED_VARIANT") { VariantBuilder.canonicalize(input) }
        checkError("MALFORMED_VARIANT") { VariantBuilder.doCanonicalize(input) }
      }
    }
    // isCanonical returns false when what it has read already rules the input out, and throws
    // when it has to decode malformed bytes.
    val truncated = new Variant(bytes(primitiveHeader(INT8), 0), emptyMetadata)
    assert(!VariantBuilder.isCanonical(truncated))
    checkError("MALFORMED_VARIANT") {
      VariantBuilder.isCanonical(new Variant(integer(1), bytes(1)))
    }
    val unknown = new Variant(bytes(primitiveHeader(17)), emptyMetadata)
    checkError("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT") { VariantBuilder.isCanonical(unknown) }
    checkError("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT") { VariantBuilder.canonicalize(unknown) }
    checkError("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT") { VariantBuilder.doCanonicalize(unknown) }
  }

  test("deterministic nested permutations converge to independent expected bytes") {
    // {"a":1,"b":[1.2,{"c":"x"}]} with every permutation of a dictionary that also holds an unused
    // key, both physical field orders and all four widths must give one byte string.
    val expectedValue = obj(Seq(0 -> integer(1), 1 -> array(Seq(
      decimal("12", 1, DECIMAL4, 4), obj(Seq(2 -> string(utf8("x"))))))))
    Seq("unused", "a", "b", "c").permutations.zipWithIndex.foreach {
      case (keys, permutationIndex) =>
        Seq(Seq(0, 1), Seq(1, 0)).foreach { order =>
          val width = permutationIndex % 4 + 1
          val largeSize = permutationIndex % 2 == 0
          val child = obj(Seq(keys.indexOf("c") -> string(utf8("x"), longEncoding = true)),
            idSize = width, offsetSize = width, largeSize = largeSize)
          val elements = array(Seq(decimal("1200", 3, DECIMAL16, 16), child), width, largeSize)
          val value = obj(Seq(keys.indexOf("a") -> integer(1, INT8, 8),
            keys.indexOf("b") -> elements), width, width, largeSize, order)
          withClue(s"dictionary=${keys.mkString(",")} order=${order.mkString(",")}: ") {
            check(new Variant(value, metadata(keys.map(utf8), width)),
              expectedValue, dictionary("a", "b", "c"))
          }
        }
    }
  }

  test("canonicalization preserves distinctions between values types and array order") {
    // Values that differ, including float 1.0 vs double 1.0 and the string "1" vs binary "1", are
    // already canonical and must stay distinct.
    val differentValues = Seq(integer(1), integer(2), trueValue, nullValue,
      floating(0x3f800000L, FLOAT, 4), floating(0x3ff0000000000000L, DOUBLE, 8),
      string(utf8("1")), Array(primitiveHeader(BINARY)) ++ bytes(1, 0, 0, 0, 49),
      array(Seq(integer(1), integer(2))), array(Seq(integer(2), integer(1))))
    val differentObjects = Seq("a", "b").map { key =>
      new Variant(obj(Seq(0 -> integer(1))), dictionary(key))
    }
    val inputs = differentValues.map(value => new Variant(value, emptyMetadata)) ++ differentObjects
    val outputs = inputs.map { input =>
      val actual = check(input, input.value, input.metadata)
      (actual.value.toSeq, actual.metadata.toSeq)
    }
    assert(outputs.distinct.size == outputs.size)
  }

  private def assertSameVariant(actual: Variant, expected: Variant, label: String): Unit = {
    assertBytes(actual.value, expected.value, s"$label value")
    assertBytes(actual.metadata, expected.metadata, s"$label metadata")
  }

  // Re-encode `input` with the same meaning and random choices wherever the spec allows them: a
  // shuffled dictionary with unused keys, random count, id and offset widths, a shuffled physical
  // field order, wider integers and long-string headers. Other scalars are copied.
  private def randomizeBinaryEncoding(input: Variant, rand: Random): Variant = {
    def randomWidth(maxValue: Int): Int = {
      val minimumWidth = (1 to 4).find(width => maxValue.toLong < (1L << (8 * width))).get
      minimumWidth + rand.nextInt(5 - minimumWidth)
    }

    val numKeys = getMetadataNumKeys(input.metadata, getMetadataOffsetSize(input.metadata))
    val sourceKeys = (0 until numKeys).map(index => getMetadataKey(input.metadata, index))
    val unusedKeys = Seq.tabulate(rand.nextInt(4))(index => s"unused_$index")
    val keys = rand.shuffle((sourceKeys ++ unusedKeys).distinct)
    val keyIds = keys.zipWithIndex.toMap

    def encode(current: Variant): Array[Byte] = current.getType match {
      case Type.OBJECT =>
        val fields = (0 until current.objectSize()).map { index =>
          val field = current.getFieldAtIndex(index)
          keyIds(field.key) -> encode(field.value)
        }
        obj(fields,
          idSize = randomWidth(fields.map(_._1).maxOption.getOrElse(0)),
          offsetSize = randomWidth(fields.map(_._2.length).sum),
          largeSize = fields.size > U8_MAX || rand.nextBoolean(),
          physicalOrder = rand.shuffle(fields.indices.toVector))
      case Type.ARRAY =>
        val elements = (0 until current.arraySize()).map { index =>
          encode(current.getElementAtIndex(index))
        }
        array(elements, randomWidth(elements.map(_.length).sum),
          largeSize = elements.size > U8_MAX || rand.nextBoolean())
      case Type.LONG =>
        val number = current.getLong
        val encodings = Seq(INT1 -> 1, INT2 -> 2, INT4 -> 4, INT8 -> 8).filter {
          case (_, width) => BigInt(number).bitLength < 8 * width
        }
        val (typeInfo, width) = encodings(rand.nextInt(encodings.size))
        integer(number, typeInfo, width)
      case Type.STRING =>
        val payload = utf8(current.getString)
        string(payload, longEncoding = payload.length > MAX_SHORT_STR_SIZE || rand.nextBoolean())
      case _ => current.getValue
    }

    val value = encode(input)
    val keyBytes = keys.map(utf8)
    val offsetSize = randomWidth(math.max(keys.size, keyBytes.map(_.length).sum))
    new Variant(value, metadata(keyBytes, offsetSize))
  }

  test("fuzz: equivalent random encodings canonicalize to the same bytes") {
    // Two parses of the same random value, rendered with shuffled fields and different number
    // spellings, must canonicalize to the same bytes, as must a plain appendVariant copy.
    import VariantCanonicalizeSuite.{generate, render}
    (0 until 300).foreach { iteration =>
      val seed = 0xC0FFEEL + iteration
      val rand = new Random(seed)
      val tree = generate(rand, 2 + rand.nextInt(4))
      val first = VariantBuilder.parseJson(render(tree, rand), false)
      val second = VariantBuilder.parseJson(render(tree, rand), false)
      withClue(s"seed=$seed: ") {
        val canonical = VariantBuilder.canonicalize(first)
        assertSameVariant(VariantBuilder.canonicalize(second), canonical, "second encoding")
        assert(VariantBuilder.isCanonical(canonical))
        assertSameVariant(VariantBuilder.doCanonicalize(canonical), canonical, "rebuild")
        if (VariantBuilder.isCanonical(first)) {
          assertSameVariant(VariantBuilder.doCanonicalize(first), first, "canonical input")
        }
        val builder = new VariantBuilder(false)
        builder.appendVariant(first)
        val copy = builder.result()
        assert(isValidVariant(copy.value, copy.metadata))
        assertSameVariant(VariantBuilder.canonicalize(copy), canonical, "appendVariant copy")
      }
    }
  }

  test("fuzz: binary widths dictionaries and value layouts canonicalize to the same bytes") {
    // Random re-encodings of one value must canonicalize to the canonical bytes of that value.
    import VariantCanonicalizeSuite.{generate, render}
    (0 until 300).foreach { iteration =>
      val seed = 0xB1A4EEL + iteration
      val rand = new Random(seed)
      val tree = generate(rand, 2 + rand.nextInt(4))
      val source = VariantBuilder.parseJson(render(tree, rand), false)
      val canonical = VariantBuilder.canonicalize(source)
      (0 until 3).foreach { encodingIndex =>
        withClue(s"seed=$seed encoding=$encodingIndex: ") {
          val input = randomizeBinaryEncoding(source, rand)
          assert(isValidVariant(input.value, input.metadata))
          check(input, canonical.value, canonical.metadata)
        }
      }
    }
  }
}

// Random values for the fuzz test. Each value is rendered to JSON twice, with object fields
// shuffled and numbers spelled differently, so the two parses differ in bytes but not in meaning.
object VariantCanonicalizeSuite {
  private sealed trait FuzzValue
  private case class FuzzObject(fields: Seq[(String, FuzzValue)]) extends FuzzValue
  private case class FuzzArray(elements: Seq[FuzzValue]) extends FuzzValue
  private case class FuzzScalar(spellings: Seq[String]) extends FuzzValue

  // scalastyle:off nonascii
  private val fuzzKeys = Seq("", "a", "b", "z", "key", "\u00e9", "\ue000", "\ud800\udc00")
  private val fuzzStrings =
    Seq("", "x", "hello", "y" * 70, "\u00e9t\u00e9", "\ud800\udc00" * 20)
  // scalastyle:on nonascii

  private def generate(rand: Random, depth: Int): FuzzValue = {
    if (depth <= 0) {
      generateScalar(rand)
    } else {
      rand.nextInt(6) match {
        case 0 => generateScalar(rand)
        case 1 | 2 => FuzzArray(Seq.fill(1 + rand.nextInt(3))(generate(rand, depth - 1)))
        case _ => FuzzObject(rand.shuffle(fuzzKeys).take(1 + rand.nextInt(3))
          .map(key => key -> generate(rand, depth - 1)))
      }
    }
  }

  private def generateScalar(rand: Random): FuzzValue = rand.nextInt(10) match {
    case 0 => FuzzScalar(Seq(rand.nextBoolean().toString))
    case 1 => FuzzScalar(Seq("null"))
    case 2 => FuzzScalar(Seq("\"" + fuzzStrings(rand.nextInt(fuzzStrings.size)) + "\""))
    case 3 =>
      val n = rand.nextInt(200) - 100
      FuzzScalar(Seq(s"$n", s"$n.0", s"$n.00"))
    case 4 =>
      val n = (rand.nextInt(200) - 100) * 100003
      FuzzScalar(Seq(s"$n", s"$n.0", s"$n.000"))
    case 5 =>
      val n = rand.nextInt(99) + 1
      FuzzScalar(Seq(s"$n.5", s"$n.50", s"$n.500"))
    case 6 => FuzzScalar(Seq("100000000000000000000", "100000000000000000000.0"))
    case 7 =>
      val doubles = Seq(Seq("1.5e0", "15e-1"), Seq("-0e0", "0e0"), Seq("2.5e2", "250e0"))
      FuzzScalar(doubles(rand.nextInt(doubles.size)))
    case 8 => FuzzObject(Seq.empty)
    case _ => FuzzArray(Seq.empty)
  }

  private def render(value: FuzzValue, rand: Random): String = value match {
    case FuzzObject(fields) =>
      rand.shuffle(fields).map { case (key, field) => "\"" + key + "\":" + render(field, rand) }
        .mkString("{", ",", "}")
    case FuzzArray(elements) => elements.map(render(_, rand)).mkString("[", ",", "]")
    case FuzzScalar(spellings) => spellings(rand.nextInt(spellings.size))
  }
}
