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

  // The input is canonical exactly when it already has the expected bytes.
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

  test("canonical encoding has pinned value and metadata bytes") {
    checkKeyless(bytes(24, 1, 0, 0, 0, 0, 0, 0, 0), bytes(12, 1))
    checkKeyless(bytes(64, 1, 0, 0, 0, 120), bytes(5, 120))
    check(VariantBuilder.parseJson("{\"b\":2,\"a\":1}", false),
      bytes(2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 2), bytes(1, 2, 0, 1, 2, 97, 98))
    checkKeyless(bytes(2, 0, 0), bytes(2, 0, 0))
    checkKeyless(bytes(3, 0, 0), bytes(3, 0, 0))
  }

  test("metadata helpers encode and decode every offset-width header") {
    Seq((1, 0x01), (2, 0x41), (3, 0x81), (4, 0xc1)).foreach {
      case (width, header) =>
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
  }

  test("getMetadataKey rejects a dictionary size whose string start overflows an int") {
    val forged = Array(metadataHeader(4)) ++ littleEndian(BigInt(Int.MaxValue - 1), 4) ++
      littleEndian(BigInt(0), 4) ++ littleEndian(BigInt(1), 4) ++ utf8("a")
    checkError("MALFORMED_VARIANT") { getMetadataKey(forged, 0) }
  }

  test("dictionary permutation and object payload order canonicalize independently") {
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
    val sourceMetadata = dictionary("b", "unused", "a", "a")
    val sourceValue = array(Seq(
      obj(Seq(2 -> integer(1))), obj(Seq(3 -> integer(2))), obj(Seq(0 -> integer(3)))))
    val expectedValue = array(Seq(
      obj(Seq(0 -> integer(1))), obj(Seq(0 -> integer(2))), obj(Seq(1 -> integer(3)))))
    check(new Variant(sourceValue, sourceMetadata), expectedValue, dictionary("a", "b"))
    check(new Variant(integer(1), sourceMetadata), integer(1))
    check(new Variant(integer(1), metadata(Seq(bytes(0xff)))), integer(1))
    check(new Variant(obj(Seq(0 -> integer(1))), bytes(1, 1, 1, 2, 120, 97)),
      obj(Seq(0 -> integer(1))), dictionary("a"))
    val emptyKey = obj(Seq(0 -> nullValue, 1 -> integer(1)))
    check(new Variant(emptyKey, dictionary("", "a")), emptyKey, dictionary("", "a"))
  }

  test("duplicate dictionary entries and repeated field IDs are never canonical") {
    check(new Variant(array(Seq(obj(Seq(0 -> integer(1))), obj(Seq(1 -> integer(2))))),
      dictionary("a", "a")),
      array(Seq(obj(Seq(0 -> integer(1))), obj(Seq(0 -> integer(2))))), dictionary("a"))
    val repeatedId = new Variant(obj(Seq(0 -> integer(1), 0 -> integer(2))), dictionary("a"))
    assert(!VariantBuilder.isCanonical(repeatedId))
    checkError("VARIANT_DUPLICATE_KEY") { VariantBuilder.canonicalize(repeatedId) }
  }

  test("dictionary and fields use unsigned UTF-8 rather than UTF-16 ordering") {
    val bmpKey = utf8("\ue000")
    val supplementaryKey = utf8("\ud800\udc00")
    val expectedMetadata = metadata(Seq(bmpKey, supplementaryKey))
    val expectedValue = obj(Seq(0 -> integer(10), 1 -> integer(20)))
    Seq(Seq(0 -> integer(20), 1 -> integer(10)),
      Seq(1 -> integer(10), 0 -> integer(20))).foreach { fields =>
      check(new Variant(obj(fields), metadata(Seq(supplementaryKey, bmpKey))),
        expectedValue, expectedMetadata)
    }
  }

  test("valid multi-byte UTF-8 keys are kept and sorted by their bytes") {
    check(new Variant(obj(Seq(2 -> integer(1), 1 -> integer(2), 0 -> integer(3))),
      dictionary("\u4f60", "caf\u00e9", "cafe")),
      obj(Seq(0 -> integer(1), 1 -> integer(2), 2 -> integer(3))),
      dictionary("cafe", "caf\u00e9", "\u4f60"))
  }

  test("all independent metadata ID offset and count widths converge") {
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

  test("metadata flags and trailing bytes are not part of canonical output") {
    Seq(1 << 4, 1 << 5, (1 << 4) | (1 << 5)).foreach { flags =>
      check(new Variant(integer(1), metadata(Seq.empty, flags = flags)), integer(1))
    }
    checkKeyless(integer(1) ++ bytes(0, 0), integer(1))
    check(new Variant(integer(1), emptyMetadata ++ bytes(0, 0)), integer(1))
  }

  test("reserved container header bits are cleared during normalization") {
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

  test("padding and shared payloads inside containers become contiguous") {
    val paddedArray = Array(arrayHeader(false, 1)) ++ bytes(1, 0, 3) ++ integer(1) ++ bytes(0)
    checkKeyless(paddedArray, array(Seq(integer(1))))
    val paddedObject =
      Array(objectHeader(false, 1, 1)) ++ bytes(1, 0, 0, 3) ++ integer(1) ++ bytes(0)
    check(new Variant(paddedObject, dictionary("a")), obj(Seq(0 -> integer(1))), dictionary("a"))
    val shared = Array(objectHeader(false, 1, 1)) ++ bytes(2, 0, 1, 0, 0, 2) ++ integer(7)
    check(new Variant(shared, dictionary("a", "b")),
      obj(Seq(0 -> integer(7), 1 -> integer(7))), dictionary("a", "b"))
  }

  test("container count and payload widths change at 255 and 256") {
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
  }

  test("object ID width follows the largest referenced global ID") {
    val keys = (0 to 256).map(index => utf8(f"key$index%03d"))
    val sourceElements = keys.indices.map(index => obj(Seq(index -> nullValue), 4, 4, true))
    val expectedElements = keys.indices.map { index =>
      obj(Seq(index -> nullValue), idSize = if (index <= U8_MAX) 1 else 2)
    }
    val expectedValue = array(expectedElements, 2, largeSize = true)
    check(new Variant(array(sourceElements, 4, largeSize = true), metadata(keys, 4)),
      expectedValue, metadata(keys, 2))
    check(new Variant(obj(Seq(256 -> integer(1)), 2), metadata(keys, 2)),
      obj(Seq(0 -> integer(1))), metadata(Seq(keys.last)))
  }

  test("metadata and container offsets have real one two and three byte boundaries") {
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
    val cases = Seq(
      ("0", 30, integer(0)),
      ("100", 2, integer(1)),
      ("-100", 2, integer(-1)),
      ("100000", 2, integer(1000, INT2, 2)),
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

  test("float zero NaN payloads infinities and finite values normalize within their type") {
    val cases = Seq(
      (0x00000000L, 0x00000000L), (0x80000000L, 0x00000000L),
      (0x7fc00000L, 0x7fc00000L), (0x7f800001L, 0x7fc00000L),
      (0x7fffffffL, 0x7fc00000L), (0xffc01234L, 0x7fc00000L),
      (0x7f800000L, 0x7f800000L), (0xff800000L, 0xff800000L),
      (0x00000001L, 0x00000001L), (0x00800000L, 0x00800000L),
      (0x7f7fffffL, 0x7f7fffffL), (0x3f800000L, 0x3f800000L),
      (0xbf800000L, 0xbf800000L))
    cases.foreach { case (rawBits, expectedBits) =>
      checkKeyless(floating(rawBits, FLOAT, 4), floating(expectedBits, FLOAT, 4))
    }
  }

  test("double zero NaN payloads infinities and finite values normalize within their type") {
    val cases = Seq(
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
    cases.foreach { case (rawBits, expectedBits) =>
      checkKeyless(floating(rawBits, DOUBLE, 8), floating(expectedBits, DOUBLE, 8))
    }
  }

  test("strings choose headers by UTF-8 byte length and preserve valid bytes") {
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

  test("invalid UTF-8 string replacement can require a long-string header") {
    Seq((bytes(0xff), "\ufffd"), (bytes(97, 0xff, 98), "a\ufffdb"),
      (bytes(0x80), "\ufffd"), (bytes(0xc0, 0xaf), "\ufffd\ufffd"),
      (bytes(0xed, 0xa0, 0x80), "\ufffd"), (bytes(0xe2, 0x82), "\ufffd"),
      (Array.fill[Byte](22)(0xff.toByte), "\ufffd" * 22))
      .foreach { case (payload, decoded) =>
        val normalized = utf8(decoded)
        val expected = string(normalized, longEncoding = normalized.length > MAX_SHORT_STR_SIZE)
        Seq(false, true).foreach { longEncoding =>
          checkKeyless(string(payload, longEncoding), expected)
        }
      }
  }

  test("invalid UTF-8 key normalization reorders both dictionary and field payloads") {
    val sourceMetadata = metadata(Seq(bytes(0xf0, 97), bytes(0xf5)))
    val sourceValue = obj(Seq(0 -> integer(1), 1 -> integer(2)))
    val expectedValue = obj(Seq(0 -> integer(2), 1 -> integer(1)))
    val actual = check(new Variant(sourceValue, sourceMetadata), expectedValue,
      dictionary("\ufffd", "\ufffda"))
    assert(actual.getFieldByKey("\ufffd").getLong == 2)
    assert(actual.getFieldByKey("\ufffda").getLong == 1)
    val expandedKey = obj(Seq(0 -> integer(1)))
    check(new Variant(expandedKey, metadata(Seq(Array.fill[Byte](86)(0xff.toByte)))),
      expandedKey, metadata(Seq(utf8("\ufffd" * 86)), 2))
  }

  test("replacement key collisions deduplicate across objects but fail within one object") {
    val sourceMetadata = metadata(Seq(bytes(0xfe), bytes(0xff)))
    val sourceValue = array(Seq(obj(Seq(0 -> integer(1))), obj(Seq(1 -> integer(2)))))
    check(new Variant(sourceValue, sourceMetadata),
      array(Seq(obj(Seq(0 -> integer(1))), obj(Seq(0 -> integer(2))))), dictionary("\ufffd"))
    val duplicate = new Variant(obj(Seq(0 -> integer(1), 1 -> integer(2))), sourceMetadata)
    assert(!VariantBuilder.isCanonical(duplicate))
    checkError("VARIANT_DUPLICATE_KEY") { VariantBuilder.canonicalize(duplicate) }
    checkError("VARIANT_DUPLICATE_KEY") { VariantBuilder.doCanonicalize(duplicate) }
  }

  test("nested objects arrays and views retain only keys used by the selected value") {
    val parent = VariantBuilder.parseJson(
      "{\"outside\":0,\"target\":{\"a\":[1,2],\"b\":3}}", false)
    val target = parent.getFieldByKey("target")
    assert(target.pos != 0)
    check(target, obj(Seq(0 -> array(Seq(integer(1), integer(2))), 1 -> integer(3))),
      dictionary("a", "b"))
    check(target.getFieldByKey("a"), array(Seq(integer(1), integer(2))))
    check(target.getFieldByKey("a").getElementAtIndex(1), integer(2))
    val arrayParent = VariantBuilder.parseJson("[0,{\"a\":1},2]", false)
    check(arrayParent.getElementAtIndex(1), obj(Seq(0 -> integer(1))), dictionary("a"))
    val nestedValue = obj(Seq(0 -> array(Seq(obj(Seq.empty), array(Seq.empty)))))
    check(new Variant(nestedValue, dictionary("a")), nestedValue, dictionary("a"))
  }

  test("deeply nested containers canonicalize at every level") {
    def nest(leaf: Array[Byte], minimalWidths: Boolean): Array[Byte] =
      (0 until 500).foldLeft(leaf) { (child, _) =>
        array(Seq(child), if (!minimalWidths) 4 else if (child.length <= U8_MAX) 1 else 2)
      }
    checkKeyless(nest(decimal("100", 2, DECIMAL4, 4), minimalWidths = false),
      nest(integer(1), minimalWidths = true))
  }

  test("single-encoding scalar types preserve their payloads through a rebuild") {
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

  test("ordinary appendVariant preserves noncanonical scalar encodings recursively") {
    val scalars = Seq(integer(1, INT8, 8), decimal("1200", 3, DECIMAL16, 16),
      floating(0x80000000L, FLOAT, 4), floating(0x7f800001L, FLOAT, 4),
      floating(0xfff8123456789abcL, DOUBLE, 8), string(utf8("x"), longEncoding = true),
      string(bytes(0xff)))
    val nested = obj(Seq(1 -> array(scalars), 0 -> scalars.head), physicalOrder = Seq(1, 0))
    val source = new Variant(array(scalars :+ nested), dictionary("b", "a"))
    val originalValue = source.value.clone()
    val originalMetadata = source.metadata.clone()
    val builder = new VariantBuilder(false)
    builder.appendVariant(source)
    val copied = builder.result()
    def compareScalars(original: Variant, actual: Variant): Unit = {
      assert(actual.getType == original.getType)
      original.getType match {
        case Type.OBJECT =>
          assert(actual.objectSize() == original.objectSize())
          (0 until original.objectSize()).foreach { index =>
            val field = original.getFieldAtIndex(index)
            compareScalars(field.value, actual.getFieldByKey(field.key))
          }
        case Type.ARRAY =>
          assert(actual.arraySize() == original.arraySize())
          (0 until original.arraySize()).foreach { index =>
            compareScalars(original.getElementAtIndex(index), actual.getElementAtIndex(index))
          }
        case _ => assertBytes(actual.getValue, original.getValue, "copied scalar")
      }
    }
    compareScalars(source, copied)
    assertBytes(source.value, originalValue, "unchanged appended value")
    assertBytes(source.metadata, originalMetadata, "unchanged appended metadata")
  }

  test("ordinary appendVariant preserves existing dictionary entries and field write order") {
    val bmpKey = utf8("\ue000")
    val supplementaryKey = utf8("\ud800\udc00")
    val firstScalar = integer(1, INT8, 8)
    val secondScalar = integer(2, INT8, 8)
    val sourceValue = obj(Seq(0 -> firstScalar, 1 -> secondScalar))
    val sourceMetadata = metadata(Seq(supplementaryKey, bmpKey))
    val source = new Variant(sourceValue.clone(), sourceMetadata.clone())
    val builder = new VariantBuilder(false)
    assert(builder.addKey("existing") == 0)
    builder.appendVariant(source)
    val copied = builder.result()
    assertBytes(copied.metadata, metadata(Seq(utf8("existing"), supplementaryKey, bmpKey)),
      "preserved dictionary insertion order")
    assertBytes(copied.value,
      obj(Seq(2 -> secondScalar, 1 -> firstScalar), physicalOrder = Seq(1, 0)),
      "preserved source field write order")
    assertBytes(source.value, sourceValue, "unchanged source field encoding")
    assertBytes(source.metadata, sourceMetadata, "unchanged source dictionary")
  }

  test("malformed structures fail with the documented error classes") {
    val malformed = Seq(
      new Variant(bytes(primitiveHeader(INT8), 0), emptyMetadata),
      new Variant(bytes(shortStrHeader(2), 97), emptyMetadata),
      new Variant(bytes(primitiveHeader(LONG_STR), 2, 0, 0, 0, 97),
        metadata(Seq.empty, 2)),
      new Variant(bytes(primitiveHeader(DECIMAL16), 0), emptyMetadata),
      new Variant(decimal("1", 39, DECIMAL16, 16), emptyMetadata),
      new Variant(decimal("1000000000", 0, DECIMAL4, 4), emptyMetadata),
      new Variant(array(Seq(bytes(primitiveHeader(INT8), 0))), emptyMetadata),
      new Variant(obj(Seq(0 -> nullValue)), emptyMetadata),
      new Variant(bytes(arrayHeader(false, 1), 1, 5, 6, 0), emptyMetadata),
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

  test("fuzz: equivalent random encodings canonicalize to the same bytes") {
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
}

// Random values for the fuzz test. Each value is rendered to JSON twice, with object fields
// shuffled and numbers spelled differently, so the two parses differ in bytes but not in meaning.
object VariantCanonicalizeSuite {
  private sealed trait FuzzValue
  private case class FuzzObject(fields: Seq[(String, FuzzValue)]) extends FuzzValue
  private case class FuzzArray(elements: Seq[FuzzValue]) extends FuzzValue
  private case class FuzzScalar(spellings: Seq[String]) extends FuzzValue

  private val fuzzKeys = Seq("", "a", "b", "z", "key", "\u00e9", "\ue000", "\ud800\udc00")
  private val fuzzStrings =
    Seq("", "x", "hello", "y" * 70, "\u00e9t\u00e9", "\ud800\udc00" * 20)

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
