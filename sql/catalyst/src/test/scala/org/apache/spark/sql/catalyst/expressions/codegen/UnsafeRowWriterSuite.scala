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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.math.{BigDecimal => JavaBigDecimal, BigInteger, RoundingMode}

import scala.util.{Random, Try}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.bitset.BitSetMethods
import org.apache.spark.unsafe.types._

class UnsafeRowWriterSuite extends SparkFunSuite with SQLHelper {

  test("writeNullable matches the setNullAt/write split for null and non-null inputs") {
    // Primitive: a null write sets the null bit, a non-null write stores the value.
    val intWriter = new UnsafeRowWriter(2)
    intWriter.resetRowWriter()
    intWriter.writeNullable(0, 42, false)
    intWriter.writeNullable(1, -1, true)
    val intRow = intWriter.getRow
    assert(!intRow.isNullAt(0) && intRow.getInt(0) == 42)
    assert(intRow.isNullAt(1))

    // Reference type: the staged value is ignored when isNull is set.
    val strWriter = new UnsafeRowWriter(1)
    strWriter.resetRowWriter()
    strWriter.writeNullable(0, UTF8String.fromString("ignored"), true)
    assert(strWriter.getRow.isNullAt(0))

    // Wide decimal (precision > 18): a null write must still reserve the fixed-width slot, exactly
    // like the write(ordinal, null, precision, scale) call the codegen setNull branch used.
    val decWriter = new UnsafeRowWriter(2)
    decWriter.resetRowWriter()
    decWriter.writeNullable(0, null, true, 38, 18)
    val dec = Decimal(123456789.123456789)
    assert(dec.changePrecision(38, 18))
    decWriter.writeNullable(1, dec, false, 38, 18)
    val decRow = decWriter.getRow
    assert(decRow.isNullAt(0))
    assert(decRow.getDecimal(1, 38, 18) == dec)

    // Compact decimal (precision <= 18): the null bit must be set through
    // write(ordinal, null, precision, scale)'s internal null check rather than an explicit
    // setNullAt branch, so it lands on the same setNullAt the codegen previously emitted.
    val compactDecWriter = new UnsafeRowWriter(2)
    compactDecWriter.resetRowWriter()
    compactDecWriter.writeNullable(0, null, true, 18, 2)
    val compactDec = Decimal(123456789.12)
    assert(compactDec.changePrecision(18, 2))
    compactDecWriter.writeNullable(1, compactDec, false, 18, 2)
    val compactDecRow = compactDecWriter.getRow
    assert(compactDecRow.isNullAt(0))
    assert(compactDecRow.getDecimal(1, 18, 2) == compactDec)

    // CalendarInterval: a null write reserves the variable-length slot via write(ordinal, null).
    val intervalWriter = new UnsafeRowWriter(2)
    intervalWriter.resetRowWriter()
    intervalWriter.writeNullable(0, null.asInstanceOf[CalendarInterval], true)
    val interval = new CalendarInterval(1, 2, 3L)
    intervalWriter.writeNullable(1, interval, false)
    val intervalRow = intervalWriter.getRow
    assert(intervalRow.isNullAt(0))
    assert(intervalRow.getInterval(1) == interval)

    // TimestampNanosVal: like CalendarInterval, a null write must reserve the fixed 16-byte slot
    // via write(ordinal, null), which is the reserved-slot contract the codegen setNull relied on.
    val tsNanosWriter = new UnsafeRowWriter(2)
    tsNanosWriter.resetRowWriter()
    tsNanosWriter.writeNullable(0, null.asInstanceOf[TimestampNanosVal], true)
    val tsNanos = new TimestampNanosVal(1234567L, 89.toShort)
    tsNanosWriter.writeNullable(1, tsNanos, false)
    val tsNanosRow = tsNanosWriter.getRow
    assert(tsNanosRow.isNullAt(0))
    assert(tsNanosRow.getTimestampNTZNanos(1) == tsNanos)
  }

  def checkDecimalSizeInBytes(decimal: Decimal, numBytes: Int): Unit = {
    assert(decimal.toJavaBigDecimal.unscaledValue().toByteArray.length == numBytes)
  }

  test("SPARK-25538: zero-out all bits for decimals") {
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal1, 8)

    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal2, 11)
    // On an UnsafeRowWriter we write decimal2 first and then decimal1
    val unsafeRowWriter1 = new UnsafeRowWriter(1)
    unsafeRowWriter1.resetRowWriter()
    unsafeRowWriter1.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter1.reset()
    unsafeRowWriter1.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res1 = unsafeRowWriter1.getRow
    // On a second UnsafeRowWriter we write directly decimal1
    val unsafeRowWriter2 = new UnsafeRowWriter(1)
    unsafeRowWriter2.resetRowWriter()
    unsafeRowWriter2.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res2 = unsafeRowWriter2.getRow
    // The two rows should be the equal
    assert(res1 == res2)
  }

  // A wide (precision > 18) decimal is stored in a zeroed 16-byte slot of the variable-length
  // region as the minimal big-endian two's-complement bytes of its unscaled value, i.e. what
  // BigInteger.toByteArray returns. The helpers below build that layout independently of
  // UnsafeRow and UnsafeRowWriter, so the tests can compare raw row bytes, on which UnsafeRow
  // equality and hashing depend.

  // Offset of the decimal slot in a 1-field row: after the null bit set and the field word.
  private val wideSlotOffset = 16L

  private val compactUnscaledLimit = BigInteger.TEN.pow(Decimal.MAX_LONG_DIGITS)

  private def expectedWideDecimalRow(unscaled: Option[BigInteger]): Array[Byte] = {
    val bytes = new Array[Byte](32)
    val base = Platform.BYTE_ARRAY_OFFSET
    unscaled match {
      case Some(v) =>
        val valueBytes = v.toByteArray
        Platform.copyMemory(valueBytes, base, bytes, base + wideSlotOffset, valueBytes.length)
        Platform.putLong(bytes, base + 8, (wideSlotOffset << 32) | valueBytes.length)
      case None =>
        BitSetMethods.set(bytes, base, 0)
        Platform.putLong(bytes, base + 8, wideSlotOffset << 32)
    }
    bytes
  }

  private def checkWideDecimalRow(
      row: UnsafeRow, unscaled: Option[BigInteger], precision: Int, scale: Int): Unit = {
    val clue = s"unscaled=$unscaled precision=$precision scale=$scale"
    assert(row.getBytes.sameElements(expectedWideDecimalRow(unscaled)), clue)
    unscaled match {
      case None =>
        assert(row.isNullAt(0), clue)
        assert(row.getDecimal(0, precision, scale) === null, clue)
      case Some(v) =>
        // The Decimal that the BigInteger-based read path produces.
        val expected = Decimal(new JavaBigDecimal(v, scale), precision, scale)
        val actual = row.getDecimal(0, precision, scale)
        assert(actual === expected, clue)
        assert(actual.precision === expected.precision, clue)
        assert(actual.scale === expected.scale, clue)
        assert(actual.hashCode === expected.hashCode, clue)
        assert(actual.toString === expected.toString, clue)
        assert(actual.toJavaBigDecimal === expected.toJavaBigDecimal, clue)
        // Conversions to integral types must agree too, including throwing when the value
        // does not fit. Compare outcomes (value or exception class) rather than values.
        def outcome[T](f: Decimal => T): Decimal => Any =
          d => Try(f(d)).fold(e => e.getClass, identity)
        Seq[Decimal => Any](
          outcome(_.toLong), outcome(_.toInt), outcome(_.toJavaBigInteger),
          outcome(_.roundToLong()), outcome(_.roundToInt()), outcome(_.roundToShort()),
          outcome(_.roundToByte()), outcome(_.floor), outcome(_.ceil)
        ).foreach { f => assert(f(actual) === f(expected), clue) }
        // Unscaled values that fit the compact representation are read back compact, so that
        // arithmetic on them can stay on the Long fast path. A compact Decimal's scale indexes
        // Decimal.POW_10, so it must be in [0, MAX_LONG_DIGITS].
        val expectCompact = v.abs.compareTo(compactUnscaledLimit) < 0 &&
          scale >= 0 && scale <= Decimal.MAX_LONG_DIGITS
        assert(actual.isCompact === expectCompact, clue)
    }
  }

  private def writeWideDecimal(input: Decimal, precision: Int, scale: Int): UnsafeRow = {
    val writer = new UnsafeRowWriter(1)
    writer.resetRowWriter()
    writer.write(0, input, precision, scale)
    writer.getRow
  }

  // Unscaled values around every change of the encoded byte count (+-2^(8k-1) and its
  // neighbours, which include Long.MinValue and Long.MaxValue) and around the compact limit.
  private val boundaryUnscaledValues: Seq[BigInteger] = {
    val magnitudes = Seq(
      BigInteger.ZERO,
      BigInteger.ONE,
      compactUnscaledLimit.subtract(BigInteger.ONE),
      compactUnscaledLimit,
      compactUnscaledLimit.add(BigInteger.ONE)) ++
      (1 to 16).flatMap { k =>
        val p = BigInteger.ONE.shiftLeft(8 * k - 1)
        Seq(p.subtract(BigInteger.ONE), p, p.add(BigInteger.ONE))
      }
    magnitudes.flatMap(m => Seq(m, m.negate())).distinct
  }

  test("SPARK-59805: wide decimals are written and read identically to their BigInteger encoding") {
    val rand = new Random(20260927L)
    for {
      precision <- (Decimal.MAX_LONG_DIGITS + 1) to DecimalType.MAX_PRECISION
      scale <- Seq(0, 1, 2, 6, 10, 18, precision - 1, precision).distinct
    } {
      val limit = BigInteger.TEN.pow(precision)
      val randomValues = Seq.fill(100) {
        val v = new BigInteger(1 + rand.nextInt(limit.bitLength()), rand.self).mod(limit)
        if (rand.nextBoolean()) v.negate() else v
      }
      val maxValue = limit.subtract(BigInteger.ONE)
      val values = (boundaryUnscaledValues ++ Seq(maxValue, maxValue.negate()) ++ randomValues)
        .filter(_.abs.compareTo(limit) < 0)
      values.foreach { v =>
        val inputs = Seq(Decimal(new JavaBigDecimal(v, scale), precision, scale)) ++ {
          if (v.abs.compareTo(compactUnscaledLimit) < 0) {
            val compact = Decimal(v.longValueExact(), precision, scale)
            assert(compact.isCompact)
            Seq(compact)
          } else {
            Nil
          }
        }
        inputs.foreach { input =>
          checkWideDecimalRow(writeWideDecimal(input.clone(), precision, scale), Some(v),
            precision, scale)
          // Update in place a slot that held the widest value of this type.
          val row = writeWideDecimal(
            Decimal(new JavaBigDecimal(maxValue.negate(), scale), precision, scale),
            precision, scale)
          row.setDecimal(0, input.clone(), precision)
          checkWideDecimalRow(row, Some(v), precision, scale)
        }
      }
    }
  }

  test("SPARK-59805: wide decimals are rescaled by UnsafeRowWriter before encoding") {
    val rand = new Random(20260928L)
    for {
      precision <- Seq(19, 20, 25, 38)
      scale <- Seq(1, 2, 10, 18)
      _ <- 1 to 200
    } {
      val unscaled = rand.nextLong() % Decimal.POW_10(Decimal.MAX_LONG_DIGITS)
      // A larger input scale rounds HALF_UP; a smaller one multiplies the unscaled value, which
      // may leave the compact range.
      Seq(scale + 1, scale - 1).foreach { inputScale =>
        val input = Decimal(unscaled, precision, inputScale)
        val expected = new JavaBigDecimal(BigInteger.valueOf(unscaled), inputScale)
          .setScale(scale, RoundingMode.HALF_UP).unscaledValue()
        checkWideDecimalRow(writeWideDecimal(input, precision, scale), Some(expected),
          precision, scale)
      }
    }
  }

  test("SPARK-59805: wide decimals: overflow and null set the null bit and keep the slot " +
      "updatable") {
    for (precision <- Seq(19, 25, 38); scale <- Seq(0, 2)) {
      val limit = BigInteger.TEN.pow(precision)
      def tooLarge: Decimal = Decimal(new JavaBigDecimal(limit, scale))
      val small = BigInteger.valueOf(-12345L)
      val large = limit.subtract(BigInteger.ONE)
      def compactSmall: Decimal = Decimal(small.longValueExact(), precision, scale)
      def bigLarge: Decimal = Decimal(new JavaBigDecimal(large, scale), precision, scale)

      checkWideDecimalRow(writeWideDecimal(tooLarge, precision, scale), None, precision, scale)
      checkWideDecimalRow(writeWideDecimal(null, precision, scale), None, precision, scale)

      val row = writeWideDecimal(bigLarge, precision, scale)
      row.setDecimal(0, tooLarge, precision)
      checkWideDecimalRow(row, None, precision, scale)
      row.setDecimal(0, compactSmall, precision)
      checkWideDecimalRow(row, Some(small), precision, scale)
      row.setDecimal(0, null, precision)
      checkWideDecimalRow(row, None, precision, scale)
      row.setDecimal(0, bigLarge, precision)
      checkWideDecimalRow(row, Some(large), precision, scale)
      row.setDecimal(0, compactSmall, precision)
      checkWideDecimalRow(row, Some(small), precision, scale)
    }
  }

  test("SPARK-59805: wide decimals with a negative scale are read back BigDecimal-backed") {
    // A negative scale (legacy) must not produce a compact Decimal: toLong and the roundTo*
    // conversions of a compact Decimal would index Decimal.POW_10 with the negative scale.
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      for (precision <- Seq(19, 25, 38); scale <- Seq(-1, -2, -10)) {
        Seq(0L, 7L, -12345L, 99L, 999999999999999999L, Long.MaxValue, Long.MinValue)
          .map(BigInteger.valueOf).foreach { v =>
            val inputs = Seq(Decimal(new JavaBigDecimal(v, scale), precision, scale)) ++ {
              if (v.abs.compareTo(compactUnscaledLimit) < 0) {
                Seq(Decimal(v.longValueExact(), precision, scale))
              } else {
                Nil
              }
            }
            inputs.foreach { input =>
              checkWideDecimalRow(writeWideDecimal(input.clone(), precision, scale), Some(v),
                precision, scale)
              val row = writeWideDecimal(
                Decimal(new JavaBigDecimal(BigInteger.TEN.pow(precision).subtract(
                  BigInteger.ONE), scale), precision, scale),
                precision, scale)
              row.setDecimal(0, input.clone(), precision)
              checkWideDecimalRow(row, Some(v), precision, scale)
            }
          }
      }
    }
  }

  test("write and get geography through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geography = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geography)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geography.getBytes)
  }

  test("write and get geometry through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geometry = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geometry)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geometry.getBytes)
  }

  test("write and get calendar intervals through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.write(0, null.asInstanceOf[CalendarInterval])
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getInterval(0) === null)
    val interval = new CalendarInterval(0, 1, 0)
    rowWriter.write(1, interval)
    assert(rowWriter.getRow.getInterval(1) === interval)
  }

  test("write and get variant through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getVariant(0) === null)
    val variant = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](-1, -2, -3, -4))
    rowWriter.write(1, variant)
    assert(rowWriter.getRow.getVariant(1).debugString() == variant.debugString())
  }
}
