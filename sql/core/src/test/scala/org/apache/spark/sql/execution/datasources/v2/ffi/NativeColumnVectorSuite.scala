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

package org.apache.spark.sql.execution.datasources.v2.ffi

import java.math.{BigDecimal => JavaBigDecimal}
import java.nio.charset.StandardCharsets

import scala.util.{Failure, Success, Try, Using}

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.spark.{SparkArithmeticException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, ReusedArrowColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.sql.vectorized.ArrowColumnVector

/**
 * Checks that a [[NativeColumnVector]] reads the nulls and values that an [[ArrowColumnVector]]
 * of the same Arrow vector reads.
 */
class NativeColumnVectorSuite extends SparkFunSuite {

  // The rows of the batches, more than 8 so that the validity bitmaps have several bytes, and the
  // rows that are null.
  private val rows = 0 until 20
  private val nulls = Set(1, 8, 9, 15)

  private def withAllocator(f: BufferAllocator => Unit): Unit = {
    Using.resource(new RootAllocator())(f)
  }

  /** The column vector of `vector`, refreshed as the reader refreshes it after an import. */
  private def native(vector: ValueVector): NativeColumnVector = {
    val column = NativeColumnVector.create(vector)
    column.refresh()
    column
  }

  /** Checks that `column` reads the nulls and the values of `vector` as ArrowColumnVector does. */
  private def checkSame(vector: ValueVector, column: ArrowColumnVector)(
      value: (ArrowColumnVector, Int) => Any): Unit = {
    val expected = new ArrowColumnVector(vector)
    (0 until vector.getValueCount).foreach { row =>
      assert(column.isNullAt(row) === expected.isNullAt(row), s"row $row")
      if (!expected.isNullAt(row)) {
        assert(value(column, row) === value(expected, row), s"row $row")
      }
    }
  }

  /** Sets the rows of a vector: nulls, and `set` for the other rows. */
  private def fill[V <: BaseFixedWidthVector](vector: V)(set: (V, Int) => Unit): V = {
    vector.allocateNew(rows.size)
    rows.foreach { row => if (nulls(row)) vector.setNull(row) else set(vector, row) }
    vector.setValueCount(rows.size)
    vector
  }

  /** Checks a vector whose values the column vector reads at the addresses of its buffers. */
  private def checkDirect(vector: ValueVector)(value: (ArrowColumnVector, Int) => Any): Unit = {
    Using.resource(vector) { vector =>
      val column = native(vector)
      assert(column.getClass !== classOf[NativeColumnVector], vector.getClass.getName)
      checkSame(vector, column)(value)
    }
  }

  test("booleans, integers, floats, dates and timestamps") {
    withAllocator { allocator =>
      val bytes = Seq(Byte.MinValue, -1: Byte, 0: Byte, 1: Byte, Byte.MaxValue)
      val shorts = Seq(Short.MinValue, -1: Short, 0: Short, 1: Short, Short.MaxValue)
      val ints = Seq(Int.MinValue, -1, 0, 1, Int.MaxValue)
      val longs = Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue)
      val floats = Seq(0.0f, -0.0f, Float.NaN, Float.NegativeInfinity, Float.MinValue, 1.5f)
      val doubles = Seq(0.0, -0.0, Double.NaN, Double.PositiveInfinity, Double.MaxValue, -2.5)

      checkDirect(fill(new BitVector("b", allocator))((v, row) => v.set(row, row % 3 % 2))) {
        _.getBoolean(_)
      }
      checkDirect(fill(new TinyIntVector("i8", allocator)) { (v, row) =>
        v.set(row, bytes(row % bytes.size))
      })(_.getByte(_))
      checkDirect(fill(new SmallIntVector("i16", allocator)) { (v, row) =>
        v.set(row, shorts(row % shorts.size))
      })(_.getShort(_))
      checkDirect(fill(new IntVector("i32", allocator))((v, row) => v.set(row, ints(row % 5)))) {
        _.getInt(_)
      }
      checkDirect(fill(new DateDayVector("day", allocator))((v, row) => v.set(row, row - 9))) {
        _.getInt(_)
      }
      checkDirect(fill(new BigIntVector("i64", allocator)) { (v, row) =>
        v.set(row, longs(row % longs.size))
      })(_.getLong(_))
      checkDirect(fill(new TimeStampMicroTZVector("ts", allocator, "UTC")) { (v, row) =>
        v.set(row, longs(row % longs.size))
      })(_.getLong(_))
      checkDirect(fill(new TimeStampMicroVector("ntz", allocator)) { (v, row) =>
        v.set(row, longs(row % longs.size))
      })(_.getLong(_))
      // The bits, so that NaN equals NaN.
      checkDirect(fill(new Float4Vector("f32", allocator)) { (v, row) =>
        v.set(row, floats(row % floats.size))
      })((column, row) => java.lang.Float.floatToRawIntBits(column.getFloat(row)))
      checkDirect(fill(new Float8Vector("f64", allocator)) { (v, row) =>
        v.set(row, doubles(row % doubles.size))
      })((column, row) => java.lang.Double.doubleToRawLongBits(column.getDouble(row)))
    }
  }

  test("strings and string views, in several batches") {
    // Values that a view holds, of up to 12 bytes, and longer ones that a data buffer holds, with
    // characters of 2 bytes in UTF-8.
    val (acute, grave) = (0xe9.toChar.toString, 0xe8.toChar.toString)
    val short = Seq(Some(""), Some("a"), None, Some("twelve bytes"), Some(acute + grave), None)
    val long = Seq(Some("thirteen byte"), None, Some(acute * 40), Some("a long value " * 20))
    // Values that do not fit in one data buffer of a view vector, which holds them in several.
    val spread = Seq(Some("a" * 100000), None, Some(acute * 50000), Some("c" * 100000))
    withAllocator { allocator =>
      def check(vector: FieldVector)(set: (Int, Array[Byte]) => Unit, setNull: Int => Unit)
          : Unit = {
        Using.resource(vector) { vector =>
          val column = NativeColumnVector.create(vector)
          assert(column.getClass !== classOf[NativeColumnVector], vector.getClass.getName)
          // Batches without and with data buffers, with values of both kinds, and with values in
          // several data buffers.
          Seq(short, long, short ++ long, spread).foreach { values =>
            vector.clear()
            vector.allocateNew()
            values.zipWithIndex.foreach {
              case (Some(value), row) => set(row, value.getBytes(StandardCharsets.UTF_8))
              case (None, row) => setNull(row)
            }
            vector.setValueCount(values.size)
            vector match {
              case views: ViewVarCharVector if values eq spread =>
                assert(views.getDataBuffers.size > 1)
              case _ =>
            }
            column.refresh()
            checkSame(vector, column)(_.getUTF8String(_))
            assert(column.getUTF8String(values.indexOf(None)) === null)
          }
        }
      }
      val strings = new VarCharVector("s", allocator)
      check(strings)(strings.setSafe(_, _), strings.setNull)
      val views = new ViewVarCharVector("v", allocator)
      check(views)(views.setSafe(_, _), views.setNull)
    }
  }

  private def withDecimalVector(precision: Int, scale: Int)(f: DecimalVector => Unit): Unit = {
    withAllocator { allocator =>
      Using.resource(new DecimalVector("decimal", allocator, precision, scale))(f)
    }
  }

  /**
   * Checks that `column` returns the decimal of a row that `expected`, an [[ArrowColumnVector]] of
   * the same vector, returns, as Spark uses it, or fails as it does. Its integral conversions are
   * those of the decimal of Spark's own column vectors, which hold the decimals that a long holds,
   * of a scale from 0 to their precision, as longs, while ArrowColumnVector holds BigDecimals,
   * which Decimal rounds to bytes, shorts and ints through doubles.
   */
  private def checkDecimal(
      expected: ArrowColumnVector,
      column: ArrowColumnVector,
      row: Int,
      precision: Int,
      scale: Int): Unit = {
    def result(decimal: Decimal): (JavaBigDecimal, Int, Int, Long, UnsafeRow) = {
      val writer = new UnsafeRowWriter(1)
      writer.write(0, decimal, precision, scale)
      (decimal.toJavaBigDecimal, decimal.precision, decimal.scale, decimal.toLong, writer.getRow)
    }
    def rounded(decimal: Decimal): Seq[Either[String, Long]] = {
      Seq[Decimal => Long](_.roundToByte(), _.roundToShort(), _.roundToInt(), _.roundToLong())
        .map(round => Try(round(decimal)).toEither.left.map(e => s"${e.getClass}: ${e.getMessage}"))
    }
    def own(decimal: Decimal): Decimal = {
      if (0 <= scale && scale <= precision && precision <= Decimal.MAX_LONG_DIGITS) {
        Decimal.createUnsafe(decimal.toUnscaledLong, precision, scale)
      } else {
        decimal
      }
    }
    Try(result(expected.getDecimal(row, precision, scale))) match {
      case Success(value) =>
        val decimal = column.getDecimal(row, precision, scale)
        assert(result(decimal) === value)
        assert(rounded(decimal) === rounded(own(expected.getDecimal(row, precision, scale))))
      case Failure(error) =>
        val actual = intercept[Exception](result(column.getDecimal(row, precision, scale)))
        assert(actual.getClass === error.getClass)
        assert(actual.getMessage === error.getMessage)
    }
  }

  test("decimals in and out of their precision, at other precisions and scales") {
    for (precision <- 1 to Decimal.MAX_LONG_DIGITS; scale <- Set(0, precision / 2, precision)) {
      withDecimalVector(precision, scale) { vector =>
        val limit = BigInt(10).pow(precision)
        // The bounds of the precision, values out of it, which the byte setter permits, and
        // values that a long does not hold.
        val values = Seq(BigInt(0), BigInt(1), BigInt(-1), limit - 1, -(limit - 1), limit, -limit,
          BigInt(Long.MaxValue), BigInt(Long.MinValue), (BigInt(1) << 64) + 7)
        vector.allocateNew(values.size + 1)
        values.zipWithIndex.foreach { case (value, row) =>
          vector.setBigEndian(row, value.toByteArray)
        }
        vector.setNull(values.size)
        vector.setValueCount(values.size + 1)
        val expected = new ArrowColumnVector(vector)
        val column = native(vector)
        assert(column.getClass !== classOf[NativeColumnVector])
        val requested = Seq((precision, scale), (Decimal.MAX_LONG_DIGITS, scale), (38, scale),
          (precision + 1, scale), (precision, scale + 1))
        for (row <- values.indices; (p, s) <- requested) {
          checkDecimal(expected, column, row, p, s)
        }
        assert(column.getDecimal(values.size, precision, scale) === null)
      }
    }
  }

  test("decimals of a negative scale") {
    val conf = new SQLConf
    val key = SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key
    SQLConf.withExistingConf(conf) {
      conf.setConfString(key, "true")
      withDecimalVector(5, -2) { vector =>
        vector.allocateNew(1)
        vector.set(0, new JavaBigDecimal("1.23E+4"))
        vector.setValueCount(1)
        val expected = new ArrowColumnVector(vector)
        val column = native(vector)
        Seq((5, -2), (8, 0), (5, -3)).foreach { case (precision, scale) =>
          checkDecimal(expected, column, 0, precision, scale)
        }
        assert(column.getDecimal(0, 5, -2).toLong === 12300L)
        conf.setConfString(key, "false")
        checkDecimal(expected, column, 0, 5, -2)
      }
    }
  }

  test("decimals that a long holds round to integers as those of Spark's own column vectors") {
    // The decimals of Spark's own column vectors and of the native one, of OnHeapColumnVector and
    // NativeColumnVector, hold longs, which Decimal rounds exactly, and those of ArrowColumnVector
    // BigDecimals, which it rounds through doubles.
    def decimals(value: String, scale: Int)(f: (Decimal, Decimal, Decimal) => Unit): Unit = {
      val precision = Decimal.MAX_LONG_DIGITS
      withDecimalVector(precision, scale) { vector =>
        vector.allocateNew(1)
        vector.set(0, new JavaBigDecimal(value))
        vector.setValueCount(1)
        Using.resource(new OnHeapColumnVector(1, DecimalType(precision, scale))) { own =>
          own.putDecimal(0, Decimal(value), precision)
          f(own.getDecimal(0, precision, scale), native(vector).getDecimal(0, precision, scale),
            new ArrowColumnVector(vector).getDecimal(0, precision, scale))
        }
      }
    }
    decimals("123456789.999999999", 9) { (own, column, arrow) =>
      assert(own.roundToInt() === 123456789)
      assert(column.roundToInt() === 123456789)
      assert(arrow.roundToInt() === 123456790)
    }
    decimals("127.999999999999999", 15) { (own, column, arrow) =>
      assert(own.roundToByte() === 127.toByte)
      assert(column.roundToByte() === 127.toByte)
      assert(intercept[SparkArithmeticException](arrow.roundToByte()).getCondition ===
        "CAST_OVERFLOW")
    }
  }

  test("reads the buffers of the batch that the reader imported last") {
    withAllocator { allocator =>
      Using.resource(new BigIntVector("id", allocator)) { vector =>
        val column = NativeColumnVector.create(vector)
        // Each batch has other buffers: with nulls, without nulls, empty, and larger.
        val batches = Seq(
          Seq(Some(1L), None, Some(3L)),
          Seq(Some(4L), Some(5L)),
          Seq.empty[Option[Long]],
          (0L until 1000L).map(id => Option.when(id % 7 != 0)(id)))
        batches.foreach { values =>
          vector.clear()
          vector.allocateNew(values.size)
          values.zipWithIndex.foreach {
            case (Some(value), row) => vector.set(row, value)
            case (None, row) => vector.setNull(row)
          }
          vector.setValueCount(values.size)
          column.refresh()
          checkSame(vector, column)(_.getLong(_))
        }
      }
    }
  }

  test("other types, through ArrowColumnVector") {
    withAllocator { allocator =>
      Using.resource(new VarBinaryVector("bytes", allocator)) { vector =>
        vector.allocateNew()
        vector.setSafe(0, Array[Byte](1, 2))
        vector.setNull(1)
        vector.setValueCount(2)
        val column = native(vector)
        assert(column.getClass === classOf[NativeColumnVector])
        checkSame(vector, column)(_.getBinary(_).toSeq)
      }
      Using.resource(StructVector.empty("struct", allocator)) { struct =>
        val field = FieldType.nullable(new ArrowType.Int(64, true))
        val child = struct.addOrGet("a", field, classOf[BigIntVector])
        struct.allocateNew()
        child.setSafe(0, 7L)
        struct.setIndexDefined(0)
        struct.setNull(1)
        child.setValueCount(2)
        struct.setValueCount(2)
        val column = native(struct)
        assert(column.getClass === classOf[NativeColumnVector])
        assert(!column.isNullAt(0) && column.isNullAt(1))
        assert(column.getChild(0).getLong(0) === 7L)
      }
    }
  }

  test("is reused by the reader, so that Spark does not close it") {
    withAllocator { allocator =>
      Using.resource(fill(new IntVector("k", allocator))((v, row) => v.set(row, row))) { vector =>
        val column = native(vector)
        assert(column.isInstanceOf[ReusedArrowColumnVector])
        column.closeIfFreeable()
        assert(column.getInt(2) === 2)
      }
    }
  }
}
