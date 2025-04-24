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
package org.apache.spark.sql.connect.client.arrow

import java.math.BigInteger
import java.time.{Duration, Period, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util
import java.util.{Collections, Objects}

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.reflect.classTag

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Encoders, Row}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, Codec, OuterScopes}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{agnosticEncoderFor, BinaryEncoder, BoxedBooleanEncoder, BoxedByteEncoder, BoxedDoubleEncoder, BoxedFloatEncoder, BoxedIntEncoder, BoxedLongEncoder, BoxedShortEncoder, CalendarIntervalEncoder, DateEncoder, DayTimeIntervalEncoder, EncoderField, InstantEncoder, IterableEncoder, JavaDecimalEncoder, LocalDateEncoder, LocalDateTimeEncoder, NullEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, RowEncoder, ScalaDecimalEncoder, StringEncoder, TimestampEncoder, TransformingEncoder, UDTEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder.{encoderFor => toRowEncoder}
import org.apache.spark.sql.catalyst.util.{DateFormatter, SparkStringUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils._
import org.apache.spark.sql.catalyst.util.SparkIntervalUtils._
import org.apache.spark.sql.connect.client.CloseableIterator
import org.apache.spark.sql.connect.client.arrow.FooEnum.FooEnum
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.types.{ArrayType, DataType, DayTimeIntervalType, Decimal, DecimalType, IntegerType, Metadata, SQLUserDefinedType, StringType, StructType, UserDefinedType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.VariantVal

/**
 * Tests for encoding external data to and from arrow.
 */
class ArrowEncoderSuite extends ConnectFunSuite with BeforeAndAfterAll {
  private val allocator = new RootAllocator()

  private def newAllocator(name: String): BufferAllocator = {
    allocator.newChildAllocator(name, 0, allocator.getLimit)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    allocator.close()
  }

  private def withAllocator[T](f: BufferAllocator => T): T = {
    val allocator = newAllocator("allocator")
    try f(allocator)
    finally {
      allocator.close()
    }
  }

  private def roundTrip[T](
      encoder: AgnosticEncoder[T],
      iterator: Iterator[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null): CloseableIterator[T] = {
    roundTripWithDifferentIOEncoders(
      encoder,
      encoder,
      iterator,
      maxRecordsPerBatch,
      maxBatchSize,
      batchSizeCheckInterval,
      inspectBatch)
  }

  private def roundTripWithDifferentIOEncoders[I, O](
      inputEncoder: AgnosticEncoder[I],
      outputEncoder: AgnosticEncoder[O],
      iterator: Iterator[I],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null): CloseableIterator[O] = {
    // Use different allocators so we can pinpoint memory leaks better.
    val serializerAllocator = newAllocator("serialization")
    val deserializerAllocator = newAllocator("deserialization")

    try {
      val arrowIterator = ArrowSerializer.serialize(
        input = iterator,
        enc = inputEncoder,
        allocator = serializerAllocator,
        maxRecordsPerBatch = maxRecordsPerBatch,
        maxBatchSize = maxBatchSize,
        batchSizeCheckInterval = batchSizeCheckInterval,
        timeZoneId = "UTC",
        largeVarTypes = false)

      val inspectedIterator = if (inspectBatch != null) {
        arrowIterator.map { batch =>
          inspectBatch(batch)
          batch
        }
      } else {
        arrowIterator
      }

      val resultIterator =
        ArrowDeserializers.deserializeFromArrow(
          inspectedIterator,
          outputEncoder,
          deserializerAllocator,
          timeZoneId = "UTC")
      new CloseableIterator[O] {
        override def close(): Unit = {
          arrowIterator.close()
          resultIterator.close()
          serializerAllocator.close()
          deserializerAllocator.close()
        }

        override def hasNext: Boolean = resultIterator.hasNext

        override def next(): O = resultIterator.next()
      }
    } catch {
      case e: Throwable =>
        serializerAllocator.close()
        deserializerAllocator.close()
        throw e
    }
  }

  private def roundTripAndCheck[T](
      encoder: AgnosticEncoder[T],
      toInputIterator: () => Iterator[Any],
      toOutputIterator: () => Iterator[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null): Unit = {
    val iterator = roundTrip(
      encoder,
      toInputIterator().asInstanceOf[Iterator[T]], // Erasure hack :)
      maxRecordsPerBatch,
      maxBatchSize,
      batchSizeCheckInterval,
      inspectBatch)
    try {
      compareIterators(toOutputIterator(), iterator)
    } finally {
      iterator.close()
    }
  }

  private def roundTripAndCheckIdentical[T](
      encoder: AgnosticEncoder[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null)(toIterator: () => Iterator[T]): Unit = {
    roundTripAndCheck(
      encoder,
      toIterator,
      toIterator,
      maxRecordsPerBatch,
      maxBatchSize,
      batchSizeCheckInterval,
      inspectBatch)
  }

  private def serializeToArrow[T](
      input: Iterator[T],
      encoder: AgnosticEncoder[T],
      allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    ArrowSerializer.serialize(
      input,
      encoder,
      allocator,
      maxRecordsPerBatch = 1024,
      maxBatchSize = 8 * 1024,
      timeZoneId = "UTC",
      largeVarTypes = false)
  }

  private def compareIterators[T](expected: Iterator[T], actual: Iterator[T]): Unit = {
    while (expected.hasNext && actual.hasNext) {
      assert(expected.next() == actual.next())
    }
    assert(!expected.hasNext, "Less results produced than expected.")
    assert(!actual.hasNext, "More results produced than expected.")
  }

  private class CountingBatchInspector extends (Array[Byte] => Unit) {
    private var _numBatches: Int = 0
    private var _sizeInBytes: Long = 0
    def numBatches: Int = _numBatches
    def sizeInBytes: Long = _sizeInBytes
    def sizeInBytesPerBatch: Long = sizeInBytes / numBatches
    override def apply(batch: Array[Byte]): Unit = {
      _numBatches += 1
      _sizeInBytes += batch.length
    }
  }

  private case class MaybeNull(interval: Int) {
    assert(interval > 1)
    private var invocations = 0
    def apply[T](value: T): T = {
      val result = if (invocations % interval == 0) {
        null.asInstanceOf[T]
      } else {
        value
      }
      invocations += 1
      result
    }
  }

  private def javaBigDecimal(i: Int): java.math.BigDecimal = {
    javaBigDecimal(i, DecimalType.DEFAULT_SCALE)
  }

  private def javaBigDecimal(i: Int, scale: Int): java.math.BigDecimal = {
    java.math.BigDecimal.valueOf(i).setScale(scale)
  }

  private val singleIntEncoder = RowEncoder(
    EncoderField("i", BoxedIntEncoder, nullable = false, Metadata.empty) :: Nil)

  /* ******************************************************************** *
   * Iterator behavior tests.
   * ******************************************************************** */

  test("empty") {
    val inspector = new CountingBatchInspector
    roundTripAndCheckIdentical(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.empty
    }
    // We always write a batch with a schema.
    assert(inspector.numBatches == 1)
    assert(inspector.sizeInBytes > 0)
  }

  test("deserializing empty iterator") {
    withAllocator { allocator =>
      val iterator = ArrowDeserializers.deserializeFromArrow(
        Iterator.empty,
        singleIntEncoder,
        allocator,
        timeZoneId = "UTC")
      assert(iterator.isEmpty)
      assert(allocator.getAllocatedMemory == 0)
    }
  }

  test("single batch") {
    val inspector = new CountingBatchInspector
    roundTripAndCheckIdentical(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.tabulate(10)(i => Row(i))
    }
    assert(inspector.numBatches == 1)
  }

  test("variant round trip") {
    val variantEncoder = toRowEncoder(new StructType().add("v", "variant"))
    roundTripAndCheckIdentical(variantEncoder) { () =>
      val maybeNull = MaybeNull(7)
      Iterator.tabulate(101)(i =>
        Row(maybeNull(new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0)))))
    }

    val nestedVariantEncoder = toRowEncoder(
      new StructType()
        .add(
          "s",
          new StructType()
            .add("i1", "int")
            .add("v1", "variant")
            .add("i2", "int")
            .add("v2", "variant"))
        .add("a", "array<variant>")
        .add("m", "map<string, variant>"))

    roundTripAndCheckIdentical(nestedVariantEncoder) { () =>
      val maybeNull5 = MaybeNull(5)
      val maybeNull7 = MaybeNull(7)
      val maybeNull11 = MaybeNull(11)
      val maybeNull13 = MaybeNull(13)
      val maybeNull17 = MaybeNull(17)
      Iterator
        .tabulate(100)(i =>
          Row(
            maybeNull5(
              Row(
                i,
                maybeNull7(new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0))),
                i + 1,
                maybeNull11(
                  new VariantVal(Array[Byte](12, (i + 1).toByte), Array[Byte](1, 0, 0))))),
            maybeNull7((0 until 10).map(j =>
              new VariantVal(Array[Byte](12, (i + j).toByte), Array[Byte](1, 0, 0)))),
            maybeNull13(
              Map(
                (
                  i.toString,
                  maybeNull17(
                    new VariantVal(Array[Byte](12, (i + 2).toByte), Array[Byte](1, 0, 0))))))))
    }
  }

  test("multiple batches - split by record count") {
    val inspector = new CountingBatchInspector
    roundTripAndCheckIdentical(
      singleIntEncoder,
      inspectBatch = inspector,
      maxBatchSize = 32 * 1024) { () =>
      Iterator.tabulate(1024 * 1024)(i => Row(i))
    }
    assert(inspector.numBatches == 256)
  }

  test("multiple batches - split by size") {
    val dataGen = { () =>
      Iterator.tabulate(4 * 1024)(i => Row(i))
    }

    // Normal interval
    val inspector1 = new CountingBatchInspector
    roundTripAndCheckIdentical(singleIntEncoder, maxBatchSize = 1024, inspectBatch = inspector1)(
      dataGen)
    assert(inspector1.numBatches == 16)
    assert(inspector1.sizeInBytesPerBatch >= 1024)
    assert(inspector1.sizeInBytesPerBatch <= 1024 + 128 * 5)

    // Lowest possible interval
    val inspector2 = new CountingBatchInspector
    roundTripAndCheckIdentical(
      singleIntEncoder,
      maxBatchSize = 1024,
      batchSizeCheckInterval = 1,
      inspectBatch = inspector2)(dataGen)
    assert(inspector2.numBatches == 20)
    assert(inspector2.sizeInBytesPerBatch >= 1024)
    assert(inspector2.sizeInBytesPerBatch <= 1024 + 128 * 2)
    assert(inspector2.sizeInBytesPerBatch < inspector1.sizeInBytesPerBatch)
  }

  test("use after close") {
    val iterator = serializeToArrow(Iterator.single(Row(0)), singleIntEncoder, allocator)
    assert(iterator.hasNext)
    iterator.close()
    assert(!iterator.hasNext)
    intercept[NoSuchElementException](iterator.next())
  }

  /* ******************************************************************** *
   * Encoder specification tests
   * ******************************************************************** */
  // Lenient mode
  // Errors

  test("primitive fields") {
    val encoder = ScalaReflection.encoderFor[PrimitiveData]
    roundTripAndCheckIdentical(encoder) { () =>
      Iterator.tabulate(10) { i =>
        PrimitiveData(i, i, i.toDouble, i.toFloat, i.toShort, i.toByte, i < 4)
      }
    }
  }

  test("boxed primitive fields") {
    val encoder = ScalaReflection.encoderFor[BoxedData]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        BoxedData(
          intField = maybeNull(i),
          longField = maybeNull(i),
          doubleField = maybeNull(i.toDouble),
          floatField = maybeNull(i.toFloat),
          shortField = maybeNull(i.toShort),
          byteField = maybeNull(i.toByte),
          booleanField = maybeNull(i > 4))
      }
    }
  }

  test("special floating point numbers") {
    val floatIterator = roundTrip(
      PrimitiveFloatEncoder,
      Iterator[Float](Float.NaN, Float.NegativeInfinity, Float.PositiveInfinity))
    assert(java.lang.Float.isNaN(floatIterator.next()))
    assert(floatIterator.next() == Float.NegativeInfinity)
    assert(floatIterator.next() == Float.PositiveInfinity)
    assert(!floatIterator.hasNext)
    floatIterator.close()

    val doubleIterator = roundTrip(
      PrimitiveDoubleEncoder,
      Iterator[Double](Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity))
    assert(java.lang.Double.isNaN(doubleIterator.next()))
    assert(doubleIterator.next() == Double.NegativeInfinity)
    assert(doubleIterator.next() == Double.PositiveInfinity)
    assert(!doubleIterator.hasNext)
    doubleIterator.close()
  }

  test("nullable fields") {
    val encoder = ScalaReflection.encoderFor[NullableData]
    // SPARK-44457: Similar to SPARK-42770, calling `truncatedTo(ChronoUnit.MICROS)`
    // on `Instant.now()` and `LocalDateTime.now()` to ensure microsecond accuracy is used.
    val instant = java.time.Instant.now().truncatedTo(ChronoUnit.MICROS)
    val now = java.time.LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)
    val today = java.time.LocalDate.now()
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        NullableData(
          string = maybeNull(if (i % 7 == 0) "" else "s" + i),
          month = maybeNull(java.time.Month.of(1 + (i % 12))),
          foo = maybeNull(FooEnum(i % FooEnum.maxId)),
          decimal = maybeNull(Decimal(i)),
          scalaBigDecimal = maybeNull(BigDecimal(javaBigDecimal(i + 1))),
          javaBigDecimal = maybeNull(javaBigDecimal(i + 2)),
          scalaBigInt = maybeNull(BigInt(i + 3)),
          javaBigInteger = maybeNull(java.math.BigInteger.valueOf(i + 4)),
          duration = maybeNull(java.time.Duration.ofDays(i)),
          period = maybeNull(java.time.Period.ofMonths(i)),
          date = maybeNull(java.sql.Date.valueOf(today.plusDays(i))),
          localDate = maybeNull(today.minusDays(i)),
          timestamp = maybeNull(java.sql.Timestamp.valueOf(now.plusSeconds(i))),
          instant = maybeNull(instant.plusSeconds(i * 100)),
          localDateTime = maybeNull(now.minusHours(i)))
      }
    }
  }

  test("binary field") {
    val encoder = ScalaReflection.encoderFor[BinaryData]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        BinaryData(maybeNull(Array.tabulate(i % 100)(_.toByte)))
      }
    }
  }

  // Row and Scala class are already covered in other tests
  test("javabean") {
    val encoder = JavaTypeInference.encoderFor[DummyBean](classOf[DummyBean])
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(6)
      Iterator.tabulate(100) { i =>
        val bean = new DummyBean()
        bean.setBigInteger(maybeNull(java.math.BigInteger.valueOf(i)))
        bean
      }
    }
  }

  test("defined by constructor parameters") {
    val encoder = ScalaReflection.encoderFor[NonProduct]
    roundTripAndCheckIdentical(encoder) { () =>
      Iterator.tabulate(100) { i =>
        new NonProduct("k" + i, i.toDouble)
      }
    }
  }

  test("option") {
    val encoder = ScalaReflection.encoderFor[Option[String]]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(6)
      Iterator.tabulate(100) { i =>
        Option(maybeNull("v" + i))
      }
    }
  }

  test("arrays") {
    val encoder = ScalaReflection.encoderFor[ArrayData]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(5)
      Iterator.tabulate(100) { i =>
        ArrayData(
          maybeNull(Array.tabulate[Double](i % 9)(_.toDouble)),
          maybeNull(Array.tabulate[String](i % 21)(i => maybeNull("s" + i))),
          maybeNull(Array.tabulate[Array[Int]](i % 13) { i =>
            maybeNull {
              Array.fill(i % 29)(i)
            }
          }))
      }
    }
  }

  test("scala iterables") {
    val encoder = ScalaReflection.encoderFor[ListData]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(5)
      Iterator.tabulate(100) { i =>
        ListData(
          maybeNull(Seq.tabulate[String](i % 9)(i => maybeNull("s" + i))),
          maybeNull(Seq.tabulate[Int](i % 10)(identity)),
          maybeNull(Set(i.toLong, i.toLong - 1, i.toLong - 33)),
          maybeNull(mutable.Queue.tabulate(5 + i % 6) { i =>
            Option(maybeNull(BigInt(i)))
          }))
      }
    }
  }

  test("java lists") {
    def genJavaData[E](n: Int, collection: util.Collection[E])(f: Int => E): Unit = {
      Iterator.tabulate(n)(f).foreach(collection.add)
    }
    val encoder = JavaTypeInference.encoderFor(classOf[JavaListData])
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(7)
      Iterator.tabulate(1) { i =>
        val bean = new JavaListData
        bean.setListOfDecimal(maybeNull {
          val list = new util.ArrayList[java.math.BigDecimal]
          genJavaData(i % 7, list) { i => maybeNull(java.math.BigDecimal.valueOf(i * 33)) }
          list
        })
        bean.setListOfBigInt(maybeNull {
          val list = new util.LinkedList[java.math.BigInteger]
          genJavaData(10, list) { i => maybeNull(java.math.BigInteger.valueOf(i * 50)) }
          list
        })
        bean.setListOfStrings(maybeNull {
          val list = new util.ArrayList[String]
          genJavaData((i + 5) % 50, list) { i => maybeNull("v" + (i * 2)) }
          list
        })
        bean.setListOfBytes(maybeNull(Collections.singletonList(i.toByte)))
        bean
      }
    }
  }

  test("wrapped array") {
    val encoder = ScalaReflection.encoderFor[mutable.ArraySeq[Int]]
    val input = mutable.ArraySeq.make[Int](Array(1, 98, 7, 6))
    val iterator = roundTrip(encoder, Iterator.single(input))
    val Seq(result) = iterator.toSeq
    assert(result == input)
    assert(result.array.getClass == classOf[Array[Int]])
    iterator.close()
  }

  test("wrapped array - empty") {
    val schema = new StructType().add("names", "array<string>")
    val encoder = toRowEncoder(schema)
    val iterator = roundTrip(encoder, Iterator.single(Row(Seq())))
    val Seq(Row(raw)) = iterator.toSeq
    val seq = raw.asInstanceOf[mutable.ArraySeq[String]]
    assert(seq.isEmpty)
    assert(seq.array.getClass == classOf[Array[String]])
    iterator.close()
  }

  test("maps") {
    val encoder = ScalaReflection.encoderFor[MapData]
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(5)
      Iterator.tabulate(100) { i =>
        MapData(
          maybeNull(
            Iterator
              .tabulate(i % 9) { i =>
                i -> maybeNull("s" + i)
              }
              .toMap),
          maybeNull(
            Iterator
              .tabulate(i % 10) { i =>
                ("s" + 1) -> maybeNull(Array.tabulate[Long]((i + 5) % 20)(_.toLong))
              }
              .toMap))
      }
    }
  }

  test("java maps") {
    val encoder = JavaTypeInference.encoderFor(classOf[JavaMapData])
    roundTripAndCheckIdentical(encoder) { () =>
      val maybeNull = MaybeNull(11)
      Iterator.tabulate(100) { i =>
        val bean = new JavaMapData
        bean.setMetricMap(maybeNull {
          val map = new util.HashMap[String, util.List[java.lang.Double]]
          (0 until (i % 20)).foreach { i =>
            val values = Array.tabulate(i % 40) { j =>
              Double.box(j.toDouble)
            }
            map.put("k" + i, maybeNull(util.Arrays.asList(values: _*)))
          }
          map
        })
        bean.setDummyToStringMap(maybeNull {
          val map = new util.HashMap[DummyBean, String]
          (0 until (i % 5)).foreach { j =>
            val dummy = new DummyBean
            dummy.setBigInteger(maybeNull(java.math.BigInteger.valueOf(i * j)))
            map.put(dummy, maybeNull("s" + i + "v" + j))
          }
          map
        })
        bean
      }
    }
  }

  test("map with null key") {
    val encoder = ScalaReflection.encoderFor[Map[String, String]]
    withAllocator { allocator =>
      val iterator = ArrowSerializer.serialize(
        Iterator(Map((null.asInstanceOf[String], "kaboom?"))),
        encoder,
        allocator,
        maxRecordsPerBatch = 128,
        maxBatchSize = 1024,
        timeZoneId = "UTC",
        largeVarTypes = false)
      intercept[NullPointerException] {
        iterator.next()
      }
      iterator.close()
    }
  }

  // TODO follow-up with more null tests here:
  // - Null primitive
  // - Non-nullable map value
  // - Non-nullable structfield
  // - Non-nullable array element.

  test("lenient field serialization - date/localdate") {
    val base = java.time.LocalDate.now()
    val localDates = () => Iterator.tabulate(10)(i => base.plusDays(i * i * 60))
    val dates = () => localDates().map(java.sql.Date.valueOf)
    val combo = () => localDates() ++ dates()
    roundTripAndCheck(DateEncoder(true), dates, dates)
    roundTripAndCheck(DateEncoder(true), localDates, dates)
    roundTripAndCheck(DateEncoder(true), combo, () => dates() ++ dates())
    roundTripAndCheck(LocalDateEncoder(true), dates, localDates)
    roundTripAndCheck(LocalDateEncoder(true), localDates, localDates)
    roundTripAndCheck(LocalDateEncoder(true), combo, () => localDates() ++ localDates())
  }

  test("lenient field serialization - timestamp/instant") {
    // SPARK-44457: Similar to SPARK-42770, calling `truncatedTo(ChronoUnit.MICROS)`
    // on `Instant.now()` to ensure microsecond accuracy is used.
    val base = java.time.Instant.now().truncatedTo(ChronoUnit.MICROS)
    val instants = () => Iterator.tabulate(10)(i => base.plusSeconds(i * i * 60))
    val timestamps = () => instants().map(java.sql.Timestamp.from)
    val combo = () => instants() ++ timestamps()
    roundTripAndCheck(InstantEncoder(true), instants, instants)
    roundTripAndCheck(InstantEncoder(true), timestamps, instants)
    roundTripAndCheck(InstantEncoder(true), combo, () => instants() ++ instants())
    roundTripAndCheck(TimestampEncoder(true), instants, timestamps)
    roundTripAndCheck(TimestampEncoder(true), timestamps, timestamps)
    roundTripAndCheck(TimestampEncoder(true), combo, () => timestamps() ++ timestamps())
  }

  test("lenient field serialization - decimal") {
    val base = javaBigDecimal(137, DecimalType.DEFAULT_SCALE)
    val bigDecimals = () =>
      Iterator.tabulate(100) { i =>
        base.multiply(javaBigDecimal(i)).setScale(DecimalType.DEFAULT_SCALE)
      }
    val bigInts = () => bigDecimals().map(_.toBigInteger)
    val scalaBigDecimals = () => bigDecimals().map(BigDecimal.apply)
    val scalaBigInts = () => bigDecimals().map(v => BigInt(v.toBigInteger))
    val sparkDecimals = () => bigDecimals().map(Decimal.apply)
    val encoder = JavaDecimalEncoder(DecimalType.SYSTEM_DEFAULT, lenientSerialization = true)
    roundTripAndCheck(encoder, bigDecimals, bigDecimals)
    roundTripAndCheck(encoder, bigInts, bigDecimals)
    roundTripAndCheck(encoder, scalaBigDecimals, bigDecimals)
    roundTripAndCheck(encoder, scalaBigInts, bigDecimals)
    roundTripAndCheck(encoder, sparkDecimals, bigDecimals)
    roundTripAndCheck(
      encoder,
      () => bigDecimals() ++ bigInts() ++ scalaBigDecimals() ++ scalaBigInts() ++ sparkDecimals(),
      () => Iterator.fill(5)(bigDecimals()).flatten)
  }

  test("lenient field serialization - iterables") {
    val encoder = IterableEncoder(
      classTag[Seq[Int]],
      BoxedIntEncoder,
      containsNull = true,
      lenientSerialization = true)
    val elements = Seq(Array(1, 7, 8), Array.emptyIntArray, Array(88))
    val primitiveArrays = () => elements.iterator
    val genericArrays = () => elements.iterator.map(v => v.map(Int.box))
    val lists = () => elements.iterator.map(v => java.util.Arrays.asList(v.map(Int.box): _*))
    val seqs = () => elements.iterator.map(_.toSeq)
    roundTripAndCheck(encoder, seqs, seqs)
    roundTripAndCheck(encoder, primitiveArrays, seqs)
    roundTripAndCheck(encoder, genericArrays, seqs)
    roundTripAndCheck(encoder, lists, seqs)
    roundTripAndCheck(
      encoder,
      () => lists() ++ seqs() ++ genericArrays() ++ primitiveArrays(),
      () => Iterator.fill(4)(seqs()).flatten)
  }

  private val wideSchemaEncoder = toRowEncoder(
    new StructType()
      .add("a", "int")
      .add("b", "string")
      .add(
        "c",
        new StructType()
          .add("ca", "array<int>")
          .add("cb", "binary")
          .add("cc", "float"))
      .add(
        "d",
        ArrayType(
          new StructType()
            .add("da", "decimal(20, 10)")
            .add("db", "string")
            .add("dc", "boolean"))))

  private val narrowSchemaEncoder = toRowEncoder(
    new StructType()
      .add("b", "string")
      .add(
        "d",
        ArrayType(
          new StructType()
            .add("da", "decimal(20, 10)")
            .add("dc", "boolean")))
      .add(
        "C",
        new StructType()
          .add("Ca", "array<int>")
          .add("Cb", "binary")))

  test("bind to schema") {
    // Binds to a wider schema. The narrow schema has fewer (nested) fields, has a slightly
    // different field order, and uses different cased names in a couple of places.
    withAllocator { allocator =>
      val input = Row(
        887,
        "foo",
        Row(Seq(1, 7, 5), Array[Byte](8.toByte, 756.toByte), 5f),
        Seq(Row(null, "a", false), Row(javaBigDecimal(57853, 10), "b", false)))
      val expected = Row(
        "foo",
        Seq(Row(null, false), Row(javaBigDecimal(57853, 10), false)),
        Row(Seq(1, 7, 5), Array[Byte](8.toByte, 756.toByte)))
      val arrowBatches = serializeToArrow(Iterator.single(input), wideSchemaEncoder, allocator)
      val result =
        ArrowDeserializers.deserializeFromArrow(
          arrowBatches,
          narrowSchemaEncoder,
          allocator,
          timeZoneId = "UTC")
      val actual = result.next()
      assert(result.isEmpty)
      assert(expected === actual)
      result.close()
      arrowBatches.close()
    }
  }

  test("unknown field") {
    withAllocator { allocator =>
      val arrowBatches = serializeToArrow(Iterator.empty, narrowSchemaEncoder, allocator)
      intercept[AnalysisException] {
        ArrowDeserializers.deserializeFromArrow(
          arrowBatches,
          wideSchemaEncoder,
          allocator,
          timeZoneId = "UTC")
      }
      arrowBatches.close()
    }
  }

  test("duplicate fields") {
    val duplicateSchemaEncoder = toRowEncoder(
      new StructType()
        .add("foO", "string")
        .add("Foo", "string"))
    val fooSchemaEncoder = toRowEncoder(
      new StructType()
        .add("foo", "string"))
    withAllocator { allocator =>
      val arrowBatches = serializeToArrow(Iterator.empty, duplicateSchemaEncoder, allocator)
      intercept[AnalysisException] {
        ArrowDeserializers.deserializeFromArrow(
          arrowBatches,
          fooSchemaEncoder,
          allocator,
          timeZoneId = "UTC")
      }
      arrowBatches.close()
    }
  }

  case class MyTestClass(value: Int)
  OuterScopes.addOuterScope(this)

  test("REPL generated classes") {
    val encoder = ScalaReflection.encoderFor[MyTestClass]
    roundTripAndCheckIdentical(encoder) { () =>
      Iterator.tabulate(10)(MyTestClass)
    }
  }

  test("java serialization") {
    val encoder = agnosticEncoderFor(Encoders.javaSerialization[(Int, String)])
    roundTripAndCheckIdentical(encoder) { () =>
      Iterator.tabulate(10)(i => (i, "itr_" + i))
    }
  }

  test("kryo serialization") {
    val e = intercept[SparkRuntimeException] {
      val encoder = agnosticEncoderFor(Encoders.kryo[(Int, String)])
      roundTripAndCheckIdentical(encoder) { () =>
        Iterator.tabulate(10)(i => (i, "itr_" + i))
      }
    }
    assert(e.getCondition == "CANNOT_USE_KRYO")
  }

  test("transforming encoder") {
    val schema = new StructType()
      .add("key", IntegerType)
      .add("value", StringType)
    val encoder =
      TransformingEncoder(classTag[(Int, String)], toRowEncoder(schema), () => new TestCodec)
    roundTripAndCheckIdentical(encoder) { () =>
      Iterator.tabulate(10)(i => (i, "v" + i))
    }
  }

  /* ******************************************************************** *
   * Arrow deserialization upcasting
   * ******************************************************************** */
  // Not supported: UDT, CalendarInterval
  // Not tested: Char/Varchar.
  private case class UpCastTestCase[I](input: AgnosticEncoder[I], generator: Int => I) {
    def test[O](output: AgnosticEncoder[O], convert: I => O): this.type = {
      val name = "upcast " + input.dataType.catalogString + " to " + output.dataType.catalogString
      ArrowEncoderSuite.this.test(name) {
        def data: Iterator[I] = Iterator.tabulate(5)(generator)
        val result = roundTripWithDifferentIOEncoders(input, output, data)
        try {
          compareIterators(data.map(convert), result)
        } finally {
          result.close()
        }
      }
      this
    }

    def nullTest[O](e: AgnosticEncoder[O]): this.type = {
      test(e, _.asInstanceOf[O])
    }
  }

  private val timestampFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)
  private val dateFormatter = DateFormatter()

  private def scalaDecimalEncoder(precision: Int, scale: Int = 0): ScalaDecimalEncoder = {
    ScalaDecimalEncoder(DecimalType(precision, scale))
  }

  UpCastTestCase(NullEncoder, _ => null)
    .nullTest(BoxedBooleanEncoder)
    .nullTest(BoxedByteEncoder)
    .nullTest(BoxedShortEncoder)
    .nullTest(BoxedIntEncoder)
    .nullTest(BoxedLongEncoder)
    .nullTest(BoxedFloatEncoder)
    .nullTest(BoxedDoubleEncoder)
    .nullTest(StringEncoder)
    .nullTest(DateEncoder(false))
    .nullTest(TimestampEncoder(false))
  UpCastTestCase(PrimitiveBooleanEncoder, _ % 2 == 0)
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveByteEncoder, i => i.toByte)
    .test(PrimitiveShortEncoder, _.toShort)
    .test(PrimitiveIntEncoder, _.toInt)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(PrimitiveFloatEncoder, _.toFloat)
    .test(PrimitiveDoubleEncoder, _.toDouble)
    .test(scalaDecimalEncoder(3), BigDecimal(_))
    .test(scalaDecimalEncoder(5, 2), BigDecimal(_))
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveShortEncoder, i => i.toShort)
    .test(PrimitiveIntEncoder, _.toInt)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(PrimitiveFloatEncoder, _.toFloat)
    .test(PrimitiveDoubleEncoder, _.toDouble)
    .test(scalaDecimalEncoder(5), BigDecimal(_))
    .test(scalaDecimalEncoder(10, 5), BigDecimal(_))
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveIntEncoder, i => i)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(PrimitiveFloatEncoder, _.toFloat)
    .test(PrimitiveDoubleEncoder, _.toDouble)
    .test(scalaDecimalEncoder(10), BigDecimal(_))
    .test(scalaDecimalEncoder(13, 3), BigDecimal(_))
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveLongEncoder, i => i.toLong)
    .test(PrimitiveFloatEncoder, _.toFloat)
    .test(PrimitiveDoubleEncoder, _.toDouble)
    .test(scalaDecimalEncoder(20), BigDecimal(_))
    .test(scalaDecimalEncoder(25, 5), BigDecimal(_))
    .test(TimestampEncoder(false), s => toJavaTimestamp(s * MICROS_PER_SECOND))
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveFloatEncoder, i => i.toFloat)
    .test(PrimitiveDoubleEncoder, _.toDouble)
    .test(StringEncoder, _.toString)
  UpCastTestCase(PrimitiveDoubleEncoder, i => i.toDouble)
    .test(StringEncoder, _.toString)
  UpCastTestCase(scalaDecimalEncoder(2), BigDecimal(_))
    .test(PrimitiveByteEncoder, _.toByte)
    .test(PrimitiveShortEncoder, _.toShort)
    .test(PrimitiveIntEncoder, _.toInt)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(scalaDecimalEncoder(7, 5), identity)
    .test(StringEncoder, _.toString())
  UpCastTestCase(scalaDecimalEncoder(4), BigDecimal(_))
    .test(PrimitiveShortEncoder, _.toShort)
    .test(PrimitiveIntEncoder, _.toInt)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(scalaDecimalEncoder(10, 1), identity)
    .test(StringEncoder, _.toString())
  UpCastTestCase(scalaDecimalEncoder(9), BigDecimal(_))
    .test(PrimitiveIntEncoder, _.toInt)
    .test(PrimitiveLongEncoder, _.toLong)
    .test(scalaDecimalEncoder(13, 4), identity)
    .test(StringEncoder, _.toString())
  UpCastTestCase(scalaDecimalEncoder(19), BigDecimal(_))
    .test(PrimitiveLongEncoder, _.toLong)
    .test(scalaDecimalEncoder(23, 1), identity)
    .test(StringEncoder, _.toString())
  UpCastTestCase(scalaDecimalEncoder(7, 3), BigDecimal(_))
    .test(scalaDecimalEncoder(9, 5), identity)
    .test(scalaDecimalEncoder(23, 3), identity)
  UpCastTestCase(DateEncoder(false), i => toJavaDate(i))
    .test(
      TimestampEncoder(false),
      date => toJavaTimestamp(daysToMicros(fromJavaDate(date), ZoneOffset.UTC)))
    .test(
      LocalDateTimeEncoder,
      date => microsToLocalDateTime(daysToMicros(fromJavaDate(date), ZoneOffset.UTC)))
    .test(StringEncoder, date => dateFormatter.format(date))
  UpCastTestCase(TimestampEncoder(false), i => toJavaTimestamp(i))
    .test(PrimitiveLongEncoder, ts => Math.floorDiv(fromJavaTimestamp(ts), MICROS_PER_SECOND))
    .test(LocalDateTimeEncoder, ts => microsToLocalDateTime(fromJavaTimestamp(ts)))
    .test(StringEncoder, ts => timestampFormatter.format(ts))
  UpCastTestCase(LocalDateTimeEncoder, i => microsToLocalDateTime(i))
    .test(TimestampEncoder(false), ldt => toJavaTimestamp(localDateTimeToMicros(ldt)))
    .test(StringEncoder, ldt => timestampFormatter.format(ldt))
  UpCastTestCase(DayTimeIntervalEncoder, i => Duration.ofDays(i))
    .test(
      StringEncoder,
      { i =>
        toDayTimeIntervalString(
          durationToMicros(i),
          ANSI_STYLE,
          DayTimeIntervalType.DEFAULT.startField,
          DayTimeIntervalType.DEFAULT.endField)
      })
  UpCastTestCase(YearMonthIntervalEncoder, i => Period.ofMonths(i))
    .test(
      StringEncoder,
      { i =>
        toYearMonthIntervalString(
          periodToMonths(i),
          ANSI_STYLE,
          YearMonthIntervalType.DEFAULT.startField,
          YearMonthIntervalType.DEFAULT.endField)
      })
  UpCastTestCase(BinaryEncoder, i => Array.tabulate(10)(j => (64 + j + i).toByte))
    .test(StringEncoder, bytes => SparkStringUtils.getHexString(bytes))

  /* ******************************************************************** *
   * Arrow serialization/deserialization specific errors
   * ******************************************************************** */
  test("unsupported encoders") {
    // CalendarIntervalEncoder
    val data = null.asInstanceOf[AnyRef]
    intercept[SparkUnsupportedOperationException](
      ArrowSerializer.serializerFor(CalendarIntervalEncoder, data))

    // UDT
    val udtEncoder = UDTEncoder(new UDTNotSupported, classOf[UDTNotSupported])
    intercept[SparkUnsupportedOperationException](ArrowSerializer.serializerFor(udtEncoder, data))
  }

  test("unsupported encoder/vector combinations") {
    // Also add a test for the serializer...
    withAllocator { allocator =>
      intercept[RuntimeException] {
        ArrowSerializer.serializerFor(StringEncoder, new VarBinaryVector("bytes", allocator))
      }
    }
  }
}

// TODO fix actual Null fields, e.g.: nullable: Null
case class NullableData(
    string: String,
    month: java.time.Month,
    foo: FooEnum,
    decimal: Decimal,
    scalaBigDecimal: BigDecimal,
    javaBigDecimal: java.math.BigDecimal,
    scalaBigInt: BigInt,
    javaBigInteger: java.math.BigInteger,
    duration: java.time.Duration,
    period: java.time.Period,
    date: java.sql.Date,
    localDate: java.time.LocalDate,
    timestamp: java.sql.Timestamp,
    instant: java.time.Instant,
    localDateTime: java.time.LocalDateTime)

case class BinaryData(binary: Array[Byte]) {
  def canEqual(other: Any): Boolean = other.isInstanceOf[BinaryData]

  override def equals(other: Any): Boolean = other match {
    case that: BinaryData if that.canEqual(this) =>
      java.util.Arrays.equals(binary, that.binary)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(binary)
}

class NonProduct(val name: String, val value: Double) extends DefinedByConstructorParams {

  def canEqual(other: Any): Boolean = other.isInstanceOf[NonProduct]

  override def equals(other: Any): Boolean = other match {
    case that: NonProduct =>
      (that canEqual this) &&
      name == that.name &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

case class ArrayData(doubles: Array[Double], strings: Array[String], nested: Array[Array[Int]]) {
  def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayData]

  override def equals(other: Any): Boolean = other match {
    case that: ArrayData if that.canEqual(this) =>
      Objects.deepEquals(that.doubles, doubles) &&
      Objects.deepEquals(that.strings, strings) &&
      Objects.deepEquals(that.nested, nested)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(doubles, strings, nested)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

case class ListData(
    seqOfStrings: Seq[String],
    seqOfInts: Seq[Int],
    setOfLongs: Set[Long],
    queueOfBigIntOptions: mutable.Queue[Option[BigInt]])

class JavaListData {
  @scala.beans.BeanProperty
  var listOfDecimal: java.util.ArrayList[java.math.BigDecimal] = _
  @scala.beans.BeanProperty
  var listOfBigInt: java.util.LinkedList[java.math.BigInteger] = _
  @scala.beans.BeanProperty
  var listOfStrings: java.util.AbstractList[String] = _
  @scala.beans.BeanProperty
  var listOfBytes: java.util.List[java.lang.Byte] = _

  def canEqual(other: Any): Boolean = other.isInstanceOf[JavaListData]

  override def equals(other: Any): Boolean = other match {
    case that: JavaListData if that canEqual this =>
      Objects.equals(listOfDecimal, that.listOfDecimal) &&
      Objects.equals(listOfBigInt, that.listOfBigInt) &&
      Objects.equals(listOfStrings, that.listOfStrings) &&
      Objects.equals(listOfBytes, that.listOfBytes)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(listOfDecimal, listOfBigInt, listOfStrings, listOfBytes)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    s"JavaListData(listOfDecimal=$listOfDecimal, " +
      s"listOfBigInt=$listOfBigInt, " +
      s"listOfStrings=$listOfStrings, " +
      s"listOfBytes=$listOfBytes)"
  }
}

case class MapData(intStringMap: Map[Int, String], metricMap: Map[String, Array[Long]]) {
  def canEqual(other: Any): Boolean = other.isInstanceOf[MapData]

  private def sameMetricMap(other: Map[String, Array[Long]]): Boolean = {
    if (metricMap == null && other == null) {
      true
    } else if (metricMap == null || other == null || metricMap.keySet != other.keySet) {
      false
    } else {
      metricMap.forall { case (key, values) =>
        java.util.Arrays.equals(values, other(key))
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: MapData if that canEqual this =>
      Objects.deepEquals(intStringMap, that.intStringMap) &&
      sameMetricMap(that.metricMap)
    case _ => false
  }

  override def hashCode(): Int = {
    java.util.Arrays.deepHashCode(Array(intStringMap, metricMap))
  }
}

class JavaMapData {
  @scala.beans.BeanProperty
  var dummyToStringMap: java.util.Map[DummyBean, String] = _

  @scala.beans.BeanProperty
  var metricMap: java.util.HashMap[String, java.util.List[java.lang.Double]] = _

  def canEqual(other: Any): Boolean = other.isInstanceOf[JavaMapData]

  override def equals(other: Any): Boolean = other match {
    case that: JavaMapData if that canEqual this =>
      dummyToStringMap == that.dummyToStringMap &&
      metricMap == that.metricMap
    case _ => false
  }

  override def hashCode(): Int = {
    java.util.Arrays.deepHashCode(Array(dummyToStringMap, metricMap))
  }
}

class DummyBean {
  @BeanProperty var bigInteger: BigInteger = _

  override def hashCode(): Int = Objects.hashCode(bigInteger)

  override def equals(obj: Any): Boolean = obj match {
    case bean: DummyBean => Objects.equals(bigInteger, bean.bigInteger)
    case _ => false
  }
}

object FooEnum extends Enumeration {
  type FooEnum = Value
  val E1, E2 = Value
}

case class PrimitiveData(
    intField: Int,
    longField: Long,
    doubleField: Double,
    floatField: Float,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean)

case class BoxedData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    doubleField: java.lang.Double,
    floatField: java.lang.Float,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

/** For testing UDT for a case class */
@SQLUserDefinedType(udt = classOf[UDTNotSupported])
case class UDTNotSupportedClass(i: Int)

class UDTNotSupported extends UserDefinedType[UDTNotSupportedClass] {
  override def sqlType: DataType = IntegerType
  override def userClass: Class[UDTNotSupportedClass] = classOf[UDTNotSupportedClass]
  override def serialize(obj: UDTNotSupportedClass): Int = obj.i
  override def deserialize(datum: Any): UDTNotSupportedClass = datum match {
    case i: Int => UDTNotSupportedClass(i)
  }
}

class TestCodec extends Codec[(Int, String), Row] {
  override def encode(in: (Int, String)): Row = Row(in._1, in._2)
  override def decode(out: Row): (Int, String) = (out.getInt(0), out.getString(1))
}
