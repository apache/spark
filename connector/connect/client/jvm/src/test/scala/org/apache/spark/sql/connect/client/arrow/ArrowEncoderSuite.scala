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
import java.util
import java.util.{Collections, Objects}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.classTag
import scala.util.control.NonFatal

import com.google.protobuf.ByteString
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.connect.proto
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BoxedIntEncoder, CalendarIntervalEncoder, DateEncoder, EncoderField, InstantEncoder, IterableEncoder, JavaDecimalEncoder, LocalDateEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, RowEncoder, StringEncoder, TimestampEncoder, UDTEncoder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder.{encoderFor => toRowEncoder}
import org.apache.spark.sql.connect.client.SparkResult
import org.apache.spark.sql.connect.client.arrow.FooEnum.FooEnum
import org.apache.spark.sql.connect.client.util.ConnectFunSuite
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DecimalType, IntegerType, Metadata, SQLUserDefinedType, StructType, UserDefinedType}

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
    // Use different allocators so we can pinpoint memory leaks better.
    val serializerAllocator = newAllocator("serialization")
    val deserializerAllocator = newAllocator("deserialization")

    val arrowIterator = ArrowSerializer.serialize(
      input = iterator,
      enc = encoder,
      allocator = serializerAllocator,
      maxRecordsPerBatch = maxRecordsPerBatch,
      maxBatchSize = maxBatchSize,
      batchSizeCheckInterval = batchSizeCheckInterval,
      timeZoneId = "UTC")

    val inspectedIterator = if (inspectBatch != null) {
      arrowIterator.map { batch =>
        inspectBatch(batch)
        batch
      }
    } else {
      arrowIterator
    }

    val resultIterator =
      try {
        deserializeFromArrow(inspectedIterator, encoder, deserializerAllocator)
      } catch {
        case NonFatal(e) =>
          arrowIterator.close()
          serializerAllocator.close()
          deserializerAllocator.close()
          throw e
      }
    new CloseableIterator[T] {
      override def close(): Unit = {
        arrowIterator.close()
        resultIterator.close()
        serializerAllocator.close()
        deserializerAllocator.close()
      }
      override def hasNext: Boolean = resultIterator.hasNext
      override def next(): T = resultIterator.next()
    }
  }

  // Temporary hack until we merge the deserializer.
  private def deserializeFromArrow[E](
      batches: Iterator[Array[Byte]],
      encoder: AgnosticEncoder[E],
      allocator: BufferAllocator): CloseableIterator[E] = {
    val responses = batches.map { batch =>
      val builder = proto.ExecutePlanResponse.newBuilder()
      builder.getArrowBatchBuilder.setData(ByteString.copyFrom(batch))
      builder.build()
    }
    val result = new SparkResult[E](responses.asJava, allocator, encoder)
    new CloseableIterator[E] {
      private val itr = result.iterator
      override def close(): Unit = itr.close()
      override def hasNext: Boolean = itr.hasNext
      override def next(): E = itr.next()
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
      timeZoneId = "UTC")
  }

  private def compareIterators[T](expected: Iterator[T], actual: Iterator[T]): Unit = {
    expected.zipAll(actual, null, null).foreach { case (expected, actual) =>
      assert(expected != null)
      assert(actual != null)
      assert(actual == expected)
    }
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

  test("single batch") {
    val inspector = new CountingBatchInspector
    roundTripAndCheckIdentical(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.tabulate(10)(i => Row(i))
    }
    assert(inspector.numBatches == 1)
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
    val instant = java.time.Instant.now()
    val now = java.time.LocalDateTime.now()
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
    val encoder = ScalaReflection.encoderFor[mutable.WrappedArray[Int]]
    val input = mutable.WrappedArray.make[Int](Array(1, 98, 7, 6))
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
    val seq = raw.asInstanceOf[mutable.WrappedArray[String]]
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
        bean.setDummyToDoubleListMap(maybeNull {
          val map = new util.HashMap[DummyBean, java.util.List[java.lang.Double]]
          (0 until (i % 5)).foreach { j =>
            val dummy = new DummyBean
            dummy.setBigInteger(maybeNull(java.math.BigInteger.valueOf(i * j)))
            val values = Array.tabulate(i % 40) { j =>
              Double.box(j.toDouble)
            }
            map.put(dummy, maybeNull(util.Arrays.asList(values: _*)))
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
        timeZoneId = "UTC")
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
    val base = java.time.Instant.now()
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
  var dummyToDoubleListMap: java.util.Map[DummyBean, java.util.List[java.lang.Double]] = _

  def canEqual(other: Any): Boolean = other.isInstanceOf[JavaMapData]

  override def equals(other: Any): Boolean = other match {
    case that: JavaMapData if that canEqual this =>
      dummyToDoubleListMap == that.dummyToDoubleListMap
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(dummyToDoubleListMap)
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
