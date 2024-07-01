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
package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong

import io.grpc.inprocess.InProcessChannelBuilder
import org.apache.arrow.memory.RootAllocator
import org.apache.commons.lang3.SystemUtils
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer}
import org.apache.spark.sql.test.ConnectFunSuite

/**
 * Test suite for SQL implicits.
 */
class SQLImplicitsTestSuite extends ConnectFunSuite with BeforeAndAfterAll {
  private var session: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val client = SparkConnectClient(InProcessChannelBuilder.forName("/dev/null").build())
    session = new SparkSession(client, planIdGenerator = new AtomicLong)
  }

  test("column resolution") {
    val spark = session
    import spark.implicits._
    def assertEqual(left: Column, right: Column): Unit = assert(left == right)
    assertEqual($"x", Column("x"))
    assertEqual(Symbol("y"), Column("y"))
  }

  test("test implicit encoder resolution") {
    val spark = session
    import org.apache.spark.util.ArrayImplicits._
    import spark.implicits._
    def testImplicit[T: Encoder](expected: T): Unit = {
      val encoder = encoderFor[T]
      val allocator = new RootAllocator()
      try {
        val batch = ArrowSerializer.serialize(
          input = Iterator.single(expected),
          enc = encoder,
          allocator = allocator,
          timeZoneId = "UTC")
        val fromArrow = ArrowDeserializers.deserializeFromArrow(
          input = Iterator.single(batch.toByteArray),
          encoder = encoder,
          allocator = allocator,
          timeZoneId = "UTC")
        try {
          assert(fromArrow.next() === expected)
          assert(!fromArrow.hasNext)
        } finally {
          fromArrow.close()
        }
      } finally {
        allocator.close()
      }
    }

    val booleans = Array(false, true, false, false)
    testImplicit(booleans.head)
    testImplicit(java.lang.Boolean.valueOf(booleans.head))
    testImplicit(booleans)
    testImplicit(booleans.toSeq)
    testImplicit(booleans.toSeq)(newBooleanSeqEncoder)
    testImplicit(booleans.toSeq)(newSequenceEncoder)
    testImplicit(booleans.toImmutableArraySeq)

    val bytes = Array(76.toByte, 59.toByte, 121.toByte)
    testImplicit(bytes.head)
    testImplicit(java.lang.Byte.valueOf(bytes.head))
    testImplicit(bytes)
    testImplicit(bytes.toSeq)
    testImplicit(bytes.toSeq)(newByteSeqEncoder)
    testImplicit(bytes.toSeq)(newSequenceEncoder)
    testImplicit(bytes.toImmutableArraySeq)

    val shorts = Array(21.toShort, (-213).toShort, 14876.toShort)
    testImplicit(shorts.head)
    testImplicit(java.lang.Short.valueOf(shorts.head))
    testImplicit(shorts)
    testImplicit(shorts.toSeq)
    testImplicit(shorts.toSeq)(newShortSeqEncoder)
    testImplicit(shorts.toSeq)(newSequenceEncoder)
    testImplicit(shorts.toImmutableArraySeq)

    val ints = Array(4, 6, 5)
    testImplicit(ints.head)
    testImplicit(java.lang.Integer.valueOf(ints.head))
    testImplicit(ints)
    testImplicit(ints.toSeq)
    testImplicit(ints.toSeq)(newIntSeqEncoder)
    testImplicit(ints.toSeq)(newSequenceEncoder)
    testImplicit(ints.toImmutableArraySeq)

    val longs = Array(System.nanoTime(), System.currentTimeMillis())
    testImplicit(longs.head)
    testImplicit(java.lang.Long.valueOf(longs.head))
    testImplicit(longs)
    testImplicit(longs.toSeq)
    testImplicit(longs.toSeq)(newLongSeqEncoder)
    testImplicit(longs.toSeq)(newSequenceEncoder)
    testImplicit(longs.toImmutableArraySeq)

    val floats = Array(3f, 10.9f)
    testImplicit(floats.head)
    testImplicit(java.lang.Float.valueOf(floats.head))
    testImplicit(floats)
    testImplicit(floats.toSeq)
    testImplicit(floats.toSeq)(newFloatSeqEncoder)
    testImplicit(floats.toSeq)(newSequenceEncoder)
    testImplicit(floats.toImmutableArraySeq)

    val doubles = Array(23.78d, -329.6d)
    testImplicit(doubles.head)
    testImplicit(java.lang.Double.valueOf(doubles.head))
    testImplicit(doubles)
    testImplicit(doubles.toSeq)
    testImplicit(doubles.toSeq)(newDoubleSeqEncoder)
    testImplicit(doubles.toSeq)(newSequenceEncoder)
    testImplicit(doubles.toImmutableArraySeq)

    val strings = Array("foo", "baz", "bar")
    testImplicit(strings.head)
    testImplicit(strings)
    testImplicit(strings.toSeq)
    testImplicit(strings.toSeq)(newStringSeqEncoder)
    testImplicit(strings.toSeq)(newSequenceEncoder)
    testImplicit(strings.toImmutableArraySeq)

    val myTypes = Array(MyType(12L, Math.E, Math.PI), MyType(0, 0, 0))
    testImplicit(myTypes.head)
    testImplicit(myTypes)
    testImplicit(myTypes.toSeq)
    testImplicit(myTypes.toSeq)(newProductSeqEncoder[MyType])
    testImplicit(myTypes.toSeq)(newSequenceEncoder)
    testImplicit(myTypes.toImmutableArraySeq)

    // Others.
    val decimal = java.math.BigDecimal.valueOf(3141527000000000000L, 18)
    testImplicit(decimal)
    testImplicit(Array(decimal).toImmutableArraySeq)
    testImplicit(BigDecimal(decimal))
    testImplicit(Date.valueOf(LocalDate.now()))
    testImplicit(LocalDate.now())
    // SPARK-42770: `LocalDateTime.now()` and `Instant.now()` it will get accurate
    // nanoseconds on Linux, but get the microseconds on MacOS. At present,
    // Spark always converts them to microseconds, this will cause the
    // test fail when using Java 17 on Linux, so add `truncatedTo(ChronoUnit.MICROS)` when
    // testing on Linux using Java 17 to ensure the accuracy of input data is microseconds.
    if (SystemUtils.IS_OS_LINUX) {
      testImplicit(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS))
      testImplicit(Instant.now().truncatedTo(ChronoUnit.MICROS))
      testImplicit(Timestamp.from(Instant.now().truncatedTo(ChronoUnit.MICROS)))
    } else {
      testImplicit(LocalDateTime.now())
      testImplicit(Instant.now())
      testImplicit(Timestamp.from(Instant.now()))
    }
    testImplicit(Period.ofYears(2))
    testImplicit(Duration.ofMinutes(77))
    testImplicit(SaveMode.Append)
    testImplicit(Map(("key", "value"), ("foo", "baz")))
    testImplicit(Set(1, 2, 4))
  }
}
