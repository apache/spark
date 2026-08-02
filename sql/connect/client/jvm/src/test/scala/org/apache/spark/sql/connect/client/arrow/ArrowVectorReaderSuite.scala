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

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{TimeStampMicroTZVector, TimeStampNanoTZVector, TimeStampNanoVector}

import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils

class ArrowVectorReaderSuite extends ConnectFunSuite {

  private val allocator = new RootAllocator()

  override def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  private def microTZVector(): TimeStampMicroTZVector = {
    val field = ArrowUtils.toArrowField("ts", TimestampType, nullable = true, "UTC")
    field.createVector(allocator).asInstanceOf[TimeStampMicroTZVector]
  }

  private def nanoVector(): TimeStampNanoVector = {
    val field = ArrowUtils.toArrowField("ts", TimestampNTZNanosType(9), nullable = true, "UTC")
    field.createVector(allocator).asInstanceOf[TimeStampNanoVector]
  }

  private def nanoTZVector(): TimeStampNanoTZVector = {
    val field = ArrowUtils.toArrowField("ts", TimestampLTZNanosType(9), nullable = true, "UTC")
    field.createVector(allocator).asInstanceOf[TimeStampNanoTZVector]
  }

  test("SPARK-57738: ArrowVectorReader reads TimestampNTZNanosType via ConnectTypeOps") {
    val epochNanos = 1_700_000_000_000_000_123L
    val vector = nanoVector()
    try {
      vector.allocateNew()
      vector.setSafe(0, epochNanos)
      vector.setValueCount(1)
      val reader = ArrowVectorReader(TimestampNTZNanosType(9), vector, "UTC")
      assert(!reader.isNull(0))
      val ldt = reader.getLocalDateTime(0)
      assert(ldt != null)
      val expected = LocalDateTime.ofEpochSecond(
        epochNanos / 1_000_000_000L,
        (epochNanos % 1_000_000_000L).toInt,
        ZoneOffset.UTC)
      assert(ldt == expected)
    } finally {
      vector.close()
    }
  }

  test("SPARK-57738: ArrowVectorReader reads TimestampLTZNanosType via ConnectTypeOps") {
    val epochNanos = 1_700_000_000_000_000_456L
    val vector = nanoTZVector()
    try {
      vector.allocateNew()
      vector.setSafe(0, epochNanos)
      vector.setValueCount(1)
      val reader = ArrowVectorReader(TimestampLTZNanosType(9), vector, "UTC")
      assert(!reader.isNull(0))
      val instant = reader.getInstant(0)
      assert(instant != null)
      val expected =
        Instant.ofEpochSecond(epochNanos / 1_000_000_000L, epochNanos % 1_000_000_000L)
      assert(instant == expected)
    } finally {
      vector.close()
    }
  }

  test("SPARK-57738: ArrowVectorReader still succeeds for plain TimestampType") {
    val vector = microTZVector()
    try {
      val reader = ArrowVectorReader(TimestampType, vector, "UTC")
      assert(reader != null)
    } finally {
      vector.close()
    }
  }
}
