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

package org.apache.spark.sql.catalyst.types.ops

import java.time.{Instant, LocalDateTime, LocalTime, ZoneOffset}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalLongType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType, TimeType}
import org.apache.spark.unsafe.types.TimestampNanosVal

class TypeOpsTestSuite extends SparkFunSuite {

  private final class Sample(
      val dataType: DataType,
      val physicalType: PhysicalDataType,
      val external: Any,
      val internal: Any,
      val formatted: String)

  private val nanos = TimestampNanosVal.fromParts(123456L, 789.toShort)
  private val samples = Seq(
    new Sample(
      TimeType(9),
      PhysicalLongType,
      LocalTime.parse("00:00:00.123456789"),
      123456789L,
      "00:00:00.123456789"),
    new Sample(
      TimestampNTZNanosType(9),
      PhysicalTimestampNTZNanosType,
      LocalDateTime.parse("1970-01-01T00:00:00.123456789"),
      nanos,
      "1970-01-01 00:00:00.123456789"),
    new Sample(
      TimestampLTZNanosType(9),
      PhysicalTimestampLTZNanosType,
      Instant.parse("1970-01-01T00:00:00.123456789Z"),
      nanos,
      "1970-01-01 00:00:00.123456789"))

  samples.foreach { sample =>
    val dataType = sample.dataType

    test(s"${dataType.sql}: physical representation") {
      assert(PhysicalDataType(dataType) === sample.physicalType)
    }

    test(s"${dataType.sql}: external value round trip") {
      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(dataType)
      val toScala = CatalystTypeConverters.createToScalaConverter(dataType)
      val internal = toCatalyst(sample.external)
      assert(internal === sample.internal)
      assert(toScala(internal) === sample.external)
    }

    test(s"${dataType.sql}: internal value formatting") {
      val apiOps = TypeApiOps(dataType, ZoneOffset.UTC).getOrElse {
        fail(s"No TypeApiOps registered for ${dataType.sql}")
      }
      assert(apiOps.format(sample.internal) === sample.formatted)
    }
  }
}
