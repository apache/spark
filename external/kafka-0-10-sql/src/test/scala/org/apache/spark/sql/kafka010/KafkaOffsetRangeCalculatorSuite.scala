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

package org.apache.spark.sql.kafka010

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KafkaOffsetRangeCalculatorSuite extends SparkFunSuite {

  def testWithMinPartitions(name: String, minPartition: Int)
      (f: KafkaOffsetRangeCalculator => Unit): Unit = {
    val options = new CaseInsensitiveStringMap(Map("minPartitions" -> minPartition.toString).asJava)
    test(s"with minPartition = $minPartition: $name") {
      f(KafkaOffsetRangeCalculator(options))
    }
  }


  test("with no minPartition: N TopicPartitions to N offset ranges") {
    val calc = KafkaOffsetRangeCalculator(CaseInsensitiveStringMap.empty())
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1),
        untilOffsets = Map(tp1 -> 2)) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))

    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1),
        untilOffsets = Map(tp1 -> 2, tp2 -> 1), Seq.empty) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))

    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1),
        untilOffsets = Map(tp1 -> 2)) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))

    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1),
        untilOffsets = Map(tp1 -> 2),
        executorLocations = Seq("location")) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, Some("location"))))
  }

  test("with no minPartition: empty ranges ignored") {
    val calc = KafkaOffsetRangeCalculator(CaseInsensitiveStringMap.empty())
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1),
        untilOffsets = Map(tp1 -> 2, tp2 -> 1)) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))
  }

  testWithMinPartitions("N TopicPartitions to N offset ranges", 3) { calc =>
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1, tp3 -> 1),
        untilOffsets = Map(tp1 -> 2, tp2 -> 2, tp3 -> 2)) ==
      Seq(
        KafkaOffsetRange(tp1, 1, 2, None),
        KafkaOffsetRange(tp2, 1, 2, None),
        KafkaOffsetRange(tp3, 1, 2, None)))
  }

  testWithMinPartitions("1 TopicPartition to N offset ranges", 4) { calc =>
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1),
        untilOffsets = Map(tp1 -> 5)) ==
      Seq(
        KafkaOffsetRange(tp1, 1, 2, None),
        KafkaOffsetRange(tp1, 2, 3, None),
        KafkaOffsetRange(tp1, 3, 4, None),
        KafkaOffsetRange(tp1, 4, 5, None)))

    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1),
        untilOffsets = Map(tp1 -> 5),
        executorLocations = Seq("location")) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 2, None),
          KafkaOffsetRange(tp1, 2, 3, None),
          KafkaOffsetRange(tp1, 3, 4, None),
          KafkaOffsetRange(tp1, 4, 5, None))) // location pref not set when minPartition is set
  }

  testWithMinPartitions("N skewed TopicPartitions to M offset ranges", 3) { calc =>
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1),
        untilOffsets = Map(tp1 -> 5, tp2 -> 21)) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None),
          KafkaOffsetRange(tp2, 1, 7, None),
          KafkaOffsetRange(tp2, 7, 14, None),
          KafkaOffsetRange(tp2, 14, 21, None)))
  }

  testWithMinPartitions("range inexact multiple of minPartitions", 3) { calc =>
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1),
        untilOffsets = Map(tp1 -> 11)) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 4, None),
          KafkaOffsetRange(tp1, 4, 7, None),
          KafkaOffsetRange(tp1, 7, 11, None)))
  }

  testWithMinPartitions("empty ranges ignored", 3) { calc =>
    assert(
      calc.getRanges(
        fromOffsets = Map(tp1 -> 1, tp2 -> 1, tp3 -> 1),
        untilOffsets = Map(tp1 -> 5, tp2 -> 21, tp3 -> 1)) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None),
          KafkaOffsetRange(tp2, 1, 7, None),
          KafkaOffsetRange(tp2, 7, 14, None),
          KafkaOffsetRange(tp2, 14, 21, None)))
  }

  private val tp1 = new TopicPartition("t1", 1)
  private val tp2 = new TopicPartition("t2", 1)
  private val tp3 = new TopicPartition("t3", 1)
}
