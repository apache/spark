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
        Seq(KafkaOffsetRange(tp1, 1, 2))) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))

    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 2)),
        executorLocations = Seq("location")) ==
      Seq(KafkaOffsetRange(tp1, 1, 2, Some("location"))))
  }

  test("with no minPartition: empty ranges ignored") {
    val calc = KafkaOffsetRangeCalculator(CaseInsensitiveStringMap.empty())
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 2),
          KafkaOffsetRange(tp2, 1, 1))) ===
      Seq(KafkaOffsetRange(tp1, 1, 2, None)))
  }

  testWithMinPartitions("N TopicPartitions to N offset ranges", 3) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 2),
          KafkaOffsetRange(tp2, 1, 2),
          KafkaOffsetRange(tp3, 1, 2))) ===
      Seq(
        KafkaOffsetRange(tp1, 1, 2, None),
        KafkaOffsetRange(tp2, 1, 2, None),
        KafkaOffsetRange(tp3, 1, 2, None)))
  }

  testWithMinPartitions("1 TopicPartition to N offset ranges", 4) { calc =>
    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 5))) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 2, None),
          KafkaOffsetRange(tp1, 2, 3, None),
          KafkaOffsetRange(tp1, 3, 4, None),
          KafkaOffsetRange(tp1, 4, 5, None)))

    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 5)),
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
        Seq(
          KafkaOffsetRange(tp1, 1, 5),
          KafkaOffsetRange(tp2, 1, 21))) ===
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None),
          KafkaOffsetRange(tp2, 1, 7, None),
          KafkaOffsetRange(tp2, 7, 14, None),
          KafkaOffsetRange(tp2, 14, 21, None)))
  }

  testWithMinPartitions("SPARK-30656: ignore empty ranges and split the rest", 4) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 1),
          KafkaOffsetRange(tp2, 1, 21))) ===
        Seq(
          KafkaOffsetRange(tp2, 1, 6, None),
          KafkaOffsetRange(tp2, 6, 11, None),
          KafkaOffsetRange(tp2, 11, 16, None),
          KafkaOffsetRange(tp2, 16, 21, None)))
  }

  testWithMinPartitions(
      "SPARK-30656: N very skewed TopicPartitions to M offset ranges",
      3) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 2),
          KafkaOffsetRange(tp2, 1, 1001))) ===
        Seq(
          KafkaOffsetRange(tp1, 1, 2, None),
          KafkaOffsetRange(tp2, 1, 334, None),
          KafkaOffsetRange(tp2, 334, 667, None),
          KafkaOffsetRange(tp2, 667, 1001, None)))
  }

  testWithMinPartitions(
      "SPARK-30656: minPartitions less than the length of topic partitions",
      1) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 5),
          KafkaOffsetRange(tp2, 1, 21))) ===
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None),
          KafkaOffsetRange(tp2, 1, 21, None)))
  }

  testWithMinPartitions("range inexact multiple of minPartitions", 3) { calc =>
    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 11))) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 4, None),
          KafkaOffsetRange(tp1, 4, 7, None),
          KafkaOffsetRange(tp1, 7, 11, None)))
  }

  testWithMinPartitions("empty ranges ignored", 3) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 5),
          KafkaOffsetRange(tp2, 1, 21),
          KafkaOffsetRange(tp3, 1, 1))) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None),
          KafkaOffsetRange(tp2, 1, 7, None),
          KafkaOffsetRange(tp2, 7, 14, None),
          KafkaOffsetRange(tp2, 14, 21, None)))
  }

  testWithMinPartitions("SPARK-28489: never drop offsets", 6) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 0, 10),
          KafkaOffsetRange(tp2, 0, 10),
          KafkaOffsetRange(tp3, 0, 1))) ==
        Seq(
          KafkaOffsetRange(tp1, 0, 3, None),
          KafkaOffsetRange(tp1, 3, 6, None),
          KafkaOffsetRange(tp1, 6, 10, None),
          KafkaOffsetRange(tp2, 0, 3, None),
          KafkaOffsetRange(tp2, 3, 6, None),
          KafkaOffsetRange(tp2, 6, 10, None),
          KafkaOffsetRange(tp3, 0, 1, None)))
  }

  private val tp1 = new TopicPartition("t1", 1)
  private val tp2 = new TopicPartition("t2", 1)
  private val tp3 = new TopicPartition("t3", 1)
}
