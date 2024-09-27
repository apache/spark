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

import scala.jdk.CollectionConverters._

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

  def testWithMaxRecordsPerPartition(name: String, maxRecordsPerPartition: Long)
      (f: KafkaOffsetRangeCalculator => Unit): Unit = {
    val options = new CaseInsensitiveStringMap(
      Map("maxRecordsPerPartition" -> maxRecordsPerPartition.toString).asJava)
    test(s"with maxRecordsPerPartition = $maxRecordsPerPartition: $name") {
      f(KafkaOffsetRangeCalculator(options))
    }
  }

  def testWithMinPartitionsAndMaxRecordsPerPartition(name: String,
      minPartition: Int,
      maxRecordsPerPartition: Long)
      (f: KafkaOffsetRangeCalculator => Unit): Unit = {
    val options = new CaseInsensitiveStringMap(Map("minPartitions" -> minPartition.toString,
      "maxRecordsPerPartition" -> maxRecordsPerPartition.toString).asJava)
    test(s"with minPartitions = $minPartition " +
      s"and maxRecordsPerPartition = $maxRecordsPerPartition: $name") {
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

  testWithMinPartitions("N TopicPartitions to N offset ranges with executors", 3) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 1, 2),
          KafkaOffsetRange(tp2, 1, 2),
          KafkaOffsetRange(tp3, 1, 2)),
        Seq("exec1", "exec2", "exec3")) ===
        Seq(
          KafkaOffsetRange(tp1, 1, 2, Some("exec3")),
          KafkaOffsetRange(tp2, 1, 2, Some("exec1")),
          KafkaOffsetRange(tp3, 1, 2, Some("exec2"))))
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

  testWithMinPartitions("N skewed TopicPartitions to M offset ranges", 4) { calc =>
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
      4) { calc =>
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

  testWithMinPartitions("empty ranges ignored", 4) { calc =>
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

  testWithMinPartitions(
    "SPARK-36576: 0 small unsplit ranges and 3 large split ranges", 9) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 0, 10000),
          KafkaOffsetRange(tp2, 0, 15000),
          KafkaOffsetRange(tp3, 0, 20000))) ===
        Seq(
          KafkaOffsetRange(tp1, 0, 5000, None),
          KafkaOffsetRange(tp1, 5000, 10000, None),
          KafkaOffsetRange(tp2, 0, 5000, None),
          KafkaOffsetRange(tp2, 5000, 10000, None),
          KafkaOffsetRange(tp2, 10000, 15000, None),
          KafkaOffsetRange(tp3, 0, 5000, None),
          KafkaOffsetRange(tp3, 5000, 10000, None),
          KafkaOffsetRange(tp3, 10000, 15000, None),
          KafkaOffsetRange(tp3, 15000, 20000, None)))
  }

  testWithMinPartitions("SPARK-36576: 1 small unsplit range and 2 large split ranges", 6) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 0, 500),
          KafkaOffsetRange(tp2, 0, 12000),
          KafkaOffsetRange(tp3, 0, 15001))) ===
        Seq(
          KafkaOffsetRange(tp1, 0, 500, None),
          KafkaOffsetRange(tp2, 0, 6000, None),
          KafkaOffsetRange(tp2, 6000, 12000, None),
          KafkaOffsetRange(tp3, 0, 5000, None),
          KafkaOffsetRange(tp3, 5000, 10000, None),
          KafkaOffsetRange(tp3, 10000, 15001, None)))
  }

  testWithMinPartitions("SPARK-36576: 2 small unsplit ranges and 1 large split range", 6) { calc =>
    assert(
      calc.getRanges(
        Seq(
          KafkaOffsetRange(tp1, 0, 1),
          KafkaOffsetRange(tp2, 0, 1),
          KafkaOffsetRange(tp3, 0, 10000))) ===
        Seq(
          KafkaOffsetRange(tp1, 0, 1, None),
          KafkaOffsetRange(tp2, 0, 1, None),
          KafkaOffsetRange(tp3, 0, 2500, None),
          KafkaOffsetRange(tp3, 2500, 5000, None),
          KafkaOffsetRange(tp3, 5000, 7500, None),
          KafkaOffsetRange(tp3, 7500, 10000, None)))
  }

  testWithMaxRecordsPerPartition("SPARK-49259: 1 TopicPartition to N offset ranges", 4) { calc =>
    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 5))) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 5, None)))

    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 2))) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 2, None)))

    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 6)),
        executorLocations = Seq("location")) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 3, None),
          KafkaOffsetRange(tp1, 3, 6, None))) // location pref not set when minPartition is set
  }

  testWithMaxRecordsPerPartition("SPARK-49259: N TopicPartition to N offset ranges", 20) { calc =>
    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 40),
          KafkaOffsetRange(tp2, 1, 50),
          KafkaOffsetRange(tp3, 1, 60))
      ) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 20, None),
          KafkaOffsetRange(tp1, 20, 40, None),
          KafkaOffsetRange(tp2, 1, 17, None),
          KafkaOffsetRange(tp2, 17, 33, None),
          KafkaOffsetRange(tp2, 33, 50, None),
          KafkaOffsetRange(tp3, 1, 20, None),
          KafkaOffsetRange(tp3, 20, 40, None),
          KafkaOffsetRange(tp3, 40, 60, None)
        ))
  }

  testWithMinPartitionsAndMaxRecordsPerPartition("SPARK-49259: 1 TopicPartition " +
    "with low minPartitions value",
    1, 20) { calc =>
    assert(
      calc.getRanges(
        Seq(KafkaOffsetRange(tp1, 1, 40)
        )) ==
        Seq(
          KafkaOffsetRange(tp1, 1, 20, None),
          KafkaOffsetRange(tp1, 20, 40, None)
        ))
  }

  testWithMinPartitionsAndMaxRecordsPerPartition("SPARK-49259: 1 TopicPartition" +
    " with high minPartitions value",
    4, 20) { calc =>
      assert(
        calc.getRanges(
          Seq(KafkaOffsetRange(tp1, 1, 40)
        )) ==
          Seq(
            KafkaOffsetRange(tp1, 1, 10, None),
            KafkaOffsetRange(tp1, 10, 20, None),
            KafkaOffsetRange(tp1, 20, 30, None),
            KafkaOffsetRange(tp1, 30, 40, None)
          ))
  }

  private val tp1 = new TopicPartition("t1", 1)
  private val tp2 = new TopicPartition("t2", 1)
  private val tp3 = new TopicPartition("t3", 1)
}
