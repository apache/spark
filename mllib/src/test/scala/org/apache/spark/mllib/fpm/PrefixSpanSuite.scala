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
package org.apache.spark.mllib.fpm

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD

class PrefixspanSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("Prefixspan sequences mining using Integer type") {
    val sequences = Array(
      Array(3, 1, 3, 4, 5),
      Array(2, 3, 1),
      Array(3, 4, 4, 3),
      Array(1, 3, 4, 5),
      Array(2, 4, 1),
      Array(6, 5, 3))

    val rdd = sc.parallelize(sequences, 2).cache()

    def formatResultString(data: RDD[(Seq[Int], Int)]): String = {
      data.map(x => x._1.mkString(",") + ": " + x._2)
        .collect()
        .sortWith(_<_)
        .mkString("; ")
    }

    val prefixspan = new PrefixSpan()
      .setMinSupport(0.34)
      .setMaxPatternLength(50)
    val result1 = prefixspan.run(rdd)
    val len1 = result1.count().toInt
    val actualValue1 = formatResultString(result1)
    val expectedValue1 =
      "1,3,4,5: 2; 1,3,4: 2; 1,3,5: 2; 1,3: 2; 1,4,5: 2;" +
      " 1,4: 2; 1,5: 2; 1: 4; 2,1: 2; 2: 2; 3,1: 2; 3,3: 2;" +
      " 3,4,5: 2; 3,4: 3; 3,5: 2; 3: 5; 4,5: 2; 4: 4; 5: 3"
    assert(expectedValue1 == actualValue1)

    prefixspan.setMinSupport(0.5).setMaxPatternLength(50)
    val result2 = prefixspan.run(rdd)
    val expectedValue2 = "1: 4; 3,4: 3; 3: 5; 4: 4; 5: 3"
    val actualValue2 = formatResultString(result2)
    assert(expectedValue2 == actualValue2)

    prefixspan.setMinSupport(0.34).setMaxPatternLength(2)
    val result3 = prefixspan.run(rdd)
    val actualValue3 = formatResultString(result3)
    val expectedValue3 =
      "1,3: 2; 1,4: 2; 1,5: 2; 1: 4; 2,1: 2; 2: 2; 3,1: 2;" +
      " 3,3: 2; 3,4: 3; 3,5: 2; 3: 5; 4,5: 2; 4: 4; 5: 3"
    assert(expectedValue3 == actualValue3)
  }
}
