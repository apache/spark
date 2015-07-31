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

class PrefixSpanSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("PrefixSpan using Integer type") {

    /*
      library("arulesSequences")
      prefixSpanSeqs = read_baskets("prefixSpanSeqs", info = c("sequenceID","eventID","SIZE"))
      freqItemSeq = cspade(
        prefixSpanSeqs,
        parameter = list(support =
          2 / length(unique(transactionInfo(prefixSpanSeqs)$sequenceID)), maxlen = 2 ))
      resSeq = as(freqItemSeq, "data.frame")
      resSeq
    */

    val sequences = Array(
      Array(1, 3, 4, 5),
      Array(2, 3, 1),
      Array(2, 4, 1),
      Array(3, 1, 3, 4, 5),
      Array(3, 4, 4, 3),
      Array(6, 5, 3))

    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixspan = new PrefixSpan()
      .setMinSupport(0.33)
      .setMaxPatternLength(50)
    val result1 = prefixspan.run(rdd)
    val expectedValue1 = Array(
      (Array(1), 4L),
      (Array(1, 3), 2L),
      (Array(1, 3, 4), 2L),
      (Array(1, 3, 4, 5), 2L),
      (Array(1, 3, 5), 2L),
      (Array(1, 4), 2L),
      (Array(1, 4, 5), 2L),
      (Array(1, 5), 2L),
      (Array(2), 2L),
      (Array(2, 1), 2L),
      (Array(3), 5L),
      (Array(3, 1), 2L),
      (Array(3, 3), 2L),
      (Array(3, 4), 3L),
      (Array(3, 4, 5), 2L),
      (Array(3, 5), 2L),
      (Array(4), 4L),
      (Array(4, 5), 2L),
      (Array(5), 3L)
    )
    assert(compareResults(expectedValue1, result1.collect()))

    prefixspan.setMinSupport(0.5).setMaxPatternLength(50)
    val result2 = prefixspan.run(rdd)
    val expectedValue2 = Array(
      (Array(1), 4L),
      (Array(3), 5L),
      (Array(3, 4), 3L),
      (Array(4), 4L),
      (Array(5), 3L)
    )
    assert(compareResults(expectedValue2, result2.collect()))

    prefixspan.setMinSupport(0.33).setMaxPatternLength(2)
    val result3 = prefixspan.run(rdd)
    val expectedValue3 = Array(
      (Array(1), 4L),
      (Array(1, 3), 2L),
      (Array(1, 4), 2L),
      (Array(1, 5), 2L),
      (Array(2, 1), 2L),
      (Array(2), 2L),
      (Array(3), 5L),
      (Array(3, 1), 2L),
      (Array(3, 3), 2L),
      (Array(3, 4), 3L),
      (Array(3, 5), 2L),
      (Array(4), 4L),
      (Array(4, 5), 2L),
      (Array(5), 3L)
    )
    assert(compareResults(expectedValue3, result3.collect()))
  }

  private def compareResults(
    expectedValue: Array[(Array[Int], Long)],
    actualValue: Array[(Array[Int], Long)]): Boolean = {
    expectedValue.map(x => (x._1.toSeq, x._2)).toSet ==
      actualValue.map(x => (x._1.toSeq, x._2)).toSet
  }

}
