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

package org.apache.spark.util.sketch

import java.util.Random

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class CountMinSketchSuite extends FunSuite { // scalastyle:ignore funsuite
  test("accuracy - long") {
    val epsOfTotalCount = 0.0001
    val confidence = 0.99
    val seed = 42

    val numItems = 1000000
    val maxScale = 20
    val items = {
      val r = new Random(seed)
      Array.fill(numItems) { r.nextInt(1 << r.nextInt(maxScale)) }
    }

    val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
    items.foreach(sketch.add)

    val exactFreq = Array.fill(1 << maxScale)(0)
    items.foreach(exactFreq(_) += 1)

    val pCorrect = {
      val numErrors = exactFreq.zipWithIndex.map { case (f, i) =>
        val ratio = (sketch.estimateCount(i) - f).toDouble / numItems
        if (ratio > epsOfTotalCount) 1 else 0
      }.sum

      1.0 - numErrors.toDouble / exactFreq.length
    }

    assert(
      pCorrect > confidence,
      s"Confidence not reached: required $confidence, reached $pCorrect"
    )
  }
}
