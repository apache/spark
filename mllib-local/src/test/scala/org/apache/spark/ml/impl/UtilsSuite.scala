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

package org.apache.spark.ml.impl

import org.apache.spark.ml.SparkMLFunSuite
import org.apache.spark.ml.impl.Utils.EPSILON


class UtilsSuite extends SparkMLFunSuite {

  test("EPSILON") {
    assert(1.0 + EPSILON > 1.0, s"EPSILON is too small: $EPSILON.")
    assert(1.0 + EPSILON / 2.0 === 1.0, s"EPSILON is too big: $EPSILON.")
  }

  test("combineWithinGroups") {
    val seq1 = Seq(
      (0, 0.0, 1L),
      (0, 1.0, 1L),
      (0, 2.0, 2L),
      (1, 1.0, 1L),
      (1, 2.0, 1L),
      (2, 0.5, 3L)
    )

    def computeAvgWithinGroups(
        iter: Iterator[(Int, Double, Long)],
        groups: Map[Int, Long]): Iterator[(Int, Double, Long)] = {
      Utils.combineWithinGroups[Int, (Double, Long), (Double, Long)](
        input = iter.map(t => (t._1, (t._2, t._3))),
        initOp = { case (v, c) => (v * c, c) },
        seqOp = { case ((sum, count), (v, c)) => (sum + v * c, count + c) },
        getSize = (k: Int) => groups.getOrElse(k, 1)
      ).map { case (key, (sum, count)) => (key, sum / count, count) }
    }

    assert(computeAvgWithinGroups(seq1.iterator, Map.empty[Int, Long]).toSeq === seq1)
    assert(computeAvgWithinGroups(seq1.iterator, Map(0 -> 2L, 1 -> 1L, 2 -> 3L)).toSeq ===
      Seq((0, 0.5, 2L), (0, 2.0, 2L), (1, 1.0, 1L), (1, 2.0, 1L), (2, 0.5, 3L)))
    assert(computeAvgWithinGroups(seq1.iterator, Map(0 -> 4L, 2 -> 5L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.0, 1L), (1, 2.0, 1L), (2, 0.5, 3L)))
    assert(computeAvgWithinGroups(seq1.iterator, Map(0 -> 3L, 1 -> 2L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.5, 2L), (2, 0.5, 3L)))
    assert(computeAvgWithinGroups(seq1.iterator, Map(0 -> 5L, 1 -> 5L, 2 -> 1L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.5, 2L), (2, 0.5, 3L)))

    val seq2 = Seq((0, 0.0, 1L))

    assert(computeAvgWithinGroups(seq2.iterator, Map.empty[Int, Long]).toSeq === seq2)
    assert(computeAvgWithinGroups(seq2.iterator, Map(0 -> 2L, 1 -> 1L, 2 -> 3L)).toSeq === seq2)
  }
}
