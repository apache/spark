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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite

import scala.annotation.nowarn
import scala.collection.mutable

class DetectingCyclesSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("test detecting cycles") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L), (1L, 4L), (2L, 5L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 1)
    @nowarn val collected =
      res
        .collect()
        .map(r => r.getAs[mutable.WrappedArray[Long]](0))

    assert(collected(0) == Seq(1, 2, 3, 1))
    res.unpersist()
  }

  test("test no cycles") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 4L), (4L, 5L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 0)
    res.unpersist()
  }

  test("test multiple cycles from one source") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 1L), (1L, 3L), (3L, 1L), (2L, 5L), (5L, 1L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 3)
    @nowarn val collected =
      res
        .sort(DetectingCycles.foundSeqCol)
        .collect()
        .map(r => r.getAs[mutable.WrappedArray[Long]](0))
    assert(collected(0) == Seq(1, 2, 1))
    assert(collected(1) == Seq(1, 2, 5, 1))
    assert(collected(2) == Seq(1, 3, 1))
    res.unpersist()
  }
}
