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

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._


class SVDPlusPlusSuite extends SparkFunSuite with LocalSparkContext {

  test("Test SVD++ with mean square error on training set") {
    withSpark { sc =>
      val svdppErr = 8.0
      val edges = sc.textFile(getClass.getResource("/als-test.data").getFile).map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
      }
      val conf = new SVDPlusPlus.Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015) // 2 iterations
      val (graph, _) = SVDPlusPlus.run(edges, conf)
      graph.cache()
      val err = graph.vertices.map { case (vid, vd) =>
        if (vid % 2 == 1) vd._4 else 0.0
      }.reduce(_ + _) / graph.numEdges
      assert(err <= svdppErr)
    }
  }

  test("Test SVD++ with no edges") {
    withSpark { sc =>
      val edges = sc.emptyRDD[Edge[Double]]
      val conf = new SVDPlusPlus.Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015) // 2 iterations
      val (graph, _) = SVDPlusPlus.run(edges, conf)
      assert(graph.vertices.count() == 0)
      assert(graph.edges.count() == 0)
    }
  }

  test("SPARK-58177: training-phase message combiner sums both vectors element-wise") {
    val g1 = (Array(1.0, 2.0), Array(10.0, 20.0), 100.0)
    val g2 = (Array(3.0, 4.0), Array(30.0, 40.0), 200.0)
    val (out1, out2, out3) = SVDPlusPlus.combineTrainMessages(g1, g2)
    // Each component is the element-wise sum of the two messages.
    assert(out1.toSeq === Seq(4.0, 6.0))
    assert(out2.toSeq === Seq(40.0, 60.0))
    assert(out3 === 300.0)
    // Inputs are cloned, not mutated.
    assert(g1._1.toSeq === Seq(1.0, 2.0))
    assert(g1._2.toSeq === Seq(10.0, 20.0))
    assert(g2._1.toSeq === Seq(3.0, 4.0))
    assert(g2._2.toSeq === Seq(30.0, 40.0))
  }
}
