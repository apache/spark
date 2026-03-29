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
}
