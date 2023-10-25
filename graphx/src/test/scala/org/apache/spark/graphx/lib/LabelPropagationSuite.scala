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

class LabelPropagationSuite extends SparkFunSuite with LocalSparkContext {
  test("Label Propagation") {
    withSpark { sc =>
      // Construct a graph with two cliques connected by a single edge
      val n = 5
      val clique1 = for (u <- 0L until n; v <- 0L until n) yield Edge(u, v, 1)
      val clique2 = for (u <- 0L to n; v <- 0L to n) yield Edge(u + n, v + n, 1)
      val twoCliques = sc.parallelize(clique1 ++ clique2 :+ Edge(0L, n, 1))
      val graph = Graph.fromEdges(twoCliques, 1)
      // Run label propagation
      val labels = LabelPropagation.run(graph, n * 4).cache()

      // All vertices within a clique should have the same label
      val clique1Labels = labels.vertices.filter(_._1 < n).map(_._2).collect().toArray
      assert(clique1Labels.forall(_ == clique1Labels(0)))
      val clique2Labels = labels.vertices.filter(_._1 >= n).map(_._2).collect().toArray
      assert(clique2Labels.forall(_ == clique2Labels(0)))
      // The two cliques should have different labels
      assert(clique1Labels(0) != clique2Labels(0))
    }
  }
}
