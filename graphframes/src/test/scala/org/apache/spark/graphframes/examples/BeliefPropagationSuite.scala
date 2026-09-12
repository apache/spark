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

package org.apache.spark.graphframes.examples

import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.examples.BeliefPropagation._
import org.apache.spark.graphframes.examples.Graphs.gridIsingModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

class BeliefPropagationSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("BP using GraphX and GraphFrame aggregateMessages") {
    val n = 3 // graph is n x n
    val numIter = 5 // iterations of BP

    // Create graphical model g.
    val g = gridIsingModel(spark, n)

    // Run BP using GraphX
    val gxResults = runBPwithGraphX(g, numIter)
    // Run BP using GraphFrames
    val gfResults = runBPwithGraphFrames(g, numIter)

    // Check beliefs.
    def checkResults(v: DataFrame): Unit = {
      v.select("belief").collect().foreach {
        case Row(belief: Double) =>
          assert(
            belief >= 0.0 && belief <= 1.0,
            s"Expected belief to be probability in [0,1], but found $belief")
        case _ => throw new GraphFramesUnreachableException()
      }
    }
    checkResults(gxResults.vertices)
    checkResults(gfResults.vertices)

    // Compare beliefs.
    val gxBeliefs = gxResults.vertices.select("id", "belief")
    val gfBeliefs = gfResults.vertices.select("id", "belief")
    gxBeliefs
      .join(gfBeliefs, "id")
      .select(gxBeliefs("belief").as("gxBelief"), gfBeliefs("belief").as("gfBelief"))
      .collect()
      .foreach {
        case Row(gxBelief: Double, gfBelief: Double) =>
          assert(math.abs(gxBelief - gfBelief) <= 1e-6)
        case _ => throw new GraphFramesUnreachableException()
      }
  }
}
