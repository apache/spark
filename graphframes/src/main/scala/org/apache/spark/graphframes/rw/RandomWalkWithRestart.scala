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

package org.apache.spark.graphframes.rw

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

/**
 * An implementation of random walk with restart. At each step of the walk, there is a probability
 * (defined by restartProbability) to reset the walk to the original starting node, otherwise the
 * walk continues to a random neighbor.
 */
/**
 * An implementation of random walk with restart. At each step of the walk, there is a probability
 * (defined by restartProbability) to reset the walk to the original starting node, otherwise the
 * walk continues to a random neighbor.
 */
class RandomWalkWithRestart extends RandomWalkBase {

  /** The probability of restarting the walk at each step (resets to starting node). */
  private var restartProbability: Double = 0.1

  /**
   * Sets the restart probability for the random walk.
   *
   * @param value
   *   the probability value (between 0.0 and 1.0)
   * @return
   *   this RandomWalkWithRestart instance for chaining
   */
  def setRestartProbability(value: Double): this.type = {
    restartProbability = value
    this
  }

  override protected def runIter(
      graph: GraphFrame,
      prevIterationDF: Option[DataFrame],
      iterSeed: Long): DataFrame = {
    val neighbors = graph.vertices.select(col(GraphFrame.ID), col(RandomWalkBase.nbrsColName))
    val walksDtype = ArrayType(graph.vertices.schema(GraphFrame.ID).dataType)
    var walks = if (prevIterationDF.isEmpty) {
      graph.vertices.select(
        col(GraphFrame.ID).alias("startingNode"),
        col(GraphFrame.ID).alias(RandomWalkBase.currVisitingVertexColName),
        explode(
          when(
            array_size(col(RandomWalkBase.nbrsColName)) > lit(0),
            array((0 until numWalksPerNode).map(_ => uuid()): _*)).otherwise(array()))
          .alias(RandomWalkBase.walkIdCol),
        array().cast(walksDtype).alias(RandomWalkBase.rwColName))
    } else {
      prevIterationDF.get.select(
        col("startingNode"),
        col(RandomWalkBase.currVisitingVertexColName),
        col(RandomWalkBase.walkIdCol),
        array().cast(walksDtype).alias(RandomWalkBase.rwColName))
    }

    val localRandom = new util.Random(iterSeed)

    for (_ <- (0 until batchSize)) {
      val currentSeed = localRandom.nextLong()

      walks = walks
        .join(
          neighbors,
          col(GraphFrame.ID) === col(RandomWalkBase.currVisitingVertexColName),
          "left")
        .withColumn("doRestart", rand(currentSeed) <= lit(restartProbability))
        .withColumn(
          "nextNode",
          when(col("doRestart"), col("startingNode")).otherwise(
            element_at(shuffle(col(RandomWalkBase.nbrsColName)), 1)))
        .select(
          col(RandomWalkBase.walkIdCol),
          col("startingNode"),
          col("nextNode").alias(RandomWalkBase.currVisitingVertexColName),
          array_append(
            col(RandomWalkBase.rwColName),
            col(RandomWalkBase.currVisitingVertexColName)).alias(RandomWalkBase.rwColName))
    }

    walks
  }
}
