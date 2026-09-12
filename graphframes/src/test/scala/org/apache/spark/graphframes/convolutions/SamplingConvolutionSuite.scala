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

package org.apache.spark.graphframes.convolutions

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class SamplingConvolutionSuite
    extends SparkFunSuite
    with GraphFrameTestSparkContext
    with BeforeAndAfterAll {

  private var testGraph: GraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test graph with 5 vertices and randomish edges
    val vertices: DataFrame = spark
      .createDataFrame(
        Seq(
          (0L, Vectors.dense(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)),
          (1L, Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
          (2L, Vectors.dense(2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0)),
          (3L, Vectors.dense(3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0)),
          (4L, Vectors.dense(4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0))))
      .toDF("id", "embedding")

    val edges: DataFrame = spark
      .createDataFrame(Seq((0L, 1L), (0L, 2L), (1L, 2L), (1L, 3L), (2L, 3L), (2L, 4L), (3L, 4L)))
      .toDF("src", "dst")

    testGraph = GraphFrame(vertices, edges)
  }

  test("big maxNbrs: result is correct average of feature vectors") {
    val conv = new SamplingConvolution()
      .onGraph(testGraph)
      .setFeaturesCol("embedding")
      .setMaxNbrs(10) // Large number, more than neighbors
      .setConcatEmbeddings(false)
      .setSeed(42L)

    val result = conv.run()
    // Collect nbr_embedding for vertex 0 (neighbors 1,2)
    val row = result.where(col("id") === 0L).select("nbr_embedding").collect()(0)
    val vec: Vector = row.getAs[Vector](0)
    val expected = Vectors.dense(
      (1 + 2) / 2.0,
      (2 + 3) / 2.0,
      (3 + 4) / 2.0,
      (4 + 5) / 2.0,
      (5 + 6) / 2.0,
      (6 + 7) / 2.0,
      (7 + 8) / 2.0,
      (8 + 9) / 2.0,
      (9 + 10) / 2.0,
      (10 + 11) / 2.0)
    assert(vec === expected)
  }

  test("small maxNbrs: min-hash sampling is reproducible") {
    val conv1 = new SamplingConvolution()
      .onGraph(testGraph)
      .setFeaturesCol("embedding")
      .setMaxNbrs(1)
      .setConcatEmbeddings(false)
      .setSeed(100L)

    val result1 = conv1.run()
    val conv2 = new SamplingConvolution()
      .onGraph(testGraph)
      .setFeaturesCol("embedding")
      .setMaxNbrs(1)
      .setConcatEmbeddings(false)
      .setSeed(100L)

    val result2 = conv2.run()
    // Both should have same nbr_embedding for same seeds
    val rows1 = result1.select("id", "nbr_embedding").collect().sortBy(r => r.getLong(0))
    val rows2 = result2.select("id", "nbr_embedding").collect().sortBy(r => r.getLong(0))
    rows1.zip(rows2).foreach { case (r1, r2) =>
      val v1: Vector = r1.getAs[DenseVector](1)
      val v2: Vector = r2.getAs[DenseVector](1)
      assert(v1 === v2)
    }
  }

  test("concatenating features increases size correctly") {
    val convConcat = new SamplingConvolution()
      .onGraph(testGraph)
      .setFeaturesCol("embedding")
      .setMaxNbrs(5)
      .setConcatEmbeddings(true)
      .setSeed(42L)

    val resultConcat = convConcat.run()
    val rowConcat = resultConcat.where(col("id") === 0L).select("embedding").collect()(0)
    val vecConcat: Vector = rowConcat.getAs[Vector](0)
    assert(vecConcat.size == 20) // Original 10 + 10 from nbr

    val convNoConcat = new SamplingConvolution()
      .onGraph(testGraph)
      .setFeaturesCol("embedding")
      .setMaxNbrs(5)
      .setConcatEmbeddings(false)
      .setSeed(42L)

    val resultNoConcat = convNoConcat.run()
    val rowNoConcat = resultNoConcat.where(col("id") === 0L).select("embedding").collect()(0)
    val vecNoConcat: Vector = rowNoConcat.getAs[Vector](0)
    assert(vecNoConcat.size == 10) // Only original
  }
}
