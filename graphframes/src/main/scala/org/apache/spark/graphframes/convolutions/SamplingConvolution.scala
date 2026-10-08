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

import org.apache.spark.graphframes.{GraphFrame, Logging}
import org.apache.spark.ml.functions._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.graphframes.expressions.KMinSampling

/**
 * A convolution operation on graph data that aggregates features from sampled neighbors using
 * min-hash sampling with a seed.
 *
 * For each vertex in the input GraphFrame, this class samples up to a maximum number of neighbors
 * using a min-hash approach based on hashing the destination vertex IDs with a provided seed. The
 * feature embeddings of these sampled neighbors are averaged, resulting in an aggregated neighbor
 * embedding. Optionally, this aggregated embedding can be concatenated with the original vertex
 * features to produce an updated feature vector.
 *
 * The graph is expected to have a standard structure with ID columns in vertices and edges (via
 * GraphFrame conventions).
 */
class SamplingConvolution extends Serializable with Logging {
  private var graph: GraphFrame = _
  private var featuresCol: String = "embedding"
  private var maxNbrs: Int = 50
  private var useEdgeDirections: Boolean = false
  private var seed: Long = 42L
  private var concatEmbeddings: Boolean = true

  /**
   * Specifies the GraphFrame to perform the convolution on.
   *
   * @param graph
   *   The input GraphFrame, which must contain vertices with an ID column and the features column
   *   as specified by setFeaturesCol. Feature columns should contain vector types (e.g., Vectors
   *   from MLlib).
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def onGraph(graph: GraphFrame): this.type = {
    this.graph = graph
    this
  }

  /**
   * Sets the name of the column in the vertices DataFrame containing the feature vectors.
   *
   * @param value
   *   The string name of the column holding vertex embeddings as Vectors.
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def setFeaturesCol(value: String): this.type = {
    featuresCol = value
    this
  }

  /**
   * Sets the maximum number of neighbors to sample per vertex.
   *
   * @param value
   *   The maximum number of neighbors (integer) to select using min-hash sampling.
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def setMaxNbrs(value: Int): this.type = {
    maxNbrs = value
    this
  }

  /**
   * Sets whether to use directed edge directions for neighbor sampling.
   *
   * If true, only outgoing edges are considered for sampling. If false, edges are treated as
   * undirected, and both incoming and outgoing directions are included.
   *
   * @param value
   *   Boolean indicating whether to consider edge directions.
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def setUseEdgeDirections(value: Boolean): this.type = {
    useEdgeDirections = value
    this
  }

  /**
   * Sets the seed for the random hash function used in min-hash sampling.
   *
   * @param value
   *   The long integer seed value for the xxhash64 hash function, ensuring reproducibility of
   *   sampling.
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def setSeed(value: Long): this.type = {
    seed = value
    this
  }

  /**
   * Sets whether to concatenate the aggregated neighbor features to the original vertex features.
   *
   * If true, the features column is updated by concatenating the aggregated neighbor embedding to
   * the original features. If false, a new column "nbr_embedding" is added containing the
   * aggregated neighbor features.
   *
   * @param value
   *   Boolean flag for concatenation.
   * @return
   *   This SamplingConvolution instance for method chaining.
   */
  def setConcatEmbeddings(value: Boolean): this.type = {
    concatEmbeddings = value
    this
  }

  /**
   * Executes the sampling convolution operation and returns the resulting DataFrame.
   *
   * The output DataFrame includes all original vertex columns. If concatEmbeddings is true, the
   * features column is updated with the concatenated [original features, aggregated neighbor
   * features]. If false, a new "nbr_embedding" column is added containing the aggregated neighbor
   * features as a Vector.
   *
   * @return
   *   A DataFrame with the convoluted embeddings, retaining all original vertex columns plus
   *   modifications.
   */
  def run(): DataFrame = {
    // Min-Hash sampling based on ID + seed
    val preAggs = (if (useEdgeDirections) {
                     graph.edges
                       .select(col(GraphFrame.SRC), col(GraphFrame.DST))
                   } else {
                     graph.edges
                       .select(GraphFrame.SRC, GraphFrame.DST)
                       .union(graph.edges.select(GraphFrame.DST, GraphFrame.SRC))
                       .distinct()
                   })
      .withColumn("hash", xxhash64(col(GraphFrame.DST), lit(seed)))
      .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))

    val vertexDtype = graph.vertices.schema(GraphFrame.ID).dataType
    val encoder = KMinSampling.getEncoder(
      graph.vertices.sparkSession,
      vertexDtype,
      Seq(GraphFrame.ID, "hash"))
    val samplingUDF = KMinSampling.fromSparkType(vertexDtype, maxNbrs, encoder)

    val sampledNbrs = preAggs
      .agg(samplingUDF(col(GraphFrame.DST), col("hash")).alias("nbrs"))
      .select(col(GraphFrame.ID), explode(col("nbrs")).alias("nbr"))

    val joined =
      sampledNbrs.join(
        graph.vertices.select(col(GraphFrame.ID).alias("nbr"), col(featuresCol)),
        Seq("nbr"),
        "left")

    val foldedNbrsFeatures =
      joined.groupBy(GraphFrame.ID).agg(Summarizer.mean(col(featuresCol)).alias("nbr_embedding"))

    val originalAndNbrs = graph.vertices.join(foldedNbrsFeatures, Seq(GraphFrame.ID), "left")

    if (concatEmbeddings) {
      originalAndNbrs.withColumn(
        featuresCol,
        array_to_vector(
          concat(vector_to_array(col(featuresCol)), vector_to_array(col("nbr_embedding")))))
    } else {
      originalAndNbrs
    }
  }
}
