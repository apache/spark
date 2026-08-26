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

package org.apache.spark.graphframes.embeddings

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.transform
import org.apache.spark.sql.types.StringType
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFramesW2VException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.convolutions.SamplingConvolution
import org.apache.spark.graphframes.embeddings.RandomWalkEmbeddings.rwModels
import org.apache.spark.graphframes.rw.RandomWalkBase
import org.apache.spark.graphframes.rw.RandomWalkWithRestart

/**
 * RandomWalkEmbeddings is a class for generating node embeddings in a graph using random walks
 * and sequence-to-vector models. This implementation supports two types of embedding models:
 * Word2Vec and Hash2Vec, each with different performance characteristics.
 *
 * Word2Vec is based on the skip-gram model, which typically provides higher quality embeddings
 * due to its ability to capture semantic relationships through gradient descent optimization.
 * However, it is computationally expensive, requires more memory, and scales to approximately 20
 * million vertices in a graph, as it depends on transforming sequences into a vocabulary.
 *
 * Hash2Vec uses random projection hashing, making it much faster and more memory-efficient, with
 * excellent horizontal scaling properties. Its drawbacks include the need for wider embedding
 * dimensions (typically 512 or more, depending on graph size) and generally lower quality due to
 * its sparse nature.
 *
 * Additionally, this class supports optional neighbor aggregation, where embeddings from sampled
 * neighbors are aggregated (using average) and concatenated with the node's own embedding. This
 * technique leverages min-hash sampling and has shown to improve predictive power by over 20% in
 * synthetic tests. It is particularly efficient for Hash2Vec, as Word2Vec already incorporates
 * neighborhood information through random walks and skip-gram learning.
 *
 * This class provides also a way to run only embedding model (or sequnce2vec model) on top of
 * cached RandomWalks. Users can provide a path to cached walks in parquet format.
 *
 * To use this class, instantiate with a GraphFrame, set the random walk generator, choose the
 * sequence model (Word2Vec or Hash2Vec), and optionally configure other parameters like seed,
 * edge direction usage, neighbor aggregation, and maximum neighbors for sampling.
 */
class RandomWalkEmbeddings private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with Logging
    with WithIntermediateStorageLevel {
  private var sequenceModel: Either[Word2Vec, Hash2Vec] = _
  private var randomWalks: RandomWalkBase = _
  private var aggregateNeighbors: Boolean = true
  private var useEdgeDirections: Boolean = false
  private var maxNbrs: Int = 50
  private var seed: Long = 42L
  private var cachedRwPath: Option[String] = None
  private var cleanUpAfterRun: Boolean = false

  /**
   * Sets the sequence model to use for generating embeddings. This can be either a Word2Vec model
   * (Left(Word2Vec)) or a Hash2Vec model (Right(Hash2Vec)). No default; this must be set before
   * running.
   * @param value
   *   The sequence model to use.
   * @return
   *   This instance for method chaining.
   */
  def setSequenceModel(value: Either[Word2Vec, Hash2Vec]): this.type = {
    sequenceModel = value
    this
  }

  /**
   * Sets the random walk generator to use. No default; this must be set before running.
   * @param value
   *   The random walk generator instance.
   * @return
   *   This instance for method chaining.
   */
  def setRandomWalks(value: RandomWalkBase): this.type = {
    randomWalks = value
    this
  }

  /**
   * Sets the random seed for reproducibility. Default: 42L.
   * @param value
   *   The random seed.
   * @return
   *   This instance for method chaining.
   */
  def setSeed(value: Long): this.type = {
    seed = value
    this
  }

  /**
   * Sets whether to use edge directions in random walks and neighbor aggregation. If true,
   * considers directed edges; otherwise, treats the graph as undirected. Default: false.
   * @param value
   *   Boolean flag for using edge directions.
   * @return
   *   This instance for method chaining.
   */
  def setUseEdgeDirections(value: Boolean): this.type = {
    useEdgeDirections = value
    this
  }

  /**
   * Sets whether to aggregate neighbor embeddings via min-hash sampling, concatenating the
   * aggregated vector with the node's own embedding. This improves predictive power (e.g., +20%
   * in tests) and is more efficient for Hash2Vec. For Word2Vec, this adds redundant information
   * since it already learns neighborhood relations. Default: true.
   * @param value
   *   Boolean flag for neighbor aggregation.
   * @return
   *   This instance for method chaining.
   */
  def setAggregateNeighbors(value: Boolean): this.type = {
    aggregateNeighbors = value
    this
  }

  /**
   * Sets the maximum number of neighbors to sample for aggregation. Used only if
   * aggregateNeighbors is true. Default: 50.
   * @param value
   *   Maximum neighbors to sample.
   * @return
   *   This instance for method chaining.
   */
  def setMaxNbrs(value: Int): this.type = {
    maxNbrs = value
    this
  }

  /**
   * Sets the path to the existing cached RandomWalks if you want to run only embeddings model and
   * skip the sequences generation step.
   *
   * @param path
   *   to walks in parquet format
   * @return
   *   This instance for method chaining.
   */
  def useCachedRandomWalks(path: String): this.type = {
    cachedRwPath = Some(path)
    this
  }

  /**
   * Sets whether to clean up temporary random walk files after generating embeddings. Default:
   * false.
   * @param value
   *   Boolean flag for clean-up.
   * @return
   *   This instance for method chaining.
   */
  def setCleanUpAfterRun(value: Boolean): this.type = {
    cleanUpAfterRun = value
    this
  }

  /**
   * Executes the random walk embedding generation process. Requires that sequenceModel and
   * randomWalks are set. The input GraphFrame must have valid vertex and edge DataFrames, with
   * vertices containing an ID column.
   *
   * The process generates random walks, applies the chosen sequence model to produce initial
   * embeddings, and optionally aggregates neighbor embeddings if aggregateNeighbors is enabled.
   *
   * @return
   *   A DataFrame containing the original vertex columns plus an additional "embedding" column
   *   (as defined by RandomWalkEmbeddings.embeddingColName) of type Vector containing the node
   *   embeddings. If aggregateNeighbors is true, the embedding will be a concatenation of the
   *   node's embedding and the averaged embeddings of sampled neighbors.
   */
  def run(): DataFrame = {
    if (rwModels == null) {
      throw new GraphFramesW2VException("model should be set!")
    }
    val spark = graph.vertices.sparkSession
    val walksGenerator = randomWalks.onGraph(graph).setUseEdgeDirection(useEdgeDirections)
    val walks = if (cachedRwPath.isDefined) {
      spark.read.parquet(cachedRwPath.get)
    } else {
      walksGenerator.run()
    }

    val embeddings = sequenceModel match {
      case Left(w2v) => {
        val model = w2v.setInputCol(RandomWalkBase.rwColName)
        val preProcessedSequences =
          if (graph.vertices.schema(GraphFrame.ID).dataType != StringType) {
            walks.withColumn(
              RandomWalkBase.rwColName,
              transform(col(RandomWalkBase.rwColName), (c: Column) => c.cast(StringType)))
          } else {
            walks
          }

        val fittedW2V = model.fit(preProcessedSequences)
        fittedW2V.getVectors.withColumnsRenamed(
          Map("word" -> GraphFrame.ID, "vector" -> RandomWalkEmbeddings.embeddingColName))
      }
      case Right(h2v) => {
        h2v.run(walks).withColumnRenamed("vector", RandomWalkEmbeddings.embeddingColName)
      }
    }

    val persistedEmbeddings = if (aggregateNeighbors) {
      embeddings.persist(intermediateStorageLevel)
    } else {
      embeddings
    }

    // If requested, do the following:
    // - sample neighbors up to maxNbrs
    // - compute avg embedding of sample
    // - concatenate self and aggregated
    val aggregated = if (aggregateNeighbors) {
      new SamplingConvolution()
        .onGraph(GraphFrame(persistedEmbeddings, graph.edges))
        .setFeaturesCol(RandomWalkEmbeddings.embeddingColName)
        .setMaxNbrs(maxNbrs)
        .setSeed(seed)
        .setUseEdgeDirections(useEdgeDirections)
        .setConcatEmbeddings(true)
        .run()
    } else {
      // we need to create a new DataFrame, so unpersisting peristedEmbeddings
      // does not accidentally unpersist the result;
      // dummy operations are cheap, but will create a different plan.
      persistedEmbeddings
        .withColumnRenamed(RandomWalkEmbeddings.embeddingColName, "x")
        .withColumnRenamed("x", RandomWalkEmbeddings.embeddingColName)
    }

    val persistedDF = aggregated.persist(intermediateStorageLevel)

    // materialize
    persistedDF.count()
    resultIsPersistent()

    // clean memory
    persistedEmbeddings.unpersist()

    if (cleanUpAfterRun) {
      walksGenerator.cleanUp()
    }

    persistedDF
  }
}

/**
 * Companion object for RandomWalkEmbeddings.
 */
object RandomWalkEmbeddings extends Serializable {

  /** Name of the embedding column in the output DataFrame. */
  val embeddingColName: String = "embedding"

  private val rwModels = Seq("rw_with_restart")

  /**
   * While this API is public, it is not recommended to use it. The only purpose of this API is to
   * provide a smooth way to initialize the whole embeddings pipeline with a single method call
   * that is usable for Python API (py4j and Spark Connect).
   *
   * Instead of this API it is recommended to use new + setters of the class!
   */
  def pythonAPI(
      graph: GraphFrame,
      useEdgeDirection: Boolean,
      rwModel: String,
      rwMaxNbrs: Int,
      rwNumWalksPerNode: Int,
      rwBatchSize: Int,
      rwNumBatches: Int,
      rwSeed: Long,
      rwRestartProbability: Double,
      rwTemporaryPrefix: String,
      rwCachedWalks: String,
      sequenceModel: String,
      hash2vecContextSize: Int,
      hash2vecNumPartitions: Int,
      hash2vecEmbeddingsDim: Int,
      hash2vecDecayFunction: String,
      hash2vecGaussianSigma: Double,
      hash2vecHashingSeed: Int,
      hash2vecSignSeed: Int,
      hash2vecDoL2Norm: Boolean,
      hash2vecSafeL2: Boolean,
      word2vecMaxIter: Int,
      word2vecEmbeddingsDim: Int,
      word2vecWindowSize: Int,
      word2vecNumPartitions: Int,
      word2vecMinCount: Int,
      word2vecMaxSentenceLength: Int,
      word2vecSeed: Long,
      word2vecStepSize: Double,
      aggregateNeighbors: Boolean,
      aggregateNeighborsMaxNbrs: Int,
      aggregateNeighborsSeed: Long,
      cleanUpAfterRun: Boolean): DataFrame = {
    val randomWalksModel: RandomWalkBase = rwModel match {
      case "rw_with_restart" =>
        new RandomWalkWithRestart()
          .setRestartProbability(rwRestartProbability)
          .setBatchSize(rwBatchSize)
          .setNumBatches(rwNumBatches)
          .setMaxNbrsPerVertex(rwMaxNbrs)
          .setNumWalksPerNode(rwNumWalksPerNode)
          .setUseEdgeDirection(useEdgeDirection)
          .setTemporaryPrefix(rwTemporaryPrefix)
          .setGlobalSeed(rwSeed)
      case _: String =>
        throw new GraphFramesW2VException(
          s"unsupported RW $rwModel, supported: ${rwModels.mkString}")
    }

    val embeddingsModel = sequenceModel match {
      case "hash2vec" =>
        Right(
          new Hash2Vec()
            .setContextSize(hash2vecContextSize)
            .setDecayFunction(hash2vecDecayFunction)
            .setDoNormalization(hash2vecDoL2Norm, hash2vecSafeL2)
            .setEmbeddingsDim(hash2vecEmbeddingsDim)
            .setGaussianSigma(hash2vecGaussianSigma)
            .setHashingSeed(hash2vecHashingSeed)
            .setNumPartitions(hash2vecNumPartitions)
            .setSignHashSeed(hash2vecSignSeed))
      case "word2vec" =>
        Left(
          new Word2Vec()
            .setMaxIter(word2vecMaxIter)
            .setMaxSentenceLength(word2vecMaxSentenceLength)
            .setMinCount(word2vecMinCount)
            .setNumPartitions(word2vecNumPartitions)
            .setSeed(word2vecSeed)
            .setStepSize(word2vecStepSize)
            .setVectorSize(word2vecEmbeddingsDim)
            .setWindowSize(word2vecWindowSize))
      case _: String =>
        throw new GraphFramesW2VException(
          s"unsupported sequence model $sequenceModel, supported are 'word2vec' and 'hash2vec'")
    }

    val embeddingsGenerator = new RandomWalkEmbeddings(graph)
      .setRandomWalks(randomWalksModel)
      .setSequenceModel(embeddingsModel)
      .setAggregateNeighbors(aggregateNeighbors)
      .setMaxNbrs(aggregateNeighborsMaxNbrs)
      .setUseEdgeDirections(useEdgeDirection)
      .setSeed(aggregateNeighborsSeed)
      .setCleanUpAfterRun(cleanUpAfterRun)

    if (rwCachedWalks == "") {
      embeddingsGenerator.run()
    } else {
      embeddingsGenerator.useCachedRandomWalks(rwCachedWalks).run()
    }
  }
}
