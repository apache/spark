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

package org.apache.spark.mllib.clustering

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{Loader, MLUtils, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkContext, SparkException}

/**
 * :: Experimental ::
 *
 * Model produced by [[PowerIterationClustering]].
 *
 * @param k number of clusters
 * @param assignments an RDD of clustering [[PowerIterationClustering#Assignment]]s
 */
@Since("1.3.0")
@Experimental
class PowerIterationClusteringModel @Since("1.3.0") (
    @Since("1.3.0") val k: Int,
    @Since("1.3.0") val assignments: RDD[PowerIterationClustering.Assignment])
  extends Saveable with Serializable {

  @Since("1.4.0")
  override def save(sc: SparkContext, path: String): Unit = {
    PowerIterationClusteringModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

@Since("1.4.0")
object PowerIterationClusteringModel extends Loader[PowerIterationClusteringModel] {

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): PowerIterationClusteringModel = {
    PowerIterationClusteringModel.SaveLoadV1_0.load(sc, path)
  }

  private[clustering]
  object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.PowerIterationClusteringModel"

    @Since("1.4.0")
    def save(sc: SparkContext, model: PowerIterationClusteringModel, path: String): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("k" -> model.k)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val dataRDD = model.assignments.toDF()
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    @Since("1.4.0")
    def load(sc: SparkContext, path: String): PowerIterationClusteringModel = {
      implicit val formats = DefaultFormats
      val sqlContext = new SQLContext(sc)

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val k = (metadata \ "k").extract[Int]
      val assignments = sqlContext.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[PowerIterationClustering.Assignment](assignments.schema)

      val assignmentsRDD = assignments.map {
        case Row(id: Long, cluster: Int) => PowerIterationClustering.Assignment(id, cluster)
      }

      new PowerIterationClusteringModel(k, assignmentsRDD)
    }
  }
}

/**
 * :: Experimental ::
 *
 * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
 * [[http://www.icml2010.org/papers/387.pdf Lin and Cohen]]. From the abstract: PIC finds a very
 * low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise
 * similarity matrix of the data.
 *
 * @param k Number of clusters.
 * @param maxIterations Maximum number of iterations of the PIC algorithm.
 * @param initMode Initialization mode.
 *
 * @see [[http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)]]
 */
@Experimental
@Since("1.3.0")
class PowerIterationClustering private[clustering] (
    private var k: Int,
    private var maxIterations: Int,
    private var initMode: String) extends Serializable {

  import org.apache.spark.mllib.clustering.PowerIterationClustering._

  /**
   * Constructs a PIC instance with default parameters: {k: 2, maxIterations: 100,
   * initMode: "random"}.
   */
  @Since("1.3.0")
  def this() = this(k = 2, maxIterations = 100, initMode = "random")

  /**
   * Set the number of clusters.
   */
  @Since("1.3.0")
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /**
   * Set maximum number of iterations of the power iteration loop
   */
  @Since("1.3.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization mode. This can be either "random" to use a random vector
   * as vertex properties, or "degree" to use normalized sum similarities. Default: random.
   */
  @Since("1.3.0")
  def setInitializationMode(mode: String): this.type = {
    this.initMode = mode match {
      case "random" | "degree" => mode
      case _ => throw new IllegalArgumentException("Invalid initialization mode: " + mode)
    }
    this
  }

  /**
   * Run the PIC algorithm on Graph.
   *
   * @param graph an affinity matrix represented as graph, which is the matrix A in the PIC paper.
   *              The similarity s,,ij,, represented as the edge between vertices (i, j) must
   *              be nonnegative. This is a symmetric matrix and hence s,,ij,, = s,,ji,,. For
   *              any (i, j) with nonzero similarity, there should be either (i, j, s,,ij,,)
   *              or (j, i, s,,ji,,) in the input. Tuples with i = j are ignored, because we
   *              assume s,,ij,, = 0.0.
   *
   * @return a [[PowerIterationClusteringModel]] that contains the clustering result
   */
  @Since("1.5.0")
  def run(graph: Graph[Double, Double]): PowerIterationClusteringModel = {
    val w = normalize(graph)
    val w0 = initMode match {
      case "random" => randomInit(w)
      case "degree" => initDegreeVector(w)
    }
    pic(w0)
  }

  /**
   * Run the PIC algorithm.
   *
   * @param similarities an RDD of (i, j, s,,ij,,) tuples representing the affinity matrix, which is
   *                     the matrix A in the PIC paper. The similarity s,,ij,, must be nonnegative.
   *                     This is a symmetric matrix and hence s,,ij,, = s,,ji,,. For any (i, j) with
   *                     nonzero similarity, there should be either (i, j, s,,ij,,) or
   *                     (j, i, s,,ji,,) in the input. Tuples with i = j are ignored, because we
   *                     assume s,,ij,, = 0.0.
   *
   * @return a [[PowerIterationClusteringModel]] that contains the clustering result
   */
  @Since("1.3.0")
  def run(similarities: RDD[(Long, Long, Double)]): PowerIterationClusteringModel = {
    val w = normalize(similarities)
    val w0 = initMode match {
      case "random" => randomInit(w)
      case "degree" => initDegreeVector(w)
    }
    pic(w0)
  }

  /**
   * A Java-friendly version of [[PowerIterationClustering.run]].
   */
  @Since("1.3.0")
  def run(similarities: JavaRDD[(java.lang.Long, java.lang.Long, java.lang.Double)])
    : PowerIterationClusteringModel = {
    run(similarities.rdd.asInstanceOf[RDD[(Long, Long, Double)]])
  }

  /**
   * Runs the PIC algorithm.
   *
   * @param w The normalized affinity matrix, which is the matrix W in the PIC paper with
   *          w,,ij,, = a,,ij,, / d,,ii,, as its edge properties and the initial vector of the power
   *          iteration as its vertex properties.
   */
  private def pic(w: Graph[Double, Double]): PowerIterationClusteringModel = {
    val v = powerIter(w, maxIterations)
    val assignments = kMeans(v, k).mapPartitions({ iter =>
      iter.map { case (id, cluster) =>
        Assignment(id, cluster)
      }
    }, preservesPartitioning = true)
    new PowerIterationClusteringModel(k, assignments)
  }
}

@Since("1.3.0")
@Experimental
object PowerIterationClustering extends Logging {

  /**
   * :: Experimental ::
   * Cluster assignment.
   * @param id node id
   * @param cluster assigned cluster id
   */
  @Since("1.3.0")
  @Experimental
  case class Assignment(id: Long, cluster: Int)

  /**
   * Normalizes the affinity graph (A) and returns the normalized affinity matrix (W).
   */
  private[clustering]
  def normalize(graph: Graph[Double, Double]): Graph[Double, Double] = {
    val vD = graph.aggregateMessages[Double](
      sendMsg = ctx => {
        val i = ctx.srcId
        val j = ctx.dstId
        val s = ctx.attr
        if (s < 0.0) {
          throw new SparkException("Similarity must be nonnegative but found s($i, $j) = $s.")
        }
        if (s > 0.0) {
          ctx.sendToSrc(s)
        }
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly)
    GraphImpl.fromExistingRDDs(vD, graph.edges)
      .mapTriplets(
        e => e.attr / math.max(e.srcAttr, MLUtils.EPSILON),
        TripletFields.Src)
  }

  /**
   * Normalizes the affinity matrix (A) by row sums and returns the normalized affinity matrix (W).
   */
  private[clustering]
  def normalize(similarities: RDD[(Long, Long, Double)])
    : Graph[Double, Double] = {
    val edges = similarities.flatMap { case (i, j, s) =>
      if (s < 0.0) {
        throw new SparkException("Similarity must be nonnegative but found s($i, $j) = $s.")
      }
      if (i != j) {
        Seq(Edge(i, j, s), Edge(j, i, s))
      } else {
        None
      }
    }
    val gA = Graph.fromEdges(edges, 0.0)
    val vD = gA.aggregateMessages[Double](
      sendMsg = ctx => {
        ctx.sendToSrc(ctx.attr)
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly)
    GraphImpl.fromExistingRDDs(vD, gA.edges)
      .mapTriplets(
        e => e.attr / math.max(e.srcAttr, MLUtils.EPSILON),
        TripletFields.Src)
  }

  /**
   * Generates random vertex properties (v0) to start power iteration.
   *
   * @param g a graph representing the normalized affinity matrix (W)
   * @return a graph with edges representing W and vertices representing a random vector
   *         with unit 1-norm
   */
  private[clustering]
  def randomInit(g: Graph[Double, Double]): Graph[Double, Double] = {
    val r = g.vertices.mapPartitionsWithIndex(
      (part, iter) => {
        val random = new XORShiftRandom(part)
        iter.map { case (id, _) =>
          (id, random.nextGaussian())
        }
      }, preservesPartitioning = true).cache()
    val sum = r.values.map(math.abs).sum()
    val v0 = r.mapValues(x => x / sum)
    GraphImpl.fromExistingRDDs(VertexRDD(v0), g.edges)
  }

  /**
   * Generates the degree vector as the vertex properties (v0) to start power iteration.
   * It is not exactly the node degrees but just the normalized sum similarities. Call it
   * as degree vector because it is used in the PIC paper.
   *
   * @param g a graph representing the normalized affinity matrix (W)
   * @return a graph with edges representing W and vertices representing the degree vector
   */
  private[clustering]
  def initDegreeVector(g: Graph[Double, Double]): Graph[Double, Double] = {
    val sum = g.vertices.values.sum()
    val v0 = g.vertices.mapValues(_ / sum)
    GraphImpl.fromExistingRDDs(VertexRDD(v0), g.edges)
  }

  /**
   * Runs power iteration.
   * @param g input graph with edges representing the normalized affinity matrix (W) and vertices
   *          representing the initial vector of the power iterations.
   * @param maxIterations maximum number of iterations
   * @return a [[VertexRDD]] representing the pseudo-eigenvector
   */
  private[clustering]
  def powerIter(
      g: Graph[Double, Double],
      maxIterations: Int): VertexRDD[Double] = {
    // the default tolerance used in the PIC paper, with a lower bound 1e-8
    val tol = math.max(1e-5 / g.vertices.count(), 1e-8)
    var prevDelta = Double.MaxValue
    var diffDelta = Double.MaxValue
    var curG = g
    for (iter <- 0 until maxIterations if math.abs(diffDelta) > tol) {
      val msgPrefix = s"Iteration $iter"
      // multiply W by vt
      val v = curG.aggregateMessages[Double](
        sendMsg = ctx => ctx.sendToSrc(ctx.attr * ctx.dstAttr),
        mergeMsg = _ + _,
        TripletFields.Dst).cache()
      // normalize v
      val norm = v.values.map(math.abs).sum()
      logInfo(s"$msgPrefix: norm(v) = $norm.")
      val v1 = v.mapValues(x => x / norm)
      // compare difference
      val delta = curG.joinVertices(v1) { case (_, x, y) =>
        math.abs(x - y)
      }.vertices.values.sum()
      logInfo(s"$msgPrefix: delta = $delta.")
      diffDelta = math.abs(delta - prevDelta)
      logInfo(s"$msgPrefix: diff(delta) = $diffDelta.")
      // update v
      curG = GraphImpl.fromExistingRDDs(VertexRDD(v1), g.edges)
      prevDelta = delta
    }
    curG.vertices
  }

  /**
   * Runs k-means clustering.
   * @param v a [[VertexRDD]] representing the pseudo-eigenvector
   * @param k number of clusters
   * @return a [[VertexRDD]] representing the clustering assignments
   */
  private[clustering]
  def kMeans(v: VertexRDD[Double], k: Int): VertexRDD[Int] = {
    val points = v.mapValues(x => Vectors.dense(x)).cache()
    val model = new KMeans()
      .setK(k)
      .setRuns(5)
      .setSeed(0L)
      .run(points.values)
    points.mapValues(p => model.predict(p)).cache()
  }
}
