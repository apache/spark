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

package org.apache.spark.mllib.classification

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgeRDDImpl, GraphImpl}
import org.apache.spark.mllib.classification.LRonGraphX._
import org.apache.spark.mllib.impl.PeriodicGraphCheckpointer
import org.apache.spark.mllib.linalg.{DenseVector => SDV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Logging, Partitioner}

import scala.math._

class LRonGraphX(
  @transient var dataSet: Graph[VD, ED],
  val stepSize: Double,
  val regParam: Double,
  val useAdaGrad: Boolean,
  @transient var storageLevel: StorageLevel) extends Serializable with Logging {

  def this(
    input: RDD[(VertexId, LabeledPoint)],
    stepSize: Double = 1e-4,
    regParam: Double = 0.0,
    useAdaGrad: Boolean,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    this(initializeDataSet(input, storageLevel), stepSize, regParam, useAdaGrad, storageLevel)
  }

  @transient private var innerIter = 1
  @transient private val checkpointInterval = 10

  // SGD
  @transient private var delta: VertexRDD[(Double, Double)] = null
  @transient private var deltaSum: VertexRDD[Double] = null
  @transient private var multiplier: VertexRDD[Double] = null
  @transient private var margin: VertexRDD[Double] = null

  // MIS
  @transient private var q: VertexRDD[Double] = null
  @transient private var qWithLabel: VertexRDD[(Double, Double)] = null
  protected var epsilon = 1e-3

  // ALL
  @transient private var gradient: VertexRDD[Double] = null
  @transient private var vertices = dataSet.vertices
  @transient private var previousVertices = vertices
  @transient private val edges = dataSet.edges.asInstanceOf[EdgeRDDImpl[ED, _]]
    .mapEdgePartitions((pid, part) => part.withoutVertexAttributes[VD]).setName("edges")

  lazy val numFeatures: Long = features.count()
  lazy val numSamples: Long = samples.count()


  if (edges.getStorageLevel == StorageLevel.NONE) {
    edges.persist(storageLevel)
  }
  edges.count()

  if (vertices.getStorageLevel == StorageLevel.NONE) {
    vertices.persist(storageLevel)
  }
  vertices.count()

  def samples: VertexRDD[VD] = {
    dataSet.vertices.filter(t => t._1 < 0)
  }

  def features: VertexRDD[VD] = {
    dataSet.vertices.filter(t => t._1 >= 0)
  }

  def runSGD(iterations: Int): Unit = {
    for (iter <- 1 to iterations) {
      logInfo(s"Start SGD train (Iteration $iter/$iterations)")
      previousVertices = dataSet.vertices
      margin = forwardSGD(iter)
      println(s"train SGD (Iteration $iter/$iterations) cost : ${errorSGD(margin)}")
      gradient = backwardSGD(margin, iter)
      gradient = updateDeltaSum(gradient, iter)

      val thisIterStepSize = if (useAdaGrad) {
        stepSize * min(iter / 11.0, 1.0)
      } else {
        stepSize / sqrt(iter)
      }
      val thisIterL1StepSize = stepSize / sqrt(iter)
      vertices = updateWeight(gradient, iter, thisIterStepSize, thisIterL1StepSize)

      checkpoint()
      vertices.count()
      dataSet = GraphImpl(vertices, edges)
      unpersistVertices()
      logInfo(s"End SGD train (Iteration $iter/$iterations)")
      innerIter += 1
    }
  }

  private def forwardSGD(iter: Int): VertexRDD[VD] = {
    dataSet.aggregateMessages[Double](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val w = ctx.srcAttr
      val z = -w * x
      assert(!z.isNaN)
      ctx.sendToDst(z)
    }, _ + _, TripletFields.Src).setName(s"margin-$iter").persist(storageLevel)
  }

  private def backwardSGD(q: VertexRDD[VD], iter: Int): VertexRDD[Double] = {
    multiplier = dataSet.vertices.leftJoin(q) { (_, y, margin) =>
      margin match {
        case Some(z) =>
          (1.0 / (1.0 + math.exp(z))) - y
        case _ => 0.0
      }
    }
    multiplier.setName(s"multiplier-$iter").persist(storageLevel)
    GraphImpl(multiplier, dataSet.edges).aggregateMessages[Double](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val m = ctx.dstAttr
      val grad = x * m
      ctx.sendToSrc(grad)
    }, _ + _, TripletFields.Dst).mapValues { gradient =>
      gradient / numSamples
    }.setName(s"gradient-$iter").persist(storageLevel)
  }


  private def errorSGD(q: VertexRDD[VD]): Double = {
    dataSet.vertices.leftJoin(q) { case (_, y, margin) =>
      margin match {
        case Some(z) =>
          if (y > 0.0) {
            MLUtils.log1pExp(z)
          } else {
            MLUtils.log1pExp(z) - z
          }
        case _ => 0.0
      }
    }.map(_._2).reduce(_ + _) / numSamples
  }

  // Modified Iterative Scaling, the paper:
  // A comparison of numerical optimizers for logistic regression
  // http://research.microsoft.com/en-us/um/people/minka/papers/logreg/minka-logreg.pdf
  def runMIS(iterations: Int): Unit = {
    for (iter <- 1 to iterations) {
      logInfo(s"Start MIS train (Iteration $iter/$iterations)")
      previousVertices = dataSet.vertices
      q = forwardMIS(iter)
      println(s"train MIS (Iteration $iter/$iterations) cast : ${errorMIS(q)}")
      gradient = backwardMIS(q, iter)
      gradient = updateDeltaSum(gradient, iter)

      val thisIterStepSize = if (useAdaGrad) {
        stepSize * min(iter / 11.0, 1.0)
      } else {
        // stepSize / sqrt(iter)
        stepSize
      }
      val thisIterL1StepSize = stepSize / sqrt(iter)
      vertices = updateWeight(gradient, iter, thisIterStepSize, thisIterL1StepSize)
      checkpoint()
      vertices.count()
      dataSet = GraphImpl(vertices, edges)
      unpersistVertices()
      logInfo(s"End MIS train (Iteration $iter/$iterations)")
      innerIter += 1
    }
  }

  def backwardMIS(q: VertexRDD[VD], iter: Int): VertexRDD[Double] = {
    qWithLabel = dataSet.vertices.leftJoin(q.mapValues { z =>
      val q = 1.0 / (1.0 + exp(z))
      // if (q.isInfinite || q.isNaN || q == 0.0) println(z)
      assert(q != 0.0)
      q
    }) { (_, label, qv) => (label, qv.getOrElse(0.0)) }
    qWithLabel.setName(s"qWithLabel-$iter").persist(storageLevel)
    GraphImpl(qWithLabel, dataSet.edges).aggregateMessages[Array[Double]](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val y = ctx.dstAttr._1
      val q = ctx.dstAttr._2 * abs(x)
      assert(q != 0.0)
      val mu = if (signum(x * y) > 0.0) {
        Array(q, 0.0)
      } else {
        Array(0.0, q)
      }
      ctx.sendToSrc(mu)
    }, (a, b) => Array(a(0) + b(0), a(1) + b(1)), TripletFields.Dst).mapValues { mu =>
      // TODO: 0.0 right?
      val grad = if (epsilon == 0.0) {
        if (mu.min == 0.0) 0.0 else math.log(mu(0) / mu(1))
      } else {
        math.log((mu(0) + epsilon) / (mu(1) + epsilon))
      }
      -grad
    }.setName(s"gradient-$iter").persist(storageLevel)
  }

  def forwardMIS(iter: Int): VertexRDD[VD] = {
    dataSet.aggregateMessages[Double](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val w = ctx.srcAttr
      val y = ctx.dstAttr
      val z = y * w * x
      assert(!z.isNaN)
      ctx.sendToDst(z)
    }, _ + _, TripletFields.All).setName(s"q-$iter").persist(storageLevel)
  }

  private def errorMIS(q: VertexRDD[VD]): Double = {
    dataSet.vertices.leftJoin(q) { case (_, y, margin) =>
      margin match {
        case Some(z) =>
          if (y > 0.0) {
            MLUtils.log1pExp(-z)
          } else {
            MLUtils.log1pExp(z) - z
          }
        case _ => 0.0
      }
    }.map(_._2).reduce(_ + _) / numSamples
  }


  def saveModel(): LogisticRegressionModel = {
    val numFeatures = features.map(_._1).max().toInt + 1
    val featureData = new Array[Double](numFeatures)
    features.toLocalIterator.foreach { case (index, value) =>
      featureData(index.toInt) = value
    }
    new LogisticRegressionModel(new SDV(featureData), 0.0)
  }

  private def updateDeltaSum(gradient: VertexRDD[Double], iter: Int): VertexRDD[Double] = {
    if (gradient.getStorageLevel == StorageLevel.NONE) {
      gradient.setName(s"gradient-$iter").persist(storageLevel)
    }
    if (useAdaGrad) {
      // delta = adaGrad(deltaSum, gradient, 1.0, 1e-2, 1 - 1e-2)
      delta = adaGrad(deltaSum, gradient, 1.0, 1e-4, 1)
      delta.setName(s"delta-$iter").persist(storageLevel).count()

      gradient.unpersist(blocking = false)
      val newGradient = delta.mapValues(_._2).setName(s"gradient-$iter").persist(storageLevel)
      newGradient.count()

      if (deltaSum != null) deltaSum.unpersist(blocking = false)
      deltaSum = delta.mapValues(_._1).setName(s"deltaSum-$iter").persist(storageLevel)
      deltaSum.count()
      delta.unpersist(blocking = false)

      newGradient
    } else {
      gradient
    }
  }

  // Updater for L1 regularized problems
  private def updateWeight(
    delta: VertexRDD[Double],
    iter: Int,
    thisIterStepSize: Double,
    thisIterL1StepSize: Double): VertexRDD[Double] = {
    dataSet.vertices.leftJoin(delta) { (_, attr, gradient) =>
      gradient match {
        case Some(gard) => {
          var weight = attr
          weight -= thisIterStepSize * gard
          if (regParam > 0.0 && weight != 0.0) {
            val shrinkageVal = regParam * thisIterL1StepSize
            weight = signum(weight) * max(0.0, abs(weight) - shrinkageVal)
          }
          assert(!weight.isNaN)
          weight
        }
        case None => attr
      }
    }.setName(s"vertices-$iter").persist(storageLevel)
  }

  private def adaGrad(
    deltaSum: VertexRDD[Double],
    gradient: VertexRDD[Double],
    gamma: Double,
    epsilon: Double,
    rho: Double): VertexRDD[(Double, Double)] = {
    val delta = if (deltaSum == null) {
      gradient.mapValues(t => 0.0)
    }
    else {
      deltaSum
    }
    delta.innerJoin(gradient) { (_, gradSum, grad) =>
      val newGradSum = gradSum * rho + pow(grad, 2)
      val newGrad = grad * gamma / (epsilon + sqrt(newGradSum))
      (newGradSum, newGrad)
    }
  }

  private def checkpoint(): Unit = {
    val sc = vertices.sparkContext
    if (innerIter % checkpointInterval == 0 && sc.getCheckpointDir.isDefined) {
      vertices.checkpoint()
      edges.checkpoint()
      if (deltaSum != null) deltaSum.checkpoint()
    }
  }

  private def unpersistVertices(): Unit = {
    if (previousVertices != null) previousVertices.unpersist(blocking = false)
    if (margin != null) margin.unpersist(blocking = false)
    if (gradient != null) gradient.unpersist(blocking = false)
    if (multiplier != null) multiplier.unpersist(blocking = false)
    if (delta != null) delta.unpersist(blocking = false)
    if (q != null) q.unpersist(blocking = false)
    if (qWithLabel != null) qWithLabel.unpersist(blocking = false)
  }
}

object LRonGraphX {
  private[mllib] type ED = Double
  private[mllib] type VD = Double

  def trainSGD(
    input: RDD[LabeledPoint],
    numIterations: Int,
    stepSize: Double,
    regParam: Double,
    useAdaGrad: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): LogisticRegressionModel = {
    val data = input.zipWithIndex().map { case (LabeledPoint(label, features), id) =>
      val newLabel = if (label > 0.0) 1.0 else 0.0
      (id, LabeledPoint(newLabel, features))
    }
    val lr = new LRonGraphX(data, stepSize, regParam, useAdaGrad, storageLevel)
    lr.runSGD(numIterations)
    val model = lr.saveModel()
    model
  }

  def trainMIS(
    input: RDD[LabeledPoint],
    numIterations: Int,
    stepSize: Double,
    regParam: Double,
    epsilon: Double = 1e-3,
    useAdaGrad: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): LogisticRegressionModel = {
    val data = input.zipWithIndex().map { case (LabeledPoint(label, features), id) =>
      val newLabel = if (label > 0.0) 1.0 else -1.0
      (id, LabeledPoint(newLabel, features))
    }
    val lr = new LRonGraphX(data, stepSize, regParam, useAdaGrad, storageLevel)
    lr.epsilon = epsilon
    lr.runMIS(numIterations)
    val model = lr.saveModel()
    data.unpersist()
    model
  }

  private def initializeDataSet(
    input: RDD[(VertexId, LabeledPoint)],
    storageLevel: StorageLevel): Graph[VD, ED] = {
    val edges = input.flatMap { case (sampleId, labelPoint) =>
      val newId = newSampleId(sampleId)
      labelPoint.features.toBreeze.activeIterator.map { case (index, value) =>
        Edge(index, newId, value)
      }
    }
    val vertices = input.map { case (sampleId, labelPoint) =>
      val newId = newSampleId(sampleId)
      (newId, labelPoint.label)
    }
    var dataSet = Graph.fromEdges(edges, null, storageLevel, storageLevel)

    // degree-based hashing
    val numPartitions = edges.partitions.size
    val partitionStrategy = new DBHPartitioner(numPartitions)
    val newEdges = dataSet.outerJoinVertices(dataSet.degrees) { (vid, data, deg) =>
      deg.getOrElse(0)
    }.triplets.mapPartitions { itr =>
      itr.map { e =>
        (partitionStrategy.getPartition(e), Edge(e.srcId, e.dstId, e.attr))
      }
    }.partitionBy(new HashPartitioner(numPartitions)).map(_._2)
    dataSet = Graph.fromEdges(newEdges, null, storageLevel, storageLevel)
    // end degree-based hashing
    // dataSet = dataSet.partitionBy(PartitionStrategy.EdgePartition2D)

    dataSet.outerJoinVertices(vertices) { (vid, data, deg) =>
      // deg.getOrElse(Utils.random.nextGaussian() * 1e-2)
      deg.getOrElse(0)
    }
  }

  private def newSampleId(id: Long): VertexId = {
    -(id + 1L)
  }
}


/**
 * Degree-Based Hashing, the paper:
 * Distributed Power-law Graph Computing: Theoretical and Empirical Analysis
 */
private class DBHPartitioner(val partitions: Int) extends Partitioner {
  val mixingPrime: Long = 1125899906842597L

  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    val edge = key.asInstanceOf[EdgeTriplet[Int, ED]]
    val srcDeg = edge.srcAttr
    val dstDeg = edge.dstAttr
    val srcId = edge.srcId
    val dstId = edge.dstId
    if (srcDeg < dstDeg) {
      getPartition(srcId)
    } else {
      getPartition(dstId)
    }
  }

  def getPartition(idx: Int): PartitionID = {
    getPartition(idx.toLong)
  }

  def getPartition(idx: Long): PartitionID = {
    (abs(idx * mixingPrime) % partitions).toInt
  }

  override def equals(other: Any): Boolean = other match {
    case h: DBHPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
