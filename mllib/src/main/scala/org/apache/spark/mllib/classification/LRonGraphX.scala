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

import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.classification.LRonGraphX._
import org.apache.spark.mllib.linalg.{DenseVector => SDV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Logging, Partitioner}

class LRonGraphX(
  @transient var dataSet: Graph[VD, ED],
  val stepSize: Double,
  val regParam: Double,
  @transient var storageLevel: StorageLevel) extends Serializable with Logging {

  def this(
    input: RDD[(VertexId, LabeledPoint)],
    stepSize: Double = 1e-4,
    regParam: Double = 0.0,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    this(initializeDataSet(input, storageLevel), stepSize, regParam, storageLevel)
  }

  if (dataSet.vertices.getStorageLevel == StorageLevel.NONE) {
    dataSet.persist(storageLevel)
  }

  @transient private var innerIter = 1
  lazy val numFeatures: Long = features.count()
  lazy val numSamples: Long = samples.count()

  def samples: VertexRDD[VD] = {
    dataSet.vertices.filter(t => t._1 < 0)
  }

  def features: VertexRDD[VD] = {
    dataSet.vertices.filter(t => t._1 >= 0)
  }

  // Modified Iterative Scaling, the paper:
  // A comparison of numerical optimizers for logistic regression
  // http://research.microsoft.com/en-us/um/people/minka/papers/logreg/minka-logreg.pdf
  def run(iterations: Int): Unit = {
    for (iter <- 1 to iterations) {
      val previousDataSet = dataSet
      logInfo(s"Start train (Iteration $iter/$iterations)")
      val q = forward()
      q.persist(storageLevel)
      println(s"train (Iteration $iter/$iterations) Log likelihood : ${logLikelihood(q)}")
      val delta = backward(q)
      delta.persist(storageLevel)
      dataSet = updateWeight(delta, iter)
      dataSet = checkpoint(dataSet)
      dataSet.persist(storageLevel)

      dataSet.vertices.count()
      dataSet.edges.count()
      q.unpersist(blocking = false)
      delta.unpersist(blocking = false)
      previousDataSet.unpersist(blocking = false)
      logInfo(s"End train (Iteration $iter/$iterations)")
      innerIter += 1
    }
  }

  def saveModel(): LogisticRegressionModel = {
    val numFeatures = features.map(_._1).max().toInt + 1
    val featureData = new Array[Double](numFeatures)
    features.toLocalIterator.foreach { case (index, value) =>
      featureData(index.toInt) = value
    }
    new LogisticRegressionModel(new SDV(featureData), 0.0)
  }

  def error(q: VertexRDD[VD]): Double = {
    samples.join(q).map { case (_, (y, q)) =>
      if (y > 0.0) {
        MLUtils.log1pExp(q)
      } else {
        val margin = math.log((1.0 / q) - 1)
        MLUtils.log1pExp(q) - margin
      }
    }.reduce(_ + _) / numSamples
  }

  def logLikelihood(q: VertexRDD[VD]): Double = {
    samples.join(q).map { case (_, (y, q)) =>
      val score = q
      val label = if (y > 0.0) 1.0 else 0.0
      if (label == 1.0) {
        math.log(score)
      } else {
        math.log(1 - score)
      }
    }.reduce(_ + _) / numSamples
  }

  def forward(): VertexRDD[VD] = {
    dataSet.aggregateMessages[Double](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val w = ctx.srcAttr
      val z = -w * x
      assert(!z.isNaN)
      ctx.sendToDst(z)
    }, _ + _, TripletFields.Src).mapValues { z =>
      val q = 1.0 / (1.0 + exp(z))
      // if (q.isInfinite || q.isNaN || q == 0.0) println(z)
      assert(q != 0.0)
      q
    }
  }

  def backward(q: VertexRDD[VD]): VertexRDD[Double] = {
    dataSet.outerJoinVertices(q) { (_, label, qv) =>
      (label, qv.getOrElse(0.0))
    }.aggregateMessages[Double](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val y = ctx.dstAttr._1
      val q = ctx.dstAttr._2
      val multiplier = q - y
      val gradient = multiplier * x
      ctx.sendToSrc(gradient)
    }, _ + _, TripletFields.Dst).mapValues { gradient =>
      gradient / numSamples
    }
  }

  // Updater for L1 regularized problems
  def updateWeight(delta: VertexRDD[Double], iter: Int): Graph[VD, ED] = {
    val thisIterStepSize = stepSize / sqrt(iter)
    // val thisIterStepSize = stepSize
    val newVertices = dataSet.vertices.leftJoin(delta) { (_, attr, gradient) =>
      gradient match {
        case Some(gard) => {
          var weight = attr
          weight -= thisIterStepSize * gard
          if (regParam > 0.0) {
            val shrinkageVal = regParam * thisIterStepSize
            weight = signum(weight) * max(0.0, abs(weight) - shrinkageVal)
          }
          assert(!weight.isNaN)
          weight
        }
        case None => attr
      }
    }
    GraphImpl(newVertices, dataSet.edges)
  }

  private def checkpoint(corpus: Graph[VD, ED]): Graph[VD, ED] = {
    if (innerIter % 10 == 0 && corpus.edges.sparkContext.getCheckpointDir.isDefined) {
      logInfo(s"start checkpoint")
      corpus.checkpoint()
      val newVertices = corpus.vertices.mapValues(t => t)
      val newCorpus = GraphImpl(newVertices, corpus.edges)
      newCorpus.checkpoint()
      logInfo(s"end checkpoint")
      newCorpus
    } else {
      corpus
    }
  }
}

object LRonGraphX {
  private[mllib] type ED = Double
  private[mllib] type VD = Double

  def train(
    input: RDD[LabeledPoint],
    numIterations: Int,
    stepSize: Double,
    regParam: Double): LogisticRegressionModel = {
    train(input, numIterations, 0, stepSize, regParam)
  }

  def train(
    input: RDD[LabeledPoint],
    numIterations: Int,
    numFeatures: Int,
    stepSize: Double,
    regParam: Double,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): LogisticRegressionModel = {
    val data = input.zipWithIndex().map { case (LabeledPoint(label, features), id) =>
      val newLabel = if (label > 0.0) 1.0 else 0.0
      (id, LabeledPoint(newLabel, features))
    }
    val lr = new LRonGraphX(data, stepSize, regParam, storageLevel)
    lr.run(numIterations)
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
