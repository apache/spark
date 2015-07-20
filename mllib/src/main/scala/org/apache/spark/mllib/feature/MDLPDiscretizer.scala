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

package org.apache.spark.mllib.feature

import scala.collection.mutable

import breeze.linalg.{SparseVector => BSV}

import org.apache.spark.SparkContext._ 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._

/**
 * Entropy minimization discretizer based on Minimum Description Length Principle (MDLP)
 * proposed by Fayyad and Irani in 1993 [1].
 * 
 * [1] Fayyad, U., & Irani, K. (1993). 
 * "Multi-interval discretization of continuous-valued attributes for classification learning."
 *
 * @param data RDD of LabeledPoint
 * 
 */
class MDLPDiscretizer private (val data: RDD[LabeledPoint]) extends Serializable with Logging {

  private val log2 = { x: Double => math.log(x) / math.log(2) }  
  private def entropy(freqs: Seq[Long], n: Long) = {
    // val n = freqs.reduce(_ + _)
    freqs.aggregate(0.0)({ case (h, q) =>
      h + (if (q == 0) 0  else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }
  
  private val isBoundary = (f1: Array[Long], f2: Array[Long]) => {
    (f1, f2).zipped.map(_ + _).filter(_ != 0).size > 1
  }
  private val maxLimitBins = Byte.MaxValue - Byte.MinValue + 1
  private val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
  private val nLabels = labels2Int.size
  
  /**
   * Get information about the attributes before performing discretization.
   * 
   * @param contIndices Indexes to discretize (if not specified, they are calculated).
   * @param nFeatures Total number of input features.
   * @return Indexes of continuous features.
   * 
   */  
  private def processContinuousAttributes(
      contIndices: Option[Seq[Int]], 
      nFeatures: Int) = {
    contIndices match {
      case Some(s) => 
        // Attributes are in range 0..nfeat
        val intersect = (0 until nFeatures).seq.intersect(s)
        require(intersect.size == s.size)
        s.toArray
      case None =>
        (0 until nFeatures).toArray
    }
  }
  
  /**
   * Computes the initial candidate points by feature.
   * 
   * @param points RDD with distinct points by feature ((feature, point), class values).
   * @param firstElements First elements in partitions 
   * @return RDD of candidate points.
   * 
   */
  private def initialThresholds(
      points: RDD[((Int, Float), Array[Long])], 
      firstElements: Array[Option[(Int, Float)]]) = {
    
    val numPartitions = points.partitions.length
    val bcFirsts = points.context.broadcast(firstElements)      

    points.mapPartitionsWithIndex({ (index, it) =>      
      if(it.hasNext) {  
        var ((lastK, lastX), lastFreqs) = it.next()
        var result = Seq.empty[((Int, Float), Array[Long])]
        var accumFreqs = lastFreqs      
        
        for (((k, x), freqs) <- it) {           
          if(k != lastK) {
            // new attribute: add last point from the previous one
            result = ((lastK, lastX), accumFreqs.clone) +: result
            accumFreqs = Array.fill(nLabels)(0L)
          } else if(isBoundary(freqs, lastFreqs)) {
            // new boundary point: midpoint between this point and the previous one
            result = ((lastK, (x + lastX) / 2), accumFreqs.clone) +: result
            accumFreqs = Array.fill(nLabels)(0L)
          }
          
          lastK = k
          lastX = x
          lastFreqs = freqs
          accumFreqs = (accumFreqs, freqs).zipped.map(_ + _)
        }
       
        // Evaluate the last point in this partition with the first one in the next partition
        val lastPoint = if(index < (numPartitions - 1)) {
          bcFirsts.value(index + 1) match {
            case Some((k, x)) => if(k != lastK) lastX else (x + lastX) / 2 
            case None => lastX // last point in the attribute
          }
        }else{
            lastX // last point in the dataset
        }                    
        (((lastK, lastPoint), accumFreqs.clone) +: result).reverse.toIterator
      } else {
        Iterator.empty
      }             
    })
  }
  
  /**
   * Evaluate boundary points and select the most relevant. This version is used when 
   * the number of candidates exceeds the maximum size per partition (distributed version).
   * 
   * @param candidates RDD of candidates points (point, class histogram).
   * @param maxBins Maximum number of points to select
   * @param elementsByPart Maximum number of elements to evaluate in each partition.
   * @return Sequence of threshold values.
   * 
   */
  private def getThresholds(
      candidates: RDD[(Float, Array[Long])],
      maxBins: Int, 
      elementsByPart: Int) = {

    // Get the number of partitions according to the maximum size established by partition
    val partitions = { x: Long => math.ceil(x.toFloat / elementsByPart).toInt }    
    
    // Insert the extreme values in the stack (recursive iteration)
    val stack = new mutable.Queue[((Float, Float), Option[Float])]
    stack.enqueue(((Float.NegativeInfinity, Float.PositiveInfinity), None))
    var result = Seq.empty[Float]

    while(stack.length > 0 && result.size < maxBins){
      val (bounds, lastThresh) = stack.dequeue
      // Filter the candidates between the last limits added to the stack
      var cands = candidates.filter({ case (th, _) => th > bounds._1 && th <= bounds._2 })
      val nCands = cands.count
      if (nCands > 0) {
        cands = cands.coalesce(partitions(nCands))
        // Selects one threshold among the candidates and returns two partitions to recurse
        evalThresholds(cands, lastThresh) match {
          case Some(th) =>
            result = th +: result
            stack.enqueue(((bounds._1, th), Some(th)))
            stack.enqueue(((th, bounds._2), Some(th)))
          case None => /* criteria not fulfilled, finish! */
        }
      }
    }
    (Float.PositiveInfinity +: result).sorted
  }
  
  /**
   * Evaluates boundary points and selects the most relevant candidates (sequential version).
   * Here, the evaluation is bounded by partition as the number of points is small enough.
   * 
   * @param candidates RDD of candidates points (point, class histogram).
   * @param maxBins Maximum number of points to select.
   * @return Sequence of threshold values.
   * 
   */
  private def getThresholds(candidates: Array[(Float, Array[Long])], maxBins: Int) = {

    val stack = new mutable.Queue[((Float, Float), Option[Float])]
    // Insert first in the stack (recursive iteration)
    stack.enqueue(((Float.NegativeInfinity, Float.PositiveInfinity), None))
    var result = Seq.empty[Float]

    while(stack.length > 0 && result.size < maxBins){
      val (bounds, lastThresh) = stack.dequeue
      // Filter the candidates between the last limits added to the stack
      val newCandidates = candidates.filter({ case (th, _) => 
          th > bounds._1 && th <= bounds._2 
        })      
      if (newCandidates.size > 0) {
        evalThresholds(newCandidates, lastThresh, nLabels) match {
          case Some(th) =>
            result = th +: result
            stack.enqueue(((bounds._1, th), Some(th)))
            stack.enqueue(((th, bounds._2), Some(th)))
          case None => /* criteria not fulfilled, finish */
        }
      }
    }
    (Float.PositiveInfinity +: result).sorted
  }

  /**
   * Compute entropy minimization for candidate points in a range,
   * and select the best one according to the MDLP criterion (RDD version).
   * 
   * @param candidates RDD of candidate points (point, class histogram).
   * @param lastSelected Last selected threshold.
   * @return The minimum-entropy candidate.
   * 
   */
  private def evalThresholds(
      candidates: RDD[(Float, Array[Long])],
      lastSelected : Option[Float]) = {

    val numPartitions = candidates.partitions.size
    val sc = candidates.sparkContext

    // Compute the accumulated frequencies by partition
    val totalsByPart = sc.runJob(candidates, { case it =>
      val accum = Array.fill(nLabels)(0L)
      for ((_, freqs) <- it) {for (i <- 0 until nLabels) accum(i) += freqs(i)}
      accum
    }: (Iterator[(Float, Array[Long])]) => Array[Long])
    
    // Compute the total frequency for all partitions
    var totals = Array.fill(nLabels)(0L)
    for (t <- totalsByPart) totals = (totals, t).zipped.map(_ + _)
    val bcTotalsByPart = sc.broadcast(totalsByPart)
    val bcTotals = sc.broadcast(totals)

    val result = candidates.mapPartitionsWithIndex({ (slice, it) =>
      // Accumulate frequencies from the left to the current partition
      var leftTotal = Array.fill(nLabels)(0L)
      for (i <- 0 until slice) leftTotal = (leftTotal, bcTotalsByPart.value(i)).zipped.map(_ + _)
      var entropyFreqs = Seq.empty[(Float, Array[Long], Array[Long], Array[Long])]
      // ... and from the current partition to the rightmost partition
      for ((cand, freqs) <- it) {
        leftTotal = (leftTotal, freqs).zipped.map(_ + _)
        val rightTotal = (bcTotals.value, leftTotal).zipped.map(_ - _)
        entropyFreqs = (cand, freqs, leftTotal.clone, rightTotal) +: entropyFreqs
      }        
      entropyFreqs.iterator
    })

    // calculate h(S)
    // s: number of elements
    // k: number of distinct classes
    // hs: entropy        
    val s  = totals.sum
    val hs = entropy(totals.toSeq, s)
    val k  = totals.filter(_ != 0).size

    // select the best threshold according to MDLP
    val finalCandidates = result.flatMap({
      case (cand, _, leftFreqs, rightFreqs) =>
        val k1 = leftFreqs.filter(_ != 0).size; val s1 = leftFreqs.sum
        val hs1 = entropy(leftFreqs, s1)
        val k2 = rightFreqs.filter(_ != 0).size; val s2 = rightFreqs.sum
        val hs2 = entropy(rightFreqs, s2)
        val weightedHs = (s1 * hs1 + s2 * hs2) / s
        val gain = hs - weightedHs
        val delta = log2(math.pow(3, k) - 2) - (k * hs - k1 * hs1 - k2 * hs2)
        var criterion = (gain - (log2(s - 1) + delta) / s) > -1e-5
        lastSelected match {
          case None =>
          case Some(last) => criterion = criterion && (cand != last)
        }
        if (criterion) Seq((weightedHs, cand)) else Seq.empty[(Double, Float)]
    })
    // Select among the list of accepted candidate, that with the minimum weightedHs
    if (finalCandidates.count > 0) Some(finalCandidates.min._2) else None
  }
  
  /**
   * Compute entropy minimization for candidate points in a range,
   * and select the best one according to MDLP criterion (sequential version).
   * 
   * @param candidates Array of candidate points (point, class histogram).
   * @param lastSelected last selected threshold.
   * @param nLabels Number of classes.
   * @return The minimum-entropy cut point.
   * 
   */
  private def evalThresholds(
      candidates: Array[(Float, Array[Long])],
      lastSelected : Option[Float],
      nLabels: Int): Option[Float] = {
    
    // Calculate the total frequencies by label
    val totals = candidates.map(_._2).reduce((freq1, freq2) => (freq1, freq2).zipped.map(_ + _))
    
    // Compute the accumulated frequencies (both left and right) by label
    var leftAccum = Array.fill(nLabels)(0L)
    var entropyFreqs = Seq.empty[(Float, Array[Long], Array[Long], Array[Long])]
    for(i <- 0 until candidates.size) {
      val (cand, freq) = candidates(i)
      leftAccum = (leftAccum, freq).zipped.map(_ + _)
      val rightTotal = (totals, leftAccum).zipped.map(_ - _)
      entropyFreqs = (cand, freq, leftAccum.clone, rightTotal) +: entropyFreqs
    }

    // calculate h(S)
    // s: number of elements
    // k: number of distinct classes
    // hs: entropy
    val s = totals.sum
    val hs = entropy(totals.toSeq, s)
    val k = totals.filter(_ != 0).size

    // select best threshold according to the criteria
    val finalCandidates = entropyFreqs.flatMap({
      case (cand, _, leftFreqs, rightFreqs) =>
        val k1 = leftFreqs.filter(_ != 0).size
        val s1 = if (k1 > 0) leftFreqs.sum else 0
        val hs1 = entropy(leftFreqs, s1)
        val k2 = rightFreqs.filter(_ != 0).size
        val s2 = if (k2 > 0) rightFreqs.sum else 0
        val hs2 = entropy(rightFreqs, s2)
        val weightedHs = (s1 * hs1 + s2 * hs2) / s
        val gain = hs - weightedHs
        val delta = log2(math.pow(3, k) - 2) - (k * hs - k1 * hs1 - k2 * hs2)
        var criterion = (gain - (log2(s - 1) + delta) / s) > -1e-5
        
        lastSelected match {
            case None =>
            case Some(last) => criterion = criterion && (cand != last)
        }

        if (criterion) Seq((weightedHs, cand)) else Seq.empty[(Double, Float)]
    })
    // Select among the list of accepted candidate, that with the minimum weightedHs
    if (finalCandidates.size > 0) Some(finalCandidates.min._2) else None
  }
 
  /**
   * Run the entropy minimization discretizer on input data.
   * 
   * @param contFeat Indices to discretize (if not specified, the algorithm try to figure it out).
   * @param elementsByPart Maximum number of elements to keep in each partition.
   * @param maxBins Maximum number of thresholds per feature.
   * @return A discretization model with the thresholds by feature.
   * 
   */
  def runAll(
      contFeat: Option[Seq[Int]], 
      elementsByPart: Int,
      maxBins: Int) = {
    
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Basic info. about the dataset
    val sc = data.context; val nInstances = data.count
    val bLabels2Int = sc.broadcast(labels2Int)
    val classDistrib = data.map(d => bLabels2Int.value(d.label)).countByValue()
    val bclassDistrib = sc.broadcast(classDistrib)
    val (dense, nFeatures) = data.first.features match {
      case v: DenseVector => 
        (true, v.size)
      case v: SparseVector =>
        (false, v.size)
    }
            
    val continuousVars = processContinuousAttributes(contFeat, nFeatures)
    logInfo("Number of continuous attributes: " + continuousVars.distinct.size)
    logInfo("Total number of attributes: " + nFeatures)      
    if(continuousVars.isEmpty) logWarning("Discretization aborted. " +
      "No continous attribute in the dataset")
    
    // Generate pairs ((feature, point), class histogram)
    val featureValues = dense match {
      case true => 
        val bContinuousVars = sc.broadcast(continuousVars)
        data.flatMap({ case LabeledPoint(label, dv: DenseVector) =>            
            val c = Array.fill[Long](nLabels)(0L)
            c(bLabels2Int.value(label)) = 1L
            for(i <- 0 until dv.values.length) yield ((i, dv(i).toFloat), c)                  
        })
      case false =>
        val bContVars = sc.broadcast(continuousVars)          
        data.flatMap{ case LabeledPoint(label, sv: SparseVector) =>
          val c = Array.fill[Long](nLabels)(0L)
          c(bLabels2Int.value(label)) = 1L
          for(i <- 0 until sv.indices.length) yield ((sv.indices(i), sv.values(i).toFloat), c)
        }
    }
    
    // Group elements by feature and point (get distinct points)
    val nonzeros = featureValues.reduceByKey{ case (v1, v2) => 
      (v1, v2).zipped.map(_ + _)
    }
      
    // Add zero elements for sparse data
    val zeros = nonzeros
      .map{case ((k, p), v) => (k, v)}
      .reduceByKey{ case (v1, v2) =>  (v1, v2).zipped.map(_ + _)}
      .map{ case (k, v) => 
        val v2 = for(i <- 0 until v.length) yield bclassDistrib.value(i) - v(i)
        ((k, 0.0F), v2.toArray)
      }.filter{case (_, v) => v.sum > 0}
    val distinctValues = nonzeros.union(zeros)
    
    // Sort these values to perform the boundary points evaluation
    val sortedValues = distinctValues.sortByKey()
          
    // Get the first elements by partition for the boundary points evaluation
    val firstElements = sc.runJob(sortedValues, { case it =>
      if (it.hasNext) Some(it.next()._1) else None
    }: (Iterator[((Int, Float), Array[Long])]) => Option[(Int, Float)])
      
    // Filter those features selected by the user
    val arr = Array.fill(nFeatures) { false }
    continuousVars.map(arr(_) = true)
    val barr = sc.broadcast(arr)
    
    // Get only boundary points from the whole set of distinct values
    val initialCandidates = initialThresholds(sortedValues, firstElements)
      .map{case ((k, point), c) => (k, (point, c))}
      .filter({case (k, _) => barr.value(k)})
      .cache() // It will be iterated for "big" features
      
    // Divide RDD into two categories according to the number of points by feature
    val bigIndexes = initialCandidates
      .countByKey()
      .filter{case (_, c) => c > elementsByPart}
    val bBigIndexes = sc.broadcast(bigIndexes)
      
    // The features with a small number of points can be processed in a parallel way
    val smallThresholds = initialCandidates
      .filter{case (k, _) => !bBigIndexes.value.contains(k) }
      .groupByKey()
      .mapValues(_.toArray)
      .mapValues(points => getThresholds(points.toArray.sortBy(_._1), maxBins))
    
    // Feature with too many points must be processed iteratively (rare condition)
    logInfo("Number of features that exceed the maximum size per partition: " + 
        bigIndexes.size)
    var bigThresholds = Map.empty[Int, Seq[Float]]
    for (k <- bigIndexes.keys){ 
      val cands = initialCandidates.filter{case (k2, _) => k == k2}.values.sortByKey()
      bigThresholds += ((k, getThresholds(cands, maxBins, elementsByPart)))
    }

    // Join all thresholds in a single structure
    val bigThRDD = sc.parallelize(bigThresholds.toSeq)
    val thrs = smallThresholds.union(bigThRDD).collect()
    
    // Update the full list features with the thresholds calculated
    val base = Array.empty[Float]
    val thresholds = Array.fill(nFeatures)(base)   
    thrs.foreach({case (k, vth) => thresholds(k) = vth.toArray})    
    logInfo("Number of features with thresholds computed: " + thrs.length)
    
    new DiscretizerModel(thresholds)
  }
}

object MDLPDiscretizer {

  /**
   * Train a entropy minimization discretizer given an RDD of LabeledPoints.
   * 
   * @param input RDD of LabeledPoint's.
   * @param continuousFeaturesIndexes Indexes of features to be discretized. 
   * If it is not provided, the algorithm selects those features with more than 
   * 256 (byte range) distinct values.
   * @param maxBins Maximum number of thresholds to select per feature.
   * @param maxByPart Maximum number of elements by partition.
   * @return A DiscretizerModel with the subsequent thresholds.
   * 
   */
  def train(
      input: RDD[LabeledPoint],
      continuousFeaturesIndexes: Option[Seq[Int]] = None,
      maxBins: Int = 15,
      maxByPart: Int = 10000) = {
    new MDLPDiscretizer(input).runAll(continuousFeaturesIndexes, maxByPart, maxBins)
  }
}
