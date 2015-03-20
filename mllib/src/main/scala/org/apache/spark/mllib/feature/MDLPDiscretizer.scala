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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._

/**
 * Entropy minimization discretizer based on Minimum Description Length Principle (MDLP)
 * proposed by Fayyad and Irani in Multi-Interval Discretization of Continuous-Valued
 * Attributes (1993) [1].
 * 
 * [1] Fayyad, U., & Irani, K. (1993). 
 * "Multi-interval discretization of continuous-valued attributes for classification learning."
 *
 * @param data RDD of LabeledPoint
 * 
 */
class MDLPDiscretizer private (val data: RDD[LabeledPoint]) extends Serializable with Logging {

  private val log2 = { x: Double => math.log(x) / math.log(2) }  
  private val isBoundary = (f1: Array[Long], f2: Array[Long]) => {
    (f1, f2).zipped.map(_ + _).filter(_ != 0).size > 1
  }
  private val maxLimitBins = Byte.MaxValue - Byte.MinValue + 1
  private val maxCandidates = 1e5  
  private val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
  private val nLabels = labels2Int.size
  
  /**
   * Get information about the attributes in order to perform a proper discretization.
   * 
   * @param contFeat Subset of indexes to be considered 
   * (in case it is not specified, they are calculated).
   * @param nFeatures Total number of input features.
   * @param dense If the dataset is dense or not.
   * @return Indexes of continuous features.
   * 
   */  
  private def processContinuousAttributes(
      contIndices: Option[Seq[Int]], 
      nFeatures: Int, 
      dense: Boolean) = {
    // Generate pairs according to the data format.
    def calcRawData = {
      dense match {
        case true =>
          data.flatMap({case LabeledPoint(label, values) =>
            for(k <- 0 until values.toArray.length) yield (k, values.toArray(k))
          })
        case false =>
          data.flatMap({case LabeledPoint(label, values) =>
            val v = values.asInstanceOf[SparseVector]
            for(i <- 0 until v.indices.size) yield (v.indices(i), v.values(i))
          })
      }    
    }
      
    /**
     *  (Pre-processing) Count the number of features 
     *  and select those with a high number of distinct values
     */
    contIndices match {
      case Some(s) => 
        // Attributes are in range 0..nfeat
        val intersect = (0 until nFeatures).seq.intersect(s)
        require(intersect.size == s.size)
        s.toArray
      case None =>        
        val countFeat = calcRawData.distinct.mapValues(d => 1L).reduceByKey(_ + _)
          .filter{case (_, c) => c > maxLimitBins}
        val cvars = countFeat.sortByKey().keys.collect()
        cvars       
    }
  }
  
  /**
   * Compute the initial candidate points by feature.
   * 
   * @param points RDD with distinct points by feature to be evaluated.
   * @param firstElements first elements by partition 
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
            // new boundary point: midpoint between this one and the previous one
            result = ((lastK, (x + lastX) / 2), accumFreqs.clone) +: result
            accumFreqs = Array.fill(nLabels)(0L)
          }
          
          lastK = k
          lastX = x
          lastFreqs = freqs
          accumFreqs = (accumFreqs, freqs).zipped.map(_ + _)
        }
       
        // Last point to add (evaluation)
        val lastPoint = if(index < (numPartitions - 1)) {
          bcFirsts.value(index + 1) match {
            case Some((k, x)) => 
              if(k != lastK) lastX else (x + lastX) / 2 
            case None => lastX // last point
          }
        }else{
            lastX // last partition
        }                    
        (((lastK, lastPoint), accumFreqs.clone) +: result).reverse.toIterator
      } else {
        Iterator.empty
      }             
    })
  }
  
  /**
   * Evaluate boundary points and select the most relevant. This version is used when 
   * the number of candidates exceeds the maximum per partition (distributed version).
   * 
   * @param candidates RDD of candidates points.
   * @param maxBins Maximum number of points to select
   * @param elementsByPart Maximum number of elements to evaluate in each partition.
   * @return Sequence of threshold values.
   * 
   */
  private def getThresholds(
      candidates: RDD[(Float, Array[Long])],
      maxBins: Int, 
      elementsByPart: Int) = {

    val partitions = { x: Long => math.ceil(x.toFloat / elementsByPart).toInt }    
    // Create queue
    val stack = new mutable.Queue[((Float, Float), Option[Float])]
    // Insert the extreme values in the stack
    stack.enqueue(((Float.NegativeInfinity, Float.PositiveInfinity), None))
    var result = Seq.empty[Float]

    // As long as there are more elements to evaluate, we continue
    while(stack.length > 0 && result.size < maxBins){
      val (bounds, lastThresh) = stack.dequeue
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
          case None => /* criteria not fulfilled, finish */
        }
      }
    }
    result.sorted
  }
  
  /**
   * Evaluates boundary points and selects the most relevant candidates.
   * Here, the evaluation is bounded by partition as the number of points is enoughly small.
   * 
   * @param candidates RDD of candidates points.
   * @param maxBins Maximum number of points to select.
   * @return Sequence of threshold values.
   * 
   */
  private def getThresholds(candidates: Array[(Float, Array[Long])], maxBins: Int) = {

    // Create queue
    val stack = new mutable.Queue[((Float, Float), Option[Float])]
    // Insert first in the stack
    stack.enqueue(((Float.NegativeInfinity, Float.PositiveInfinity), None))
    var result = Seq.empty[Float]

    while(stack.length > 0 && result.size < maxBins){
      val (bounds, lastThresh) = stack.dequeue
      // Filter candidates within the last range added
      val newCandidates = candidates.filter({ case (th, _) => 
        th > bounds._1 && th <= bounds._2 })      
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
    result.sorted
  }

  /**
   * Compute entropy minimization for candidate points in a range,
   * and select the best one according to MDLP criterion (RDD version).
   * 
   * @param candidates RDD of candidate points.
   * @param lastSelected Last selected threshold.
   * @param nLabels Number of labels in output attribute.
   * @return The minimum-entropy cut point.
   * 
   */
  private def evalThresholds(
      candidates: RDD[(Float, Array[Long])],
      lastSelected : Option[Float]) = {

    val numPartitions = candidates.partitions.size
    val sc = candidates.sparkContext

    // store total frequencies for each partition
    val totalsByPart = sc.runJob(candidates, { case it =>
      val accum = Array.fill(nLabels)(0L)
      for ((_, freqs) <- it) {for (i <- 0 until nLabels) accum(i) += freqs(i)}
      accum
    }: (Iterator[(Float, Array[Long])]) => Array[Long])
    
    // compute the global total
    var totals = Array.fill(nLabels)(0L)
    for (t <- totalsByPart) totals = (totals, t).zipped.map(_ + _)
    val bcTotalsByPart = sc.broadcast(totalsByPart)
    val bcTotals = sc.broadcast(totals)

    val result = candidates.mapPartitionsWithIndex({ (slice, it) =>
      // accumulate frequencies from left to the current partition
      var leftTotal = Array.fill(nLabels)(0L)
      for (i <- 0 until slice) leftTotal = (leftTotal, bcTotalsByPart.value(i)).zipped.map(_ + _)
      var entropyFreqs = Seq.empty[(Float, Array[Long], Array[Long], Array[Long])]
      // ... and from the current partition to the most-right partition
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
    val hs = InfoTheory.entropy(totals.toSeq, s)
    val k  = totals.filter(_ != 0).size

    // select the best threshold according to the criterion
    val finalCandidates = result.flatMap({
      case (cand, _, leftFreqs, rightFreqs) =>
        val k1 = leftFreqs.filter(_ != 0).size
        val s1 = leftFreqs.sum
        val hs1 = InfoTheory.entropy(leftFreqs, s1)
        val k2 = rightFreqs.filter(_ != 0).size
        val s2 = rightFreqs.sum
        val hs2 = InfoTheory.entropy(rightFreqs, s2)
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
    // Select the accepted candidate with the minimum weightedHs
    if (finalCandidates.count > 0) Some(finalCandidates.min._2) else None
  }
  
  /**
   * Compute entropy minimization for candidate points in a range,
   * and select the best one according to MDLP criterion (single-step version).
   * 
   * @param candidates Array of candidate points.
   * @param lastSelected last selected threshold.
   * @param nLabels Number of classes.
   * @return The minimum-entropy cut point.
   * 
   */
  private def evalThresholds(
      candidates: Array[(Float, Array[Long])],
      lastSelected : Option[Float],
      nLabels: Int): Option[Float] = {
    
    // Calculate total frequencies by label
    val totals = candidates.map(_._2).reduce((freq1, freq2) => (freq1, freq2).zipped.map(_ + _))
    
    // Compute accumulated frequencies (both left and right) by label
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
    val hs = InfoTheory.entropy(totals.toSeq, s)
    val k = totals.filter(_ != 0).size

    // select best threshold according to the criteria
    val finalCandidates = entropyFreqs.flatMap({
      case (cand, _, leftFreqs, rightFreqs) =>
        val k1 = leftFreqs.filter(_ != 0).size
        val s1 = if (k1 > 0) leftFreqs.sum else 0
        val hs1 = InfoTheory.entropy(leftFreqs, s1)
        val k2 = rightFreqs.filter(_ != 0).size
        val s2 = if (k2 > 0) rightFreqs.sum else 0
        val hs2 = InfoTheory.entropy(rightFreqs, s2)
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
    // Select the accepted candidate with the minimum weightedHs
    if (finalCandidates.size > 0) Some(finalCandidates.min._2) else None
  }
 
  /**
   * Run the entropy minimization discretizer on previously stored input data.
   * 
   * @param contFeat Feature indexes to discretize (in case not specified, they are calculated).
   * @param elementsByPart Maximum number of elements to evaluate in each partition.
   * @param maxBins Maximum number of bins by feature.
   * @return A discretization model with the thresholds by feature.
   * 
   */
  def runAll(
      contFeat: Option[Seq[Int]], 
      elementsByPart: Int,
      maxBins: Int) = {

    val sc = data.context 
    val nInstances = data.count
    val bLabels2Int = sc.broadcast(labels2Int)
    val classDistrib = data.map(d => bLabels2Int.value(d.label)).countByValue()
    val bclassDistrib = sc.broadcast(classDistrib)
    val (dense, nFeatures) = data.first.features match {
      case v: DenseVector => 
        (true, v.size)
      case v: SparseVector =>         
          (false, v.size)
    }
            
    val continuousVars = processContinuousAttributes(contFeat, nFeatures, dense)
    logInfo("Number of continuous attributes: " + continuousVars.distinct.size)
    logInfo("Total number of attributes: " + nFeatures)      
    if(continuousVars.isEmpty) logWarning("Discretization aborted." +
      "No continous attribute in the dataset")
    
    // Generate pairs ((attribute, value), class count)
    val featureValues = dense match {
      case true => 
        val bContinuousVars = sc.broadcast(continuousVars)
        data.flatMap({case LabeledPoint(label, values) =>            
          bContinuousVars.value.map{ k =>
            val c = Array.fill[Long](nLabels)(0L)
            c(bLabels2Int.value(label)) = 1L
            // BigDecimal(d).setScale(6, BigDecimal.RoundingMode.HALF_UP).toFloat
            ((k, values(k).toFloat), c)
          }                   
        })
      case false =>
        val bContVars = sc.broadcast(continuousVars)          
        data.flatMap({case LabeledPoint(label, values: Vector) =>
          val c = Array.fill[Long](nLabels)(0L)
          val v = FeatureUtils.compress(values, bContVars.value)
          v match {
            case SparseVector(size, newind, newval) => 
              c(bLabels2Int.value(label)) = 1L
              // BigDecimal(d).setScale(6, BigDecimal.RoundingMode.HALF_UP).toFloat
              for(i <- 0 until newind.size) yield ((newind(i), newval(i).toFloat), c)
          }
        })
    }
    
    // Group elements by attribute and value (distinct points)
    val nonzeros = featureValues.reduceByKey{ case (v1, v2) => 
      (v1, v2).zipped.map(_ + _)
    }
      
    // Add zero elements for sparse data
    val zeros = nonzeros
      .map{case ((k, p), v) => (k, v)}
      .reduceByKey{ case (v1, v2) =>  (v1, v2).zipped.map(_ + _)}
      .map{case (k, v) => 
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
      
    // Get only boundary points from the whole set of distinct values
    val initialCandidates = initialThresholds(sortedValues, firstElements)
      .map{case ((k, point), c) => (k, (point, c))}
      .cache() // It will be iterated through "big features" loop
      
    // Divide RDD into two categories according to the number of points by feature
    val bigIndexes = initialCandidates
      .countByKey()
      .filter{case (_, c) => c > maxCandidates}
    val bBigIndexes = sc.broadcast(bigIndexes)
      
    // Those feature with few points can be processed in a parallel way
    val smallCandidatesByAtt = initialCandidates
      .filter{case (k, _) => !bBigIndexes.value.contains(k) }
      .groupByKey()
      .mapValues(_.toArray)
                    
    val smallThresholds = smallCandidatesByAtt
      .mapValues(points => getThresholds(points.toArray.sortBy(_._1), maxBins))
    
    // Feature with too many points must be processed iteratively (rare condition) exceed
    logInfo("Number of features that exceed the maximum: " + 
        bigIndexes.size)
    var bigThresholds = Map.empty[Int, Seq[Float]]
    for (k <- bigIndexes.keys){ 
      val cands = initialCandidates.filter{case (k2, _) => k == k2}.values.sortByKey()
      bigThresholds += ((k, getThresholds(cands, maxBins, elementsByPart)))
    }

    // Join all thresholds and return them
    val bigThRDD = sc.parallelize(bigThresholds.toSeq)
    val thresholds = smallThresholds.union(bigThRDD)
      .sortByKey() // Important!
      .collect                        
    new DiscretizerModel(thresholds)
  }
}

object MDLPDiscretizer {

  /**
   * Train a entropy minimization discretizer given an RDD of LabeledPoints.
   * 
   * @param input RDD of LabeledPoint's.
   * @param continuousFeaturesIndexes Indexes of features to be discretized. 
   * Optional, if it is not provided the algorithm selects those features 
   * with more than 256 (byte range) distinct values.
   * @param maxBins Maximum number of bins by feature.
   * @param elementsPerPartition Maximum number of elements by partition.
   * @return A DiscretizerModel with the subsequent thresholds.
   * 
   */
  def train(
      input:
      RDD[LabeledPoint],
      continuousFeaturesIndexes: Option[Seq[Int]],
      maxBins: Int = Byte.MaxValue - Byte.MinValue + 1,
      elementsByPart: Int = 10000) = {
    new MDLPDiscretizer(input).runAll(continuousFeaturesIndexes, elementsByPart, maxBins)
  }
}
