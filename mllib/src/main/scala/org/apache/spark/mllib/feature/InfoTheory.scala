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

import breeze.linalg._
import breeze.numerics._
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM}
import scala.collection.immutable.HashMap
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkException
import org.apache.spark.HashPartitioner

/*
 * Basic and distributed primitives for Info-Theory computations: Mutual Information (MI)
 * and Conditional Mutual Information (CMI). These are adapted to compute and re-used some 
 * information between processes according to the generic formula proposed by Brown et al. in [1].
 * Data must be in a columnar format. 
 *
 * [1] Brown, G., Pocock, A., Zhao, M. J., & Lujn, M. (2012). 
 * "Conditional likelihood maximization: a unifying framework 
 * for information theoretic feature selection." 
 * The Journal of Machine Learning Research, 13(1), 27-66.
 * 
 *
 */

class InfoTheory extends Serializable {
  
  /**
   * Computes MI between two variables using histograms as input data.
   * 
   * @param data RDD of tuples (feature, 2-dim histogram).
   * @param yProb Vector of proportions for the secondary feature.
   * @param nInstances Number of instances.
   * @result A RDD of tuples (feature, MI).
   * 
   */
  protected def computeMutualInfo(
      data: RDD[(Int, BDM[Long])],
      yProb: BDV[Float],
      nInstances: Long) = {    
    
    val byProb = data.context.broadcast(yProb)       
    val result = data.mapValues({ m =>
      var mi = 0.0d
      // Aggregate by row (x)
      val xProb = sum(m(*, ::)).map(_.toFloat / nInstances)
      for(i <- 0 until m.rows){
        for(j <- 0 until m.cols){
          val pxy = m(i, j).toFloat / nInstances
          val py = byProb.value(j); val px = xProb(i)
          if(pxy != 0 && px != 0 && py != 0) // To avoid NaNs
            mi += pxy * (math.log(pxy / (px * py)) / math.log(2))
        }
      } 
      mi.toFloat        
    })
    result
  }
  
  /**
   * Computes MI and CMI between three variables using histograms as input data.
   * 
   * @param data RDD of tuples (feature, 3-dim histogram).
   * @param varY Index of the secondary feature.
   * @param varZ Index of the conditional feature.
   * @param marginalProb RDD of tuples (feature, marginal vector)
   * @param jointProb RDD of tuples (feature, joint matrices)
   * @param n Number of instances.
   * @result A RDD of tuples (feature, CMI).
   * 
   */
  protected def computeConditionalMutualInfo(
      data: RDD[(Int, BDV[BDM[Long]])],
      varY: Int,
      varZ: Int,
      marginalProb: RDD[(Int, BDV[Float])],
      jointProb: RDD[(Int, BDM[Float])],
      n: Long) = {

    val sc = data.context
    val yProb = sc.broadcast(marginalProb.lookup(varY)(0))
    val zProb = sc.broadcast(marginalProb.lookup(varZ)(0))
    val yzProb = sc.broadcast(jointProb.lookup(varY)(0))    

    val result = data.mapValues({ m =>
      var cmi = 0.0d; var mi = 0.0d
      // Aggregate values by row (X)
      val aggX = m.map(h1 => sum(h1(*, ::)).toDenseVector)
      // Use the previous variable to sum up and so obtaining X accumulators 
      val xProb = aggX.reduce(_ + _).apply(0).map(_.toFloat / n)
      // Aggregate all matrices in Z to obtain the joint probabilities for X and Y
      val xyProb = m.reduce(_ + _).apply(0).map(_.toFloat / n)  
      val xzProb = aggX.map(_.map(_.toFloat / n))
      
      for(z <- 0 until m.length){
        for(x <- 0 until m(z).rows){
          for(y <- 0 until m(z).cols) {
            val pz = zProb.value(z); val pxyz = (m(z)(x, y).toFloat / n) / pz
            val pxz = xzProb(z)(x) / pz; val pyz = yzProb.value(y, z) / pz
            if(pxz != 0 && pyz != 0 && pxyz != 0)
              cmi += pz * pxyz * (math.log(pxyz / (pxz * pyz)) / math.log(2))
            if (z == 0) { // Do MI computations only once
              val px = xProb(x); val pxy = xyProb(x, y); val py = yProb.value(y)
              if(pxy != 0 && px != 0 && py != 0)
                mi += pxy * (math.log(pxy / (px * py)) / math.log(2))
            }
          }            
        }
      } 
      (mi.toFloat, cmi.toFloat)        
    })
    result
  }
  
}

/**
 * Class that computes histograms for the Info-Theory primitives and starts 
 * the selection process (sparse and high-dimensional version).
 *
 * The constructor method caches a single attribute (usually the class) to be 
 * re-used in the next iterations as conditional variable. This also computes 
 * and caches the relevance values, and the marginal and joint proportions derived 
 * from this operation.
 * 
 * @param data RDD of tuples (feature, values).
 * @param fixedFeat Index of the fixed attribute (usually the class). 
 * @param nInstances Number of samples.
 * @param nFeatures Number of features.
 *
 */
class InfoTheorySparse (
    val data: RDD[(Int, BV[Byte])], 
    fixedFeat: Int,
    val nInstances: Long,      
    val nFeatures: Int) extends InfoTheory with Serializable {
  
  // Broadcast the class attribute (fixed)
  val fixedVal = data.lookup(fixedFeat)(0)  
  val fixedCol = (fixedFeat, data.context.broadcast(fixedVal))
  val fixedColHistogram = computeFrequency(fixedCol._2.value, nInstances)
  
  // Compute and cache the relevance values, and the marginal and joint proportions
  val (marginalProb, jointProb, relevances) = {
    val histograms = computeHistograms(data, 
      fixedCol, fixedColHistogram)
    val jointTable = histograms.mapValues(_.map(_.toFloat / nInstances))
      .partitionBy(new HashPartitioner(400))
      .cache()
    val marginalTable = jointTable.mapValues(h => sum(h(*, ::)).toDenseVector)
      .partitionBy(new HashPartitioner(400))
      .cache()
    
    // Remove the class attribute from the computations
    val label = nFeatures - 1 
    val fdata = histograms.filter{case (k, _) => k != label}
    val marginalY = marginalTable.lookup(fixedFeat)(0)
    
    // Compute MI between all input features and the class (relevances)
    val relevances = computeMutualInfo(fdata, marginalY, nInstances).cache()
    (marginalTable, jointTable, relevances)
  }

  private def computeFrequency(data: BV[Byte], nInstances: Long) = {
    val tmp = data.activeValuesIterator.toArray
      .groupBy(l => l).map(t => (t._1, t._2.size.toLong))
    val lastElem = (0: Byte, nInstances - tmp.filter({case (v, _) => v != 0}).values.sum)
    tmp + lastElem
  }
  
  def getRelevances(varY: Int) = relevances
  
  /**
   * Computes simple and conditional redundancy for all input attributes with respect to 
   * a secondary variable (Y) and a conditional variable (already cached).
   * 
   * @param varY Index of the secondary feature (class).
   * @result A RDD of tuples (feature, (redundancy, conditional redundancy)).
   * 
   */
  def getRedundancies(
      varY: Int) = {
    
    // Get and broadcast Y and the fixed variable
    val ycol = data.lookup(varY)(0)
    val (varZ, zcol) = fixedCol

    // Compute conditional histograms for all variables with respect to Y and the fixed variable
    val histograms3d = computeConditionalHistograms(
        data, (varY, ycol))
        .filter{case (k, _) => k != varZ && k != varY}
    
    // Compute CMI and MI for all input variables with respect to Y and Z
    computeConditionalMutualInfo(histograms3d, varY, varZ, 
        marginalProb, jointProb, nInstances)
 }
    
  /**
   * Computes 2-dim histograms for all input attributes with respect to 
   * a secondary variable (class).
   * 
   * @param RDD of tuples (feature, values)
   * @param ycol (feature, values).
   * @param yhist Histogram for variable Y (class).
   * 
   * @result A RDD of tuples (feature, histogram).
   * 
   */
  private def computeHistograms(
      filterData:  RDD[(Int, BV[Byte])],
      ycol: (Int, Broadcast[BV[Byte]]),
      yhist: Map[Byte, Long]) = {
    
    val bycol = ycol._2
    // Distinct values for Y
    val ys = if(ycol._2.value.size > 0) ycol._2.value.activeValuesIterator.max + 1 else 1
      
    filterData.map({ case (feat, xcol) =>  
      val xs = if(xcol.size > 0) xcol.activeValuesIterator.max + 1 else 1 
      val result = BDM.zeros[Long](xs, ys)
      
      val histCls = mutable.HashMap.empty ++= yhist // clone
      for ((inst, x) <- xcol.activeIterator){
        val y = bycol.value(inst)  
        histCls += y -> (histCls(y) - 1)
        result(xcol(inst), y) += 1
      }
      // Zeros count
      histCls.foreach({ case (c, q) => result(0, c) += q })
      feat -> result
    })
  }
  
  /**
   * Computes 3-dim histograms for all input attributes with respect to 
   * a secondary variable and the conditional variable. Conditional feature 
   * (class) must be already cached. 
   * 
   * @param filterData RDD of tuples (feature, values)
   * @param ycol (feature, value vector).
   * 
   * @result A RDD of tuples (feature, histogram).
   * 
   */
  private def computeConditionalHistograms(
    filterData: RDD[(Int, BV[Byte])],
    ycol: (Int, BV[Byte])) = {
    
      // Compute the histogram for variable Y and get its values.
      val bycol = filterData.context.broadcast(ycol._2)      
      val yhist = new mutable.HashMap[(Byte, Byte), Long]()
      ycol._2.activeIterator.foreach({case (inst, y) => 
          val z = fixedCol._2.value(inst)
          yhist += (y, z) -> (yhist.getOrElse((y, z), 0L) + 1)
      })      
      val byhist = filterData.context.broadcast(yhist)
      
      // Get the vector for the conditional variable and compute its histogram
      val bzcol = fixedCol._2
      val bzhist = fixedColHistogram
      
      // Get the maximum sizes for both single variables
      val ys = if(ycol._2.size > 0) ycol._2.activeValuesIterator.max + 1 else 1
      val zs = if(fixedCol._2.value.size > 0) fixedCol._2.value.activeValuesIterator.max + 1 else 1
      
      // Map operation to compute histogram per feature
      val result = filterData.map({ case (feat, xcol) =>   
        // Initialization
        val xs = if(xcol.size > 0) xcol.activeValuesIterator.max + 1 else 1
        val result = BDV.fill[BDM[Long]](zs){
          BDM.zeros[Long](xs, ys)
        }
        
        // Computations for all elements in X also appearing in Y        
        val yzhist = mutable.HashMap.empty ++= byhist.value
        for ((inst, x) <- xcol.activeIterator){     
          val y = bycol.value(inst)
          val z = bzcol.value(inst)        
          if(y != 0) yzhist += (y, z) -> (yzhist((y,z)) - 1)
          result(z)(xcol(inst), y) += 1
        }
        
        // Computations for non-zero elements in Y and not appearing in X
        yzhist.foreach({case ((y, z), q) => result(z)(0, y) += q})
        
        // Computations for Z elements with X and Y equal to zero
        bzhist.map({ case (zval, _) => 
          val rest = bzhist(zval) - sum(result(zval))
          result(zval)(0, 0) += rest
        })
        
        feat -> result
    })
    bycol.unpersist()
    result
  }
}

/**
 * Class that computes histograms for the Info-Theory primitives and starts 
 * the selection process (dense version).
 *
 * The constructor method caches a single attribute (usually the class) to be 
 * re-used in the next iterations as conditional variable. This also computes 
 * and caches the relevance values, and the marginal and joint proportions derived 
 * from this operation.
 * 
 * @param data RDD of tuples (feature, values).
 * @param fixedFeat Index of the fixed attribute (usually the class). 
 * @param nInstances Number of samples.
 * @param nFeatures Number of features.
 *
 */
class InfoTheoryDense (
    val data: RDD[(Int, (Int, Array[Byte]))], 
    fixedFeat: Int,
    val nInstances: Long,      
    val nFeatures: Int) extends InfoTheory with Serializable {
    
  // Count the number of distinct values per feature to limit the size of matrices
  val counterByFeat = {
      val counter = data.mapValues({ case (_, v) => if(!v.isEmpty) v.max + 1 else 1})
          .reduceByKey((m1, m2) => if(m1 > m2) m1 else m2)
          .collectAsMap()
          .toMap
      data.context.broadcast(counter)
  }
  
  // Broadcast fixed attribute
  val fixedCol = {
    val yvals = data.lookup(fixedFeat)
    val ycol = Array.ofDim[Array[Byte]](yvals.length)
    yvals.foreach({ case (b, v) => ycol(b) = v })
    fixedFeat -> data.context.broadcast(ycol)
  }
  
  // Compute and cache the relevance values, and the marginal and joint proportions derived
  val (marginalProb, jointProb, relevances) = {
    val histograms = computeHistograms(data, fixedCol)
    val jointTable = histograms.mapValues(_.map(_.toFloat / nInstances))
      .partitionBy(new HashPartitioner(400))
      .cache()
    val marginalTable = jointTable.mapValues(h => sum(h(*, ::)).toDenseVector)
      .partitionBy(new HashPartitioner(400))
      .cache()
    
    // Remove output feature from the computations and compute MI with respect to the fixed var
    val fdata = histograms.filter{case (k, _) => k != fixedFeat}
    val marginalY = marginalTable.lookup(fixedFeat)(0)
    val relevances = computeMutualInfo(fdata, marginalY, nInstances).cache()
    (marginalTable, jointTable, relevances)
  }
    
  def getRelevances(varY: Int) = relevances
  
  /**
   * Computes simple and conditional redundancy for all input attributes with respect to 
   * a secondary variable (Y) and a conditional variable (already cached).
   * 
   * @param varY Index of the secondary feature (class).
   * @result A RDD of tuples (feature, (redundancy, conditional redundancy)).
   * 
   */
  def getRedundancies(varY: Int) = {
    
    // Get and broadcast Y and the fixed variable (conditional)
    val yvals = data.lookup(varY)
    var ycol = Array.ofDim[Array[Byte]](yvals.length)
    yvals.foreach({ case (b, v) => ycol(b) = v })
    val (varZ, _) = fixedCol

    // Compute histograms for all variables with respect to Y and the fixed variable
    val histograms3d = computeConditionalHistograms(
        data, (varY, ycol), fixedCol)
        .filter{case (k, _) => k != varZ && k != varY}
      
    // Compute CMI and MI for all histograms with respect to two variables
    computeConditionalMutualInfo(histograms3d, varY, varZ, 
        marginalProb, jointProb, nInstances)
  }
    
  
  /**
   * Computes 2-dim histograms for all input attributes 
   * with respect to a secondary variable (class).
   * 
   * @param RDD of tuples (feature, values)
   * @param ycol (feature, values).
   * @param yhist Histogram for variable Y (class).
   * 
   * @result A RDD of tuples (feature, histogram).
   * 
   */
  private def computeHistograms(
      data:  RDD[(Int, (Int, Array[Byte]))],
      ycol: (Int, Broadcast[Array[Array[Byte]]])) = {
    
    val maxSize = 256; val bycol = ycol._2
    val counter = counterByFeat 
    val ys = counter.value.getOrElse(ycol._1, maxSize).toInt
      
    data.mapPartitions({ it =>
      var result = Map.empty[Int, BDM[Long]]
      // For each feature and block, this generates a histogram (a single matrix)
      for((feat, (block, arr)) <- it) {
        val m = result.getOrElse(feat, 
            BDM.zeros[Long](counter.value.getOrElse(feat, maxSize).toInt, ys)) 
        for(i <- 0 until arr.length) 
          m(arr(i), bycol.value(block)(i)) += 1
        result += feat -> m
      }
      result.toIterator
    }).reduceByKey(_ + _) // Then, those histograms with the same key are aggregated
  }
  
  /**
   * Computes 3-dim histograms for all input attributes with respect to 
   * a secondary variable and the conditional variable. Conditional feature 
   * (class) must be already cached. 
   * 
   * @param filterData RDD of tuples (feature, values)
   * @param ycol (feature, value vector).
   * 
   * @result A RDD of tuples (feature, histogram).
   * 
   */
  private def computeConditionalHistograms(
    data: RDD[(Int, (Int, Array[Byte]))],
    ycol: (Int, Array[Array[Byte]]),
    zcol: (Int, Broadcast[Array[Array[Byte]]])) = {
    
      val bycol = data.context.broadcast(ycol._2)
      val bzcol = zcol._2
      val bcounter = counterByFeat // In order to avoid serialize the whole object
      val ys = counterByFeat.value.getOrElse(ycol._1, 256)
      val zs = counterByFeat.value.getOrElse(zcol._1, 256)
      
      val result = data.mapPartitions({ it =>
        var result = Map.empty[Int, BDV[BDM[Long]]]
        // For each feature and block, this generates a 3-dim histogram (several matrices)
        for((feat, (block, arr)) <- it) {
          // We create a vector (z) of matrices (x,y) to represent a 3-dim matrix
          val m = result.getOrElse(feat, 
              BDV.fill[BDM[Long]](zs){BDM.zeros[Long](bcounter.value.getOrElse(feat, 256), ys)})
          for(i <- 0 until arr.length){
            val y = bycol.value(block)(i)
            val z = bzcol.value(block)(i)
            m(z)(arr(i), y) += 1
          }
          result += feat -> m
        }
        result.toIterator
      }).reduceByKey(_ + _) // Matrices are aggregated
      
      bycol.unpersist()
      result
  }
  
}

object InfoTheory {
  
  /**
   * Creates an Info-Theory object to compute MI and CMI using a greedy approach (sparse version). 
   * This apply this primitives to all the input attributes with respect to a fixed variable
   * (typically the class) and a secondary (changing) variable, typically the last selected feature.
   *
   * @param   data RDD of tuples in columnar format (feature, vector).
   * @param   fixedFeat Index of the fixed attribute (usually the class).
   * @param   nInstances Number of samples.
   * @param   nFeatures Number of features.
   * @return  An info-theory object which contains the relevances and some proportions cached.
   * 
   */
  def initializeSparse(data: RDD[(Int, BV[Byte])], 
    fixedFeat: Int,
    nInstances: Long,      
    nFeatures: Int) = {
      new InfoTheorySparse(data, fixedFeat, nInstances, nFeatures)
  }
  
  /**
   * Creates an Info-Theory object to compute MI and CMI using a greedy approach (dense version). 
   * This apply this primitives to all the input attributes with respect to a fixed variable
   * (typically the class) and a secondary (changing) variable, typically the last selected feature.
   *
   * @param   data RDD of tuples in columnar format (feature, (block, vector)).
   * @param   fixedFeat Index of the fixed attribute (usually the class).
   * @param   nInstances Number of samples.
   * @param   nFeatures Number of features.
   * @return  An info-theory object which contains the relevances and some proportions cached.
   * 
   */
  def initializeDense(data: RDD[(Int, (Int, Array[Byte]))], 
    fixedFeat: Int,
    nInstances: Long,      
    nFeatures: Int) = {
      new InfoTheoryDense(data, fixedFeat, nInstances, nFeatures)
  }
  
  private val log2 = { x: Double => math.log(x) / math.log(2) } 
  
  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n Number of elements
   * 
   */
  private[feature] def entropy(freqs: Seq[Long], n: Long) = {
    freqs.aggregate(0.0)({ case (h, q) =>
      h + (if (q == 0) 0  else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  private[feature] def entropy(freqs: Seq[Long]): Double = {
    entropy(freqs, freqs.reduce(_ + _))
  }
  
}
