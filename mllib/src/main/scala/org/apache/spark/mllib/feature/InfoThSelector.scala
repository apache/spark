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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Vector, DenseVector, SparseVector}
import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.mllib.feature.{InfoThCriterionFactory => FT}
import org.apache.spark.mllib.feature.{InfoTheory => IT}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.HashPartitioner
import scala.collection.BitSet

/**
 * Train a info-theory feature selection (FS) model according to a criterion.
 * 
 * @param criterionFactory Factory to create info-theory measurements for each feature.
 * 
 */
class InfoThSelector private[feature] (val criterionFactory: FT) extends Serializable with Logging {

  // Case class for criteria/feature
  protected case class F(feat: Int, crit: Double) 
  // Case class for columnar data (dense and sparse version)
  private case class ColumnarData(dense: RDD[(Int, (Int, Array[Byte]))], 
      sparse: RDD[(Int, BV[Byte])],
      isDense: Boolean)

  /**
   * Performs a info-theory FS process.
   * 
   * @param data Columnar data (last element is the class attribute).
   * @param nToSelect Number of features to select.
   * @param nInstances Number of samples.
   * @param nFeatures Number of features.
   * @return A list with the most relevant features and its scores.
   * 
   */
  private[feature] def selectFeatures(
      data: ColumnarData, 
      nToSelect: Int,
      nInstances: Long,
      nFeatures: Int) = {
    
    val label = nFeatures - 1
    // Initialize all criteria with the relevance computed in this phase. 
    // It also computes and saved some information to be re-used.
    val (it, relevances) = if(data.isDense) {
      val it = InfoTheory.initializeDense(data.dense, label, nInstances, nFeatures)
      (it, it.relevances)
    } else {
      val it = InfoTheory.initializeSparse(data.sparse, label, nInstances, nFeatures)
      (it, it.relevances)
    }

    // Initialize all (except the class) criteria with the relevance values
    val pool = Array.fill[InfoThCriterion](nFeatures - 1) {
      val crit = criterionFactory.getCriterion.init(Float.NegativeInfinity)
      crit.setValid(false)
    }    
    relevances.collect().foreach{ case (x, mi) => 
      pool(x) = criterionFactory.getCriterion.init(mi.toFloat) 
    }
    
    // Print most relevant features
    val topByRelevance = relevances.sortBy(_._2, false).take(nToSelect)
    val strRels = topByRelevance.map({case (f, mi) => (f + 1) + "\t" + "%.4f" format mi})
      .mkString("\n")
    println("\n*** MaxRel features ***\nFeature\tScore\n" + strRels) 
    
    // Get the maximum and initialize the set of selected features with it
    val (max, mid) = pool.zipWithIndex.maxBy(_._1.relevance)
    var selected = Seq(F(mid, max.score))
    pool(mid).setValid(false)
      
    // MIM does not use redundancy, so for this criterion all the features are selected now
    if (criterionFactory.getCriterion.toString == "MIM") {
      selected = topByRelevance.map({case (id, relv) => F(id, relv)}).reverse
    }
    
    var moreFeat = true
    // Iterative process for redundancy and conditional redundancy
    while (selected.size < nToSelect && moreFeat) {

      val redundancies = it match {
        case dit: InfoTheoryDense => dit.getRedundancies(selected.head.feat)
        case sit: InfoTheorySparse => sit.getRedundancies(selected.head.feat)
      }
      
      // Update criteria with the new redundancy values
      redundancies.collect().par.foreach({case (k, (mi, cmi)) =>
         pool(k).update(mi.toFloat, cmi.toFloat) 
      })
      
      // select the best feature and remove from the whole set of features
      val (max, maxi) = pool.par.zipWithIndex.filter(_._1.valid).maxBy(_._1)
      if(maxi != -1){
        selected = F(maxi, max.score) +: selected
        pool(maxi).setValid(false)
      } else {
        moreFeat = false
      }
    }
    selected.reverse
  }

  /**
   * Process in charge of transforming data in a columnar format and launching the FS process.
   * 
   * @param data RDD of LabeledPoint.
   * @param nToSelect Number of features to select.
   * @param nPart Number of partitions to use in the new format.
   * @return A feature selection model which contains a subset of selected features.
   * 
   */
  private[feature] def run(
      data: RDD[LabeledPoint], 
      nToSelect: Int, 
      nPart: Int) = {  
    
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
      
    // Feature vector must be composed of bytes, not the class
    val requireByteValues = (v: Vector) => {        
      val values = v match {
        case sv: SparseVector =>
          sv.values
        case dv: DenseVector =>
          dv.values
      }
      val condition = (value: Double) => value <= Byte.MaxValue && 
        value >= Byte.MinValue && value % 1 == 0.0
      if (!values.forall(condition(_))) {
        throw new SparkException(
            s"Info-Theoretic Framework requires positive values in range [0, 255]")
      }           
    }
        
    // Get basic info
    val first = data.first
    val dense = first.features.isInstanceOf[DenseVector]    
    val nInstances = data.count()
    val nFeatures = first.features.size + 1
    require(nToSelect < nFeatures)  
    
    // Start the transformation to the columnar format
    val colData = if(dense) {
      
      val np = if(nPart == 0) nFeatures else nPart
      if(np > nFeatures) {
        logWarning("Number of partitions should be equal or less than the number of features."
          + " At least, less than 2x the number of features.")
      }
      
      val classMap = data.map(_.label).distinct.collect()
        .zipWithIndex.map(t => t._1 -> t._2.toByte)
        .toMap
      
      // Transform data into a columnar format by transposing the local matrix in each partition
      val columnarData = data.mapPartitionsWithIndex({ (index, it) =>
        val data = it.toArray
        val mat = Array.ofDim[Byte](nFeatures, data.length)
        var j = 0
        for(reg <- data) {
          requireByteValues(reg.features)
          for(i <- 0 until reg.features.size) mat(i)(j) = reg.features(i).toByte
          mat(reg.features.size)(j) = classMap(reg.label)
          j += 1
        }
        val chunks = for(i <- 0 until nFeatures) yield (i -> (index, mat(i)))
        chunks.toIterator
      })      
      
      // Sort to group all chunks for the same feature closely. 
      // It will avoid to shuffle too much histograms
      val denseData = columnarData.sortByKey(numPartitions = np).persist(StorageLevel.MEMORY_ONLY)
      
      ColumnarData(denseData, null, true)      
    } else {      
      
      val np = if(nPart == 0) data.conf.getInt("spark.default.parallelism", 750) else nPart
      val classMap = data.map(_.label).distinct.collect()
        .zipWithIndex.map(t => t._1 -> t._2.toByte)
        .toMap
        
      val sparseData = data.zipWithIndex().flatMap ({ case (lp, r) => 
          requireByteValues(lp.features)
          val sv = lp.features.asInstanceOf[SparseVector]
          val output = (nFeatures - 1) -> (r, classMap(lp.label))
          val inputs = for(i <- 0 until sv.indices.length) 
            yield (sv.indices(i), (r, sv.values(i).toByte))
          output +: inputs           
      })
      
      // Transform sparse data into a columnar format 
      // by grouping all values for the same feature in a single vector
      val columnarData = sparseData.groupByKey(new HashPartitioner(np))
        .mapValues({a => 
          if(a.size >= nInstances) {
            val init = Array.fill[Byte](nInstances.toInt)(0)
            val result: BV[Byte] = new BDV(init)
            a.foreach({case (k, v) => result(k.toInt) = v})
            result
          } else {
            val init = a.toArray.sortBy(_._1)
            new BSV(init.map(_._1.toInt), init.map(_._2), nInstances.toInt)
          }
        }).persist(StorageLevel.MEMORY_ONLY)
      
      ColumnarData(null, columnarData, false)
    }
    
    // Start the main algorithm
    val selected = selectFeatures(colData, nToSelect, nInstances, nFeatures)          
    if(dense) colData.dense.unpersist() else colData.sparse.unpersist()
  
    // Print best features according to the mRMR measure
    val out = selected.map{case F(feat, rel) => 
        (feat + 1) + "\t" + "%.4f".format(rel)
      }.mkString("\n")
    println("\n*** mRMR features ***\nFeature\tScore\n" + out)
    // Features must be sorted
    new SelectorModel(selected.map{case F(feat, rel) => feat}.sorted.toArray)
  }
}

object InfoThSelector {

  /**
   * Train a feature selection model according to a given criterion
   * and return a subset of data.
   *
   * @param   criterionFactory Initialized criterion to use in this selector
   * @param   data RDD of LabeledPoint (discrete data as integers in range [0, 255]).
   * @param   nToSelect Maximum number of features to select
   * @param   numPartitions Number of partitions to structure the data.
   * @return  A feature selection model which contains a subset of selected features.
   * 
   * Note: LabeledPoint data must be integer values in double representation 
   * with a maximum of 256 distinct values. By doing so, data can be transformed
   * to byte class directly, making the selection process much more efficient.
   * 
   * Note: numPartitions must be less or equal to the number of features to achieve 
   * a better performance. Therefore, the number of histograms to be shuffled is reduced. 
   * 
   */
  def train(
      criterionFactory: FT, 
      data: RDD[LabeledPoint],
      nToSelect: Int = 25,
      numPartitions: Int = 0) = {
    new InfoThSelector(criterionFactory).run(data, nToSelect, numPartitions)
  }
}
